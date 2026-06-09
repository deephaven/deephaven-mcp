"""Entry point for the multiplexed Deephaven MCP systems server.

This module exposes a single console script — ``dh-mcp-systems-server``
(see ``[project.scripts]`` in ``pyproject.toml``) — that hosts every
configured Deephaven Community session and Deephaven Enterprise system
in one process. Two transports are supported:

- ``stdio`` (default): no authentication; the OS pipe is the trust
  boundary. Suitable for local-IDE integrations such as Claude Desktop.
- ``http``: a single PSK shared between client and server, transmitted
  in the ``X-Deephaven-PSK`` header. Bind address is restricted to
  loopback (``127.0.0.1``, ``::1``, or ``localhost``); no TLS is
  performed and binding to any non-loopback host is refused.

Configuration is read from a per-user directory tree validated by
:class:`~deephaven_mcp.config.ConfigTreeLoader`; ``server.json``
inside that tree carries the PSK used by the HTTP transport. There is no
file-config-via-env-var, no header-based per-request authentication, and
no ``mcp_reload`` tool — configuration changes require a restart.

Every operator-tunable knob lives on ``ServerConfig`` and is read from
``server.json``. CLI flags below override the JSON values when
supplied. The only environment variable this server itself consults at
startup is ``DH_MCP_DATA_DIR`` (which moves the user-data root and is
read transitively when ``--config-dir`` and ``--runtime-dir`` are
unset).
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import click
from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._health import HEALTH_PATH
from deephaven_mcp._logging import (
    setup_global_exception_logging,
    setup_logging,
    setup_signal_handler_logging,
)
from deephaven_mcp._monkeypatch import monkeypatch_uvicorn_exception_handling
from deephaven_mcp.config import DATA_DIR_ENV_VAR

from ._http import _plan_daemon, _plan_default, _run_http
from ._idle import IdleWatcher
from ._lifespan import LifespanContext, make_lifespan
from ._tools import (
    catalog,
    pq,
    script,
    session,
    session_community,
    session_enterprise,
    table,
)
from .config import ConfigTree, ConfigTreeLoader, ServerConfig

__all__ = ["main"]

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration loading
# ---------------------------------------------------------------------------


async def _load_multi_config_or_exit(
    config_dir: Path | None,
) -> ConfigTree:
    """Load and validate the entire on-disk configuration tree.

    Returns the :class:`ConfigTree` produced by
    :class:`ConfigTreeLoader`. The result carries the resolved
    ``config_dir`` and the optional ``server`` block; downstream
    callers extract whatever they need (PSK, host, port, ...) from the
    one returned instance instead of re-parsing the tree.

    Args:
        config_dir (Path | None): The directory passed via
            ``--config-dir``, or ``None`` to use
            ``$DH_MCP_DATA_DIR/config`` (or the platform default
            user-data root's ``config`` subdirectory).

    Returns:
        ConfigTree: The validated multi-system configuration.

    Raises:
        SystemExit: When configuration loading fails with a
            :class:`ConfigurationError`.
    """
    mgr = ConfigTreeLoader(config_dir)
    try:
        multi = await mgr.initialize()
    except ConfigurationError as exc:
        _LOGGER.error(
            f"[mcp_systems_server:_load_multi_config_or_exit] "
            f"Failed to load configuration: {exc}"
        )
        sys.exit(1)
    _LOGGER.debug(
        f"[mcp_systems_server:_load_multi_config_or_exit] "
        f"Loaded configuration from {multi.config_dir}"
    )
    return multi


# ---------------------------------------------------------------------------
# FastMCP construction (shared by all transports)
# ---------------------------------------------------------------------------


def _register_tools(server: FastMCP[LifespanContext], multi_config: ConfigTree) -> None:
    """Register every MCP tool on the multiplexed systems server.

    Section-specific tool modules are gated on which configuration
    sections were loaded: community-only tools register only when
    ``multi_config.community is not None``; enterprise-only tools
    (``session_enterprise``, ``catalog``, ``pq``) register only when
    ``multi_config.enterprise is not None``. Cross-cutting tools that
    operate on whatever sessions exist are always registered.

    Args:
        server (FastMCP[LifespanContext]): The FastMCP instance to
            register tool modules on.
        multi_config (ConfigTree): The validated configuration;
            used to gate registration on loaded sections.
    """
    session.register_tools(server)
    table.register_tools(server)
    script.register_tools(server)
    sections: list[str] = ["shared"]
    if multi_config.community is not None:
        session_community.register_tools(server)
        sections.append("community")
    if multi_config.enterprise is not None:
        session_enterprise.register_tools(server)
        catalog.register_tools(server)
        pq.register_tools(server)
        sections.append("enterprise")
    _LOGGER.debug(
        f"[mcp_systems_server:_register_tools] Registered tool sections: "
        f"{', '.join(sections)}"
    )


def _register_health_endpoint(server: FastMCP[LifespanContext]) -> None:
    """Register the ``/health`` liveness/readiness route on ``server``.

    The route is registered for both transports but is only ever
    exercised under HTTP. It is also added to
    :class:`PSKMiddleware`'s ``bypass_paths`` so external probes do not
    need to share the PSK.

    Args:
        server (FastMCP[LifespanContext]): The FastMCP instance whose
            ASGI app will own the route.
    """

    @server.custom_route(HEALTH_PATH, methods=["GET"])  # type: ignore[untyped-decorator]
    async def health_check(_request: Request) -> JSONResponse:
        """Return a 200/JSON liveness probe for the systems server."""
        _LOGGER.debug("[mcp_systems_server:health_check] Health check requested")
        return JSONResponse({"status": "ok"})


def _build_fastmcp(
    multi_config: ConfigTree,
    server_name: str,
    *,
    idle: IdleWatcher | None,
) -> FastMCP[LifespanContext]:
    """Build the FastMCP instance with lifespan + tools + health route.

    Args:
        multi_config (ConfigTree): Pre-validated configuration
            forwarded to :func:`make_lifespan` so the lifespan does not
            re-parse the on-disk tree.
        server_name (str): The FastMCP server name advertised in MCP
            handshakes; sourced from ``ServerConfig.server_name``.
        idle (IdleWatcher | None): Unstarted watcher controlling idle
            shutdown. Forwarded verbatim to :func:`make_lifespan`,
            which owns the watcher's lifecycle. Pass ``None`` to
            disable supervision (stdio and default HTTP both pass
            ``None``; only daemon mode supplies a watcher).

    Returns:
        FastMCP[LifespanContext]: The fully wired MCP server. The
            caller selects the transport (``run_stdio_async`` vs the
            streamable-HTTP app) without further mutation of the
            instance.
    """
    server: FastMCP[LifespanContext] = FastMCP(
        name=server_name,
        lifespan=make_lifespan(multi_config, idle=idle),
    )
    _register_tools(server, multi_config)
    _register_health_endpoint(server)
    _LOGGER.debug(
        f"[mcp_systems_server:_build_fastmcp] Built FastMCP instance "
        f"name={server_name!r}, idle={'on' if idle is not None else 'off'}"
    )
    return server


# ---------------------------------------------------------------------------
# stdio transport
# ---------------------------------------------------------------------------


def _run_stdio(server: FastMCP[LifespanContext]) -> None:
    """Run the FastMCP instance under stdio transport.

    Args:
        server (FastMCP[LifespanContext]): The configured server. The
            lifespan attached at construction is responsible for
            loading configuration and tearing it down on exit.
    """
    _LOGGER.info("[mcp_systems_server:_run_stdio] Starting stdio transport")
    asyncio.run(server.run_stdio_async())
    _LOGGER.info("[mcp_systems_server:_run_stdio] stdio transport stopped")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _validate_cli_args(
    *,
    transport: str | None,
    host: str | None,
    port: int | None,
    daemon: bool,
) -> None:
    """Reject CLI flag combinations that would behave surprisingly.

    Three categories of rejection:

    - **Daemon-mode conflicts**: ``--daemon`` is a preset whose
      transport / bind / port are fixed by the registry wire
      format. Combining it with ``--transport``, ``--host``, or
      ``--port`` would leave the operator with no hint that those
      flags were ignored, so reject up-front.
    - **Out-of-range port**: TCP ports are ``[1, 65535]``; uvicorn
      would also error, but a click-level :class:`UsageError`
      yields a cleaner stderr message.
    - **Empty host**: ``--host ""`` would silently fall through to
      :class:`ServerConfig` defaults; reject so the operator
      notices the typo.

    Behaviour-neutral combinations are *not* rejected:

    - ``--runtime-dir`` without ``--daemon`` is silently ignored
      (the runtime dir is only consulted on the daemon path);
      rejecting would surprise users with shared shell aliases.
    - ``--transport stdio`` with ``--host`` / ``--port`` / ``--psk``
      is also silently ignored (HTTP-only flags on a non-HTTP
      transport).

    Args:
        transport (str | None): Value of ``--transport``.
        host (str | None): Value of ``--host``.
        port (int | None): Value of ``--port``.
        daemon (bool): Value of ``--daemon`` flag.

    Raises:
        click.UsageError: For each of the rejection categories
            above; click renders the message to stderr and the
            wrapping :func:`main` translates that to ``sys.exit(2)``.
    """
    if daemon:
        conflicts: list[str] = []
        if transport is not None:
            conflicts.append("--transport")
        if host is not None:
            conflicts.append("--host")
        if port is not None:
            conflicts.append("--port")
        if conflicts:
            raise click.UsageError(
                "--daemon is a preset; "
                f"{', '.join(conflicts)} may not be combined with it"
            )
    if port is not None and not (1 <= port <= 65535):
        raise click.UsageError(f"--port must be in [1, 65535], got {port}")
    if host is not None and host.strip() == "":
        raise click.UsageError("--host must be a non-empty string")


@click.command(
    name="dh-mcp-systems-server",
    help=(
        "Multiplexed Deephaven MCP systems server. Hosts every "
        "configured Community session and Enterprise system in one "
        "process."
    ),
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "http"]),
    default=None,
    help=(
        "Transport to expose. 'stdio' is local-pipe-based with no "
        "authentication. 'http' serves the streamable-HTTP MCP "
        "transport, gated by the PSK in server.json and bound to "
        "loopback only. Overrides server.json's 'transport' field; "
        "defaults to 'stdio' when neither is supplied."
    ),
)
@click.option(
    "--host",
    type=str,
    default=None,
    help=(
        "HTTP transport bind address. Must be a loopback host "
        "(127.0.0.1, ::1, or 'localhost'). Ignored for stdio. "
        "Overrides server.json's 'host' field; defaults to '127.0.0.1'."
    ),
)
@click.option(
    "--port",
    type=int,
    default=None,
    help=(
        "HTTP transport TCP port. Ignored for stdio. Overrides "
        "server.json's 'port' field; defaults to 8000."
    ),
)
@click.option(
    "--psk",
    type=str,
    default=None,
    help=(
        "Pre-shared key for HTTP transport. Overrides server.json's "
        "'psk' field. Ignored for stdio."
    ),
)
@click.option(
    "--config-dir",
    "config_dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Override for the configuration directory. When unset the "
        "server defaults to the ``config`` subdirectory under "
        f"${DATA_DIR_ENV_VAR} (or the platform default user-data "
        "root, e.g. ~/.deephaven/ai/config on POSIX)."
    ),
)
@click.option(
    "--runtime-dir",
    "runtime_dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Override for the runtime directory (where the daemon "
        "registry, lock, and log live). When unset the server "
        "defaults to the ``runtime`` subdirectory under "
        f"${DATA_DIR_ENV_VAR} (or the platform default user-data "
        "root, e.g. ~/.deephaven/ai/runtime on POSIX). Bypasses "
        f"${DATA_DIR_ENV_VAR} for the runtime subdir; the env var "
        "still applies to the config subdir unless --config-dir "
        "also overrides it. Only meaningful under --daemon."
    ),
)
@click.option(
    "--daemon",
    "daemon",
    is_flag=True,
    default=False,
    help=(
        "Launch as a per-user local daemon for the dh-mcp CLI. "
        "Preset that forces HTTP transport on 127.0.0.1:0 (kernel-"
        "chosen ephemeral port), enables idle shutdown when "
        "server.json:daemon.idle_shutdown_seconds > 0, and writes "
        "a registry file under the runtime directory so the CLI "
        "can discover the bound port and PSK. Combining --daemon "
        "with --transport, --host, or --port is rejected with a "
        "usage error (the daemon shape is fixed by the registry "
        "wire format). server.json:psk is ignored. An explicit "
        "--psk is honoured as a testing/debug override; otherwise "
        "a fresh PSK is auto-generated and recorded in the registry."
    ),
)
def _command(
    transport: str | None,
    host: str | None,
    port: int | None,
    psk: str | None,
    config_dir: Path | None,
    runtime_dir: Path | None,
    daemon: bool,
) -> None:
    """Click command for ``dh-mcp-systems-server``.

    The ``@click.command`` decorator wraps this function into a
    :class:`click.core.Command` instance bound to the module-level
    name :data:`_command`; the function body below is invoked by
    click as the command's callback after argument parsing. The
    callable read by Python at module load time is therefore the
    ``Command`` object, not this raw function — see
    :func:`main` for the entry-point invocation.

    Argument-handling notes:

    All flags default to ``None`` / ``False`` so the JSON-loaded
    :class:`ServerConfig` provides the effective value per field.
    Click's ``path_type=Path`` returns the user-supplied path
    verbatim (no resolution); ``--config-dir`` is resolved here via
    :meth:`Path.expanduser` + :meth:`Path.resolve` so the rest of
    the pipeline sees an absolute path, while ``--runtime-dir`` is
    only ``expanduser()``-ed (matching the prior argparse-era
    semantics — :func:`resolve_runtime_dir` does the rest).
    """
    setup_logging()
    setup_global_exception_logging()
    setup_signal_handler_logging()
    monkeypatch_uvicorn_exception_handling()

    _validate_cli_args(transport=transport, host=host, port=port, daemon=daemon)

    _LOGGER.debug(
        f"[mcp_systems_server:_command] Resolved CLI args: "
        f"transport={transport!r}, host={host!r}, port={port!r}, "
        f"daemon={daemon}, config_dir={config_dir}, "
        f"runtime_dir={runtime_dir}, psk={'<set>' if psk else None}"
    )

    config_dir_resolved = (
        config_dir.expanduser().resolve() if config_dir is not None else None
    )

    # Load the entire configuration tree once. Every operator-tunable
    # knob (transport, host, port, server_name, psk, sessions, systems,
    # ...) lives in this validated tree. CLI flags override the JSON
    # values per-field when supplied. The resulting ``ConfigTree``
    # is then threaded into ``_build_fastmcp`` (for the lifespan) and
    # the planners (for the resolved bind / PSK).
    multi_config = asyncio.run(_load_multi_config_or_exit(config_dir_resolved))
    server_cfg = (
        multi_config.server if multi_config.server is not None else ServerConfig()
    )

    # Three-arm dispatch: daemon mode always implies HTTP (the daemon
    # planner owns its bind/PSK/registry); stdio bypasses the HTTP
    # runner entirely; default HTTP runs the same runner as daemon
    # HTTP via a different planner.
    transport_resolved = transport if transport is not None else server_cfg.transport
    if daemon:
        # ``--transport``, ``--host``, ``--port`` were already rejected
        # by ``_validate_cli_args`` when combined with ``--daemon``;
        # ``--psk`` is honoured by the daemon planner as a debug
        # override, otherwise the planner auto-generates a fresh PSK.
        runtime_override = runtime_dir.expanduser() if runtime_dir is not None else None
        plan = _plan_daemon(
            multi_config,
            server_cfg,
            runtime_dir_override=runtime_override,
            cli_psk=psk,
        )
        _run_http(plan, _build_fastmcp)
    elif transport_resolved == "stdio":
        server = _build_fastmcp(multi_config, server_cfg.server_name, idle=None)
        _run_stdio(server)
    else:
        plan = _plan_default(
            multi_config,
            server_cfg,
            cli_host=host,
            cli_port=port,
            cli_psk=psk,
        )
        _run_http(plan, _build_fastmcp)


def main(argv: list[str] | None = None) -> None:
    """Console entry point for ``dh-mcp-systems-server``.

    Thin wrapper around the click command. ``argv=None`` defers to
    ``sys.argv[1:]``; supplying an explicit list lets tests drive
    the CLI without touching the global argv.

    Runs click with ``standalone_mode=False`` so successful
    completion returns ``None`` (rather than calling ``sys.exit(0)``);
    this preserves the pre-click test contract where callers can
    invoke ``main([...])`` and observe side effects directly.
    Click handles ``--help`` internally (prints help text, returns
    the exit code without raising) so the wrapper does not need to
    translate it. Bad flags raise :class:`click.exceptions.UsageError`
    which is rendered to stderr and translated to ``sys.exit(2)``;
    :class:`SystemExit` raised by inner helpers (e.g.
    :func:`_resolve_psk_or_exit`) propagates unchanged. Note that
    :data:`_command` is a :class:`click.core.Command` instance
    (the result of decorating ``_command``'s callback with
    ``@click.command``); ``_command.main(...)`` is click's public
    entry point on that instance, not a recursive call into this
    module's :func:`main`.

    Args:
        argv (list[str] | None): Argument list, or ``None`` to use
            ``sys.argv[1:]``.
    """
    try:
        _command.main(
            args=argv, prog_name="dh-mcp-systems-server", standalone_mode=False
        )
    except click.exceptions.UsageError as exc:
        # Mirrors click's default standalone-mode behaviour: render
        # the usage message to stderr and exit 2.
        exc.show()
        sys.exit(2)
