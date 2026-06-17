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

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._logging import (
    setup_global_exception_logging,
    setup_logging,
    setup_signal_handler_logging,
)
from deephaven_mcp._monkeypatch import monkeypatch_uvicorn_exception_handling
from deephaven_mcp.config import DATA_DIR_ENV_VAR, resolve_runtime_dir
from deephaven_mcp.config.schema import ServerConfig
from deephaven_mcp.config.tree import ConfigTree, ConfigTreeLoader

from . import _fastmcp
from ._http import _plan_daemon, _plan_default, _run_http
from ._lifespan import LifespanContext, ProcessResources, process_lifespan

__all__ = ["main"]

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration loading
# ---------------------------------------------------------------------------


async def _load_multi_config_or_exit(
    config_dir: Path | None,
) -> ConfigTree:
    """Load and validate the entire on-disk configuration tree.

    Args:
        config_dir (Path | None): The directory passed via ``--config-dir``,
            or ``None`` to use ``$DH_MCP_DATA_DIR/config`` (or the platform
            default user-data root's ``config`` subdirectory).

    Returns:
        ConfigTree: The validated multi-system configuration, carrying the
            resolved ``config_dir`` and the optional ``server`` block.

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
# stdio transport
# ---------------------------------------------------------------------------


def _run_stdio(
    server: FastMCP[LifespanContext],
    multi_config: ConfigTree,
    holder: ProcessResources,
    runtime_dir: Path,
) -> None:
    """Run the FastMCP instance under stdio transport.

    The process-scoped subsystems are built once by :func:`process_lifespan`
    wrapping ``run_stdio_async`` and stored on ``holder``; the per-session
    lifespan attached at construction reads them from there.

    Args:
        server (FastMCP[LifespanContext]): The configured server.
        multi_config (ConfigTree): Pre-validated configuration passed to
            :func:`process_lifespan`.
        holder (ProcessResources): Holder shared with the server's
            per-session lifespan; populated by :func:`process_lifespan`.
        runtime_dir (Path): Resolved runtime directory passed to
            :func:`process_lifespan` for the instance tracker.
    """
    _LOGGER.info("[mcp_systems_server:_run_stdio] Starting stdio transport")

    async def _serve() -> None:
        async with process_lifespan(
            multi_config, idle=None, holder=holder, runtime_dir=runtime_dir
        ):
            await server.run_stdio_async()

    asyncio.run(_serve())
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

    Rejections:

    - **Daemon-mode conflicts**: ``--daemon`` is a preset, so combining it
      with ``--transport``, ``--host``, or ``--port`` is rejected.
    - **Out-of-range port**: TCP ports must be in ``[1, 65535]``.
    - **Empty host**: ``--host ""`` is rejected rather than silently falling
      through to the :class:`ServerConfig` default.

    Behavior-neutral combinations are not rejected: HTTP-only flags
    (``--host`` / ``--port`` / ``--psk``) with ``--transport stdio`` are
    silently ignored. ``--runtime-dir`` is honored in every transport, so it
    needs no validation here.

    Args:
        transport (str | None): Value of ``--transport``.
        host (str | None): Value of ``--host``.
        port (int | None): Value of ``--port``.
        daemon (bool): Value of ``--daemon`` flag.

    Raises:
        click.UsageError: For each rejection category above; click renders it
            to stderr and :func:`main` translates it to ``sys.exit(2)``.
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
        "registry, lock, and log and the per-instance metadata "
        "live). Honored in every transport, parallel to "
        "--config-dir. When unset the server defaults to the "
        "'runtime' subdirectory under "
        f"${DATA_DIR_ENV_VAR} (or the platform default user-data "
        "root, e.g. ~/.deephaven/ai/runtime on POSIX). Bypasses "
        f"${DATA_DIR_ENV_VAR} for the runtime subdir; the env var "
        "still applies to the config subdir unless --config-dir "
        "also overrides it."
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
    """Click callback for ``dh-mcp-systems-server``.

    Sets up logging, validates the flag combination, loads the configuration
    tree, and dispatches to the stdio or HTTP transport. All flags default to
    ``None`` / ``False`` so ``server.json`` provides the effective value per
    field. ``--config-dir`` is resolved to an absolute path here; the runtime
    directory is resolved once via :func:`resolve_runtime_dir` (honoring
    ``--runtime-dir``, then ``$DH_MCP_DATA_DIR``, then the platform default)
    and threaded into every dispatch arm.
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

    # Resolve the runtime directory once for every transport: ``--runtime-dir``
    # override (if given) → ``$DH_MCP_DATA_DIR`` → platform default. ``None``
    # selects the env/default path; an explicit path is ``expanduser()``-ed.
    runtime_dir_resolved = resolve_runtime_dir(runtime_dir)

    # Load the entire configuration tree once; CLI flags override the JSON
    # values per-field when supplied.
    multi_config = asyncio.run(_load_multi_config_or_exit(config_dir_resolved))
    server_cfg = (
        multi_config.server if multi_config.server is not None else ServerConfig()
    )

    # Composition root: build the holder and the FastMCP server once, then
    # hand both to whichever transport runs. The server's per-session lifespan
    # reads the holder, which the process-scoped lifespan populates.
    holder = ProcessResources()
    server = _fastmcp.build_fastmcp(server_cfg.server_name, holder=holder)

    # Three-arm dispatch: daemon mode always implies HTTP (the daemon
    # planner owns its bind/PSK/registry); stdio bypasses the HTTP
    # runner entirely; default HTTP runs the same runner as daemon
    # HTTP via a different planner.
    transport_resolved = transport if transport is not None else server_cfg.transport
    if daemon:
        # ``--psk`` is honoured by the daemon planner as a debug override,
        # otherwise the planner auto-generates a fresh PSK.
        plan = _plan_daemon(
            multi_config,
            server_cfg,
            runtime_dir=runtime_dir_resolved,
            cli_psk=psk,
        )
        _run_http(plan, server, holder)
    elif transport_resolved == "stdio":
        _run_stdio(server, multi_config, holder, runtime_dir=runtime_dir_resolved)
    else:
        plan = _plan_default(
            multi_config,
            server_cfg,
            runtime_dir=runtime_dir_resolved,
            cli_host=host,
            cli_port=port,
            cli_psk=psk,
        )
        _run_http(plan, server, holder)


def main(argv: list[str] | None = None) -> None:
    """Console entry point for ``dh-mcp-systems-server``.

    Runs the click command with ``standalone_mode=False`` so successful
    completion returns ``None`` instead of raising ``SystemExit(0)``. Bad
    flags raise :class:`click.exceptions.UsageError`, which is rendered to
    stderr and translated to ``sys.exit(2)``; a :class:`SystemExit` from an
    inner helper propagates unchanged.

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
