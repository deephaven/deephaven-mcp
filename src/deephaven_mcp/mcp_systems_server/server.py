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
:class:`~deephaven_mcp.config.MultiSystemConfigManager`; ``server.json``
inside that tree carries the PSK used by the HTTP transport. There is no
file-config-via-env-var, no header-based per-request authentication, and
no ``mcp_reload`` tool — configuration changes require a restart.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import logging
import socket
import sys
from pathlib import Path

import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.middleware import Middleware
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
from deephaven_mcp.auth.middleware import PSK_HEADER_NAME, PSKMiddleware
from deephaven_mcp.config import CONFIG_DIR_ENV_VAR

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
from .config import MultiSystemConfig, MultiSystemConfigManager, ServerConfig

__all__ = ["main"]

_LOGGER = logging.getLogger(__name__)

# Every operator-tunable knob now lives on ``ServerConfig`` and is
# read from ``server.json``. CLI flags below override the JSON
# values when supplied. The only environment variable the systems
# server itself consults at startup is ``DH_MCP_CONFIG_DIR``
# (and only as a fallback for ``--config-dir``).


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the systems-server CLI arguments.

    Args:
        argv (list[str] | None): Argument list to parse, or ``None`` to
            use ``sys.argv[1:]``. Useful for tests.

    Returns:
        argparse.Namespace: Parsed namespace with ``transport``,
            ``host``, ``port``, ``psk``, and ``config_dir`` attributes.
    """
    parser = argparse.ArgumentParser(
        prog="dh-mcp-systems-server",
        description=(
            "Multiplexed Deephaven MCP systems server. Hosts every "
            "configured Community session and Enterprise system in one "
            "process."
        ),
    )
    parser.add_argument(
        "--transport",
        choices=("stdio", "http"),
        default=None,
        help=(
            "Transport to expose. 'stdio' is local-pipe-based with no "
            "authentication. 'http' serves the streamable-HTTP MCP "
            "transport, gated by the PSK in server.json and bound to "
            "loopback only. Overrides server.json's 'transport' field; "
            "defaults to 'stdio' when neither is supplied."
        ),
    )
    parser.add_argument(
        "--host",
        default=None,
        help=(
            "HTTP transport bind address. Must be a loopback host "
            "(127.0.0.1, ::1, or 'localhost'). Ignored for stdio. "
            "Overrides server.json's 'host' field; defaults to '127.0.0.1'."
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=(
            "HTTP transport TCP port. Ignored for stdio. Overrides "
            "server.json's 'port' field; defaults to 8000."
        ),
    )
    parser.add_argument(
        "--psk",
        default=None,
        help=(
            "Pre-shared key for HTTP transport. Overrides server.json's "
            "'psk' field. Ignored for stdio."
        ),
    )
    parser.add_argument(
        "--config-dir",
        default=None,
        help=(
            "Override for the configuration directory. When unset the "
            f"server reads ${CONFIG_DIR_ENV_VAR} or falls back to the "
            "platform default (e.g. ~/.deephaven/ai/config on POSIX)."
        ),
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Loopback validation
# ---------------------------------------------------------------------------


def _is_loopback_host(host: str) -> bool:
    """Return ``True`` iff ``host`` resolves exclusively to loopback addresses.

    Args:
        host (str): The host string the server will bind to.

    Returns:
        bool: ``True`` for ``"localhost"`` (case-insensitive), every
            address in ``127.0.0.0/8``, ``::1``, and any hostname whose
            ``getaddrinfo`` resolution yields only loopback addresses.
            ``False`` for unresolvable hosts so the safe default is to
            refuse to bind.
    """

    def _addr_is_loopback(addr: str) -> bool:
        try:
            return ipaddress.ip_address(addr).is_loopback
        except ValueError:
            # ``getaddrinfo`` can yield scoped IPv6 link-locals
            # (e.g. ``fe80::1%en0``) which ``ip_address`` rejects.
            # Treat as non-loopback so the caller refuses to bind.
            return False

    if host.lower() == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            # ``socket.getaddrinfo`` typeshed widens the sockaddr's
            # first element to ``str | int`` because non-IP socket
            # families exist. We only consult IP families here, so
            # filter the integer case out defensively.
            resolved = {
                info[4][0]
                for info in socket.getaddrinfo(host, None)
                if isinstance(info[4][0], str)
            }
        except socket.gaierror:
            return False
        return all(_addr_is_loopback(addr) for addr in resolved)
    return ip.is_loopback


# ---------------------------------------------------------------------------
# Tool + health-route registration
# ---------------------------------------------------------------------------


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


def _register_tools(
    server: FastMCP[LifespanContext], multi_config: MultiSystemConfig
) -> None:
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
        multi_config (MultiSystemConfig): The validated configuration;
            used to gate registration on loaded sections.
    """
    session.register_tools(server)
    table.register_tools(server)
    script.register_tools(server)
    if multi_config.community is not None:
        session_community.register_tools(server)
    if multi_config.enterprise is not None:
        session_enterprise.register_tools(server)
        catalog.register_tools(server)
        pq.register_tools(server)


# ---------------------------------------------------------------------------
# Transport entry points
# ---------------------------------------------------------------------------


def _parse_config_dir_arg(
    explicit_arg: str | None,
) -> Path | None:
    """Translate the CLI ``--config-dir`` flag into a Path or ``None``.

    Args:
        explicit_arg (str | None): The value of ``--config-dir`` (or
            ``None`` when the flag is not used).

    Returns:
        Path | None: An absolute Path when ``explicit_arg`` is provided;
            otherwise ``None`` so :class:`MultiSystemConfigManager`
            applies its own resolution (env var, then platform
            default).
    """
    if explicit_arg is None:
        return None
    return Path(explicit_arg).expanduser().resolve()


async def _load_multi_config_or_exit(
    config_dir: Path | None,
) -> MultiSystemConfig:
    """Load and validate the entire on-disk configuration tree.

    Returns the :class:`MultiSystemConfig` produced by
    :class:`MultiSystemConfigManager`. The result carries the resolved
    ``config_dir`` and the optional ``server`` block; downstream
    callers extract whatever they need (PSK, host, port, ...) from the
    one returned instance instead of re-parsing the tree.

    Args:
        config_dir (Path | None): The directory passed via
            ``--config-dir``, or ``None`` to use
            ``$DH_MCP_CONFIG_DIR`` / the platform default.

    Returns:
        MultiSystemConfig: The validated multi-system configuration.

    Raises:
        SystemExit: When configuration loading fails with a
            :class:`ConfigurationError`.
    """
    mgr = MultiSystemConfigManager(config_dir)
    try:
        return await mgr.initialize()
    except ConfigurationError as exc:
        _LOGGER.error(
            f"[mcp_systems_server:_load_multi_config_or_exit] "
            f"Failed to load configuration: {exc}"
        )
        sys.exit(1)


def _resolve_psk_or_exit(
    cli_psk: str | None,
    server_cfg: ServerConfig,
    config_dir: Path,
) -> str:
    """Resolve the HTTP-transport PSK with CLI > JSON precedence.

    Args:
        cli_psk (str | None): Value of the ``--psk`` CLI flag, or
            ``None`` when the flag was not supplied.
        server_cfg (ServerConfig): The validated server config
            (already loaded by :func:`_load_server_config_or_exit`).
        config_dir (Path): The resolved config-directory path,
            used only for the error message when no PSK is available.

    Returns:
        str: The non-empty PSK string from the highest-precedence
            source.

    Raises:
        SystemExit: When neither the CLI flag nor ``server.json``
            supplies a non-empty PSK.
    """
    if cli_psk:
        _LOGGER.info(
            "[mcp_systems_server:_resolve_psk_or_exit] Using PSK from --psk CLI flag"
        )
        return cli_psk
    if server_cfg.psk is not None and server_cfg.psk.get_secret_value():
        _LOGGER.info(
            "[mcp_systems_server:_resolve_psk_or_exit] Using PSK from server.json"
        )
        return server_cfg.psk.get_secret_value()
    _LOGGER.error(
        "[mcp_systems_server:_resolve_psk_or_exit] HTTP transport requires a PSK. "
        f"Set --psk or configure 'psk' in {config_dir}/server.json. "
        "Use --transport stdio to skip auth."
    )
    sys.exit(1)


def _build_fastmcp(
    multi_config: MultiSystemConfig, server_name: str
) -> FastMCP[LifespanContext]:
    """Build the FastMCP instance with lifespan + tools + health route.

    Args:
        multi_config (MultiSystemConfig): Pre-validated configuration
            forwarded to :func:`make_lifespan` so the lifespan does not
            re-parse the on-disk tree.
        server_name (str): The FastMCP server name advertised in MCP
            handshakes; sourced from ``ServerConfig.server_name``.

    Returns:
        FastMCP[LifespanContext]: The fully wired MCP server. The
            caller selects the transport (``run_stdio_async`` vs the
            streamable-HTTP app) without further mutation of the
            instance.
    """
    server: FastMCP[LifespanContext] = FastMCP(
        name=server_name,
        lifespan=make_lifespan(multi_config),
    )
    _register_tools(server, multi_config)
    _register_health_endpoint(server)
    return server


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


def _run_http(
    server: FastMCP[LifespanContext],
    *,
    host: str,
    port: int,
    psk: str,
) -> None:
    """Run the FastMCP instance under streamable-HTTP transport with PSK.

    Args:
        server (FastMCP[LifespanContext]): The configured server.
        host (str): Loopback bind address. The caller is expected to
            have validated this with :func:`_is_loopback_host`.
        port (int): TCP port to listen on.
        psk (str): The non-empty PSK that
            :class:`PSKMiddleware` will require on every request.
    """
    app = server.streamable_http_app()
    # ``streamable_http_app`` returns a fresh Starlette instance whose
    # middleware stack has not yet been built. Insert PSK at index 0
    # so authentication runs before any other middleware (e.g. logging
    # middleware that would otherwise see un-authed request bodies).
    app.user_middleware.insert(
        0,
        Middleware(
            PSKMiddleware,
            expected_psk=psk,
            bypass_paths=(HEALTH_PATH,),
        ),
    )
    _LOGGER.info(
        f"[mcp_systems_server:_run_http] Starting HTTP transport on {host}:{port} "
        f"(PSK gate via {PSK_HEADER_NAME!r})"
    )
    config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level="info",
        loop="asyncio",
    )
    uvicorn.Server(config).run()
    _LOGGER.info("[mcp_systems_server:_run_http] HTTP transport stopped")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Entry point for ``dh-mcp-systems-server``.

    Args:
        argv (list[str] | None): Argument list, or ``None`` to use
            ``sys.argv[1:]``.
    """
    setup_logging()
    setup_global_exception_logging()
    setup_signal_handler_logging()
    monkeypatch_uvicorn_exception_handling()
    args = _parse_args(argv)
    config_dir = _parse_config_dir_arg(args.config_dir)

    # Load the entire configuration tree once. Every operator-tunable
    # knob (transport, host, port, server_name, psk, sessions, systems,
    # ...) lives in this validated tree. CLI flags override the JSON
    # values per-field when supplied. The resulting ``MultiSystemConfig``
    # is then threaded into both ``_build_fastmcp`` (for the lifespan)
    # and the local PSK/host/port resolution logic below, avoiding a
    # redundant second parse pass.
    multi_config = asyncio.run(_load_multi_config_or_exit(config_dir))
    server_cfg = (
        multi_config.server if multi_config.server is not None else ServerConfig()
    )
    transport = args.transport if args.transport is not None else server_cfg.transport

    if transport == "stdio":
        server = _build_fastmcp(multi_config, server_cfg.server_name)
        _run_stdio(server)
        return

    # HTTP transport: validate host, resolve PSK, then run.
    host = args.host if args.host is not None else server_cfg.host
    port = args.port if args.port is not None else server_cfg.port
    if not _is_loopback_host(host):
        _LOGGER.error(
            f"[mcp_systems_server:main] HTTP transport refuses non-loopback host "
            f"{host!r}. Bind to 127.0.0.1, ::1, or 'localhost'."
        )
        sys.exit(2)

    psk = _resolve_psk_or_exit(args.psk, server_cfg, multi_config.config_dir)
    server = _build_fastmcp(multi_config, server_cfg.server_name)
    _run_http(server, host=host, port=port, psk=psk)


if __name__ == "__main__":  # pragma: no cover
    main()
