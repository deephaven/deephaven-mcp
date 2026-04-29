"""MCP Server Entry Points and Assembly.

Provides the CLI entry points for the DHE and DHC MCP servers plus the shared
assembly logic (environment setup, argument parsing, shared tool registration)
used by both.

Entry points (registered in pyproject.toml):
  - ``dh-mcp-enterprise-server``: Start the Deephaven Enterprise (DHE) MCP server.
  - ``dh-mcp-community-server``:  Start the Deephaven Community (DHC) MCP server.

Configuration precedence for both servers (first wins):
  1. ``--config`` CLI argument
  2. ``DH_MCP_CONFIG_FILE`` environment variable

Host/port precedence for both servers (first wins):
  1. ``--host`` / ``--port`` CLI arguments
  2. ``MCP_HOST`` / ``MCP_PORT`` environment variables
  3. Per-server defaults (enterprise: 8002, community: 8003)
"""

import argparse
import asyncio
import ipaddress
import logging
import os
import socket
import sys
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from typing import Any

from mcp.server.fastmcp import FastMCP
from starlette.middleware import Middleware

from deephaven_mcp.auth.backends import (
    AuthBackend,
    PasswordBackend,
    PrivateKeyBackend,
    PSKBackend,
)
from deephaven_mcp.auth.middleware import AuthenticationMiddleware
from deephaven_mcp.config import (
    CommunityServerConfigManager,
    ConfigManager,
    EnterpriseServerConfigManager,
    resolve_secret_field,
)
from deephaven_mcp.config.enterprise import (
    get_enterprise_allow_effective_user,
    get_enterprise_auth_backends,
)
from deephaven_mcp.mcp_systems_server._lifespan import (
    LifespanContext,
    make_community_lifespan,
    make_enterprise_lifespan,
)
from deephaven_mcp.mcp_systems_server._session_registry_manager import (
    SessionRegistryManager,
)
from deephaven_mcp.mcp_systems_server._tools import (
    catalog,
    pq,
    reload,
    script,
    session,
    session_community,
    session_enterprise,
    table,
)
from deephaven_mcp.resource_manager import (
    BaseRegistry,
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
)

_LOGGER = logging.getLogger(__name__)

# Tools registered on every server regardless of type. The per-server
# ``_register_community_tools`` / ``_register_enterprise_tools`` functions
# combine these with per-server exclusive tools and the per-server ``reload``
# variant so that each entry point has a single, authoritative tool manifest.
_SHARED_TOOLS = (session, table, script)


def _setup_env() -> None:
    """Initialize logging and monkeypatching for MCP server entry points."""
    from deephaven_mcp._logging import (
        setup_global_exception_logging,
        setup_logging,
        setup_signal_handler_logging,
    )
    from deephaven_mcp._monkeypatch import monkeypatch_uvicorn_exception_handling

    setup_logging()
    setup_global_exception_logging()
    setup_signal_handler_logging()
    monkeypatch_uvicorn_exception_handling()


def _parse_args(description: str, default_port: int) -> tuple[str | None, str, int]:
    """Parse ``--config`` / ``--host`` / ``--port`` from argv and env vars.

    Precedence for each value (first wins):
      1. CLI argument
      2. Environment variable (``DH_MCP_CONFIG_FILE``, ``MCP_HOST``, ``MCP_PORT``)
      3. Default (``None`` for config, ``"127.0.0.1"`` for host, ``default_port`` for port)

    Args:
        description (str): Description string for the ``ArgumentParser``.
        default_port (int): Default port number when neither CLI arg nor env var is set.

    Returns:
        tuple[str | None, str, int]: ``(config_path, host, port)``
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to the config file. Falls back to DH_MCP_CONFIG_FILE env var.",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Host to bind to. Falls back to MCP_HOST env var, then 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Port to listen on. Falls back to MCP_PORT env var, then {default_port}.",
    )
    args = parser.parse_args()
    config_path: str | None = args.config or os.environ.get("DH_MCP_CONFIG_FILE")
    host: str = args.host or os.environ.get("MCP_HOST", "127.0.0.1")
    port: int = args.port or int(os.environ.get("MCP_PORT", str(default_port)))
    return config_path, host, port


async def _load_community_startup_state(
    manager: CommunityServerConfigManager,
) -> tuple[float, str | None]:
    """Load the community config and return (idle_timeout, resolved_psk).

    Single entry point for startup-time config reads: validates the config
    (via ``get_config()``) and resolves the community PSK from the same
    cached dict. The PSK is ``None`` when ``auth.enabled`` is explicitly
    ``False``, which is only valid on loopback binds.
    """
    config = await manager.get_config()
    idle_timeout = await manager.get_mcp_session_idle_timeout_seconds()
    auth = config["auth"]
    # auth.enabled = false means the server runs without PSK auth
    # (loopback binds only). Otherwise, exactly one of psk / psk_env_var
    # is present (validator-enforced) and resolve_secret_field returns it.
    resolved_psk = (
        None
        if not auth.get("enabled", True)
        else resolve_secret_field(
            config=auth,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="community 'auth' section",
        )
    )
    return idle_timeout, resolved_psk


async def _load_enterprise_startup_state(
    manager: EnterpriseServerConfigManager,
) -> tuple[float, tuple[list[str], bool]]:
    """Load the enterprise config and return startup-relevant fields.

    Single entry point for startup-time enterprise config reads:
    validates via ``get_config()`` and pulls the auth-related fields
    from the same cached dict so the file is read exactly once.

    Returns:
        tuple[float, tuple[list[str], bool]]: ``(idle_timeout, (backends,
            allow_effective_user))``. The auth fields are nested as a
            tuple so that the whole loader result matches the ``(float,
            U)`` shape consumed by :func:`_run_server`, with ``U`` being
            the middleware-builder's input.
    """
    config = await manager.get_config()
    idle_timeout = await manager.get_mcp_session_idle_timeout_seconds()
    backends = get_enterprise_auth_backends(config)
    allow_effective_user = get_enterprise_allow_effective_user(config)
    return idle_timeout, (backends, allow_effective_user)


def _run_startup_validation_or_exit[M: ConfigManager, T](
    config_path: str | None,
    manager_class: type[M],
    async_loader: Callable[[M], Coroutine[Any, Any, T]],
    label: str,
) -> T:
    """Run ``async_loader`` inside a temporary event loop; ``sys.exit(1)`` on failure.

    Builds a single ``manager_class`` instance bound to ``config_path`` and hands
    it to ``async_loader``, which is responsible for validating the config (via
    ``manager.get_config()``) and extracting whatever startup-relevant fields the
    caller needs from the same cached dict. The manager instance is discarded
    when this function returns — it lives only for the pre-flight validation
    loop, separate from uvicorn's serving loop.

    Args:
        config_path (str | None): Explicit config path, or ``None`` to fall back
            to the config manager's default (``DH_MCP_CONFIG_FILE``).
        manager_class (type[ConfigManager]): The concrete ``ConfigManager``
            subclass to instantiate (community or enterprise).
        async_loader (Callable): Coroutine that takes the manager and returns
            the startup-state tuple for the caller.
        label (str): Short server label (``"community"`` / ``"enterprise"``)
            used to prefix log lines.

    Returns:
        T: Whatever ``async_loader`` produces on success.
    """
    _LOGGER.info(f"[{label}] Validating configuration before server startup...")
    manager = manager_class(config_path=config_path)
    try:
        result = asyncio.run(async_loader(manager))
    except Exception as e:
        _LOGGER.error(f"[{label}] Configuration error — server will not start: {e}")
        sys.exit(1)
    _LOGGER.info(f"[{label}] Configuration validated successfully.")
    return result


def _is_loopback_host(host: str) -> bool:
    """Return ``True`` iff ``host`` resolves exclusively to loopback addresses.

    Accepts ``"localhost"`` (case-insensitive), any IPv4 address in
    ``127.0.0.0/8``, and ``::1``. An unresolvable host is treated as not
    loopback so the safe default for unknown values is to refuse to disable
    auth.

    Args:
        host (str): The host string the server will bind to.

    Returns:
        bool: ``True`` if the host is a loopback address.
    """
    if host.lower() == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            resolved = {info[4][0] for info in socket.getaddrinfo(host, None)}
        except socket.gaierror:
            return False
        return all(ipaddress.ip_address(addr).is_loopback for addr in resolved)
    return ip.is_loopback


def _build_enterprise_middleware(
    state: tuple[list[str], bool], host: str
) -> list[Middleware]:
    """Build the Starlette middleware stack for the enterprise server.

    Maps each name in ``state[0]`` to its concrete
    :class:`~deephaven_mcp.auth.backends.AuthBackend` implementation, instantiates
    them in the order declared, and wraps them in a single
    :class:`~deephaven_mcp.auth.middleware.AuthenticationMiddleware`. Unlike the
    community server, enterprise has **no loopback escape**: per-user
    credentials are mandatory at all times.

    Args:
        state (tuple[list[str], bool]): ``(backends, allow_effective_user)``
            where ``backends`` is a non-empty subset of
            :data:`deephaven_mcp.config.enterprise.SUPPORTED_AUTH_BACKENDS`
            and ``allow_effective_user`` is whether the password backend
            should honor the optional ``X-Deephaven-Effective-User``
            header (ignored when ``"password"`` is not in ``backends``).
        host (str): Accepted to match the signature expected by
            :func:`_run_server`; the enterprise builder does not use it.

    Returns:
        list[Middleware]: A single-entry middleware stack mounting the
            auth chain in front of the FastMCP streamable-HTTP app.
    """
    del host  # explicitly unused; enterprise auth is host-agnostic
    backends, allow_effective_user = state
    instances: list[AuthBackend] = []
    for name in backends:
        if name == "password":
            instances.append(PasswordBackend(allow_effective_user=allow_effective_user))
        elif name == "private_key":
            instances.append(PrivateKeyBackend())
        else:
            # Defensive: validate_enterprise_config rejects unknown
            # backend names, so this path should be unreachable.
            raise ValueError(
                f"[enterprise] Unsupported auth backend '{name}' in config."
            )
    _LOGGER.info(
        f"[enterprise] Mounting auth middleware with backends={backends!r} "
        f"(allow_effective_user={allow_effective_user})"
    )
    return [
        Middleware(AuthenticationMiddleware, backends=tuple(instances)),
    ]


def _build_community_middleware(state: str | None, host: str) -> list[Middleware]:
    """Build the Starlette middleware stack for the community server.

    Enforces the loopback-only rule for disabled auth. When ``state`` is
    ``None`` (``community.auth.enabled`` was ``false``), the server may
    only bind to a loopback host; any other host is a hard startup error.

    Args:
        state (str | None): The resolved community pre-shared key, or
            ``None`` if ``auth.enabled`` is ``false``.
        host (str): The host the server will bind to. Used for the
            loopback-enforcement check when auth is disabled.

    Returns:
        list[Middleware]: The Starlette middleware stack to mount in front
            of the FastMCP streamable-HTTP app. Empty when auth is
            explicitly disabled on a loopback bind.

    Raises:
        SystemExit: If auth is disabled and ``host`` is not loopback.
    """
    psk = state
    if psk is None:
        if not _is_loopback_host(host):
            _LOGGER.error(
                "[community] Refusing to start: authentication is disabled "
                f"(config has 'auth.enabled: false') but the server is set "
                f"to bind to {host!r}, which can accept connections from "
                "other machines on the network.\n"
                "\n"
                "Disabling authentication is only safe when the server "
                "accepts connections from the same machine only. "
                "Choose one:\n"
                "\n"
                "  1. RECOMMENDED: enable authentication. In your config "
                "file, add either a direct PSK or an env-var indirection "
                "to the 'auth' block:\n"
                "\n"
                '         "auth": { "psk": "<your-secret-here>" }\n'
                "\n"
                '         "auth": { "psk_env_var": "DH_MCP_COMMUNITY_PSK" }\n'
                "\n"
                "  2. Bind only to this machine. Pass '--host 127.0.0.1' "
                "on the command line, or set 'MCP_HOST=127.0.0.1' in the "
                "environment, or remove the --host / MCP_HOST override "
                "entirely (127.0.0.1 is the default)."
            )
            sys.exit(1)
        _LOGGER.warning(
            "[community] "
            "======================================================================\n"
            "WARNING: AUTHENTICATION IS DISABLED.\n"
            "\n"
            f"Server is binding to {host!r}, which only accepts connections "
            "from this same machine. However, ANY user, container, or "
            "program running on this machine can use this server with no "
            "credentials.\n"
            "\n"
            "This mode is for local development only. To enable "
            "authentication for production, set 'auth.psk' or "
            "'auth.psk_env_var' in your config file.\n"
            "======================================================================"
        )
        return []
    backend = PSKBackend(expected_psk=psk)
    _LOGGER.info(
        "[community] Authentication is ENABLED. Clients must present the "
        "configured pre-shared key in the 'X-Deephaven-PSK' HTTP header on "
        "every request."
    )
    return [
        Middleware(AuthenticationMiddleware, backends=(backend,)),
    ]


def _run_with_middleware(
    server: FastMCP,
    middleware: list[Middleware],
    host: str,
    port: int,
) -> None:
    """Run the FastMCP streamable-HTTP app with extra Starlette middleware.

    FastMCP's built-in ``server.run(transport="streamable-http")`` does not
    expose a way to inject additional middleware. We replicate its small
    body here: grab the Starlette app via
    :meth:`FastMCP.streamable_http_app`, layer our middleware on top, and
    hand the result to ``uvicorn``. ``middleware`` may be empty, in which
    case no extra layers are added.

    Args:
        server (FastMCP): The configured FastMCP instance.
        middleware (list[Middleware]): Additional middleware to mount
            ahead of the FastMCP app. May be empty.
        host (str): Host to bind.
        port (int): TCP port to bind.
    """
    import uvicorn  # local import: avoid top-level dependency on uvicorn

    starlette_app = server.streamable_http_app()
    for entry in middleware:
        starlette_app.add_middleware(entry.cls, *entry.args, **entry.kwargs)
    config = uvicorn.Config(starlette_app, host=host, port=port, log_config=None)
    uvicorn.Server(config).run()


def _register_shared_tools(server: FastMCP) -> None:
    """Register the tools common to both DHE and DHC servers."""
    for module in _SHARED_TOOLS:
        module.register_tools(server)


def _register_community_tools(server: FastMCP) -> None:
    """Register every MCP tool exposed on the DHC server.

    This is the single, authoritative tool manifest for the community
    server: shared tools first, then community-exclusive tools.
    """
    _register_shared_tools(server)
    reload.register_community_tools(server)
    session_community.register_tools(server)


def _register_enterprise_tools(server: FastMCP) -> None:
    """Register every MCP tool exposed on the DHE server.

    This is the single, authoritative tool manifest for the enterprise
    server: shared tools first, then enterprise-exclusive tools.
    """
    _register_shared_tools(server)
    reload.register_enterprise_tools(server)
    session_enterprise.register_tools(server)
    catalog.register_tools(server)
    pq.register_tools(server)


def _run_server[M: ConfigManager, R: BaseRegistry, U](
    *,
    label: str,
    description: str,
    default_port: int,
    server_name: str,
    manager_class: type[M],
    async_loader: Callable[[M], Coroutine[Any, Any, tuple[float, U]]],
    registry_class: type[R],
    lifespan_factory: Callable[
        [SessionRegistryManager[R], str | None],
        Callable[
            [FastMCP[LifespanContext[R]]],
            AbstractAsyncContextManager[LifespanContext[R]],
        ],
    ],
    build_middleware: Callable[[U, str], list[Middleware]],
    register_tools: Callable[[FastMCP], None],
) -> None:
    """Drive a full MCP-server startup: args → validate → middleware → serve.

    Captures the common lifecycle shared by :func:`community` and
    :func:`enterprise`. Each per-server difference (manager class, loader,
    registry class, lifespan factory, middleware builder, tool registration)
    is a parameter — the entry points are pure parameter dispatch.

    Args:
        label (str): Short server label (``"community"`` / ``"enterprise"``)
            used as a log-line prefix.
        description (str): ``argparse`` description for the CLI ``--help`` text.
        default_port (int): Port used when neither ``--port`` nor ``MCP_PORT``
            is set.
        server_name (str): FastMCP server name (e.g. ``"deephaven-mcp-community"``).
        manager_class (type[ConfigManager]): Concrete ``ConfigManager`` subclass
            to instantiate for pre-flight validation.
        async_loader (Callable): Coroutine that validates the config and returns
            ``(idle_timeout, middleware_state)``.
        registry_class (type[BaseRegistry]): Concrete ``BaseRegistry`` subclass
            used by ``SessionRegistryManager``.
        lifespan_factory (Callable): Factory that builds the FastMCP lifespan
            from a ``SessionRegistryManager`` and config path.
        build_middleware (Callable): Builds the Starlette middleware stack
            from the loader's middleware-state tuple and the bind host.
        register_tools (Callable): Registers every MCP tool exposed on this
            server (shared + per-server). Typically
            :func:`_register_community_tools` or
            :func:`_register_enterprise_tools`.
    """
    _setup_env()
    config_path, host, port = _parse_args(description, default_port=default_port)
    _LOGGER.info(
        f"[{label}] Starting MCP server {server_name!r} on {host}:{port} "
        f"(streamable-http), config={config_path!r}"
    )
    idle_timeout, mw_state = _run_startup_validation_or_exit(
        config_path, manager_class, async_loader, label
    )
    middleware = build_middleware(mw_state, host)
    session_registry_manager: SessionRegistryManager[R] = SessionRegistryManager(
        registry_class=registry_class,
        idle_timeout_seconds=idle_timeout,
    )
    server: FastMCP[LifespanContext[R]] = FastMCP(
        server_name,
        lifespan=lifespan_factory(session_registry_manager, config_path),
        host=host,
        port=port,
    )
    register_tools(server)
    try:
        _run_with_middleware(server, middleware, host, port)
    finally:
        _LOGGER.info(f"[{label}] MCP server {server.name!r} stopped.")


def community() -> None:
    """Entry point: start the Deephaven Community (DHC) MCP server."""
    _run_server(
        label="community",
        description="Start the Deephaven Community MCP server (HTTP transport only).",
        default_port=8003,
        server_name="deephaven-mcp-community",
        manager_class=CommunityServerConfigManager,
        async_loader=_load_community_startup_state,
        registry_class=CommunitySessionRegistry,
        lifespan_factory=make_community_lifespan,
        build_middleware=_build_community_middleware,
        register_tools=_register_community_tools,
    )


def enterprise() -> None:
    """Entry point: start the Deephaven Enterprise (DHE) MCP server."""
    _run_server(
        label="enterprise",
        description="Start the Deephaven Enterprise MCP server (HTTP transport only).",
        default_port=8002,
        server_name="deephaven-mcp-enterprise",
        manager_class=EnterpriseServerConfigManager,
        async_loader=_load_enterprise_startup_state,
        registry_class=EnterpriseSessionRegistry,
        lifespan_factory=make_enterprise_lifespan,
        build_middleware=_build_enterprise_middleware,
        register_tools=_register_enterprise_tools,
    )
