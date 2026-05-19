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
import dataclasses
import ipaddress
import logging
import pathlib
import socket
import sys
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from typing import Any

from mcp.server.fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from deephaven_mcp._env import env_bool, env_int, env_str
from deephaven_mcp._health import HEALTH_PATH
from deephaven_mcp.auth.backends import (
    AuthBackend,
    PasswordBackend,
    PrivateKeyBackend,
    PSKBackend,
)
from deephaven_mcp.auth.middleware import (
    AuthenticationMiddleware,
    TlsEnforcementMiddleware,
    TransportSecurityPolicy,
    parse_forwarded_allow_ips,
)
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

_LOGGER = logging.getLogger(__name__)

_HEALTH_BYPASS_PATHS: frozenset[str] = frozenset({HEALTH_PATH})
"""``bypass_paths`` set passed to both :class:`TransportSecurityPolicy`
and :class:`AuthenticationMiddleware` so health probes skip TLS
enforcement *and* authentication on every systems server.

:data:`~deephaven_mcp._health.HEALTH_PATH` is the single source of
truth for the path string itself; defining the frozenset once here
avoids recreating it for every policy/middleware build and guarantees
the TLS layer and the auth layer agree on exactly which paths are
exempt.
"""

# Tools registered on every server regardless of type. The per-server
# ``_register_community_tools`` / ``_register_enterprise_tools`` functions
# combine these with per-server exclusive tools and the per-server ``reload``
# variant so that each entry point has a single, authoritative tool manifest.
_SHARED_TOOLS = (session, table, script)


@dataclasses.dataclass(frozen=True)
class _ParsedArgs:
    """Result of CLI/environment parsing for an MCP server entry point.

    Holds every command-line and environment-derived value consumed by
    :func:`_run_server`. Frozen so the values cannot drift after
    parsing; this also makes the parser easy to unit-test by
    constructing instances directly.
    """

    config_path: str | None
    host: str
    port: int
    ssl_keyfile: str | None
    ssl_certfile: str | None
    trust_forwarded_proto: bool
    forwarded_allow_ips: str
    allow_cleartext: bool


def _parse_args(description: str, default_port: int) -> _ParsedArgs:
    """Parse all CLI arguments and corresponding environment variables.

    Precedence for each value (first wins):
      1. CLI argument
      2. Environment variable (see table)
      3. Documented default

    | Field | CLI | Env var | Default |
    |---|---|---|---|
    | ``config_path`` | ``--config`` / ``-c`` | ``DH_MCP_CONFIG_FILE`` | ``None`` |
    | ``host`` | ``--host`` | ``MCP_HOST`` | ``"127.0.0.1"`` |
    | ``port`` | ``--port`` | ``MCP_PORT`` | ``default_port`` |
    | ``ssl_keyfile`` | ``--ssl-keyfile`` | ``MCP_SSL_KEYFILE`` | ``None`` |
    | ``ssl_certfile`` | ``--ssl-certfile`` | ``MCP_SSL_CERTFILE`` | ``None`` |
    | ``trust_forwarded_proto`` | ``--trust-forwarded-proto`` | ``MCP_TRUST_FORWARDED_PROTO`` | ``False`` |
    | ``forwarded_allow_ips`` | ``--forwarded-allow-ips`` | ``MCP_FORWARDED_ALLOW_IPS`` | ``"127.0.0.1"`` |
    | ``allow_cleartext`` | ``--allow-cleartext`` | ``MCP_ALLOW_CLEARTEXT`` | ``False`` |

    Args:
        description (str): Description string for the ``ArgumentParser``.
        default_port (int): Default port number when neither CLI arg nor
            env var is set.

    Returns:
        _ParsedArgs: Frozen dataclass holding every parsed value.
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
    parser.add_argument(
        "--ssl-keyfile",
        default=None,
        help=(
            "Path to TLS private-key PEM file. When set with --ssl-certfile, "
            "the server terminates TLS itself. Falls back to MCP_SSL_KEYFILE."
        ),
    )
    parser.add_argument(
        "--ssl-certfile",
        default=None,
        help=(
            "Path to TLS certificate PEM file. Must be paired with "
            "--ssl-keyfile. Falls back to MCP_SSL_CERTFILE."
        ),
    )
    parser.add_argument(
        "--trust-forwarded-proto",
        action="store_true",
        default=None,
        help=(
            "Honor X-Forwarded-Proto: https from a fronting reverse proxy. "
            "Use only when a TLS-terminating proxy stands in front of this "
            "server. Combine with --forwarded-allow-ips to restrict which "
            "peers are trusted to set the header. Falls back to "
            "MCP_TRUST_FORWARDED_PROTO=1."
        ),
    )
    parser.add_argument(
        "--forwarded-allow-ips",
        default=None,
        help=(
            "Comma-separated peer-IP allowlist for --trust-forwarded-proto. "
            "Accepts single IPs, CIDR blocks, or '*' for any peer. "
            "Default '127.0.0.1'. Falls back to MCP_FORWARDED_ALLOW_IPS."
        ),
    )
    parser.add_argument(
        "--allow-cleartext",
        action="store_true",
        default=None,
        help=(
            "Emergency opt-out: accept cleartext non-loopback traffic "
            "with a loud warning. Auth headers travel UNENCRYPTED. "
            "Falls back to MCP_ALLOW_CLEARTEXT=1."
        ),
    )
    args = parser.parse_args()
    # Use ``is not None`` (rather than truthiness) so legitimate falsy CLI
    # values are not silently overridden by env-var fallback. ``--port 0``
    # (ephemeral, used in tests) is the canonical example, but the same
    # rule applies to every field: an explicit CLI value always wins, even
    # when it is empty/zero.
    config_path: str | None = (
        args.config if args.config is not None else env_str("DH_MCP_CONFIG_FILE")
    )
    host: str = args.host if args.host is not None else env_str("MCP_HOST", "127.0.0.1")
    port: int = (
        args.port if args.port is not None else env_int("MCP_PORT", default_port)
    )
    ssl_keyfile: str | None = (
        args.ssl_keyfile if args.ssl_keyfile is not None else env_str("MCP_SSL_KEYFILE")
    )
    ssl_certfile: str | None = (
        args.ssl_certfile
        if args.ssl_certfile is not None
        else env_str("MCP_SSL_CERTFILE")
    )
    trust_forwarded_proto: bool = (
        bool(args.trust_forwarded_proto)
        if args.trust_forwarded_proto is not None
        else env_bool("MCP_TRUST_FORWARDED_PROTO")
    )
    forwarded_allow_ips: str = (
        args.forwarded_allow_ips
        if args.forwarded_allow_ips is not None
        else env_str("MCP_FORWARDED_ALLOW_IPS", "127.0.0.1")
    )
    allow_cleartext: bool = (
        bool(args.allow_cleartext)
        if args.allow_cleartext is not None
        else env_bool("MCP_ALLOW_CLEARTEXT")
    )
    return _ParsedArgs(
        config_path=config_path,
        host=host,
        port=port,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        trust_forwarded_proto=trust_forwarded_proto,
        forwarded_allow_ips=forwarded_allow_ips,
        allow_cleartext=allow_cleartext,
    )


def _setup_env() -> None:
    """Initialize logging and monkeypatching for MCP server entry points.

    Imports are intentionally **local** to this function rather than at
    module top-level: ``setup_logging()`` must run before any other code
    has a chance to materialize loggers or emit records, so we defer
    importing :mod:`deephaven_mcp._logging` and
    :mod:`deephaven_mcp._monkeypatch` until the entry point invokes us.
    Hoisting these imports to the module top is a footgun — any future
    import ordering change could cause records to be emitted before the
    root logger is configured.
    """
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


async def _load_community_startup_state(
    manager: CommunityServerConfigManager,
) -> tuple[float, float, str | None]:
    """Load the community config and return the startup-state tuple.

    Reads the config exactly once and resolves the community PSK from
    the same cached dict.

    Args:
        manager (CommunityServerConfigManager): Community config manager
            bound to the resolved config path.

    Returns:
        tuple[float, float, str | None]: ``(idle_timeout, sweep_interval,
            resolved_psk)``.  ``resolved_psk`` is ``None`` when
            ``auth.enabled`` is ``false`` (loopback-only).
    """
    config = await manager.get_config()
    idle_timeout = await manager.get_session_idle_timeout_seconds()
    sweep_interval = await manager.get_session_idle_sweep_interval_seconds()
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
    return idle_timeout, sweep_interval, resolved_psk


async def _load_enterprise_startup_state(
    manager: EnterpriseServerConfigManager,
) -> tuple[float, float, tuple[list[str], bool]]:
    """Load the enterprise config and return the startup-state tuple.

    Reads the config exactly once and pulls the auth-related fields
    from the same cached dict.

    Args:
        manager (EnterpriseServerConfigManager): Enterprise config
            manager bound to the resolved config path.

    Returns:
        tuple[float, float, tuple[list[str], bool]]: ``(idle_timeout,
            sweep_interval, (backends, allow_effective_user))``.  The auth
            fields are nested so the whole result matches the
            ``(float, float, U)`` shape consumed by :func:`_run_server`.
    """
    config = await manager.get_config()
    idle_timeout = await manager.get_session_idle_timeout_seconds()
    sweep_interval = await manager.get_session_idle_sweep_interval_seconds()
    backends = get_enterprise_auth_backends(config)
    allow_effective_user = get_enterprise_allow_effective_user(config)
    return idle_timeout, sweep_interval, (backends, allow_effective_user)


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

    Raises:
        SystemExit: If ``async_loader`` raises any exception; the
            exception is logged with ``exc_info=True`` under the
            ``[{label}]`` prefix before the process exits with
            code 1.
    """
    _LOGGER.info(f"[{label}] Validating configuration before server startup...")
    manager = manager_class(config_path=config_path)
    try:
        result = asyncio.run(async_loader(manager))
    except Exception as e:
        _LOGGER.error(
            f"[{label}] Configuration error — server will not start: {e}",
            exc_info=True,
        )
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


def _validate_ssl_paths_or_exit(*, label: str, args: _ParsedArgs) -> None:
    """Reject misconfigured ``--ssl-keyfile``/``--ssl-certfile`` at startup.

    Two checks: paired-presence (both or neither) and existence on
    disk (must be a regular file). Either failure exits with code 1
    rather than deferring to uvicorn's bind step, where the same
    error appears as a confusing late-stage traceback.

    Args:
        label (str): Short server label (``"community"`` /
            ``"enterprise"``) used as a log-line prefix.
        args (_ParsedArgs): Parsed CLI/env args. Only
            ``args.ssl_keyfile`` and ``args.ssl_certfile`` are read.

    Raises:
        SystemExit: If the flags are not paired (only one of the two
            is set) or if either path does not point to a regular
            file on disk.
    """
    # Paired-presence: both flags must come together.
    if bool(args.ssl_keyfile) != bool(args.ssl_certfile):
        _LOGGER.error(
            f"[{label}] Refusing to start: --ssl-keyfile and --ssl-certfile "
            "must be set together (both or neither). Got "
            f"ssl_keyfile={args.ssl_keyfile!r}, ssl_certfile={args.ssl_certfile!r}."
        )
        sys.exit(1)
    # Existence check (when paired).
    if args.ssl_keyfile and args.ssl_certfile:
        for field, path in (
            ("--ssl-keyfile", args.ssl_keyfile),
            ("--ssl-certfile", args.ssl_certfile),
        ):
            if not pathlib.Path(path).is_file():
                _LOGGER.error(
                    f"[{label}] Refusing to start: {field} path {path!r} "
                    "does not exist or is not a regular file."
                )
                sys.exit(1)


def _validate_transport_security_or_exit(
    *, label: str, args: _ParsedArgs
) -> tuple[TransportSecurityPolicy, str | None, str | None]:
    """Decide the transport-security policy at startup or refuse to start.

    Auth headers (``X-Deephaven-Password``, ``X-Deephaven-Private-Key``,
    ``X-Deephaven-PSK``) carry secrets in cleartext on the wire. This
    function enforces, before the server starts accepting requests,
    that one of the following is true:

    1. The server is binding to a loopback host (traffic never leaves
       the kernel).
    2. Native TLS is configured (``--ssl-keyfile`` and
       ``--ssl-certfile`` both set).
    3. The operator opts in to trusting a fronting TLS-terminating
       proxy via ``--trust-forwarded-proto`` (with the optional
       ``--forwarded-allow-ips`` peer-IP allowlist).
    4. The operator explicitly accepts cleartext via
       ``--allow-cleartext`` (logged as a loud WARNING banner every
       startup).

    Anything else is a hard startup error: a non-loopback bind with no
    transport-security mechanism is the dangerous misconfiguration
    we want to prevent. Mirrors the existing community-auth-disabled
    error pattern in :func:`_build_community_middleware`.

    Args:
        label (str): Short server label (``"community"`` /
            ``"enterprise"``) used as a log-line prefix.
        args (_ParsedArgs): Parsed CLI/env args. The bind host is read
            from ``args.host``.

    Returns:
        tuple: ``(policy, ssl_keyfile, ssl_certfile)``. ``policy`` is
            the immutable :class:`TransportSecurityPolicy` consumed by
            :class:`TlsEnforcementMiddleware`. ``ssl_keyfile`` and
            ``ssl_certfile`` are the (validated, paired) paths to pass
            to :class:`uvicorn.Config`, or ``None`` when native TLS is
            not enabled.

    Raises:
        SystemExit: On any of: paired-only ``--ssl-*`` flags broken,
            invalid ``--forwarded-allow-ips``, or non-loopback bind
            without any transport-security opt-in.
    """
    _validate_ssl_paths_or_exit(label=label, args=args)

    try:
        allow_networks, allow_any = parse_forwarded_allow_ips(args.forwarded_allow_ips)
    except ValueError as exc:
        _LOGGER.error(
            f"[{label}] Refusing to start: invalid --forwarded-allow-ips "
            f"value {args.forwarded_allow_ips!r}: {exc}"
        )
        sys.exit(1)

    native_tls = bool(args.ssl_keyfile)
    is_loopback = _is_loopback_host(args.host)

    if is_loopback:
        _LOGGER.info(
            f"[{label}] Transport security: bind is loopback ({args.host!r}); "
            "cleartext is safe (traffic never leaves the kernel)."
        )
    elif native_tls:
        _LOGGER.info(
            f"[{label}] Transport security: native TLS enabled "
            f"(ssl_keyfile={args.ssl_keyfile!r}, ssl_certfile={args.ssl_certfile!r})."
        )
    elif args.trust_forwarded_proto:
        if allow_any:
            _LOGGER.warning(
                f"[{label}] Transport security: trusting X-Forwarded-Proto from "
                "ANY peer (--forwarded-allow-ips=*). Ensure no untrusted client "
                "can reach this server directly, or every X-Forwarded-Proto "
                "header can be spoofed."
            )
        else:
            _LOGGER.info(
                f"[{label}] Transport security: trusting X-Forwarded-Proto from "
                f"peers in {[str(n) for n in allow_networks]}."
            )
            # If every allowed peer is loopback, --trust-forwarded-proto
            # is a no-op: loopback peers already bypass TLS via the
            # ``is_loopback`` short-circuit. The operator almost
            # certainly forgot to set --forwarded-allow-ips to the
            # actual proxy CIDR. Loud-warn so this misconfiguration is
            # visible in startup logs.
            if all(
                ipaddress.ip_address(net.network_address).is_loopback
                for net in allow_networks
            ):
                _LOGGER.warning(
                    f"[{label}] --trust-forwarded-proto is enabled but "
                    "--forwarded-allow-ips contains only loopback peers "
                    f"({[str(n) for n in allow_networks]}). Loopback "
                    "traffic already bypasses TLS enforcement, so this "
                    "combination has no effect on non-loopback requests. "
                    "Set --forwarded-allow-ips (or MCP_FORWARDED_ALLOW_IPS) "
                    "to the CIDR of the TLS-terminating proxy you intend "
                    "to trust."
                )
    elif args.allow_cleartext:
        _LOGGER.warning(
            f"[{label}] "
            "======================================================================\n"
            "WARNING: CLEARTEXT TRAFFIC IS EXPLICITLY ALLOWED.\n"
            "\n"
            f"Server is binding to {args.host!r} (non-loopback) and --allow-cleartext "
            "was set. Authentication headers (X-Deephaven-Password, "
            "X-Deephaven-Private-Key, X-Deephaven-PSK) will travel UNENCRYPTED "
            "on the wire and are observable by anyone on the network path.\n"
            "\n"
            "This mode is intended ONLY for trusted private networks (LAN-only, "
            "air-gapped) where you have other controls in place. For production "
            "use, configure native TLS (--ssl-keyfile/--ssl-certfile) or run "
            "behind a TLS-terminating proxy with --trust-forwarded-proto.\n"
            "======================================================================"
        )
    else:
        _LOGGER.error(
            f"[{label}] Refusing to start: server is set to bind to {args.host!r} "
            "(non-loopback) without any transport-security mechanism enabled.\n"
            "\n"
            "Authentication headers (X-Deephaven-Password, "
            "X-Deephaven-Private-Key, X-Deephaven-PSK) carry secrets in "
            "cleartext on the wire and must travel over TLS. Choose one:\n"
            "\n"
            "  1. Enable native TLS by passing both --ssl-keyfile <path> "
            "and --ssl-certfile <path> (or set MCP_SSL_KEYFILE / "
            "MCP_SSL_CERTFILE env vars).\n"
            "\n"
            "  2. Run behind a TLS-terminating reverse proxy (nginx, Envoy, "
            "Cloud Run, ALB, ...) and pass --trust-forwarded-proto. The "
            "server will trust X-Forwarded-Proto: https only from peers "
            "in --forwarded-allow-ips (default '127.0.0.1').\n"
            "\n"
            "  3. Bind only to the local machine. Pass '--host 127.0.0.1' "
            "on the command line, set 'MCP_HOST=127.0.0.1' in the "
            "environment, or remove the --host / MCP_HOST override "
            "entirely (127.0.0.1 is the default).\n"
            "\n"
            "  4. Emergency only: --allow-cleartext (or MCP_ALLOW_CLEARTEXT=1) "
            "opts out of this check entirely. Auth headers will travel "
            "UNENCRYPTED on the wire."
        )
        sys.exit(1)

    policy = TransportSecurityPolicy(
        trust_forwarded_proto=args.trust_forwarded_proto,
        forwarded_allow_ips=allow_networks,
        allow_any_forwarded_ip=allow_any,
        allow_cleartext=args.allow_cleartext,
        bypass_paths=_HEALTH_BYPASS_PATHS,
    )
    return policy, args.ssl_keyfile, args.ssl_certfile


def _build_enterprise_middleware(
    state: tuple[list[str], bool], _host: str
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
        _host (str): Accepted to match the signature expected by
            :func:`_run_server`; the enterprise builder does not use
            it. The leading underscore marks it as intentionally unused.

    Returns:
        list[Middleware]: A single-entry middleware stack mounting the
            auth chain in front of the FastMCP streamable-HTTP app.
    """
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
        Middleware(
            AuthenticationMiddleware,
            backends=tuple(instances),
            bypass_paths=_HEALTH_BYPASS_PATHS,
        ),
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
        Middleware(
            AuthenticationMiddleware,
            backends=(backend,),
            bypass_paths=_HEALTH_BYPASS_PATHS,
        ),
    ]


def _run_with_middleware(
    server: FastMCP,
    middleware: list[Middleware],
    host: str,
    port: int,
    *,
    ssl_keyfile: str | None = None,
    ssl_certfile: str | None = None,
) -> None:
    """Run the FastMCP streamable-HTTP app with extra Starlette middleware.

    FastMCP's built-in ``server.run(transport="streamable-http")`` does not
    expose a way to inject additional middleware or SSL options. We
    replicate its small body here: grab the Starlette app via
    :meth:`FastMCP.streamable_http_app`, layer our middleware on top, and
    hand the result to ``uvicorn``. ``middleware`` may be empty, in which
    case no extra layers are added.

    Middleware ordering: Starlette's :meth:`add_middleware` inserts at
    position 0, so the **last** middleware iterated is the **outermost**
    layer (runs first on each request). Callers must therefore order
    ``middleware`` so that auth-style layers come before transport-style
    layers — see :func:`_run_server`, which appends the TLS-enforcement
    layer after the auth layer for exactly this reason.

    Args:
        server (FastMCP): The configured FastMCP instance.
        middleware (list[Middleware]): Additional middleware to mount
            ahead of the FastMCP app. May be empty. Last entry becomes
            the outermost layer.
        host (str): Host to bind.
        port (int): TCP port to bind.
        ssl_keyfile (str | None): Path to a TLS private-key PEM file.
            When set, ``ssl_certfile`` must also be set; uvicorn
            terminates TLS for incoming connections.
        ssl_certfile (str | None): Path to a TLS certificate PEM file.
            Must be paired with ``ssl_keyfile``.
    """
    import uvicorn  # local import: avoid top-level dependency on uvicorn

    starlette_app = server.streamable_http_app()
    for entry in middleware:
        starlette_app.add_middleware(entry.cls, *entry.args, **entry.kwargs)
    config = uvicorn.Config(
        starlette_app,
        host=host,
        port=port,
        log_config=None,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
    )
    uvicorn.Server(config).run()


def _register_health_endpoint(server: FastMCP) -> None:
    """Register the ``/health`` liveness/readiness route on ``server``.

    Mirrors the docs server's ``/health`` endpoint
    (:mod:`deephaven_mcp.mcp_docs_server._mcp`). Returns HTTP 200 with
    JSON body ``{"status": "ok"}``. The route bypasses both the TLS
    enforcement layer and the auth layer: :data:`_HEALTH_BYPASS_PATHS`
    is passed as ``bypass_paths`` to :class:`TransportSecurityPolicy`
    (in :func:`_validate_transport_security_or_exit`) and to
    :class:`AuthenticationMiddleware` (in
    :func:`_build_community_middleware` /
    :func:`_build_enterprise_middleware`), so probes from any peer
    succeed over cleartext with no credentials.
    """

    @server.custom_route(HEALTH_PATH, methods=["GET"])  # type: ignore[untyped-decorator]
    async def health_check(_request: Request) -> JSONResponse:
        """Return a 200/JSON liveness probe for the systems server."""
        _LOGGER.debug("[mcp_systems_server:health_check] Health check requested")
        return JSONResponse({"status": "ok"})


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


def _run_server[M: ConfigManager, U](
    *,
    label: str,
    description: str,
    default_port: int,
    server_name: str,
    manager_class: type[M],
    async_loader: Callable[[M], Coroutine[Any, Any, tuple[float, float, U]]],
    lifespan_factory: Callable[
        [float | None, float | None, str | None],
        Callable[
            [FastMCP[LifespanContext]],
            AbstractAsyncContextManager[LifespanContext],
        ],
    ],
    build_middleware: Callable[[U, str], list[Middleware]],
    register_tools: Callable[[FastMCP], None],
) -> None:
    """Drive a full MCP-server startup: args → validate → middleware → serve.

    Captures the common lifecycle shared by :func:`community` and
    :func:`enterprise`. Each per-server difference (manager class, loader,
    lifespan factory, middleware builder, tool registration) is a
    parameter — the entry points are pure parameter dispatch. The
    lifespan factory already encodes which concrete registry class is
    instantiated for the server, so there is no separate ``registry_class``
    argument here.

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
            ``(idle_timeout, sweep_interval, middleware_state)``.
        lifespan_factory (Callable): Factory that builds the FastMCP lifespan
            from the idle timeout, sweep interval, and config path.
        build_middleware (Callable): Builds the Starlette middleware stack
            from the loader's middleware-state value and the bind host.
            (Community's value is the resolved PSK or ``None``; enterprise's
            is the ``(backends, allow_effective_user)`` tuple.)
        register_tools (Callable): Registers every MCP tool exposed on this
            server (shared + per-server). Typically
            :func:`_register_community_tools` or
            :func:`_register_enterprise_tools`.
    """
    _setup_env()
    parsed = _parse_args(description, default_port=default_port)
    _LOGGER.info(
        f"[{label}] Starting MCP server {server_name!r} on {parsed.host}:{parsed.port} "
        f"(streamable-http), config={parsed.config_path!r}"
    )
    policy, ssl_keyfile, ssl_certfile = _validate_transport_security_or_exit(
        label=label, args=parsed
    )
    idle_timeout, sweep_interval, mw_state = _run_startup_validation_or_exit(
        parsed.config_path, manager_class, async_loader, label
    )
    auth_middleware = build_middleware(mw_state, parsed.host)
    # Append the TLS layer LAST so Starlette's add_middleware (insert(0, ...))
    # places it as the OUTERMOST layer that runs first on every request —
    # transport-security checks must precede authentication checks.
    middleware = auth_middleware + [
        Middleware(TlsEnforcementMiddleware, policy=policy),
    ]
    server: FastMCP[LifespanContext] = FastMCP(
        server_name,
        lifespan=lifespan_factory(idle_timeout, sweep_interval, parsed.config_path),
        host=parsed.host,
        port=parsed.port,
    )
    register_tools(server)
    _register_health_endpoint(server)
    try:
        _run_with_middleware(
            server,
            middleware,
            parsed.host,
            parsed.port,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
        )
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
        lifespan_factory=make_enterprise_lifespan,
        build_middleware=_build_enterprise_middleware,
        register_tools=_register_enterprise_tools,
    )
