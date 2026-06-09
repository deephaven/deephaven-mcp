"""Streamable-HTTP transport machinery for the systems server.

Encapsulates everything HTTP-specific that ``server.py`` would
otherwise carry: bind / PSK helpers, the loopback-restriction check,
the ASGI app builder, the resolved-plan dataclasses, the two policy
planners, and the unified runner. ``server.py`` keeps the lifecycle
entry-point (config loading, FastMCP construction, stdio runner, CLI
entry).

The runner :func:`_run_http` accepts ``build_fastmcp`` as an injected
callable so this module never imports back into :mod:`server` —
keeping the import graph one-way (``server -> _http`` only).

All names in this module are private (leading underscore), consumed
only by :mod:`server` via direct attribute access.
"""

from __future__ import annotations

import ipaddress
import logging
import secrets
import socket
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from pydantic import SecretStr
from starlette.middleware import Middleware
from starlette.types import ASGIApp

from deephaven_mcp._exceptions import DaemonAlreadyPublishedError
from deephaven_mcp._health import HEALTH_PATH
from deephaven_mcp._processes import ProcessIdentity
from deephaven_mcp.auth.middleware import PSK_HEADER_NAME, PSKMiddleware
from deephaven_mcp.config import harden_private_dir, resolve_runtime_dir
from deephaven_mcp.daemon_registry import DaemonDirectory, DaemonRegistryEntry

from ._idle import ActivityMiddleware, IdleTimer, IdleWatcher
from ._lifespan import LifespanContext
from .config import ConfigTree, DaemonConfig, ServerConfig

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP transport: helpers
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
        # An empty ``resolved`` set must not pass: ``all([])`` is
        # ``True``, which would treat an unresolvable-after-filtering
        # host as loopback. The safe default is to refuse to bind.
        return bool(resolved) and all(_addr_is_loopback(addr) for addr in resolved)
    return ip.is_loopback


def _resolve_psk_or_exit(
    cli_psk: str | None,
    server_cfg: ServerConfig,
    config_dir: Path,
) -> str:
    """Resolve the HTTP-transport PSK with CLI > JSON precedence.

    Args:
        cli_psk (str | None): Value of the ``--psk`` CLI flag, or
            ``None`` when the flag was not supplied.
        server_cfg (ServerConfig): The validated server config (the
            ``server`` block extracted from the
            :class:`ConfigTree` returned by the config loader).
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
        _LOGGER.debug(
            "[mcp_systems_server._http:_resolve_psk_or_exit] "
            "Using PSK from --psk CLI flag"
        )
        return cli_psk
    if server_cfg.psk is not None and server_cfg.psk.get_secret_value():
        _LOGGER.debug(
            "[mcp_systems_server._http:_resolve_psk_or_exit] "
            "Using PSK from server.json"
        )
        return server_cfg.psk.get_secret_value()
    _LOGGER.error(
        "[mcp_systems_server._http:_resolve_psk_or_exit] HTTP transport requires a PSK. "
        f"Set --psk or configure 'psk' in {config_dir}/server.json. "
        "Use --transport stdio to skip auth."
    )
    sys.exit(1)


def _generate_daemon_psk() -> str:
    """Return a fresh random PSK suitable for the daemon HTTP transport.

    Uses :func:`secrets.token_urlsafe` with 32 bytes of entropy,
    producing a ~43-character URL-safe base64 string. Comfortably
    above :data:`PSKMiddleware.MINIMUM_PSK_LENGTH` so the gate
    refuses to construct only when the project changes that floor.
    """
    return secrets.token_urlsafe(32)


def _acquire_loopback_socket() -> socket.socket:
    """Bind and listen on an IPv4 loopback socket on a kernel-chosen port.

    Used by :func:`_plan_daemon` to discover the bound port
    *before* uvicorn begins serving, so the daemon registry can be
    published with the correct port without racing the first
    client request. Uvicorn inherits the descriptor via
    ``Config(fd=sock.fileno())``.

    The socket is put into the listening state here, not just bound.
    The daemon publishes ``daemon.json`` before :meth:`uvicorn.Server.run`
    reaches its own ``listen``, and the CLI connects the moment the
    registry shows a live entry; a merely-bound socket would refuse
    those connections (``ECONNREFUSED``) until uvicorn caught up.
    Listening up front lets the kernel complete the handshake and
    queue the request, which uvicorn services once it starts
    accepting. uvicorn re-``listen``-ing on the inherited descriptor
    is idempotent — the same handoff systemd socket activation relies
    on.

    Hardcoded to ``AF_INET`` / ``127.0.0.1`` / port ``0``: daemon
    mode is IPv4-loopback-only because :class:`DaemonRegistryEntry`
    types ``host`` as ``Literal["127.0.0.1"]`` and the CLI's MCP
    client builds ``http://{host}:{port}/mcp`` without IPv6
    bracketing.

    Returns:
        socket.socket: A bound *and listening* IPv4 stream socket.
            Caller must close it on any error path that does not hand
            the descriptor to uvicorn; uvicorn closes it on normal
            shutdown.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    return sock


def _build_http_app(
    server: FastMCP[LifespanContext],
    *,
    psk: str,
    activity_timer: IdleTimer | None,
) -> ASGIApp:
    """Assemble the streamable-HTTP ASGI app with PSK gate (and optional activity bump).

    ``streamable_http_app`` returns a fresh Starlette instance whose
    middleware stack has not yet been built. The PSK gate is inserted
    at index 0 so authentication runs before any other middleware
    (e.g. logging middleware that would otherwise see un-authed
    request bodies). The optional :class:`ActivityMiddleware` is
    inserted at index 1 so it runs *after* the gate — rejected /
    anonymous traffic must not reset the idle timer.

    Args:
        server (FastMCP[LifespanContext]): The FastMCP instance to
            host.
        psk (str): The non-empty PSK that :class:`PSKMiddleware` will
            require on every request.
        activity_timer (IdleTimer | None): When supplied, an
            :class:`ActivityMiddleware` is added that bumps this timer
            on every successful response. Pair with a lifespan-owned
            :func:`._idle.idle_watcher`.

    Returns:
        ASGIApp: The fully wired Starlette app suitable for uvicorn.
    """
    app = server.streamable_http_app()
    app.user_middleware.insert(
        0,
        Middleware(
            PSKMiddleware,
            expected_psk=psk,
            bypass_paths=(HEALTH_PATH,),
        ),
    )
    if activity_timer is not None:
        app.user_middleware.insert(
            1,
            Middleware(ActivityMiddleware, timer=activity_timer),
        )
        _LOGGER.debug(
            "[mcp_systems_server._http:_build_http_app] "
            "Inserted ActivityMiddleware after PSK gate"
        )
    return app


# ---------------------------------------------------------------------------
# HTTP transport: data shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BindSpec:
    """Resolved uvicorn binding for one streamable-HTTP run.

    Two modes coexist as a discriminated union: a *direct* bind
    (caller hands ``host`` + ``port`` to uvicorn) and a *pre-bound*
    handoff (caller has already bound a loopback socket and uvicorn
    inherits it via ``fd=``). Exactly one of these is meaningful per
    instance, encoded as ``sock is None`` (direct) vs ``sock is not
    None`` (handoff). The construction-time invariant is enforced by
    :meth:`__post_init__` so the runner can read ``to_uvicorn_kwargs``
    without re-checking.

    ``port`` is always populated, even in handoff mode: the daemon
    publishes the kernel-chosen port to ``daemon.json`` and reads it
    off the plan rather than the live socket, so the publish path is
    a single field access.

    Attributes:
        host (str | None): Bind address for direct mode; ``None``
            when ``sock`` is set.
        port (int): Bound or to-be-bound TCP port. In handoff mode
            this is ``sock.getsockname()[1]`` captured at planner
            time (before uvicorn starts).
        sock (socket.socket | None): A pre-bound IPv4 stream socket
            for handoff mode; ``None`` for direct mode.
    """

    host: str | None
    port: int
    sock: socket.socket | None

    def __post_init__(self) -> None:
        """Validate the discriminated-union invariant.

        Raises:
            ValueError: When neither or both of ``host`` and ``sock``
                are set. This is a developer error: the planners
                that construct :class:`_BindSpec` instances are
                expected to populate exactly one mode. Surfaced as
                ``ValueError`` rather than ``AssertionError`` so it
                cannot be silenced by ``python -O``.
        """
        if (self.host is None) == (self.sock is None):
            raise ValueError(
                "_BindSpec: exactly one of host or sock must be set "
                f"(host={self.host!r}, sock={self.sock!r})"
            )

    def to_uvicorn_kwargs(self) -> dict[str, Any]:
        """Return the uvicorn :class:`~uvicorn.Config` kwargs for this bind.

        Returns:
            dict[str, Any]: ``{"fd": sock.fileno()}`` when ``sock`` is
                set, else ``{"host": host, "port": port}``. Spread
                into :class:`uvicorn.Config` by the runner; uvicorn
                takes ownership of the file descriptor when
                supplied.
        """
        if self.sock is not None:
            return {"fd": self.sock.fileno()}
        return {"host": self.host, "port": self.port}

    def close_unhanded(self) -> None:
        """Close the pre-bound socket if uvicorn never took ownership.

        Called from the runner's ``except BaseException`` arm: if a
        failure occurred *before* :meth:`uvicorn.Server.run` started
        servicing the descriptor (e.g. registry write raised), the
        planner-owned socket would leak otherwise. No-op in direct
        mode (where the planner never bound a socket itself).

        After uvicorn starts the descriptor is its responsibility
        and the runner does not call this method on the success
        path.
        """
        if self.sock is None:
            return
        try:
            self.sock.close()
        except OSError:
            pass


@dataclass(frozen=True)
class _HttpRun:
    """Mode-resolved plan for one streamable-HTTP run.

    Produced by exactly one of the two planners
    (:func:`_plan_default`, :func:`_plan_daemon`) and
    consumed by :func:`_run_http`. Every field is the *result* of a
    mode-specific decision; the runner reads but never decides.
    Nullable / zero-valued fields encode optional features so the
    runner only needs to branch on feature presence, not mode:

    - ``idle_seconds == 0``: idle supervision is off.
    - ``daemon_handle is None``: registry publishing is off (which
      also implies ``daemon_process_name is None`` — the planners
      pair them).

    Attributes:
        multi_config (ConfigTree): Threaded into
            ``build_fastmcp`` for the lifespan and reused for
            the registry entry's ``config_dir``.
        server_name (str): FastMCP server name advertised in MCP
            handshakes. Sourced from ``ServerConfig.server_name``.
        psk (str): Non-empty PSK installed on the
            :class:`PSKMiddleware`.
        bind (_BindSpec): Resolved uvicorn bind (direct or handoff).
        idle_seconds (int): Idle window. ``0`` disables the watcher
            and the activity middleware.
        daemon_handle (DaemonDirectory | None): Hardened daemon
            directory handle for registry publish/delete; ``None``
            when this run does not publish a registry.
        daemon_process_name (str | None): Process name written into
            ``daemon.json`` for liveness checks. Always set
            alongside ``daemon_handle``; always ``None`` when
            ``daemon_handle`` is.
    """

    multi_config: ConfigTree
    server_name: str
    psk: str
    bind: _BindSpec
    idle_seconds: int
    daemon_handle: DaemonDirectory | None
    daemon_process_name: str | None

    def __post_init__(self) -> None:
        """Validate paired-field invariants.

        Raises:
            ValueError: When ``daemon_handle`` and
                ``daemon_process_name`` are not paired (both set or
                both ``None``). Developer error from a planner; see
                :meth:`_BindSpec.__post_init__` for the same
                rationale.
        """
        if (self.daemon_handle is None) != (self.daemon_process_name is None):
            raise ValueError(
                "_HttpRun: daemon_handle and daemon_process_name must "
                "be paired (both set or both None); got "
                f"daemon_handle={self.daemon_handle!r}, "
                f"daemon_process_name={self.daemon_process_name!r}"
            )


# ---------------------------------------------------------------------------
# HTTP transport: planners
# ---------------------------------------------------------------------------


def _plan_default(
    multi_config: ConfigTree,
    server_cfg: ServerConfig,
    *,
    cli_host: str | None,
    cli_port: int | None,
    cli_psk: str | None,
) -> _HttpRun:
    """Resolve default-mode HTTP policy into an :class:`_HttpRun` plan.

    Default-mode HTTP is the non-``--daemon`` case: a human (or
    systemd / supervisor) manages the server's lifecycle, supplies
    the bind address out-of-band (config, env, CLI flags), and is
    responsible for distributing the PSK to clients. There is no
    idle shutdown and no registry publishing.

    Args:
        multi_config (ConfigTree): The validated configuration
            tree; threaded into the plan for the lifespan and the
            ``config_dir`` reference.
        server_cfg (ServerConfig): Resolved ``server`` block (the
            caller substitutes :class:`ServerConfig` defaults when
            ``multi_config.server`` is ``None``).
        cli_host (str | None): Value of ``--host``; falls through to
            ``server_cfg.host`` when ``None``.
        cli_port (int | None): Value of ``--port``; falls through to
            ``server_cfg.port`` when ``None``.
        cli_psk (str | None): Value of ``--psk``; otherwise the PSK
            comes from ``server.json``.

    Returns:
        _HttpRun: A plan with ``idle_seconds=0`` and
            ``daemon_handle=None`` so the runner skips both
            optional features. ``bind`` is a direct
            ``_BindSpec(host, port, sock=None)``.

    Raises:
        SystemExit: Exit code 2 if the resolved host fails the
            loopback check; exit code 1 if no PSK is available
            (delegated to :func:`_resolve_psk_or_exit`).
    """
    host = cli_host if cli_host is not None else server_cfg.host
    port = cli_port if cli_port is not None else server_cfg.port
    if not _is_loopback_host(host):
        _LOGGER.error(
            f"[mcp_systems_server._http:_plan_default] HTTP transport refuses "
            f"non-loopback host {host!r}. Bind to 127.0.0.1, ::1, or 'localhost'."
        )
        sys.exit(2)
    psk = _resolve_psk_or_exit(cli_psk, server_cfg, multi_config.config_dir)
    _LOGGER.debug(
        f"[mcp_systems_server._http:_plan_default] Resolved default HTTP "
        f"plan: bind={host}:{port}, server_name={server_cfg.server_name!r}"
    )
    return _HttpRun(
        multi_config=multi_config,
        server_name=server_cfg.server_name,
        psk=psk,
        bind=_BindSpec(host=host, port=port, sock=None),
        idle_seconds=0,
        daemon_handle=None,
        daemon_process_name=None,
    )


def _plan_daemon(
    multi_config: ConfigTree,
    server_cfg: ServerConfig,
    *,
    runtime_dir_override: Path | None,
    cli_psk: str | None,
) -> _HttpRun:
    """Resolve daemon-mode HTTP policy into an :class:`_HttpRun` plan.

    Daemon HTTP is a preset that bundles four mode-specific
    behaviours into the plan:

    - **PSK auto-generation**: ``--psk`` is honoured as a debug
      override; otherwise a fresh PSK is minted via
      :func:`_generate_daemon_psk`. ``ServerConfig.psk`` is
      intentionally ignored (the daemon publishes its PSK to
      ``daemon.json`` for the CLI to read).
    - **Loopback handoff**: a ``127.0.0.1:0`` socket is pre-bound and
      put into the listening state so the kernel-chosen port is known
      *and already accepting connections* before uvicorn starts. The
      registry is published with the correct port without racing the
      first client request, and a client that connects before uvicorn
      finishes starting is queued rather than refused.
    - **Idle shutdown**: the watcher is wired up when
      ``server.daemon.idle_shutdown_seconds > 0``; encoded in the
      plan as a positive ``idle_seconds`` field.
    - **Registry publishing**: a hardened :class:`DaemonDirectory`
      is created and threaded through as ``daemon_handle`` so the
      runner writes / deletes ``daemon.json``.

    Side effects: the planner takes ownership of an OS-level
    resource (the pre-bound socket); the runner's
    :meth:`_BindSpec.close_unhanded` reclaims it on failure paths,
    and uvicorn closes it on the success path.

    Args:
        multi_config (ConfigTree): The validated
            configuration tree; threaded into the plan and reused
            for the registry entry's ``config_dir``.
        server_cfg (ServerConfig): Resolved ``server`` block, which
            owns the daemon sub-block (idle window, process name).
        runtime_dir_override (Path | None): Operator (or test)
            override for the runtime directory. ``None`` falls
            through to ``$DH_MCP_DATA_DIR/runtime`` (or the
            platform-default user-data root's ``runtime``
            subdirectory).
        cli_psk (str | None): Value of ``--psk`` (debug override);
            ``None`` triggers :func:`_generate_daemon_psk`.

    Returns:
        _HttpRun: A plan with ``daemon_handle`` set,
            ``idle_seconds`` carrying the configured window
            (``0`` is allowed and disables supervision in the
            runner), and a handoff-mode ``_BindSpec``.
    """
    daemon_cfg: DaemonConfig = server_cfg.daemon
    runtime_dir = resolve_runtime_dir(runtime_dir_override)
    handle = DaemonDirectory.for_runtime_dir(runtime_dir)
    harden_private_dir(handle.path)
    _LOGGER.debug(
        f"[mcp_systems_server._http:_plan_daemon] Hardened daemon directory "
        f"at {handle.path} (runtime_dir={runtime_dir})"
    )
    if cli_psk:
        psk = cli_psk
        _LOGGER.debug(
            "[mcp_systems_server._http:_plan_daemon] Using PSK from --psk "
            "override (debug)"
        )
    else:
        psk = _generate_daemon_psk()
        _LOGGER.debug(
            "[mcp_systems_server._http:_plan_daemon] Generated fresh daemon PSK"
        )
    sock = _acquire_loopback_socket()
    bound_port = sock.getsockname()[1]
    _LOGGER.info(
        f"[mcp_systems_server._http:_plan_daemon] Pre-bound loopback socket on "
        f"127.0.0.1:{bound_port}; idle_shutdown_seconds="
        f"{daemon_cfg.idle_shutdown_seconds}"
    )
    return _HttpRun(
        multi_config=multi_config,
        server_name=server_cfg.server_name,
        psk=psk,
        bind=_BindSpec(host=None, port=int(bound_port), sock=sock),
        idle_seconds=daemon_cfg.idle_shutdown_seconds,
        daemon_handle=handle,
        daemon_process_name=daemon_cfg.process_name,
    )


# ---------------------------------------------------------------------------
# HTTP transport: unified runner
# ---------------------------------------------------------------------------


def _publish_daemon_registry(
    plan: _HttpRun,
    handle: DaemonDirectory,
    process_name: str,
) -> None:
    """Write the daemon registry entry derived from ``plan``.

    Acquires the :class:`DaemonDirectory.locked` session for the
    decision-and-publish window so a peer process cannot race a
    read-then-mutate against this publish. Inside the lock the
    helper re-reads the registry; if a still-live entry is found,
    it raises :class:`DaemonAlreadyPublishedError` rather than
    overwriting (the CLI's spawn lock prevents this in normal
    operation, so the error implies a non-CLI-spawned peer). On
    success it clears the ``daemon.starting`` marker the spawning
    CLI left behind.

    ``handle`` and ``process_name`` are required positional
    arguments (not read off the plan) so the caller is forced to
    perform the not-``None`` narrowing at the call site.

    Args:
        plan (_HttpRun): The fully-resolved plan; supplies the
            non-handle entry fields (port, PSK, config_dir,
            server_name) and the live process (PID + start time).
        handle (DaemonDirectory): The daemon directory handle to
            write the registry entry into. Caller narrows
            ``plan.daemon_handle`` to non-``None`` and passes it.
        process_name (str): The process name written into
            ``daemon.json``. Caller narrows
            ``plan.daemon_process_name`` to non-``None`` and passes
            it.

    Raises:
        DaemonAlreadyPublishedError: A peer daemon's registry
            entry is still live; refusing to overwrite.
    """
    # Capture pid + kernel create-time atomically. Storing both in
    # the registry lets the CLI detect PID reuse: a recycled pid
    # presents a newer create_time_ns than the recorded one.
    identity = ProcessIdentity.of_current_process()
    entry = DaemonRegistryEntry(
        pid=identity.pid,
        create_time_ns=identity.create_time_ns,
        process_name=process_name,
        host="127.0.0.1",
        port=plan.bind.port,
        psk=SecretStr(plan.psk),
        started_at=datetime.now(UTC),
        config_dir=plan.multi_config.config_dir,
        server_name=plan.server_name,
    )
    with handle.locked() as reg:
        # Defensive re-check: corrupt entries are treated as stale
        # (we cannot identity-check them, and the CLI's reset verb
        # is the operator-driven recovery). A *parseable* entry
        # whose recorded identity is still alive is a real peer
        # daemon — refuse rather than overwrite.
        try:
            existing = reg.read()
        except Exception as exc:  # noqa: BLE001 - any parse failure -> stale
            _LOGGER.warning(
                f"[mcp_systems_server._http:_publish_daemon_registry] Existing "
                f"registry at {handle.registry_path} is unreadable; treating "
                f"as stale and overwriting: {exc}"
            )
            existing = None
        if existing is not None and existing.is_live():
            raise DaemonAlreadyPublishedError(
                f"Refusing to publish daemon registry: a live daemon is "
                f"already registered (pid={existing.pid}, "
                f"port={existing.port}). The CLI's spawn lock should "
                f"prevent this; was the daemon started outside "
                f"`dh-mcp daemon start`?"
            )
        reg.write(entry)
        # The spawning CLI wrote a ``daemon.starting`` marker before
        # spawning us; clear it now that our entry is live so a peer
        # CLI sees the published daemon rather than a stale marker.
        reg.clear_start_marker()
    _LOGGER.debug(
        f"[mcp_systems_server._http:_publish_daemon_registry] Published registry "
        f"entry at {handle.registry_path} "
        f"(pid={entry.pid}, port={plan.bind.port})"
    )


def _unpublish_daemon_registry(plan: _HttpRun) -> None:
    """Delete the daemon registry entry, if one was published.

    Idempotent / safe to call from a ``finally`` arm: a no-op when
    ``plan.daemon_handle`` is ``None`` (default HTTP). Acquires
    the :class:`DaemonDirectory.locked` session so the delete
    cannot race a peer's read-then-mutate.

    Args:
        plan (_HttpRun): The same plan that was passed to
            :func:`_publish_daemon_registry`.
    """
    if plan.daemon_handle is None:
        return
    with plan.daemon_handle.locked() as reg:
        reg.delete()
    _LOGGER.debug(
        "[mcp_systems_server._http:_unpublish_daemon_registry] "
        "Deleted daemon registry entry"
    )


def _log_http_started(plan: _HttpRun) -> None:
    """Emit the started-banner log line appropriate for the plan's mode.

    Daemon plans log the registry path and idle window; operator
    plans log the bind host/port and the PSK header name. The
    conditional is on ``plan.daemon_handle``, which is the
    registry-publishing toggle.

    Args:
        plan (_HttpRun): The plan that was just successfully wired.
            Called after :func:`_publish_daemon_registry` returns
            (when applicable) but before :meth:`uvicorn.Server.run`.
    """
    if plan.daemon_handle is not None:
        _LOGGER.info(
            f"[mcp_systems_server._http:_log_http_started] Daemon listening on "
            f"127.0.0.1:{plan.bind.port}; "
            f"idle_shutdown_seconds={plan.idle_seconds}"
        )
    else:
        _LOGGER.info(
            f"[mcp_systems_server._http:_log_http_started] Starting HTTP transport "
            f"on {plan.bind.host}:{plan.bind.port} "
            f"(PSK gate via {PSK_HEADER_NAME!r})"
        )


def _run_http(
    plan: _HttpRun,
    build_fastmcp: Callable[..., FastMCP[LifespanContext]],
) -> None:
    """Execute an :class:`_HttpRun` plan: streamable-HTTP under uvicorn.

    Mode-agnostic. Every mode-specific decision has already been
    resolved by the planner and is read off ``plan``; the runner's
    only conditionals are *feature toggles* (``idle_seconds > 0``,
    ``daemon_handle is not None``), not mode flags.

    Phases (top-to-bottom):

    1. **Idle wiring** (no-op when ``plan.idle_seconds == 0``).
       Construct the :class:`IdleTimer` and, when supervision is
       on, an :class:`IdleWatcher` whose ``exit_fn`` flips
       :attr:`uvicorn.Server.should_exit`. The forward-declared
       ``uvicorn_server`` resolves to the live server at call time
       via Python closure-by-reference.
    2. **FastMCP + ASGI app**. The watcher (or ``None``) is
       threaded into the lifespan; the activity middleware is
       inserted only when supervision is on.
    3. **Uvicorn config**. Bind kwargs come from
       :meth:`_BindSpec.to_uvicorn_kwargs` so the runner stays
       agnostic to direct-vs-handoff bind.
    4. **Optional registry publish** (``plan.daemon_handle is not
       None``). :func:`_publish_daemon_registry` writes
       ``daemon.json`` *after* :class:`uvicorn.Server` is
       constructed but *before* :meth:`run`, so the CLI cannot
       observe a running daemon without a registry file.
    5. **Run**. The ``finally`` arm calls
       :func:`_unpublish_daemon_registry` on every exit path
       (clean or exception); it is a no-op for default plans.
    6. **Cleanup**. The outer ``except BaseException`` calls
       :meth:`_BindSpec.close_unhanded` so a pre-bound socket that
       uvicorn never took ownership of is released.

    Args:
        plan (_HttpRun): The fully-resolved plan from one of the
            two planners. Read-only; the runner mutates nothing on
            the plan itself.
        build_fastmcp (Callable[..., FastMCP[LifespanContext]]):
            Factory that constructs the FastMCP instance with
            lifespan + tools + health route. Injected as a
            parameter (rather than imported) so this runner has
            no upward import to :mod:`server`. Must accept
            ``(multi_config, server_name, *, idle)``.
    """
    # 1. Idle wiring (no-op when idle_seconds == 0)
    timer = IdleTimer(plan.idle_seconds)
    idle_enabled = plan.idle_seconds > 0
    _LOGGER.info(
        f"[mcp_systems_server._http:_run_http] Idle supervision "
        f"{'enabled' if idle_enabled else 'disabled'} "
        f"(idle_seconds={plan.idle_seconds})"
    )

    # ``uvicorn_server`` is bound only after the FastMCP instance and
    # ASGI app exist, but the idle-watcher callback needs to be passed
    # to ``build_fastmcp`` before that. Forward-declare and rely on
    # Python closure-by-reference: the ``_exit_fn`` body reads
    # ``uvicorn_server`` at call time, by which point the reassignment
    # below has taken effect.
    uvicorn_server: uvicorn.Server | None = None

    def _exit_fn() -> None:
        if uvicorn_server is not None:
            uvicorn_server.should_exit = True

    idle = IdleWatcher(timer=timer, exit_fn=_exit_fn) if idle_enabled else None

    # The ``try`` covers every step from here on: ``build_fastmcp``,
    # ``_build_http_app``, and ``uvicorn.Config(...)`` can all raise
    # before uvicorn takes ownership of the planner's pre-bound
    # socket, so the cleanup arm must wrap them too.
    try:
        # 2. FastMCP + ASGI app
        server = build_fastmcp(plan.multi_config, plan.server_name, idle=idle)
        app = _build_http_app(
            server,
            psk=plan.psk,
            activity_timer=timer if idle_enabled else None,
        )

        # 3. Uvicorn config (direct host/port or fd handoff; plan.bind owns the choice)
        config = uvicorn.Config(
            app=app,
            log_level="info",
            loop="asyncio",
            **plan.bind.to_uvicorn_kwargs(),
        )
        uvicorn_server = uvicorn.Server(config)

        # 4. Optional registry publish. ``_HttpRun.__post_init__``
        # pairs ``daemon_handle`` and ``daemon_process_name``;
        # narrow both at the call site so the helper body is free
        # of defensive guards and mypy stays happy.
        if plan.daemon_handle is not None and plan.daemon_process_name is not None:
            _publish_daemon_registry(plan, plan.daemon_handle, plan.daemon_process_name)
        _log_http_started(plan)

        # 5. Run
        try:
            uvicorn_server.run()
        finally:
            _unpublish_daemon_registry(plan)
            _LOGGER.info("[mcp_systems_server._http:_run_http] HTTP transport stopped")
    except BaseException:
        # 6. Cleanup: release a pre-bound socket if uvicorn never took it.
        plan.bind.close_unhanded()
        raise
