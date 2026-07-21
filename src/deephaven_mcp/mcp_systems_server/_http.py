"""Streamable-HTTP transport machinery for the systems server.

Provides the bind / PSK helpers, the loopback-restriction check, the
ASGI app builder, the resolved-plan dataclasses, the two policy
planners, and the unified runner. :func:`_run_http` receives the
already-built :class:`FastMCP` instance, so this module depends on
neither :mod:`server` nor the tool surface.
"""

from __future__ import annotations

import ipaddress
import logging
import secrets
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from pydantic import SecretStr
from starlette.applications import Starlette
from starlette.middleware import Middleware

from deephaven_mcp._exceptions import DaemonAlreadyPublishedError
from deephaven_mcp._health import HEALTH_PATH
from deephaven_mcp._processes import ProcessIdentity
from deephaven_mcp.auth.middleware import PSK_HEADER_NAME, PSKMiddleware
from deephaven_mcp.config import harden_private_dir
from deephaven_mcp.config.schema import DaemonProcessConfig, ServerConfig
from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.daemon_registry import (
    DaemonBuildIdentity,
    DaemonDirectory,
    DaemonRegistryEntry,
)

from ._idle import ActivityMiddleware, IdleTimer, IdleWatcher
from ._lifespan import LifespanContext, ProcessResources, process_lifespan

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
            # Scoped IPv6 link-locals (e.g. fe80::1%en0) raise here; treat as
            # non-loopback so the caller refuses to bind.
            return False

    if host.lower() == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            # getaddrinfo's sockaddr is typed str | int; keep only str (IP) families.
            resolved = {
                info[4][0]
                for info in socket.getaddrinfo(host, None)
                if isinstance(info[4][0], str)
            }
        except socket.gaierror:
            return False
        # bool(resolved) guards the empty set: all([]) is True, which would
        # wrongly treat an unresolvable host as loopback.
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
    """Return a fresh random PSK for the daemon HTTP transport.

    Uses :func:`secrets.token_urlsafe` with 32 bytes of entropy (a
    ~43-character URL-safe string).
    """
    return secrets.token_urlsafe(32)


def _acquire_loopback_socket() -> socket.socket:
    """Bind and listen on an IPv4 loopback socket on a kernel-chosen port.

    Hardcoded to ``AF_INET`` / ``127.0.0.1`` / port ``0`` (daemon mode is
    IPv4-loopback-only). The socket is put into the listening state, not just
    bound, so connections that arrive before uvicorn starts are queued rather
    than refused; uvicorn inherits the descriptor via ``Config(fd=...)``.

    Returns:
        socket.socket: A bound and listening IPv4 stream socket. The caller
            must close it on any error path that does not hand the descriptor
            to uvicorn; uvicorn closes it on normal shutdown.
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
) -> Starlette:
    """Assemble the streamable-HTTP ASGI app with the PSK gate (and optional activity bump).

    Inserts :class:`PSKMiddleware` at index 0 (authentication runs before all
    other middleware) and, when ``activity_timer`` is supplied,
    :class:`ActivityMiddleware` at index 1 (after the gate, so rejected traffic
    does not reset the idle timer). Process-scoped lifecycle is wired separately
    by :func:`_install_process_lifespan`.

    Args:
        server (FastMCP[LifespanContext]): The FastMCP instance to host.
        psk (str): The non-empty PSK that :class:`PSKMiddleware` requires on
            every request.
        activity_timer (IdleTimer | None): When supplied, an
            :class:`ActivityMiddleware` bumps this timer on every successful
            response.

    Returns:
        Starlette: The middleware-wired app, ready for
            :func:`_install_process_lifespan` and uvicorn.
    """
    app: Starlette = server.streamable_http_app()
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


def _install_process_lifespan(
    app: Starlette,
    *,
    multi_config: ConfigTree,
    idle: IdleWatcher | None,
    holder: ProcessResources,
    runtime_dir: Path,
) -> None:
    """Wrap the app's lifespan with the process-scoped subsystem lifecycle.

    The Starlette app lifespan runs the streamable-HTTP session manager once
    per process. This wraps it with :func:`._lifespan.process_lifespan` so the
    process-scoped subsystems are built once around the session manager (and
    shared across every MCP session via ``holder``), entering before the
    session manager and exiting after it.

    Args:
        app (Starlette): The app from :func:`_build_http_app`.
        multi_config (ConfigTree): Pre-validated configuration passed to
            :func:`._lifespan.process_lifespan`.
        idle (IdleWatcher | None): Unstarted watcher controlling idle
            shutdown. Pass ``None`` to disable supervision.
        holder (ProcessResources): Holder populated by
            :func:`._lifespan.process_lifespan` and read by the per-session
            lifespan.
        runtime_dir (Path): Resolved runtime directory passed to
            :func:`._lifespan.process_lifespan` for the instance tracker.
    """
    session_manager_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def _process_scoped_lifespan(app_: Starlette) -> AsyncIterator[None]:
        async with process_lifespan(
            multi_config, idle=idle, holder=holder, runtime_dir=runtime_dir
        ):
            async with session_manager_lifespan(app_):
                yield

    app.router.lifespan_context = _process_scoped_lifespan


# ---------------------------------------------------------------------------
# HTTP transport: data shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BindSpec:
    """Resolved uvicorn binding for one streamable-HTTP run.

    A discriminated union of two modes: a *direct* bind (``host`` + ``port``
    handed to uvicorn, ``sock is None``) and a *pre-bound* handoff (a loopback
    socket uvicorn inherits via ``fd=``, ``sock is not None``). Exactly one
    mode is set, enforced by :meth:`__post_init__`. ``port`` is always
    populated, including handoff mode (the daemon publishes it to
    ``daemon.json``).
    """

    host: str | None
    """Bind address for direct mode; ``None`` when ``sock`` is set."""

    port: int
    """Bound or to-be-bound TCP port. In handoff mode this is
    ``sock.getsockname()[1]`` captured at planner time (before uvicorn
    starts)."""

    sock: socket.socket | None
    """A pre-bound IPv4 stream socket for handoff mode; ``None`` for
    direct mode."""

    def __post_init__(self) -> None:
        """Validate that exactly one of ``host`` or ``sock`` is set.

        Raises:
            ValueError: When neither or both of ``host`` and ``sock`` are set.
        """
        if (self.host is None) == (self.sock is None):
            raise ValueError(
                "_BindSpec: exactly one of host or sock must be set "
                f"(host={self.host!r}, sock={self.sock!r})"
            )

    def to_uvicorn_kwargs(self) -> dict[str, Any]:
        """Return the uvicorn :class:`~uvicorn.Config` kwargs for this bind.

        Returns:
            dict[str, Any]: ``{"fd": sock.fileno()}`` when ``sock`` is set
                (uvicorn takes ownership of the descriptor), else
                ``{"host": host, "port": port}``.
        """
        if self.sock is not None:
            return {"fd": self.sock.fileno()}
        return {"host": self.host, "port": self.port}

    def close_unhanded(self) -> None:
        """Close the pre-bound socket if uvicorn never took ownership.

        No-op in direct mode (no socket was bound). Called from the runner's
        failure path to release a handoff socket that uvicorn never started
        servicing.
        """
        if self.sock is None:
            return
        try:
            self.sock.close()
        except OSError:
            pass


@dataclass(frozen=True)
class _DaemonPublish:
    """Registry-publishing parameters for a daemon-mode run.

    Present on :attr:`_HttpRun.daemon` only when the run publishes a
    ``daemon.json`` registry entry; ``None`` otherwise.
    """

    handle: DaemonDirectory
    """Hardened daemon directory handle for registry publish/delete."""

    process_name: str
    """Process name written into ``daemon.json`` for liveness checks."""


@dataclass(frozen=True)
class _HttpRun:
    """Mode-resolved plan for one streamable-HTTP run.

    Produced by one of the two planners (:func:`_plan_default`,
    :func:`_plan_daemon`) and consumed by :func:`_run_http`. Optional
    features are encoded so the runner branches on feature presence, not
    mode: ``idle_seconds == 0`` disables idle supervision, and
    ``daemon is None`` disables registry publishing.
    """

    multi_config: ConfigTree
    """Threaded into ``_install_process_lifespan`` for the process-scoped
    lifespan, and reused for the registry entry's ``config_dir``."""

    runtime_dir: Path
    """Resolved runtime directory, threaded into the process-scoped lifespan
    so the instance tracker writes under its ``instances`` subdirectory."""

    server_name: str
    """FastMCP server name advertised in MCP handshakes. Sourced from
    ``ServerConfig.server_name``."""

    psk: str
    """Non-empty PSK installed on the :class:`PSKMiddleware`."""

    bind: _BindSpec
    """Resolved uvicorn bind (direct or handoff)."""

    idle_seconds: int
    """Idle window. ``0`` disables the watcher and the activity
    middleware."""

    daemon: _DaemonPublish | None
    """Registry-publishing parameters; ``None`` when this run does not
    publish a registry."""


# ---------------------------------------------------------------------------
# HTTP transport: planners
# ---------------------------------------------------------------------------


def _plan_default(
    multi_config: ConfigTree,
    server_cfg: ServerConfig,
    *,
    runtime_dir: Path,
    cli_host: str | None,
    cli_port: int | None,
    cli_psk: str | None,
) -> _HttpRun:
    """Resolve default-mode HTTP policy into an :class:`_HttpRun` plan.

    Default mode is the non-``--daemon`` case: the bind address comes from
    config / CLI flags, the PSK from ``--psk`` or ``server.json``, and there
    is no idle shutdown or registry publishing.

    Args:
        multi_config (ConfigTree): The validated configuration tree; threaded
            into the plan for the lifespan and the ``config_dir`` reference.
        server_cfg (ServerConfig): Resolved ``server`` block.
        runtime_dir (Path): Resolved runtime directory, carried on the plan for
            the instance tracker.
        cli_host (str | None): Value of ``--host``; falls through to
            ``server_cfg.host`` when ``None``.
        cli_port (int | None): Value of ``--port``; falls through to
            ``server_cfg.port`` when ``None``.
        cli_psk (str | None): Value of ``--psk``; otherwise the PSK comes from
            ``server.json``.

    Returns:
        _HttpRun: A plan with idle supervision and registry publishing off and
            a direct-mode ``bind``.

    Raises:
        SystemExit: Exit code 2 if the resolved host fails the loopback check;
            exit code 1 if no PSK is available (via
            :func:`_resolve_psk_or_exit`).
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
        runtime_dir=runtime_dir,
        server_name=server_cfg.server_name,
        psk=psk,
        bind=_BindSpec(host=host, port=port, sock=None),
        idle_seconds=0,
        daemon=None,
    )


def _plan_daemon(
    multi_config: ConfigTree,
    server_cfg: ServerConfig,
    *,
    runtime_dir: Path,
    cli_psk: str | None,
) -> _HttpRun:
    """Resolve daemon-mode HTTP policy into an :class:`_HttpRun` plan.

    Daemon HTTP is a preset that:

    - **Auto-generates the PSK**: ``--psk`` is honored as a debug override;
      otherwise a fresh PSK is minted via :func:`_generate_daemon_psk`.
      ``ServerConfig.psk`` is ignored.
    - **Pre-binds a loopback socket** (``127.0.0.1:0``, listening) so the
      kernel-chosen port is known before uvicorn starts (handoff bind).
    - **Enables idle shutdown** when ``server.daemon.idle_shutdown_seconds >
      0`` (carried as ``idle_seconds``).
    - **Publishes a registry**: a hardened :class:`DaemonDirectory` so the
      runner writes / deletes ``daemon.json``.

    Args:
        multi_config (ConfigTree): The validated configuration tree; threaded
            into the plan and reused for the registry entry's ``config_dir``.
        server_cfg (ServerConfig): Resolved ``server`` block, which owns the
            daemon sub-block (idle window, process name).
        runtime_dir (Path): Resolved runtime directory. The daemon registry,
            lock, and log live in its ``daemon`` subdirectory.
        cli_psk (str | None): Value of ``--psk`` (debug override); ``None``
            triggers :func:`_generate_daemon_psk`.

    Returns:
        _HttpRun: A plan with registry publishing on, ``idle_seconds`` carrying
            the configured window (``0`` disables supervision), and a
            handoff-mode ``bind``.
    """
    daemon_cfg: DaemonProcessConfig = server_cfg.daemon
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
        runtime_dir=runtime_dir,
        server_name=server_cfg.server_name,
        psk=psk,
        bind=_BindSpec(host=None, port=int(bound_port), sock=sock),
        idle_seconds=daemon_cfg.idle_shutdown_seconds,
        daemon=_DaemonPublish(handle=handle, process_name=daemon_cfg.process_name),
    )


# ---------------------------------------------------------------------------
# HTTP transport: unified runner
# ---------------------------------------------------------------------------


def _publish_daemon_registry(
    plan: _HttpRun,
    daemon: _DaemonPublish,
) -> None:
    """Write the daemon registry entry derived from ``plan``.

    Acquires the :class:`DaemonDirectory.locked` session so a peer process
    cannot race a read-then-mutate against this publish. Inside the lock it
    re-reads the registry: a still-live entry raises
    :class:`DaemonAlreadyPublishedError` rather than being overwritten; on
    success it clears the ``daemon.starting`` marker the spawning CLI left.

    Args:
        plan (_HttpRun): The fully-resolved plan; supplies the entry fields
            (port, PSK, config_dir, server_name) and the live process
            (PID + start time).
        daemon (_DaemonPublish): The directory handle and process name to
            write into ``daemon.json``.

    Raises:
        DaemonAlreadyPublishedError: A peer daemon's registry entry is still
            live; refusing to overwrite.
    """
    # Storing pid + kernel create-time lets the CLI detect PID reuse: a
    # recycled pid presents a newer create_time_ns than the recorded one.
    identity = ProcessIdentity.of_current_process()
    entry = DaemonRegistryEntry(
        pid=identity.pid,
        create_time_ns=identity.create_time_ns,
        process_name=daemon.process_name,
        host="127.0.0.1",
        port=plan.bind.port,
        psk=SecretStr(plan.psk),
        started_at=datetime.now(UTC),
        config_dir=plan.multi_config.config_dir,
        server_name=plan.server_name,
        build_identity=DaemonBuildIdentity.current(),
    )
    with daemon.handle.locked() as reg:
        # A parseable entry whose recorded identity is still alive is a real
        # peer daemon — refuse rather than overwrite. Corrupt entries are
        # treated as stale (the CLI's reset verb is the recovery path).
        try:
            existing = reg.read()
        except Exception as exc:  # noqa: BLE001 - any parse failure -> stale
            _LOGGER.warning(
                f"[mcp_systems_server._http:_publish_daemon_registry] Existing "
                f"registry at {daemon.handle.registry_path} is unreadable; "
                f"treating as stale and overwriting: {exc}"
            )
            existing = None
        if existing is not None and existing.is_live():
            raise DaemonAlreadyPublishedError(
                f"Refusing to publish daemon registry: a live daemon is "
                f"already registered (pid={existing.pid}, "
                f"port={existing.port}). The CLI's spawn lock should "
                f"prevent this; was the daemon started outside "
                f"`dhcli daemon start`?"
            )
        reg.write(entry)
        # Clear the daemon.starting marker the spawning CLI left, so a peer
        # CLI sees the published daemon rather than a stale marker.
        reg.clear_start_marker()
    _LOGGER.debug(
        f"[mcp_systems_server._http:_publish_daemon_registry] Published registry "
        f"entry at {daemon.handle.registry_path} "
        f"(pid={entry.pid}, port={plan.bind.port})"
    )


def _unpublish_daemon_registry(plan: _HttpRun) -> None:
    """Delete the daemon registry entry, if one was published.

    Idempotent and safe to call from a ``finally`` arm: a no-op when
    ``plan.daemon`` is ``None`` (default HTTP). Acquires the
    :class:`DaemonDirectory.locked` session so the delete cannot race a
    peer's read-then-mutate.

    Args:
        plan (_HttpRun): The same plan that was passed to
            :func:`_publish_daemon_registry`.
    """
    if plan.daemon is None:
        return
    with plan.daemon.handle.locked() as reg:
        reg.delete()
    _LOGGER.debug(
        "[mcp_systems_server._http:_unpublish_daemon_registry] "
        "Deleted daemon registry entry"
    )


def _log_http_started(plan: _HttpRun) -> None:
    """Emit the started-banner log line for the plan's mode.

    Daemon plans log the bound port and idle window; default plans log the
    bind host/port and the PSK header name.

    Args:
        plan (_HttpRun): The plan that was just successfully wired.
    """
    if plan.daemon is not None:
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
    server: FastMCP[LifespanContext],
    holder: ProcessResources,
) -> None:
    """Execute an :class:`_HttpRun` plan: streamable-HTTP under uvicorn.

    Mode-agnostic — every mode-specific decision is read off ``plan``; the
    runner's only conditionals are feature toggles (``idle_seconds > 0``,
    registry publishing). The numbered steps in the body wire idle
    supervision, the ASGI app and process-scoped lifespan, the uvicorn
    config, the optional registry publish, the run, and socket cleanup.

    Args:
        plan (_HttpRun): The fully-resolved plan from one of the two planners.
            Read-only.
        server (FastMCP[LifespanContext]): The built server. Its per-session
            lifespan reads ``holder``, so the caller must build it with the
            same ``holder`` passed here.
        holder (ProcessResources): Holder shared between ``server``'s
            per-session lifespan and the process-scoped lifespan; populated by
            :func:`._lifespan.process_lifespan`.
    """
    # 1. Idle wiring (no-op when idle_seconds == 0)
    timer = IdleTimer(plan.idle_seconds)
    idle_enabled = plan.idle_seconds > 0
    _LOGGER.info(
        f"[mcp_systems_server._http:_run_http] Idle supervision "
        f"{'enabled' if idle_enabled else 'disabled'} "
        f"(idle_seconds={plan.idle_seconds})"
    )

    # uvicorn_server is created below, after the app exists, but _exit_fn must
    # be wired into the watcher before that. Forward-declare and rely on
    # closure-by-reference: _exit_fn reads uvicorn_server at call time.
    uvicorn_server: uvicorn.Server | None = None

    def _exit_fn() -> None:
        if uvicorn_server is not None:
            uvicorn_server.should_exit = True

    idle = IdleWatcher(timer=timer, exit_fn=_exit_fn) if idle_enabled else None

    # build_fastmcp, _build_http_app, and uvicorn.Config can all raise before
    # uvicorn takes ownership of the pre-bound socket, so the cleanup arm
    # (close_unhanded) must wrap them.
    try:
        # 2. ASGI app + process-scoped lifespan. ``server``'s per-session
        # lifespan reads ``holder``; ``_install_process_lifespan`` populates
        # ``holder`` once per process.
        app = _build_http_app(
            server,
            psk=plan.psk,
            activity_timer=timer if idle_enabled else None,
        )
        _install_process_lifespan(
            app,
            multi_config=plan.multi_config,
            idle=idle,
            holder=holder,
            runtime_dir=plan.runtime_dir,
        )

        # 3. Uvicorn config (direct host/port or fd handoff; plan.bind owns the choice)
        config = uvicorn.Config(
            app=app,
            log_level="info",
            loop="asyncio",
            **plan.bind.to_uvicorn_kwargs(),
        )
        uvicorn_server = uvicorn.Server(config)

        # 4. Optional registry publish (daemon mode only). Published after the
        # uvicorn server is constructed but before run(), so the CLI cannot
        # observe a running daemon without a registry file.
        if plan.daemon is not None:
            _publish_daemon_registry(plan, plan.daemon)
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
