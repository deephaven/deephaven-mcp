"""Tests for ``deephaven_mcp.mcp_systems_server._http``.

Covers the streamable-HTTP transport machinery split out from
``server.py``:

- ``_is_loopback_host``: classification of literal IPs, hostname
  resolution, and unresolvable inputs.
- ``_resolve_psk_or_exit``: success, missing PSK, error remediations.
- ``_BindSpec``: discriminated-union invariant.
- ``_HttpRun`` / ``_DaemonPublish``: plan shape and optional daemon publish.
- ``_plan_default`` / ``_plan_daemon``: policy resolution
  into runnable plans.
- ``_publish_daemon_registry`` / ``_unpublish_daemon_registry`` /
  ``_log_http_started``: small runner helpers.
- ``_run_http``: unified streamable-HTTP runner.
- ``_acquire_loopback_socket``: planner-side socket factory.

The integration tests for ``main()`` that drive the HTTP path live in
``test_server.py`` (they exercise ``_command``'s dispatch into the
planners + runner end-to-end).
"""

from __future__ import annotations

import contextlib
import os
import socket
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import psutil
import pytest
from pydantic import SecretStr

from deephaven_mcp._exceptions import DaemonAlreadyPublishedError
from deephaven_mcp.config.schema import ServerConfig
from deephaven_mcp.daemon_registry import (
    DaemonDirectory,
    DaemonRegistryEntry,
    LockedRegistry,
)
from deephaven_mcp.mcp_systems_server import _http as http_module
from deephaven_mcp.mcp_systems_server._http import (
    _is_loopback_host,
    _resolve_psk_or_exit,
)
from deephaven_mcp.mcp_systems_server._idle import (
    ActivityMiddleware,
    IdleTimer,
    IdleWatcher,
)
from deephaven_mcp.mcp_systems_server._lifespan import ProcessResources


def _multi_config_with(
    server_cfg: ServerConfig | None, config_dir: Path | None = None
) -> MagicMock:
    """Build a ConfigTree mock with ``cfg.server`` and ``cfg.config_dir`` set.

    Mirrors the helper in ``test_server.py`` but is duplicated here
    so the two test files have no cross-dependencies.
    """
    multi = MagicMock()
    multi.server = server_cfg
    multi.config_dir = config_dir if config_dir is not None else Path("/tmp/cfg")
    return multi


# ---------------------------------------------------------------------------
# _is_loopback_host
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host", ["localhost", "LOCALHOST", "127.0.0.1", "127.5.6.7", "::1"]
)
def test_is_loopback_host_accepts_loopback(host):
    assert _is_loopback_host(host) is True


@pytest.mark.parametrize("host", ["10.0.0.1", "8.8.8.8", "2001:db8::1"])
def test_is_loopback_host_rejects_public_ips(host):
    assert _is_loopback_host(host) is False


def test_is_loopback_host_unresolvable_hostname_is_false():
    with patch.object(socket, "getaddrinfo", side_effect=socket.gaierror):
        assert _is_loopback_host("definitely-not-a-real-host") is False


def test_is_loopback_host_resolves_hostname_to_loopback():
    fake_resolution = [(socket.AF_INET, 0, 0, "", ("127.0.0.1", 0))]
    with patch.object(socket, "getaddrinfo", return_value=fake_resolution):
        assert _is_loopback_host("my-loopback-alias") is True


def test_is_loopback_host_mixed_resolution_is_false():
    fake_resolution = [
        (socket.AF_INET, 0, 0, "", ("127.0.0.1", 0)),
        (socket.AF_INET, 0, 0, "", ("8.8.8.8", 0)),
    ]
    with patch.object(socket, "getaddrinfo", return_value=fake_resolution):
        assert _is_loopback_host("dual-host") is False


def test_is_loopback_host_empty_resolution_is_false():
    """A resolution that yields no IP addresses must refuse to bind.

    ``all([])`` is ``True``; without the explicit ``bool(resolved)``
    guard this would fail *open* and treat the host as loopback. Here
    every entry carries an integer sockaddr (filtered out), leaving
    the set empty.
    """
    fake_resolution = [(socket.AF_INET, 0, 0, "", (42, 0))]
    with patch.object(socket, "getaddrinfo", return_value=fake_resolution):
        assert _is_loopback_host("opaque-host") is False


def test_is_loopback_host_unparseable_resolved_addr_is_false():
    """A resolved address string ``ip_address`` cannot parse is non-loopback.

    Defensive: if ``getaddrinfo`` ever yields a string the
    :func:`ipaddress.ip_address` classifier rejects with
    ``ValueError`` (the inner ``_addr_is_loopback`` except arm), the
    host must be treated as non-loopback so the caller refuses to
    bind rather than crash.
    """
    fake_resolution = [(socket.AF_INET6, 0, 0, "", ("not-a-valid-ip", 0, 0, 0))]
    with patch.object(socket, "getaddrinfo", return_value=fake_resolution):
        assert _is_loopback_host("opaque-resolved-host") is False


# ---------------------------------------------------------------------------
# _resolve_psk_or_exit
# ---------------------------------------------------------------------------


def test_resolve_psk_or_exit_cli_flag_wins(tmp_path):
    """CLI ``--psk`` takes precedence over ``server.json``'s ``psk`` field."""
    server_cfg = ServerConfig.model_validate({"psk": "from-server-json"})
    assert _resolve_psk_or_exit("from-cli", server_cfg, tmp_path) == "from-cli"


def test_resolve_psk_or_exit_uses_server_json_psk(tmp_path):
    server_cfg = ServerConfig.model_validate({"psk": "hunter2"})
    assert _resolve_psk_or_exit(None, server_cfg, tmp_path) == "hunter2"


def test_resolve_psk_or_exit_missing_psk_exits(tmp_path):
    """A :class:`ServerConfig` without ``psk`` causes the entry point to exit."""
    server_cfg = ServerConfig()
    with pytest.raises(SystemExit) as exc_info:
        _resolve_psk_or_exit(None, server_cfg, tmp_path)
    assert exc_info.value.code == 1


def test_resolve_psk_or_exit_message_names_remediations(
    tmp_path, caplog: pytest.LogCaptureFixture
):
    """Missing-PSK error message names ``--psk`` and ``--transport stdio``.

    Pins the operator-visible diagnostic so an operator who started
    HTTP without a PSK (the case the safeguard exists to catch) sees
    the two remediations they have: supply a PSK, or use stdio.
    """
    caplog.set_level("ERROR", logger="deephaven_mcp.mcp_systems_server._http")
    with pytest.raises(SystemExit):
        _resolve_psk_or_exit(None, ServerConfig(), tmp_path)
    msg = " ".join(rec.message for rec in caplog.records)
    assert "--psk" in msg
    assert "stdio" in msg


def test_resolve_psk_or_exit_treats_empty_cli_psk_as_missing(tmp_path):
    """``--psk \"\"`` (empty) falls through to server.json / error path.

    The current implementation uses a falsy check (``if cli_psk:``)
    so an empty string from argparse is treated as not-supplied. This
    test pins that behaviour: an empty CLI PSK with no server.json
    PSK still hits the error path (rule 4).
    """
    with pytest.raises(SystemExit):
        _resolve_psk_or_exit("", ServerConfig(), tmp_path)


def test_resolve_psk_or_exit_treats_empty_cli_psk_as_missing_with_server_json(
    tmp_path,
):
    """``--psk \"\"`` (empty) falls through to server.json's PSK.

    Empty CLI PSK is missing; server.json's PSK fills in.
    """
    server_cfg = ServerConfig.model_validate({"psk": "from-json"})
    assert _resolve_psk_or_exit("", server_cfg, tmp_path) == "from-json"


# ---------------------------------------------------------------------------
# _BindSpec
# ---------------------------------------------------------------------------


def test_bind_spec_to_uvicorn_kwargs_direct_mode():
    """Direct bind returns ``{host, port}`` for ``uvicorn.Config``."""
    spec = http_module._BindSpec(host="127.0.0.1", port=8000, sock=None)
    assert spec.to_uvicorn_kwargs() == {"host": "127.0.0.1", "port": 8000}


def test_bind_spec_to_uvicorn_kwargs_handoff_mode():
    """Handoff bind returns ``{fd}`` for ``uvicorn.Config``."""
    sock = http_module._acquire_loopback_socket()
    try:
        spec = http_module._BindSpec(host=None, port=sock.getsockname()[1], sock=sock)
        kwargs = spec.to_uvicorn_kwargs()
        assert kwargs == {"fd": sock.fileno()}
    finally:
        sock.close()


def test_bind_spec_close_unhanded_is_noop_for_direct():
    """``close_unhanded`` does nothing in direct-bind mode (no socket to release)."""
    spec = http_module._BindSpec(host="127.0.0.1", port=8000, sock=None)
    spec.close_unhanded()  # must not raise


def test_bind_spec_close_unhanded_closes_handoff_socket():
    """``close_unhanded`` releases a pre-bound socket that uvicorn never took."""
    sock = http_module._acquire_loopback_socket()
    spec = http_module._BindSpec(host=None, port=sock.getsockname()[1], sock=sock)
    spec.close_unhanded()
    # Closing a closed socket raises ``OSError`` (EBADF). The helper
    # already swallowed the close call above; verify the fd is closed
    # by attempting another close and observing the error.
    with pytest.raises(OSError):
        sock.getsockname()


def test_bind_spec_close_unhanded_swallows_oserror_on_already_closed():
    """``close_unhanded`` swallows ``OSError`` so failure paths cannot derail.

    Pins the inner ``except OSError: pass`` arm: if the planner-owned
    socket was already closed by some prior cleanup, the runner's
    failure-path call into ``close_unhanded`` must not re-raise and
    obscure the original exception.
    """
    # ``socket.close()`` on an already-closed fd is a no-op in
    # CPython, and real ``socket`` instances reject attribute
    # patching ("attribute is read-only"). Use a duck-typed mock
    # whose ``close`` raises ``OSError`` so the inner
    # ``except OSError`` arm runs unconditionally.
    fake_sock = MagicMock(spec=socket.socket)
    fake_sock.close.side_effect = OSError("simulated")
    spec = http_module._BindSpec(host=None, port=8000, sock=fake_sock)
    spec.close_unhanded()  # must not raise
    fake_sock.close.assert_called_once_with()


@pytest.mark.parametrize(
    "host, sock_factory, label",
    [
        (None, lambda: None, "neither set"),
        (
            "127.0.0.1",
            lambda: http_module._acquire_loopback_socket(),
            "both set",
        ),
    ],
)
def test_bind_spec_invariant_rejects_mixed_state(host, sock_factory, label):
    """Constructing with neither or both of host/sock raises ``ValueError``."""
    sock = sock_factory()
    try:
        with pytest.raises(ValueError, match="exactly one of host or sock"):
            http_module._BindSpec(host=host, port=8000, sock=sock)
    finally:
        if sock is not None:
            sock.close()


# ---------------------------------------------------------------------------
# _HttpRun
# ---------------------------------------------------------------------------


def test_http_run_daemon_bundles_handle_and_process_name():
    """``_DaemonPublish`` carries the handle and process name as a pair."""
    bind = http_module._BindSpec(host="127.0.0.1", port=8000, sock=None)
    handle = MagicMock()
    plan = http_module._HttpRun(
        multi_config=MagicMock(),
        runtime_dir=Path("/tmp/runtime"),
        server_name="srv",
        psk="x" * 32,
        bind=bind,
        idle_seconds=0,
        daemon=http_module._DaemonPublish(
            handle=handle, process_name="dh-mcp-systems-server"
        ),
    )
    assert plan.daemon is not None
    assert plan.daemon.handle is handle
    assert plan.daemon.process_name == "dh-mcp-systems-server"


def test_http_run_default_has_no_daemon():
    """A default-mode plan carries ``daemon=None`` (no registry publishing)."""
    bind = http_module._BindSpec(host="127.0.0.1", port=8000, sock=None)
    plan = http_module._HttpRun(
        multi_config=MagicMock(),
        runtime_dir=Path("/tmp/runtime"),
        server_name="srv",
        psk="x" * 32,
        bind=bind,
        idle_seconds=0,
        daemon=None,
    )
    assert plan.daemon is None


# ---------------------------------------------------------------------------
# _plan_default
# ---------------------------------------------------------------------------


def _operator_multi(server_cfg: ServerConfig, *, config_dir: Path) -> MagicMock:
    """Build a ConfigTree-shaped mock for planner tests."""
    return _multi_config_with(server_cfg, config_dir)


def test_plan_default_resolves_cli_overrides(tmp_path):
    """CLI flags override ``server.json`` field-by-field."""
    server_cfg = ServerConfig.model_validate(
        {"host": "::1", "port": 9000, "psk": "from-json"}
    )
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_default(
        multi,
        server_cfg,
        runtime_dir=tmp_path / "runtime",
        cli_host="127.0.0.1",
        cli_port=8765,
        cli_psk="from-cli",
    )
    assert plan.bind.host == "127.0.0.1"
    assert plan.bind.port == 8765
    assert plan.bind.sock is None
    assert plan.psk == "from-cli"
    assert plan.idle_seconds == 0
    assert plan.daemon is None
    assert plan.server_name == server_cfg.server_name
    assert plan.multi_config is multi
    assert plan.runtime_dir == tmp_path / "runtime"


def test_plan_default_falls_back_to_server_cfg(tmp_path):
    """Without CLI overrides, the plan reflects ``server.json``."""
    server_cfg = ServerConfig.model_validate(
        {"host": "127.0.0.1", "port": 9000, "psk": "from-json"}
    )
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_default(
        multi,
        server_cfg,
        runtime_dir=tmp_path / "runtime",
        cli_host=None,
        cli_port=None,
        cli_psk=None,
    )
    assert plan.bind.host == "127.0.0.1"
    assert plan.bind.port == 9000
    assert plan.psk == "from-json"


def test_plan_default_exits_on_non_loopback(tmp_path):
    """Non-loopback host fails the planner with ``SystemExit(2)``."""
    server_cfg = ServerConfig.model_validate({"psk": "x" * 32})
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    with (
        patch.object(http_module, "_is_loopback_host", return_value=False),
        pytest.raises(SystemExit) as exc_info,
    ):
        http_module._plan_default(
            multi,
            server_cfg,
            runtime_dir=tmp_path / "runtime",
            cli_host="0.0.0.0",
            cli_port=None,
            cli_psk=None,
        )
    assert exc_info.value.code == 2


def test_plan_default_exits_on_missing_psk(tmp_path):
    """No PSK in CLI or ``server.json`` fails the planner with ``SystemExit(1)``."""
    server_cfg = ServerConfig()  # default: psk=None
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    with pytest.raises(SystemExit) as exc_info:
        http_module._plan_default(
            multi,
            server_cfg,
            runtime_dir=tmp_path / "runtime",
            cli_host=None,
            cli_port=None,
            cli_psk=None,
        )
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# _plan_daemon
# ---------------------------------------------------------------------------


def test_plan_daemon_auto_generates_psk(tmp_path):
    """When ``cli_psk`` is empty, the planner mints one via ``_generate_daemon_psk``."""
    from deephaven_mcp.auth.middleware._psk import MINIMUM_PSK_LENGTH

    server_cfg = ServerConfig()
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_daemon(
        multi, server_cfg, runtime_dir=tmp_path, cli_psk=None
    )
    try:
        assert plan.psk
        assert len(plan.psk) >= MINIMUM_PSK_LENGTH
    finally:
        # The planner pre-bound a socket; release it so the test runner
        # does not leak file descriptors.
        plan.bind.close_unhanded()


def test_plan_daemon_uses_cli_psk_override(tmp_path):
    """``--psk SECRET`` is honoured as a debug override; auto-gen is skipped."""
    server_cfg = ServerConfig()
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_daemon(
        multi,
        server_cfg,
        runtime_dir=tmp_path,
        cli_psk="operator-explicit-secret",
    )
    try:
        assert plan.psk == "operator-explicit-secret"
    finally:
        plan.bind.close_unhanded()


def test_plan_daemon_ignores_server_json_psk(tmp_path):
    """``server.json:psk`` is intentionally ignored in daemon mode.

    Documented contract: the daemon publishes its PSK to ``daemon.json``
    so the CLI can read it; the operator-set ``server.json:psk`` is
    irrelevant. Auto-generation runs whenever ``--psk`` is not
    supplied, even if ``server.json:psk`` is set.
    """
    server_cfg = ServerConfig.model_validate({"psk": "from-server-json"})
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_daemon(
        multi, server_cfg, runtime_dir=tmp_path, cli_psk=None
    )
    try:
        assert plan.psk
        assert plan.psk != "from-server-json"
    finally:
        plan.bind.close_unhanded()


def test_plan_daemon_pre_binds_loopback_socket(tmp_path):
    """The plan carries a handoff-mode ``_BindSpec`` with the bound port."""
    server_cfg = ServerConfig()
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_daemon(
        multi, server_cfg, runtime_dir=tmp_path, cli_psk=None
    )
    try:
        assert plan.bind.host is None
        assert plan.bind.sock is not None
        assert plan.bind.port == plan.bind.sock.getsockname()[1]
        assert plan.bind.sock.family == socket.AF_INET
    finally:
        plan.bind.close_unhanded()


def test_plan_daemon_hardens_runtime_dir(tmp_path):
    """The planner hardens the daemon directory to ``0o700`` before returning."""
    server_cfg = ServerConfig()
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    captured: dict[str, object] = {}

    def fake_harden(path):
        captured["path"] = path

    with patch.object(http_module, "harden_private_dir", side_effect=fake_harden):
        plan = http_module._plan_daemon(
            multi, server_cfg, runtime_dir=tmp_path, cli_psk=None
        )
    try:
        assert plan.daemon is not None
        assert captured["path"] == plan.daemon.handle.path
    finally:
        plan.bind.close_unhanded()


def test_plan_daemon_threads_idle_seconds_from_daemon_cfg(tmp_path):
    """``server.daemon.idle_shutdown_seconds`` flows through to the plan."""
    server_cfg = ServerConfig.model_validate(
        {"daemon": {"idle_shutdown_seconds": 1234}}
    )
    multi = _operator_multi(server_cfg, config_dir=tmp_path / "cfg")
    plan = http_module._plan_daemon(
        multi, server_cfg, runtime_dir=tmp_path, cli_psk=None
    )
    try:
        assert plan.idle_seconds == 1234
        assert plan.daemon is not None
        assert plan.daemon.process_name == server_cfg.daemon.process_name
    finally:
        plan.bind.close_unhanded()


# ---------------------------------------------------------------------------
# Daemon-mode helpers
# ---------------------------------------------------------------------------


def test_generate_daemon_psk_is_unique_and_long_enough():
    a = http_module._generate_daemon_psk()
    b = http_module._generate_daemon_psk()
    assert a != b
    from deephaven_mcp.auth.middleware._psk import MINIMUM_PSK_LENGTH

    assert len(a) >= MINIMUM_PSK_LENGTH


# ---------------------------------------------------------------------------
# _run_http (unified runner)
# ---------------------------------------------------------------------------


def _operator_plan(*, psk: str = "secret", port: int = 9999) -> "http_module._HttpRun":
    """Build an operator-style ``_HttpRun`` for runner unit tests."""
    return http_module._HttpRun(
        multi_config=MagicMock(),
        runtime_dir=Path("/tmp/runtime"),
        server_name="srv",
        psk=psk,
        bind=http_module._BindSpec(host="127.0.0.1", port=port, sock=None),
        idle_seconds=0,
        daemon=None,
    )


def test_run_http_appends_psk_middleware_and_starts_uvicorn():
    """``_run_http`` mounts PSKMiddleware on the streamable-HTTP app then runs uvicorn."""
    fake_server = MagicMock()
    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_server.streamable_http_app = MagicMock(return_value=fake_app)

    fake_config = object()
    fake_uvicorn_server = MagicMock()

    with (
        patch.object(
            http_module.uvicorn, "Config", return_value=fake_config
        ) as mock_config_cls,
        patch.object(
            http_module.uvicorn, "Server", return_value=fake_uvicorn_server
        ) as mock_server_cls,
    ):
        http_module._run_http(
            _operator_plan(psk="secret", port=9999),
            fake_server,
            ProcessResources(),
        )

    from deephaven_mcp._health import HEALTH_PATH
    from deephaven_mcp.auth.middleware import PSKMiddleware

    assert len(fake_app.user_middleware) == 1
    mw = fake_app.user_middleware[0]
    assert mw.cls is PSKMiddleware
    assert mw.kwargs["expected_psk"] == "secret"
    assert mw.kwargs["bypass_paths"] == (HEALTH_PATH,)

    # Direct bind: uvicorn.Config invoked with host/port (no fd).
    mock_config_cls.assert_called_once()
    assert mock_config_cls.call_args.kwargs["host"] == "127.0.0.1"
    assert mock_config_cls.call_args.kwargs["port"] == 9999
    assert "fd" not in mock_config_cls.call_args.kwargs
    assert mock_config_cls.call_args.kwargs["app"] is fake_app
    mock_server_cls.assert_called_once_with(fake_config)
    fake_uvicorn_server.run.assert_called_once_with()


def test_run_http_inserts_psk_middleware_at_index_zero():
    """``_run_http`` inserts PSKMiddleware before any existing middleware."""
    fake_server = MagicMock()
    fake_app = MagicMock()
    sentinel_pre = MagicMock(name="pre_existing_middleware")
    fake_app.user_middleware = [sentinel_pre]
    fake_server.streamable_http_app = MagicMock(return_value=fake_app)

    with (
        patch.object(http_module.uvicorn, "Config", return_value=object()),
        patch.object(http_module.uvicorn, "Server", return_value=MagicMock()),
    ):
        http_module._run_http(_operator_plan(), fake_server, ProcessResources())

    from deephaven_mcp.auth.middleware import PSKMiddleware

    assert len(fake_app.user_middleware) == 2
    assert fake_app.user_middleware[0].cls is PSKMiddleware
    assert fake_app.user_middleware[1] is sentinel_pre


def test_run_http_no_registry_when_daemon_none():
    """Operator-style plan never calls registry write/delete."""
    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_fastmcp = MagicMock()
    fake_fastmcp.streamable_http_app.return_value = fake_app

    with (
        patch.object(http_module.uvicorn, "Config", return_value=MagicMock()),
        patch.object(http_module.uvicorn, "Server", return_value=MagicMock()),
        patch.object(LockedRegistry, "write") as mock_write,
        patch.object(LockedRegistry, "delete") as mock_delete,
    ):
        http_module._run_http(_operator_plan(), fake_fastmcp, ProcessResources())

    mock_write.assert_not_called()
    mock_delete.assert_not_called()


def _daemon_plan_for_test(
    server_cfg: ServerConfig, *, config_dir: Path, runtime_dir: Path
) -> "http_module._HttpRun":
    """Construct a daemon-style ``_HttpRun`` via the real planner.

    The planner is exercised end-to-end so that runner tests validate
    the *integrated* path the production code takes (planner →
    runner). The only tweak is that the caller selects
    ``runtime_dir`` so the test owns the daemon directory layout.
    """
    multi = _multi_config_with(server_cfg, config_dir)
    return http_module._plan_daemon(
        multi, server_cfg, runtime_dir=runtime_dir, cli_psk=None
    )


def test_run_http_writes_registry_then_deletes_on_exit(tmp_path):
    """Daemon-style plan publishes ``daemon.json`` then deletes on exit."""
    from deephaven_mcp.auth.middleware import PSKMiddleware
    from deephaven_mcp.mcp_systems_server._idle import (
        ActivityMiddleware,
        IdleTimer,
        IdleWatcher,
    )

    server_cfg = ServerConfig.model_validate(
        {"daemon": {"idle_shutdown_seconds": 3600}}
    )
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )

    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_fastmcp = MagicMock()
    fake_fastmcp.streamable_http_app.return_value = fake_app

    fake_uvicorn_server = MagicMock()
    captured_kwargs: dict[str, object] = {}

    # The idle watcher is threaded into ``_install_process_lifespan`` (which
    # wraps the app lifespan). Wrap the real installer to capture its kwargs;
    # ``_build_http_app`` runs real so its middleware insertion (asserted
    # below) still happens.
    real_install = http_module._install_process_lifespan

    def capture_install(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return real_install(*args, **kwargs)

    with (
        patch.object(
            http_module, "_install_process_lifespan", side_effect=capture_install
        ),
        patch.object(http_module.uvicorn, "Config", return_value=MagicMock()),
        patch.object(http_module.uvicorn, "Server", return_value=fake_uvicorn_server),
    ):
        http_module._run_http(plan, fake_fastmcp, ProcessResources())

    fake_uvicorn_server.run.assert_called_once_with()
    # Two middlewares: PSK gate first, then activity tracker.
    assert fake_app.user_middleware[0].cls is PSKMiddleware
    assert fake_app.user_middleware[1].cls is ActivityMiddleware
    # The process-scoped lifespan was handed an unstarted IdleWatcher.
    idle = captured_kwargs["idle"]
    assert isinstance(idle, IdleWatcher)
    assert isinstance(idle.timer, IdleTimer)
    assert idle.timer.idle_seconds == 3600
    # The exit_fn binds the uvicorn server lazily via closure capture —
    # invoking it must flip ``should_exit`` on the real uvicorn instance
    # the runner constructed.
    idle.exit_fn()
    assert fake_uvicorn_server.should_exit is True
    # Registry was deleted in the finally block.
    assert not (tmp_path / "daemon" / "daemon.json").exists()


def test_run_http_disables_idle_when_seconds_zero(tmp_path):
    """``idle_seconds == 0`` leaves the lifespan with no watcher."""
    server_cfg = ServerConfig.model_validate({"daemon": {"idle_shutdown_seconds": 0}})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )

    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_fastmcp = MagicMock()
    fake_fastmcp.streamable_http_app.return_value = fake_app
    captured_kwargs: dict[str, object] = {}

    real_install = http_module._install_process_lifespan

    def capture_install(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return real_install(*args, **kwargs)

    with (
        patch.object(
            http_module, "_install_process_lifespan", side_effect=capture_install
        ),
        patch.object(http_module.uvicorn, "Config", return_value=MagicMock()),
        patch.object(http_module.uvicorn, "Server", return_value=MagicMock()),
    ):
        http_module._run_http(plan, fake_fastmcp, ProcessResources())

    assert captured_kwargs["idle"] is None
    # Activity middleware is also skipped when supervision is off.
    from deephaven_mcp.mcp_systems_server._idle import ActivityMiddleware

    assert all(mw.cls is not ActivityMiddleware for mw in fake_app.user_middleware)


def test_run_http_deletes_registry_even_on_uvicorn_error(tmp_path):
    """Uvicorn raising must not leak a stale registry file."""
    server_cfg = ServerConfig.model_validate({})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )

    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_fastmcp = MagicMock()
    fake_fastmcp.streamable_http_app.return_value = fake_app

    fake_uvicorn_server = MagicMock()
    fake_uvicorn_server.run.side_effect = RuntimeError("boom")

    with (
        patch.object(http_module.uvicorn, "Config", return_value=MagicMock()),
        patch.object(http_module.uvicorn, "Server", return_value=fake_uvicorn_server),
        pytest.raises(RuntimeError, match="boom"),
    ):
        http_module._run_http(plan, fake_fastmcp, ProcessResources())

    assert not (tmp_path / "daemon" / "daemon.json").exists()


@pytest.mark.parametrize(
    "failure_target",
    ["_build_http_app", "uvicorn.Config", "uvicorn.Server"],
)
def test_run_http_closes_unhanded_socket_on_pre_run_failure(
    tmp_path: Path, failure_target: str
) -> None:
    """Any failure before ``run()`` releases the planner's pre-bound socket.

    Pins the runner's ``try`` arm width: ``_build_http_app(...)``,
    ``uvicorn.Config(...)``, and ``uvicorn.Server(...)`` all run
    *after* the planner has bound a 127.0.0.1:0 socket but *before*
    uvicorn takes ownership of the descriptor. A failure in any of
    them must trigger ``_BindSpec.close_unhanded`` so the fd does
    not leak. Earlier versions of the runner only wrapped
    ``uvicorn.Server`` and the registry publish in the ``try``,
    leaving fd leaks on the other two paths.
    """
    server_cfg = ServerConfig.model_validate({})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )
    pre_bound_sock = plan.bind.sock
    assert pre_bound_sock is not None

    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_fastmcp = MagicMock()
    fake_fastmcp.streamable_http_app.return_value = fake_app

    boom = RuntimeError(f"{failure_target} boom")
    patches = []
    if failure_target == "_build_http_app":
        patches.append(patch.object(http_module, "_build_http_app", side_effect=boom))
    if failure_target == "uvicorn.Config":
        patches.append(patch.object(http_module.uvicorn, "Config", side_effect=boom))
    else:
        patches.append(
            patch.object(http_module.uvicorn, "Config", return_value=MagicMock())
        )
    if failure_target == "uvicorn.Server":
        patches.append(patch.object(http_module.uvicorn, "Server", side_effect=boom))
    else:
        patches.append(
            patch.object(http_module.uvicorn, "Server", return_value=MagicMock())
        )

    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        with pytest.raises(RuntimeError, match=f"{failure_target} boom"):
            http_module._run_http(plan, fake_fastmcp, ProcessResources())

    # The pre-bound socket is closed; ``getsockname`` raises EBADF.
    with pytest.raises(OSError):
        pre_bound_sock.getsockname()


@pytest.mark.parametrize("operator_host", ["::1", "localhost", "0.0.0.0", "10.0.0.5"])
def test_run_http_daemon_plan_publishes_127_0_0_1_regardless_of_server_host(
    tmp_path: Path, operator_host: str
) -> None:
    """Daemon mode is IPv4-loopback-only; ``server.host`` is silently overridden.

    The daemon shape is fixed by the registry wire format
    (``DaemonRegistryEntry.host`` is ``Literal["127.0.0.1"]``) so any
    value the operator put in ``server.json:host`` — even ``::1`` or
    ``localhost`` which the default HTTP path accepts — is silently
    overridden to ``127.0.0.1``. Pinned against accidental
    reintroduction of a runtime host check on the daemon path.
    """
    server_cfg = ServerConfig.model_validate({"host": operator_host})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )

    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_fastmcp = MagicMock()
    fake_fastmcp.streamable_http_app.return_value = fake_app

    captured_entry: dict[str, object] = {}
    real_write = LockedRegistry.write

    def capturing_write(self, entry):
        captured_entry["entry"] = entry
        return real_write(self, entry)

    with (
        patch.object(http_module.uvicorn, "Config", return_value=MagicMock()),
        patch.object(http_module.uvicorn, "Server", return_value=MagicMock()),
        patch.object(LockedRegistry, "write", capturing_write),
    ):
        http_module._run_http(plan, fake_fastmcp, ProcessResources())

    entry = captured_entry["entry"]
    assert entry.host == "127.0.0.1"


# ---------------------------------------------------------------------------
# _acquire_loopback_socket
# ---------------------------------------------------------------------------


def test_acquire_loopback_socket_binds_loopback_port() -> None:
    """The helper binds an ephemeral port on 127.0.0.1."""
    sock = http_module._acquire_loopback_socket()
    try:
        host, port = sock.getsockname()
        assert host == "127.0.0.1"
        assert port > 0
    finally:
        sock.close()


def test_acquire_loopback_socket_returns_inet_stream() -> None:
    """The socket is unconditionally ``AF_INET`` + ``SOCK_STREAM``.

    Daemon mode is IPv4-loopback-only; the socket family is the
    most fundamental enforcement of that invariant. If a future
    change accidentally widened this to ``AF_INET6`` or
    ``AF_UNSPEC``, the CLI's MCP client (which builds plain
    ``http://{host}:{port}`` URLs without IPv6 bracketing) would
    silently break.
    """
    sock = http_module._acquire_loopback_socket()
    try:
        assert sock.family == socket.AF_INET
        assert sock.type == socket.SOCK_STREAM
    finally:
        sock.close()


def test_acquire_loopback_socket_accepts_connections_before_uvicorn() -> None:
    """The socket is already listening, so a client is queued, not refused.

    Closes the daemon startup race: the registry is published before
    uvicorn calls ``listen``, and the CLI connects as soon as the
    registry shows a live entry. A merely-bound socket would answer
    that connect with ``ECONNREFUSED``; a listening one completes the
    handshake and queues the request for uvicorn to service.
    """
    sock = http_module._acquire_loopback_socket()
    try:
        host, port = sock.getsockname()
        # A connect must succeed (handshake completes against the
        # listen backlog) even though nothing is calling ``accept``.
        with socket.create_connection((host, port), timeout=1.0) as client:
            assert client.getpeername() == (host, port)
    finally:
        sock.close()


# ---------------------------------------------------------------------------
# _build_http_app — PSK gate end-to-end (real Starlette stack)
# ---------------------------------------------------------------------------


def test_build_http_app_psk_gate_enforced_end_to_end() -> None:
    """The inserted ``PSKMiddleware`` actually gates a real Starlette app.

    Guards against a fail-*open* regression: the unit tests elsewhere
    assert the ``user_middleware.insert`` call against a mock app, but
    only a real request through a real Starlette stack proves the gate
    is enforced (and that ``HEALTH_PATH`` bypasses it). If a future
    Starlette/FastMCP built or cached the middleware stack before
    ``_build_http_app`` ran, the insert would silently no-op and this
    test would fail rather than ship an unauthenticated server.
    """
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route
    from starlette.testclient import TestClient

    from deephaven_mcp._health import HEALTH_PATH
    from deephaven_mcp.auth.middleware import PSK_HEADER_NAME

    psk = "x" * 32

    async def _ok(_request: object) -> PlainTextResponse:
        return PlainTextResponse("ok")

    real_app = Starlette(routes=[Route("/mcp", _ok), Route(HEALTH_PATH, _ok)])
    fake_server = MagicMock()
    fake_server.streamable_http_app = MagicMock(return_value=real_app)

    app = http_module._build_http_app(fake_server, psk=psk, activity_timer=None)
    client = TestClient(app)

    # No PSK header -> gate rejects with 401 before reaching the route.
    missing = client.get("/mcp")
    assert missing.status_code == 401
    assert missing.json()["code"] == "psk_missing"

    # Wrong PSK -> rejected.
    wrong = client.get("/mcp", headers={PSK_HEADER_NAME: "not-the-psk"})
    assert wrong.status_code == 401
    assert wrong.json()["code"] == "psk_invalid"

    # Correct PSK -> passes the gate to the route.
    ok = client.get("/mcp", headers={PSK_HEADER_NAME: psk})
    assert ok.status_code == 200
    assert ok.text == "ok"

    # Health path bypasses the gate without any PSK header.
    health = client.get(HEALTH_PATH)
    assert health.status_code == 200


@pytest.mark.asyncio
async def test_install_process_lifespan_wraps_session_manager() -> None:
    """``_install_process_lifespan`` wraps the SDK's app lifespan so
    process-scoped resources are built around (outside) the session-manager
    lifespan, once per process."""
    from starlette.applications import Starlette

    events: list[str] = []

    @contextlib.asynccontextmanager
    async def _session_manager_lifespan(_app):
        events.append("sm-enter")
        try:
            yield
        finally:
            events.append("sm-exit")

    app = Starlette()
    app.router.lifespan_context = _session_manager_lifespan

    holder = ProcessResources()
    multi_config = MagicMock()
    captured: dict[str, object] = {}

    @contextlib.asynccontextmanager
    async def _fake_process_lifespan(mc, *, idle, holder, runtime_dir):
        captured["args"] = (mc, idle, holder)
        captured["runtime_dir"] = runtime_dir
        events.append("proc-enter")
        try:
            yield
        finally:
            events.append("proc-exit")

    with patch.object(http_module, "process_lifespan", _fake_process_lifespan):
        http_module._install_process_lifespan(
            app,
            multi_config=multi_config,
            idle=None,
            holder=holder,
            runtime_dir=Path("/tmp/runtime"),
        )
        async with app.router.lifespan_context(app):
            pass

    # process_lifespan wraps the session-manager lifespan: it enters first
    # and exits last, so the registry outlives every MCP session.
    assert events == ["proc-enter", "sm-enter", "sm-exit", "proc-exit"]
    assert captured["args"] == (multi_config, None, holder)


# ---------------------------------------------------------------------------
# _publish_daemon_registry / _unpublish_daemon_registry / _log_http_started
# ---------------------------------------------------------------------------


def _operator_run(*, psk: str = "secret") -> "http_module._HttpRun":
    """Build a daemonless ``_HttpRun`` for helper unit tests."""
    return http_module._HttpRun(
        multi_config=MagicMock(config_dir=Path("/tmp/cfg")),
        runtime_dir=Path("/tmp/runtime"),
        server_name="srv",
        psk=psk,
        bind=http_module._BindSpec(host="127.0.0.1", port=8000, sock=None),
        idle_seconds=0,
        daemon=None,
    )


def test_publish_daemon_registry_writes_entry(tmp_path: Path) -> None:
    """The helper composes a registry entry from the plan and writes it."""
    server_cfg = ServerConfig.model_validate({"daemon": {"idle_shutdown_seconds": 60}})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )
    assert plan.daemon is not None

    captured: dict[str, object] = {}
    real_write = LockedRegistry.write

    def capturing_write(self, entry):
        captured["entry"] = entry
        return real_write(self, entry)

    try:
        with patch.object(LockedRegistry, "write", capturing_write):
            http_module._publish_daemon_registry(plan, plan.daemon)
        entry = captured["entry"]
        assert entry.host == "127.0.0.1"
        assert entry.port == plan.bind.port
        assert entry.process_name == plan.daemon.process_name
        assert entry.server_name == plan.server_name
        # The build identity is stamped from the running process so the CLI
        # can verify it is reusing the same build.
        from deephaven_mcp.daemon_registry import DaemonBuildIdentity

        assert entry.build_identity == DaemonBuildIdentity.current()
    finally:
        plan.bind.close_unhanded()


def test_unpublish_daemon_registry_no_op_for_operator_plan() -> None:
    """Operator plan (no ``daemon``) does not touch the registry."""
    plan = _operator_run()
    with patch.object(LockedRegistry, "delete") as mock_delete:
        http_module._unpublish_daemon_registry(plan)
    mock_delete.assert_not_called()


def test_unpublish_daemon_registry_deletes_when_handle_set(
    tmp_path: Path,
) -> None:
    """Daemon plan: the helper calls ``reg.delete`` exactly once."""
    server_cfg = ServerConfig.model_validate({})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )
    try:
        with patch.object(LockedRegistry, "delete") as mock_delete:
            http_module._unpublish_daemon_registry(plan)
        mock_delete.assert_called_once_with()
    finally:
        plan.bind.close_unhanded()


def test_log_http_started_operator_branch(caplog) -> None:
    """Operator banner mentions the bind host:port and PSK header."""
    plan = _operator_run()
    with caplog.at_level("INFO", logger="deephaven_mcp.mcp_systems_server._http"):
        http_module._log_http_started(plan)
    assert any(
        "Starting HTTP transport" in r.message and "127.0.0.1:8000" in r.message
        for r in caplog.records
    )


def test_log_http_started_daemon_branch(tmp_path: Path, caplog) -> None:
    """Daemon banner mentions the bound port and idle window."""
    server_cfg = ServerConfig.model_validate({"daemon": {"idle_shutdown_seconds": 60}})
    plan = _daemon_plan_for_test(
        server_cfg, config_dir=tmp_path / "cfg", runtime_dir=tmp_path
    )
    try:
        with caplog.at_level("INFO", logger="deephaven_mcp.mcp_systems_server._http"):
            http_module._log_http_started(plan)
        assert any(
            "Daemon listening" in r.message and "idle_shutdown_seconds=60" in r.message
            for r in caplog.records
        )
    finally:
        plan.bind.close_unhanded()


# ---------------------------------------------------------------------------
# _publish_daemon_registry — defensive re-check against a live/stale peer
# ---------------------------------------------------------------------------


def _make_live_entry(**overrides: object) -> DaemonRegistryEntry:
    """Build an entry whose identity is the live test process.

    Pre-published into the registry so the publish path's
    ``identity.is_alive()`` re-check sees a genuinely-live peer
    unless a test forces it dead.
    """
    defaults: dict[str, object] = {
        "pid": os.getpid(),
        "create_time_ns": int(psutil.Process(os.getpid()).create_time() * 1e9),
        "process_name": "python",
        "host": "127.0.0.1",
        "port": 12345,
        "psk": SecretStr("x" * 16),
        "started_at": datetime(2026, 5, 27, 0, 0, 0, tzinfo=UTC),
        "config_dir": Path("/tmp/cfg"),
        "server_name": "dh-test",
        "build_identity": {
            "version": "1.2.3",
            "venv": "/venv/x",
            "fingerprint": "f" * 64,
        },
    }
    defaults.update(overrides)
    return DaemonRegistryEntry.model_validate(defaults)


def _daemon_publish_plan(handle: DaemonDirectory) -> "http_module._HttpRun":
    """An operator-bound ``_HttpRun`` carrying a daemon handle for publish tests."""
    return http_module._HttpRun(
        multi_config=type("M", (), {"config_dir": Path("/tmp/cfg")})(),
        runtime_dir=Path("/tmp/runtime"),
        server_name="srv",
        psk="secret",
        bind=http_module._BindSpec(host="127.0.0.1", port=22000, sock=None),
        idle_seconds=0,
        daemon=http_module._DaemonPublish(handle=handle, process_name="python"),
    )


def test_publish_refuses_when_live_peer_already_registered(tmp_path: Path) -> None:
    """The daemon's publish path raises if a live peer is registered.

    The defensive re-check inside the registry lock surfaces
    :class:`DaemonAlreadyPublishedError` rather than silently
    overwriting a peer's still-live entry.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    # Pre-publish a live entry (current test process is the live PID).
    with dd.locked() as reg:
        reg.write(_make_live_entry(port=11000))

    plan = _daemon_publish_plan(dd)
    with pytest.raises(DaemonAlreadyPublishedError, match="already registered"):
        http_module._publish_daemon_registry(plan, plan.daemon)


def test_publish_overwrites_stale_entry(tmp_path: Path) -> None:
    """A stale (dead PID) entry does not block publish.

    The defensive re-check uses ``identity.is_alive()``; a
    not-alive entry is treated as stale and overwritten so a
    crashed-prior-daemon does not wedge the registry.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    # Pre-publish an entry whose identity will be reported dead.
    with dd.locked() as reg:
        reg.write(_make_live_entry(port=11000))

    plan = _daemon_publish_plan(dd)
    # Force the existing entry's identity to report dead.
    with patch(
        "deephaven_mcp._processes.ProcessIdentity.is_alive",
        return_value=False,
    ):
        http_module._publish_daemon_registry(plan, plan.daemon)

    # Replaced with the new port.
    new_entry = dd.read_entry()
    assert new_entry is not None
    assert new_entry.port == 22000


def test_publish_treats_corrupt_existing_entry_as_stale(tmp_path: Path) -> None:
    """A corrupt registry does not block publish.

    The defensive re-check cannot identity-check a corrupt entry;
    treating it as stale lets the daemon publish over it (the
    operator has the explicit ``daemon repair`` recovery verb if
    that is the wrong call).
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.registry_path.write_text("not json")

    plan = _daemon_publish_plan(dd)
    http_module._publish_daemon_registry(plan, plan.daemon)
    new_entry = dd.read_entry()
    assert new_entry is not None
    assert new_entry.port == 22000


def test_publish_clears_start_marker(tmp_path: Path) -> None:
    """A successful publish clears the ``daemon.starting`` marker.

    The spawning CLI writes the marker before spawning the daemon;
    publish clears it so a peer CLI sees the live entry rather than
    deferring to an in-progress spawn that already completed.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    with dd.locked() as reg:
        reg.write_start_marker(datetime.now(UTC))
        assert reg.read_start_marker() is not None

    plan = _daemon_publish_plan(dd)
    http_module._publish_daemon_registry(plan, plan.daemon)

    with dd.locked() as reg:
        assert reg.read_start_marker() is None
    new_entry = dd.read_entry()
    assert new_entry is not None
    assert new_entry.port == 22000
