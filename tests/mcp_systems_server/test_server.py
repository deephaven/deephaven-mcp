"""Tests for ``deephaven_mcp.mcp_systems_server.server``.

Covers the CLI surface that boots the multiplexed systems server:

- ``_is_loopback_host``: classification of literal IPs, hostname
  resolution, and unresolvable inputs.
- ``_load_multi_config_or_exit``: returns the full ``ConfigTree``
  (server, community, enterprise) loaded once and exits on
  ConfigurationError.
- ``_resolve_psk_or_exit``: success, missing PSK.
- ``main``: click-driven CLI surface — defaults, explicit overrides,
  unknown transport rejection, stdio path, HTTP path with PSK, and the
  non-loopback / no-PSK refusal paths.
"""

from __future__ import annotations

import contextlib
import socket
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import click
import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config.schema import ServerConfig
from deephaven_mcp.mcp_systems_server import _fastmcp as fastmcp_module
from deephaven_mcp.mcp_systems_server import _http as http_module
from deephaven_mcp.mcp_systems_server import server as server_module
from deephaven_mcp.mcp_systems_server._http import (
    _is_loopback_host,
    _resolve_psk_or_exit,
)
from deephaven_mcp.mcp_systems_server.server import _load_multi_config_or_exit, main

# ---------------------------------------------------------------------------
# CLI argument parsing (click-driven; covered via ``main(argv)``)
# ---------------------------------------------------------------------------


def test_main_rejects_unknown_transport():
    """Click's ``Choice`` rejects unsupported transports with exit code 2."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--transport", "sse"])
    assert exc_info.value.code == 2


def test_main_help_prints_usage(capsys):
    """``--help`` prints usage to stdout and returns without raising.

    With ``standalone_mode=False`` click swallows its own
    :class:`click.exceptions.Exit` for ``--help`` and returns the
    exit code from ``_cli.main``; the wrapper ignores that return
    value, so ``main(["--help"])`` simply returns ``None`` after
    rendering the help text.
    """
    result = main(["--help"])
    assert result is None
    out = capsys.readouterr().out
    assert "dh-mcp-systems-server" in out
    assert "--transport" in out
    assert "--daemon" in out


# ---------------------------------------------------------------------------
# _load_multi_config_or_exit
# ---------------------------------------------------------------------------


def _multi_config_with(
    server_cfg: ServerConfig | None, config_dir: Path | None = None
) -> MagicMock:
    """Build a ConfigTree mock with ``cfg.server`` and ``cfg.config_dir`` set."""
    multi = MagicMock()
    multi.server = server_cfg
    multi.config_dir = config_dir if config_dir is not None else Path("/tmp/cfg")
    multi.has_usable_system.return_value = True
    return multi


@pytest.mark.asyncio
async def test_load_multi_config_returns_loaded_config(tmp_path):
    loaded = ServerConfig.model_validate({"psk": "hunter2", "port": 9100})
    multi = _multi_config_with(loaded, tmp_path)
    with patch.object(
        server_module,
        "ConfigTreeLoader",
        MagicMock(
            return_value=MagicMock(
                initialize=AsyncMock(return_value=multi),
            )
        ),
    ):
        result = await _load_multi_config_or_exit(tmp_path)
    assert result is multi
    assert result.server is loaded
    assert result.config_dir == tmp_path


@pytest.mark.asyncio
async def test_load_multi_config_returns_config_with_no_server(tmp_path):
    """When ``server.json`` is absent, ``multi.server`` is ``None``; main() supplies defaults."""
    multi = _multi_config_with(None, tmp_path)
    with patch.object(
        server_module,
        "ConfigTreeLoader",
        MagicMock(
            return_value=MagicMock(
                initialize=AsyncMock(return_value=multi),
            )
        ),
    ):
        result = await _load_multi_config_or_exit(tmp_path)
    assert result.server is None
    assert result.config_dir == tmp_path


@pytest.mark.asyncio
async def test_load_multi_config_exits_on_configuration_error():
    with (
        patch.object(
            server_module,
            "ConfigTreeLoader",
            MagicMock(
                return_value=MagicMock(
                    initialize=AsyncMock(side_effect=ConfigurationError("bad config"))
                )
            ),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        await _load_multi_config_or_exit(None)
    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_load_multi_config_exits_on_zero_system_tree(tmp_path):
    """A valid tree with no servable system is refused at server startup."""
    multi = _multi_config_with(None, tmp_path)
    multi.has_usable_system.return_value = False
    with (
        patch.object(
            server_module,
            "ConfigTreeLoader",
            MagicMock(
                return_value=MagicMock(
                    initialize=AsyncMock(return_value=multi),
                )
            ),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        await _load_multi_config_or_exit(tmp_path)
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# main — transport selection
# ---------------------------------------------------------------------------


@pytest.fixture
def _mute_logging_setup():
    """Suppress the boot-time logging/signal/uvicorn helpers under main()."""
    patches = [
        patch.object(server_module, "setup_logging"),
        patch.object(server_module, "setup_global_exception_logging"),
        patch.object(server_module, "setup_signal_handler_logging"),
        patch.object(server_module, "monkeypatch_uvicorn_exception_handling"),
    ]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


def _patch_load_server_config(server_cfg: ServerConfig, config_dir: Path | None = None):
    """Patch ``_load_multi_config_or_exit`` to return a ConfigTree mock."""
    multi = _multi_config_with(server_cfg, config_dir)
    return patch.object(
        server_module,
        "_load_multi_config_or_exit",
        AsyncMock(return_value=multi),
    )


def test_main_stdio_path_from_cli_flag(_mute_logging_setup):
    fake_server = MagicMock()
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(
            fastmcp_module, "build_fastmcp", return_value=fake_server
        ) as mock_build,
        patch.object(server_module, "_run_stdio") as mock_stdio,
        patch.object(server_module, "_run_http") as mock_http,
    ):
        main(["--transport", "stdio"])
    mock_build.assert_called_once()
    # _run_stdio(server, multi_config, holder): the server is the first arg.
    mock_stdio.assert_called_once()
    assert mock_stdio.call_args.args[0] is fake_server
    mock_http.assert_not_called()


def test_main_stdio_path_from_server_json(_mute_logging_setup):
    """`transport="stdio"` from ``server.json`` is honored when CLI omits ``--transport``."""
    fake_server = MagicMock()
    with (
        _patch_load_server_config(ServerConfig.model_validate({"transport": "stdio"})),
        patch.object(fastmcp_module, "build_fastmcp", return_value=fake_server),
        patch.object(server_module, "_run_stdio") as mock_stdio,
        patch.object(server_module, "_run_http") as mock_http,
    ):
        main([])
    mock_stdio.assert_called_once()
    assert mock_stdio.call_args.args[0] is fake_server
    mock_http.assert_not_called()


def test_main_http_path_calls_run_http_with_cli_overrides(_mute_logging_setup):
    """``main`` builds a default-mode plan from CLI flags and runs it.

    With the plan-then-run refactor, ``main`` no longer constructs a
    FastMCP server itself for the HTTP path; the default-mode planner
    resolves CLI flags into an :class:`_HttpRun` and the unified
    runner builds the server from the plan. The patched ``_run_http``
    therefore receives a single positional plan argument, which we
    inspect for the default-mode field shape.
    """
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(http_module, "_is_loopback_host", return_value=True),
        patch.object(http_module, "_resolve_psk_or_exit", MagicMock(return_value="pw")),
        patch.object(server_module, "_run_http") as mock_http,
        patch.object(server_module, "_run_stdio") as mock_stdio,
    ):
        main(["--transport", "http", "--host", "127.0.0.1", "--port", "8765"])
    mock_http.assert_called_once()
    plan = mock_http.call_args.args[0]
    assert plan.psk == "pw"
    assert plan.bind.host == "127.0.0.1"
    assert plan.bind.port == 8765
    assert plan.bind.sock is None
    assert plan.idle_seconds == 0
    assert plan.daemon is None
    mock_stdio.assert_not_called()


def test_main_http_path_uses_server_json_values(_mute_logging_setup):
    """With no CLI overrides, host/port/psk come from ``server.json``."""
    server_cfg = ServerConfig.model_validate(
        {"transport": "http", "host": "::1", "port": 9000, "psk": "json-psk"}
    )
    with (
        _patch_load_server_config(server_cfg),
        patch.object(http_module, "_is_loopback_host", return_value=True),
        patch.object(server_module, "_run_http") as mock_http,
    ):
        main([])
    mock_http.assert_called_once()
    plan = mock_http.call_args.args[0]
    assert plan.psk == "json-psk"
    assert plan.bind.host == "::1"
    assert plan.bind.port == 9000


def test_main_http_refuses_non_loopback_host(_mute_logging_setup):
    """``_plan_default`` exits 2 when the resolved host fails the loopback check."""
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(http_module, "_is_loopback_host", return_value=False),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["--transport", "http", "--host", "0.0.0.0"])
    assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# _run_stdio
# ---------------------------------------------------------------------------


def test_run_stdio_wraps_run_stdio_async_in_process_lifespan():
    """``_run_stdio`` runs the stdio transport inside ``process_lifespan``.

    The process-scoped resources are built around ``run_stdio_async`` so the
    per-session lifespan can read them from the shared holder.
    """
    fake_server = MagicMock()
    fake_server.run_stdio_async = AsyncMock()
    multi = _multi_config_with(ServerConfig(), Path("/tmp/x"))
    holder = object()
    runtime_dir = Path("/tmp/rt")

    captured: dict[str, object] = {}

    @contextlib.asynccontextmanager
    async def _fake_process_lifespan(mc, *, idle, holder, runtime_dir):
        captured["args"] = (mc, idle, holder)
        captured["runtime_dir"] = runtime_dir
        yield MagicMock()

    with patch.object(server_module, "process_lifespan", _fake_process_lifespan):
        server_module._run_stdio(fake_server, multi, holder, runtime_dir=runtime_dir)

    fake_server.run_stdio_async.assert_awaited_once_with()
    assert captured["args"] == (multi, None, holder)
    # The caller's resolved runtime_dir is forwarded to process_lifespan.
    assert captured["runtime_dir"] == runtime_dir


# ---------------------------------------------------------------------------
# main(--daemon) integration
# ---------------------------------------------------------------------------


def test_main_daemon_path_invokes_run_http_with_daemon_plan(_mute_logging_setup):
    """``--daemon`` plans a daemon-mode run and dispatches to the unified runner."""
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_run_http") as mock_run,
        patch.object(server_module, "_run_stdio") as mock_stdio,
    ):
        main(["--daemon"])
    mock_run.assert_called_once()
    mock_stdio.assert_not_called()
    plan = mock_run.call_args.args[0]
    # Daemon-mode shape: handoff bind, registry publish params, idle_seconds
    # populated from daemon_cfg defaults.
    assert plan.bind.host is None
    assert plan.bind.sock is not None
    assert plan.daemon is not None
    assert plan.daemon.handle is not None
    assert plan.daemon.process_name is not None
    plan.bind.close_unhanded()


def test_main_daemon_path_threads_runtime_dir_override(_mute_logging_setup):
    """``--runtime-dir`` is resolved and reaches the daemon planner."""
    captured: dict[str, object] = {}

    def fake_planner(*args, **kwargs):
        captured.update(kwargs)
        # Return a minimal valid plan so the runner stub does not see
        # an exception. The runner is also mocked below.
        return http_module._HttpRun(
            multi_config=MagicMock(),
            runtime_dir=Path("/tmp/runtime"),
            server_name="srv",
            psk="x" * 32,
            bind=http_module._BindSpec(host="127.0.0.1", port=8000, sock=None),
            idle_seconds=0,
            daemon=None,
        )

    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_plan_daemon", side_effect=fake_planner),
        patch.object(server_module, "_run_http"),
    ):
        main(["--daemon", "--runtime-dir", "/var/tmp/dh-runtime"])
    assert captured["runtime_dir"] == Path("/var/tmp/dh-runtime")


@pytest.mark.parametrize(
    "extra_argv",
    [
        ["--transport", "http"],
        ["--host", "127.0.0.1"],
        ["--port", "8000"],
        ["--transport", "http", "--host", "127.0.0.1", "--port", "8000"],
    ],
)
def test_main_daemon_path_rejects_conflicting_flags(
    _mute_logging_setup, extra_argv: list[str]
) -> None:
    """``--daemon`` rejects ``--transport``/``--host``/``--port``.

    The daemon shape is fixed by the registry wire format
    (``DaemonRegistryEntry.host`` is ``Literal["127.0.0.1"]``,
    transport is HTTP, port is kernel-chosen). Allowing operators
    to pass conflicting flags would silently ignore them — bad UX.
    The :func:`_validate_cli_args` step rejects with
    :class:`click.UsageError`, which :func:`main` translates to
    ``sys.exit(2)``.
    """
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_run_http") as mock_run,
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["--daemon", *extra_argv])
    assert exc_info.value.code == 2
    mock_run.assert_not_called()


def test_main_rejects_port_below_range(_mute_logging_setup) -> None:
    """``--port 0`` is out of range and rejected up-front."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--transport", "http", "--port", "0"])
    assert exc_info.value.code == 2


def test_main_rejects_port_above_range(_mute_logging_setup) -> None:
    """``--port 65536`` is out of range and rejected up-front."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--transport", "http", "--port", "65536"])
    assert exc_info.value.code == 2


def test_main_rejects_empty_host(_mute_logging_setup) -> None:
    """``--host ''`` is rejected so the operator notices the typo."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--transport", "http", "--host", ""])
    assert exc_info.value.code == 2


def test_main_runtime_dir_without_daemon_is_honored(_mute_logging_setup) -> None:
    """``--runtime-dir`` without ``--daemon`` is honored on the stdio path.

    The flag is a universal per-subdir override (parallel to
    ``--config-dir``), so it reaches the stdio runner as the resolved
    runtime directory rather than being silently dropped.
    """
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_run_stdio") as mock_stdio,
        patch.object(server_module, "_run_http") as mock_http,
        patch.object(fastmcp_module, "build_fastmcp", return_value=MagicMock()),
    ):
        main(["--runtime-dir", "/var/tmp/dh-runtime"])
    mock_stdio.assert_called_once()
    mock_http.assert_not_called()
    assert mock_stdio.call_args.kwargs["runtime_dir"] == Path("/var/tmp/dh-runtime")


def test_main_daemon_path_threads_psk_override(_mute_logging_setup, tmp_path):
    """``--daemon --psk SECRET`` reaches the daemon planner as ``cli_psk``."""
    captured: dict[str, object] = {}

    def fake_planner(*args, **kwargs):
        captured.update(kwargs)
        return http_module._HttpRun(
            multi_config=MagicMock(),
            runtime_dir=Path("/tmp/runtime"),
            server_name="srv",
            psk="x" * 32,
            bind=http_module._BindSpec(host="127.0.0.1", port=8000, sock=None),
            idle_seconds=0,
            daemon=None,
        )

    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_plan_daemon", side_effect=fake_planner),
        patch.object(server_module, "_run_http"),
    ):
        main(
            [
                "--daemon",
                "--psk",
                "operator-explicit-secret",
                "--runtime-dir",
                str(tmp_path),
            ]
        )
    assert captured["cli_psk"] == "operator-explicit-secret"


# ---------------------------------------------------------------------------
# _validate_cli_args
# ---------------------------------------------------------------------------


def test_validate_cli_args_accepts_clean_combinations() -> None:
    """No-op for valid combinations: stdio / default HTTP / daemon."""
    server_module._validate_cli_args(
        transport="stdio", host=None, port=None, daemon=False
    )
    server_module._validate_cli_args(
        transport="http", host="127.0.0.1", port=8000, daemon=False
    )
    server_module._validate_cli_args(transport=None, host=None, port=None, daemon=True)


@pytest.mark.parametrize(
    "transport, host, port, expected_in_msg",
    [
        ("http", None, None, "--transport"),
        (None, "127.0.0.1", None, "--host"),
        (None, None, 8000, "--port"),
    ],
)
def test_validate_cli_args_rejects_each_daemon_conflict(
    transport: str | None,
    host: str | None,
    port: int | None,
    expected_in_msg: str,
) -> None:
    """Each conflicting flag is mentioned in the rejection message."""
    with pytest.raises(click.UsageError) as exc_info:
        server_module._validate_cli_args(
            transport=transport, host=host, port=port, daemon=True
        )
    assert expected_in_msg in str(exc_info.value)


@pytest.mark.parametrize("port", [0, -1, 65536, 999999])
def test_validate_cli_args_rejects_out_of_range_port(port: int) -> None:
    """Port outside ``[1, 65535]`` is rejected with a clear message."""
    with pytest.raises(click.UsageError, match=r"\[1, 65535\]"):
        server_module._validate_cli_args(
            transport=None, host=None, port=port, daemon=False
        )


@pytest.mark.parametrize("host", ["", " ", "   ", "\t", "\n", " \t \n "])
def test_validate_cli_args_rejects_empty_or_whitespace_host(host: str) -> None:
    """Empty or whitespace-only ``--host`` is rejected to surface operator typos."""
    with pytest.raises(click.UsageError, match="non-empty"):
        server_module._validate_cli_args(
            transport=None, host=host, port=None, daemon=False
        )


def test_validate_cli_args_does_not_take_runtime_dir() -> None:
    """``_validate_cli_args`` does not see ``runtime_dir``: it needs no validation.

    ``--runtime-dir`` is honored in every transport, so it is never a
    rejected combination. Its effect is verified at the :func:`main`
    level by :func:`test_main_runtime_dir_without_daemon_is_honored`;
    here we pin that the validator's signature does not even take it.
    """
    import inspect

    sig = inspect.signature(server_module._validate_cli_args)
    assert "runtime_dir" not in sig.parameters
