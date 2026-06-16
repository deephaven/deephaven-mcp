"""Tests for ``deephaven_mcp.cli._commands.system``."""

from __future__ import annotations

import json
import webbrowser
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _browser as browser_mod
from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.schema import CliConfig, ServerConfig
from deephaven_mcp.config.schema._enterprise import EnterpriseConfig, EnterpriseSettings
from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.sessions._enterprise import EnterpriseSystemConfig

from .._helpers import fake_load_runtime, make_entry, make_runtime

_LIST_PAYLOAD = {
    "success": True,
    "systems": [
        {"name": "community", "type": "community"},
        {"name": "prod", "type": "enterprise"},
    ],
}

_STATUS_PAYLOAD = {
    "success": True,
    "systems": [
        {
            "name": "prod",
            "type": "enterprise",
            "liveness_status": "ONLINE",
            "is_alive": True,
        },
    ],
}


def _invoke(args: list[str], runtime: Runtime, *, standalone_mode: bool = True):
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args, standalone_mode=standalone_mode)


def _result(payload: dict) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload,
    )


def _patch(result: CallToolResult):
    """Patch the system module's acquire + call_tool seam with one result."""
    return (
        patch.object(wrapping_mod, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(wrapping_mod, "call_tool", AsyncMock(return_value=result)),
    )


# ---------------------------------------------------------------------------
# system list
# ---------------------------------------------------------------------------


def test_list_success_human(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_LIST_PAYLOAD))
    with acquire_p, call_p as call:
        result = _invoke(["system", "list"], rt)
    assert result.exit_code == 0
    assert "community" in result.output
    assert "prod" in result.output
    name, args = call.await_args.args[2], call.await_args.args[3]
    assert name == "list_systems"
    assert args == {}


def test_list_success_json_is_bare_array(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_LIST_PAYLOAD))
    with acquire_p, call_p:
        result = _invoke(["-o", "json", "system", "list"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == _LIST_PAYLOAD["systems"]


def test_list_success_yaml(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_LIST_PAYLOAD))
    with acquire_p, call_p:
        result = _invoke(["-o", "yaml", "system", "list"], rt)
    assert result.exit_code == 0
    assert "name: community" in result.output


def test_list_tool_failure_exits_3(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    failure = {"success": False, "error": "boom", "isError": True}
    acquire_p, call_p = _patch(_result(failure))
    with acquire_p, call_p:
        result = _invoke(["system", "list"], rt)
    assert result.exit_code == 3
    assert "boom" in result.output


# ---------------------------------------------------------------------------
# system status
# ---------------------------------------------------------------------------


def test_status_success_human(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_STATUS_PAYLOAD))
    with acquire_p, call_p as call:
        result = _invoke(["system", "status"], rt)
    assert result.exit_code == 0
    assert "ONLINE" in result.output
    name, args = call.await_args.args[2], call.await_args.args[3]
    assert name == "enterprise_systems_status"
    assert args == {}


def test_status_forwards_system_and_connect(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_STATUS_PAYLOAD))
    with acquire_p, call_p as call:
        result = _invoke(["system", "status", "--system", "prod", "--connect"], rt)
    assert result.exit_code == 0
    args = call.await_args.args[3]
    assert args == {"system": "prod", "attempt_to_connect": True}


def test_status_success_json(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_STATUS_PAYLOAD))
    with acquire_p, call_p:
        result = _invoke(["-o", "json", "system", "status"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == _STATUS_PAYLOAD["systems"]


def test_status_human_renders_as_aligned_table(tmp_path: Path) -> None:
    """Human mode renders the compact status array as a header + row table.

    With ``config`` removed, every cell is scalar, so the renderer takes the
    aligned-table path; no stacked-block fallback for the normal case.
    """
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_STATUS_PAYLOAD))
    with acquire_p, call_p:
        result = _invoke(["system", "status"], rt)
    assert result.exit_code == 0
    lines = result.output.splitlines()
    headers = lines[0].split()
    assert headers == ["name", "type", "liveness_status", "is_alive"]
    data = lines[1].split()
    assert data == ["prod", "enterprise", "ONLINE", "True"]


def test_status_completed_partial_result_emits_no_stderr_noise(
    tmp_path: Path,
) -> None:
    """``phase=='completed'`` is fully attributed per-row via ``liveness_detail``,
    so ``system status`` emits no stderr warning at all — the "had connection
    issues" banner would only restate the table."""
    payload = {
        "success": True,
        "systems": [
            {
                "name": "prod",
                "type": "enterprise",
                "liveness_status": "OFFLINE",
                "is_alive": False,
                "liveness_detail": "DeephavenConnectionError",
            },
        ],
        "partial_result": {
            "phase": "completed",
            "detail": "Some enterprise systems had connection issues during discovery.",
            "errors": {"prod": "DeephavenConnectionError: Network is unreachable"},
        },
    }
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(payload))
    with acquire_p, call_p:
        result = _invoke(["system", "status"], rt)
    assert result.exit_code == 0
    # partial_result key stays out of stdout.
    assert "partial_result" not in result.stdout
    # Stderr is entirely empty for the COMPLETED-with-errors case.
    assert result.stderr == ""
    # The short reason still appears in the table row on stdout.
    assert "DeephavenConnectionError" in result.stdout


def test_status_loading_partial_result_still_warns(tmp_path: Path) -> None:
    """``phase=='loading'`` carries a timing signal ("retry in a moment") the
    rows cannot convey, so it still emits a stderr warning even with the
    per-row attribution opt-in."""
    payload = {
        "success": True,
        "systems": [
            {
                "name": "prod",
                "type": "enterprise",
                "liveness_status": "OFFLINE",
                "is_alive": False,
                "liveness_detail": "No item cached",
            },
        ],
        "partial_result": {
            "phase": "loading",
            "detail": "Enterprise session discovery is actively running. Some sessions or systems may not yet be visible.",
        },
    }
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(payload))
    with acquire_p, call_p:
        result = _invoke(["system", "status"], rt)
    assert result.exit_code == 0
    assert "actively running" in result.stderr


def test_status_tool_failure_exits_3(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    failure = {"success": False, "error": "no enterprise", "isError": True}
    acquire_p, call_p = _patch(_result(failure))
    with acquire_p, call_p:
        result = _invoke(["system", "status"], rt)
    assert result.exit_code == 3
    assert "no enterprise" in result.output


# ---------------------------------------------------------------------------
# url / open
# ---------------------------------------------------------------------------

_CONNECTION_URL = "https://dhe.example.com:8123/iris/connection.json"
_WEB_CONSOLE_URL = "https://dhe.example.com:8123/iriside"


def _enterprise_system(
    *, name: str = "prod", connection_json_url: str = _CONNECTION_URL
) -> EnterpriseSystemConfig:
    return EnterpriseSystemConfig.model_validate(
        {
            "name": name,
            "system_name": name,
            "connection_json_url": connection_json_url,
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "shh",
                }
            },
        }
    )


def _enterprise_runtime(
    tmp_path: Path,
    systems: dict[str, EnterpriseSystemConfig] | None,
) -> Runtime:
    """Build a Runtime whose config carries the given enterprise systems."""
    enterprise = (
        None
        if systems is None
        else EnterpriseConfig(settings=EnterpriseSettings(), systems=systems)
    )
    config = ConfigTree(
        config_dir=tmp_path / "cfg",
        cli=CliConfig(),
        server=ServerConfig(),
        enterprise=enterprise,
    )
    return make_runtime(tmp_path, config=config)


def test_url_prints_web_console(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    result = _invoke(["system", "url", "prod"], rt)
    assert result.exit_code == 0
    assert result.output.strip() == _WEB_CONSOLE_URL


def test_url_unknown_system_exits_2(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    result = _invoke(["system", "url", "bogus"], rt, standalone_mode=False)
    assert result.exception.code.value == "system_not_found"
    assert result.exception.exit_code == 2
    assert "system list" in str(result.exception)


def test_url_community_name_points_to_session_url(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    result = _invoke(["system", "url", "community"], rt, standalone_mode=False)
    assert result.exception.code.value == "system_not_found"
    assert "session url" in str(result.exception)


def test_url_no_enterprise_config_exits_2(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, None)
    result = _invoke(["system", "url", "prod"], rt, standalone_mode=False)
    assert result.exception.code.value == "system_not_found"


def test_url_malformed_connection_url_exits_2(tmp_path: Path) -> None:
    rt = _enterprise_runtime(
        tmp_path,
        {"prod": _enterprise_system(connection_json_url="not-a-url")},
    )
    result = _invoke(["system", "url", "prod"], rt, standalone_mode=False)
    assert result.exception.code.value == "config_invalid"
    assert result.exception.exit_code == 2


def test_open_launches_browser(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    with patch.object(browser_mod.webbrowser, "open", return_value=True) as wb:
        result = _invoke(["-o", "json", "system", "open", "prod"], rt)
    assert result.exit_code == 0
    wb.assert_called_once_with(_WEB_CONSOLE_URL)
    assert json.loads(result.output) == {"opened": _WEB_CONSOLE_URL, "launched": True}


def test_open_print_only_does_not_launch(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    with patch.object(browser_mod.webbrowser, "open", return_value=True) as wb:
        result = _invoke(["-o", "json", "system", "open", "prod", "--print"], rt)
    assert result.exit_code == 0
    wb.assert_not_called()
    assert json.loads(result.output) == {"opened": _WEB_CONSOLE_URL, "launched": False}


def test_open_no_browser_found_exits_2(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    with patch.object(browser_mod.webbrowser, "open", return_value=False):
        result = _invoke(["system", "open", "prod"], rt, standalone_mode=False)
    assert result.exception.code.value == "browser_launch_failed"
    assert "manually" in str(result.exception)


def test_open_browser_error_exits_2(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    with patch.object(
        browser_mod.webbrowser, "open", side_effect=webbrowser.Error("nope")
    ):
        result = _invoke(["system", "open", "prod"], rt, standalone_mode=False)
    assert result.exception.code.value == "browser_launch_failed"
    assert "Could not launch" in str(result.exception)


def test_open_unknown_system_exits_2(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path, {"prod": _enterprise_system()})
    result = _invoke(["system", "open", "bogus"], rt, standalone_mode=False)
    assert result.exception.code.value == "system_not_found"
