"""Tests for ``deephaven_mcp.cli._commands.system``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime

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
        {"name": "prod", "liveness_status": "ONLINE", "is_alive": True},
    ],
}


def _invoke(args: list[str], runtime: Runtime):
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args)


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
    assert payload == {"systems": _STATUS_PAYLOAD["systems"]}
    assert "success" not in payload


def test_status_tool_failure_exits_3(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    failure = {"success": False, "error": "no enterprise", "isError": True}
    acquire_p, call_p = _patch(_result(failure))
    with acquire_p, call_p:
        result = _invoke(["system", "status"], rt)
    assert result.exit_code == 3
    assert "no enterprise" in result.output
