"""Tests for ``deephaven_mcp.cli._commands.script``."""

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

_SID = "community:community:dev"


def _run(args: list[str], payload: dict, tmp_path: Path):
    rt = make_runtime(tmp_path)
    result = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload,
    )
    with (
        patch.object(wrapping_mod, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(wrapping_mod, "call_tool", AsyncMock(return_value=result)) as call,
        patch.object(_main, "load_runtime", fake_load_runtime(rt)),
    ):
        return CliRunner().invoke(cli, args), call


def test_run_inline_script(tmp_path: Path) -> None:
    result, call = _run(
        ["script", "run", _SID, "--script", "print(1)"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_script_run"
    assert call.await_args.args[3] == {"id": _SID, "script": "print(1)"}


def test_run_script_path(tmp_path: Path) -> None:
    result, call = _run(
        ["script", "run", _SID, "--script-path", "/tmp/j.py"],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID, "script_path": "/tmp/j.py"}


def test_run_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["script", "run", _SID, "--script", "boom()"],
        {"success": False, "error": "script blew up"},
        tmp_path,
    )
    assert result.exit_code == 3
    assert "script blew up" in result.output


def test_run_requires_a_source(tmp_path: Path) -> None:
    result, call = _run(["script", "run", _SID], {"success": True}, tmp_path)
    assert result.exit_code == 2
    assert "Provide a script source" in result.output
    call.assert_not_awaited()


def test_run_rejects_both_sources(tmp_path: Path) -> None:
    result, call = _run(
        ["script", "run", _SID, "--script", "print(1)", "--script-path", "/tmp/j.py"],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    call.assert_not_awaited()


def test_pip_list_emits_array(tmp_path: Path) -> None:
    payload = {"success": True, "packages": [{"package": "numpy", "version": "1.25"}]}
    result, call = _run(["-o", "json", "script", "pip-list", _SID], payload, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.output) == payload["packages"]
    assert call.await_args.args[2] == "session_pip_list"
