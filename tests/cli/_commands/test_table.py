"""Tests for ``deephaven_mcp.cli._commands.table``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._context import ContextKey
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime

from .._helpers import fake_load_runtime, make_entry, make_runtime

_SID = "community:community:dev"


def _run(
    args: list[str], payload: dict, tmp_path: Path, runtime: Runtime | None = None
):
    rt = runtime or make_runtime(tmp_path)
    result = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload,
    )
    with (
        patch.object(wrapping_mod, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(wrapping_mod, "call_tool", AsyncMock(return_value=result)) as call,
        patch.object(runtime_mod, "load_runtime", fake_load_runtime(rt)),
    ):
        return CliRunner().invoke(cli, args), call


def test_list_emits_table_names(tmp_path: Path) -> None:
    payload = {"success": True, "table_names": ["a", "b"], "count": 2}
    result, call = _run(["-o", "json", "table", "list", _SID], payload, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.output) == ["a", "b"]
    assert call.await_args.args[2] == "session_tables_list"


def test_list_falls_back_to_context_session(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SESSION, _SID)
    payload = {"success": True, "table_names": ["a"], "count": 1}
    result, call = _run(["-o", "json", "table", "list"], payload, tmp_path, runtime=rt)
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID}


def test_list_no_id_and_no_context_fails(tmp_path: Path) -> None:
    result, call = _run(["table", "list"], {"success": True}, tmp_path)
    assert result.exit_code == 2
    assert "no sticky context session is set" in result.output
    call.assert_not_awaited()


def test_schema_single_table(tmp_path: Path) -> None:
    result, call = _run(
        ["table", "schema", _SID, "trades"],
        {
            "success": True,
            "id": _SID,
            "table_name": "trades",
            "schema": [],
            "column_count": 0,
        },
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_table_schema"
    assert call.await_args.args[3] == {"id": _SID, "table_name": "trades"}


def test_schema_full_flag_is_removed(tmp_path: Path) -> None:
    """Sparse column_type is always included; no full mode."""
    result, _ = _run(
        ["table", "schema", _SID, "trades", "--full"],
        {
            "success": True,
            "id": _SID,
            "table_name": "trades",
            "schema": [],
            "column_count": 0,
        },
        tmp_path,
    )
    assert result.exit_code == 2
    assert "no such option" in result.output.lower()


def test_data_defaults(tmp_path: Path) -> None:
    result, call = _run(
        ["table", "data", _SID, "trades"], {"success": True, "rows": []}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_table_data"
    assert call.await_args.args[3] == {
        "id": _SID,
        "table_name": "trades",
        "head": True,
        "format": "json-row",
    }


def test_data_options(tmp_path: Path) -> None:
    result, call = _run(
        [
            "table",
            "data",
            _SID,
            "trades",
            "--max-rows",
            "50",
            "--tail",
        ],
        {"success": True, "rows": []},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "id": _SID,
        "table_name": "trades",
        "head": False,
        "max_rows": 50,
        "format": "json-row",
    }


def test_list_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["table", "list", _SID], {"success": False, "error": "no session"}, tmp_path
    )
    assert result.exit_code == 3
