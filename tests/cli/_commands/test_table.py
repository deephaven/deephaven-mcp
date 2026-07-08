"""Tests for ``deephaven_mcp.cli._commands.table``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._main import cli

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


def test_list_emits_table_names(tmp_path: Path) -> None:
    payload = {"success": True, "table_names": ["a", "b"], "count": 2}
    result, call = _run(["-o", "json", "table", "list", _SID], payload, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.output) == ["a", "b"]
    assert call.await_args.args[2] == "session_tables_list"


def test_schema_all_tables(tmp_path: Path) -> None:
    result, call = _run(
        ["table", "schema", _SID], {"success": True, "schemas": []}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID}


def test_schema_named_tables(tmp_path: Path) -> None:
    result, call = _run(
        ["table", "schema", _SID, "trades", "quotes"],
        {"success": True, "schemas": []},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "id": _SID,
        "table_names": ["trades", "quotes"],
    }


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
