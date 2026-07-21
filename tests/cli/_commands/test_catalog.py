"""Tests for ``deephaven_mcp.cli._commands.catalog``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._main import cli

from .._helpers import fake_load_runtime, make_entry, make_runtime

_SID = "enterprise:prod:rpt"


def _run(args: list[str], payload: dict, tmp_path: Path):
    rt = make_runtime(tmp_path)
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


_TABLES = {
    "success": True,
    "id": _SID,
    "tables": [
        {"namespace": "Market", "table_name": "Trades"},
        {"namespace": "Market", "table_name": "Quotes"},
    ],
    "count": 2,
    "is_complete": True,
}


def test_tables_minimal(tmp_path: Path) -> None:
    result, call = _run(["-o", "json", "catalog", "tables", _SID], _TABLES, tmp_path)
    assert result.exit_code == 0
    # stdout is the bare tables array, not the tool envelope.
    assert json.loads(result.stdout) == _TABLES["tables"]
    assert call.await_args.args[2] == "catalog_tables_list"
    assert call.await_args.args[3] == {"id": _SID}


def test_tables_with_options(tmp_path: Path) -> None:
    result, call = _run(
        [
            "catalog",
            "tables",
            _SID,
            "--max-rows",
            "5",
            "--filter",
            "a>1",
            "--filter",
            "b<2",
        ],
        _TABLES,
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "id": _SID,
        "max_rows": 5,
        "filters": ["a>1", "b<2"],
    }


_NAMESPACES = {
    "success": True,
    "id": _SID,
    "namespaces": ["market_data", "reference"],
    "count": 2,
    "is_complete": True,
}


def test_namespaces(tmp_path: Path) -> None:
    result, call = _run(
        ["-o", "json", "catalog", "namespaces", _SID], _NAMESPACES, tmp_path
    )
    assert result.exit_code == 0
    # stdout is the bare namespaces array, not the tool envelope.
    assert json.loads(result.stdout) == _NAMESPACES["namespaces"]
    assert result.stderr == ""
    assert call.await_args.args[2] == "catalog_namespaces_list"
    assert call.await_args.args[3] == {"id": _SID}


def test_namespaces_with_options(tmp_path: Path) -> None:
    result, call = _run(
        ["catalog", "namespaces", _SID, "--max-rows", "5", "--filter", "a>1"],
        _NAMESPACES,
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID, "max_rows": 5, "filters": ["a>1"]}


def test_namespaces_truncated_warns_on_stderr(tmp_path: Path) -> None:
    """`is_complete: false` warns on stderr; stdout stays the bare array."""
    payload = dict(_NAMESPACES, is_complete=False)
    result, _ = _run(["-o", "json", "catalog", "namespaces", _SID], payload, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.stdout) == _NAMESPACES["namespaces"]
    assert "truncated" in result.stderr


_SCHEMA = {
    "success": True,
    "id": _SID,
    "namespace": "Market",
    "table_name": "Trades",
    "schema": [],
    "column_count": 0,
}


def test_schema_single_table(tmp_path: Path) -> None:
    result, call = _run(
        ["catalog", "schema", _SID, "Market", "Trades"], _SCHEMA, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "catalog_table_schema"
    assert call.await_args.args[3] == {
        "id": _SID,
        "namespace": "Market",
        "table_name": "Trades",
    }


def test_schema_full_flag_is_removed(tmp_path: Path) -> None:
    """Sparse column_type is always included; no full mode."""
    result, _ = _run(
        ["catalog", "schema", _SID, "Market", "Trades", "--full"], _SCHEMA, tmp_path
    )
    assert result.exit_code == 2
    assert "no such option" in result.output.lower()


def test_sample_defaults(tmp_path: Path) -> None:
    result, call = _run(
        ["catalog", "sample", _SID, "Market", "Trades"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "catalog_table_sample"
    assert call.await_args.args[3] == {
        "id": _SID,
        "namespace": "Market",
        "table_name": "Trades",
        "head": True,
        "format": "json-row",
    }


def test_sample_options(tmp_path: Path) -> None:
    result, call = _run(
        [
            "catalog",
            "sample",
            _SID,
            "Market",
            "Trades",
            "--max-rows",
            "20",
            "--tail",
            "--filter",
            "p>0",
        ],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "id": _SID,
        "namespace": "Market",
        "table_name": "Trades",
        "head": False,
        "max_rows": 20,
        "filters": ["p>0"],
        "format": "json-row",
    }


def test_tables_truncated_warns_on_stderr(tmp_path: Path) -> None:
    """`is_complete: false` warns on stderr; stdout stays the bare array."""
    payload = dict(_TABLES, is_complete=False)
    result, _ = _run(["-o", "json", "catalog", "tables", _SID], payload, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.stdout) == _TABLES["tables"]
    assert "truncated" in result.stderr


def test_tables_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["catalog", "tables", _SID],
        {"success": False, "error": "not enterprise"},
        tmp_path,
    )
    assert result.exit_code == 3


def test_format_flag_is_removed(tmp_path: Path) -> None:
    """The tool's data encoding is not a user knob; -o owns presentation."""
    result, _ = _run(["catalog", "tables", _SID, "--format", "csv"], _TABLES, tmp_path)
    assert result.exit_code == 2
    assert "no such option" in result.output.lower()
