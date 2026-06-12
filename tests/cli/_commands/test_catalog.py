"""Tests for ``deephaven_mcp.cli._commands.catalog``."""

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
        patch.object(_main, "load_runtime", fake_load_runtime(rt)),
    ):
        return CliRunner().invoke(cli, args), call


def test_tables_minimal(tmp_path: Path) -> None:
    result, call = _run(["catalog", "tables", _SID], {"success": True}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == "catalog_tables_list"
    assert call.await_args.args[3] == {"session_id": _SID, "format": "json-row"}


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
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "session_id": _SID,
        "max_rows": 5,
        "filters": ["a>1", "b<2"],
        "format": "json-row",
    }


def test_namespaces(tmp_path: Path) -> None:
    result, call = _run(["catalog", "namespaces", _SID], {"success": True}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == "catalog_namespaces_list"


def test_schema_with_namespace_and_names(tmp_path: Path) -> None:
    result, call = _run(
        [
            "catalog",
            "schema",
            _SID,
            "Trades",
            "--namespace",
            "Market",
            "--filter",
            "x",
            "--max-tables",
            "10",
        ],
        {"success": True, "schemas": []},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "session_id": _SID,
        "table_names": ["Trades"],
        "namespace": "Market",
        "filters": ["x"],
        "max_tables": 10,
    }


def test_schema_minimal(tmp_path: Path) -> None:
    result, call = _run(
        ["catalog", "schema", _SID], {"success": True, "schemas": []}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"session_id": _SID}


def test_sample_defaults(tmp_path: Path) -> None:
    result, call = _run(
        ["catalog", "sample", _SID, "Market", "Trades"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "catalog_table_sample"
    assert call.await_args.args[3] == {
        "session_id": _SID,
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
        "session_id": _SID,
        "namespace": "Market",
        "table_name": "Trades",
        "head": False,
        "max_rows": 20,
        "filters": ["p>0"],
        "format": "json-row",
    }


def test_tables_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["catalog", "tables", _SID],
        {"success": False, "error": "not enterprise"},
        tmp_path,
    )
    assert result.exit_code == 3


def test_format_flag_is_removed(tmp_path: Path) -> None:
    """The tool's data encoding is not a user knob; -o owns presentation."""
    result, _ = _run(
        ["catalog", "tables", _SID, "--format", "csv"], {"success": True}, tmp_path
    )
    assert result.exit_code == 2
    assert "no such option" in result.output.lower()
