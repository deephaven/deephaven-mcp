"""Tests for ``deephaven_mcp.cli._commands.pq``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._main import cli

from .._helpers import fake_load_runtime, make_entry, make_runtime


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


def test_list(tmp_path: Path) -> None:
    result, call = _run(["pq", "list", "prod"], {"success": True, "pqs": []}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_list"
    assert call.await_args.args[3] == {"system": "prod"}


def test_details(tmp_path: Path) -> None:
    result, call = _run(["pq", "details", "123"], {"success": True}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_details"
    assert call.await_args.args[3] == {"id": "123"}


def test_name_to_id(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "name-to-id", "prod", "nightly"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_name_to_id"
    assert call.await_args.args[3] == {"system": "prod", "pq_name": "nightly"}


def test_create_builds_args(tmp_path: Path) -> None:
    result, call = _run(
        [
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--script-path",
            "/pq/n.py",
            "--jvm-arg",
            "-Xmx2g",
            "--schedule",
            "daily",
        ],
        {"success": True, "id": "999"},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_create"
    args = call.await_args.args[3]
    assert args["pq_name"] == "nightly"
    assert args["system"] == "prod"
    assert args["heap_size_gb"] == 4.0
    assert args["enabled"] is True
    assert args["script_path"] == "/pq/n.py"
    assert args["extra_jvm_args"] == ["-Xmx2g"]
    assert args["schedule"] == ["daily"]
    # Unset options are omitted so the controller defaults apply.
    assert "server" not in args


def test_modify_only_passes_given_fields(tmp_path: Path) -> None:
    result, call = _run(
        [
            "pq",
            "modify",
            "123",
            "--disabled",
            "--pq-name",
            "new",
            "--heap-size-gb",
            "8",
        ],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_modify"
    args = call.await_args.args[3]
    assert args["id"] == "123"
    assert args["enabled"] is False
    assert args["pq_name"] == "new"
    assert args["heap_size_gb"] == 8.0
    assert "server" not in args


def test_modify_restart_flag(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "modify", "123", "--restart"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[3]["restart"] is True


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--script-body", "print(1)", "--script-path", "/pq/n.py"],
        ["--auto-delete-timeout", "60", "--schedule", "daily"],
    ],
)
def test_create_rejects_mutually_exclusive_options(
    extra_args: list[str], tmp_path: Path
) -> None:
    result, call = _run(
        ["pq", "create", "n", "--system", "prod", "--heap-size-gb", "4", *extra_args],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    call.assert_not_awaited()


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--script-body", "print(1)", "--script-path", "/pq/n.py"],
        ["--auto-delete-timeout", "60", "--schedule", "daily"],
    ],
)
def test_modify_rejects_mutually_exclusive_options(
    extra_args: list[str], tmp_path: Path
) -> None:
    result, call = _run(
        ["pq", "modify", "123", *extra_args], {"success": True}, tmp_path
    )
    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    call.assert_not_awaited()


def test_delete_multiple_with_max_concurrent(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "delete", "1", "2", "--max-concurrent", "3"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_delete"
    assert call.await_args.args[3] == {"id": ["1", "2"], "max_concurrent": 3}


def test_delete_requires_an_id(tmp_path: Path) -> None:
    result, _ = _run(["pq", "delete"], {"success": True}, tmp_path)
    assert result.exit_code == 2
    assert "At least one ID" in result.output


@pytest.mark.parametrize(
    "verb,tool", [("start", "pq_start"), ("stop", "pq_stop"), ("restart", "pq_restart")]
)
def test_lifecycle_no_wait(verb: str, tool: str, tmp_path: Path) -> None:
    result, call = _run(["pq", verb, "1", "--no-wait"], {"success": True}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == tool
    assert call.await_args.args[3] == {"id": ["1"], "wait": False}


def test_lifecycle_default_wait_and_max_concurrent(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "start", "1", "2", "--max-concurrent", "2"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "id": ["1", "2"],
        "wait": True,
        "max_concurrent": 2,
    }


def test_lifecycle_requires_an_id(tmp_path: Path) -> None:
    result, _ = _run(["pq", "start"], {"success": True}, tmp_path)
    assert result.exit_code == 2


def test_list_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["pq", "list", "prod"], {"success": False, "error": "boom"}, tmp_path
    )
    assert result.exit_code == 3
