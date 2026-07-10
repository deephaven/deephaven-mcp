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


def _run(args: list[str], payload: dict, tmp_path: Path, input: str | None = None):
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
        return CliRunner().invoke(cli, args, input=input), call


_PQS = [
    {"id": "enterprise:prod:12345", "serial": 12345, "name": "analytics"},
    {"id": "enterprise:prod:67890", "serial": 67890, "name": "reporting"},
]


def test_list(tmp_path: Path) -> None:
    result, call = _run(
        ["-o", "json", "pq", "list", "prod"],
        {"success": True, "system": "prod", "pqs": _PQS},
        tmp_path,
    )
    assert result.exit_code == 0
    # stdout is the bare pqs array, not the tool envelope.
    assert json.loads(result.output) == _PQS
    assert call.await_args.args[2] == "pq_list"
    assert call.await_args.args[3] == {"system": "prod"}


def test_list_empty(tmp_path: Path) -> None:
    result, _ = _run(
        ["-o", "json", "pq", "list", "prod"], {"success": True, "pqs": []}, tmp_path
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == []


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
            "--git-script-path",
            "IrisQueries/py/n.py",
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
    assert args["script_path"] == "IrisQueries/py/n.py"
    assert args["extra_jvm_args"] == ["-Xmx2g"]
    assert args["schedule"] == ["daily"]
    # Unset options are omitted so the controller defaults apply.
    assert "server" not in args


def test_create_script_body_path_reads_file_client_side(tmp_path: Path) -> None:
    script_file = tmp_path / "n.py"
    script_file.write_text("print('from file')\n")
    result, call = _run(
        [
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--script-body-path",
            str(script_file),
        ],
        {"success": True, "id": "999"},
        tmp_path,
    )
    assert result.exit_code == 0
    args = call.await_args.args[3]
    # The local file is materialized into script_body; the client-only
    # script_body_path key is never forwarded.
    assert args["script_body"] == "print('from file')\n"
    assert "script_body_path" not in args
    assert "script_path" not in args


def test_modify_script_body_path_stdin(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "modify", "123", "--script-body-path", "-"],
        {"success": True},
        tmp_path,
        input="print('from stdin')\n",
    )
    assert result.exit_code == 0
    args = call.await_args.args[3]
    assert args["script_body"] == "print('from stdin')\n"
    assert "script_body_path" not in args


def test_create_script_body_path_unreadable_exits_2(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.py"
    result, call = _run(
        [
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--script-body-path",
            str(missing),
        ],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 2
    assert "Could not read script file" in result.output
    call.assert_not_awaited()


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
        ["--script-body", "print(1)", "--git-script-path", "IrisQueries/py/n.py"],
        ["--script-body", "print(1)", "--script-body-path", "/tmp/n.py"],
        ["--script-body-path", "/tmp/n.py", "--git-script-path", "IrisQueries/py/n.py"],
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
        ["--script-body", "print(1)", "--git-script-path", "IrisQueries/py/n.py"],
        ["--script-body", "print(1)", "--script-body-path", "/tmp/n.py"],
        ["--script-body-path", "/tmp/n.py", "--git-script-path", "IrisQueries/py/n.py"],
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
