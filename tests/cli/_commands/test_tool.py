"""Tests for ``deephaven_mcp.cli._commands.tool``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent, Tool

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import shared as shared_mod
from deephaven_mcp.cli._commands import tool as tool_mod
from deephaven_mcp.cli._commands.tool import _parse_arg_pair
from deephaven_mcp.cli._daemon import (
    DaemonClientError,
    DaemonStartupTimeoutError,
)
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._mcp_client import McpClientError
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import RegistryCorruptError

from .._helpers import fake_load_runtime, make_entry, make_runtime


def _invoke(args: list[str], runtime: Runtime):
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args)


# ---------------------------------------------------------------------------
# parse_arg_pair
# ---------------------------------------------------------------------------


def test_parse_arg_pair_string_value() -> None:
    assert _parse_arg_pair("k=hello") == ("k", "hello")


def test_parse_arg_pair_json_int() -> None:
    assert _parse_arg_pair("k=42") == ("k", 42)


def test_parse_arg_pair_json_object() -> None:
    assert _parse_arg_pair('k={"x":1}') == ("k", {"x": 1})


def test_parse_arg_pair_missing_eq_raises() -> None:
    with pytest.raises(ValueError, match="key=value"):
        _parse_arg_pair("nokey")


def test_parse_arg_pair_empty_key_raises() -> None:
    with pytest.raises(ValueError, match="empty key"):
        _parse_arg_pair("=oops")


# ---------------------------------------------------------------------------
# tool list
# ---------------------------------------------------------------------------


def test_tool_list_hides_private_by_default(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.list_tools.return_value = [
        Tool(name="public", description="d", inputSchema={"type": "object"}),
        Tool(name="_private", description="hidden", inputSchema={"type": "object"}),
    ]
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "list"], rt)
    assert result.exit_code == 0
    assert "public" in result.output
    assert "_private" not in result.output


def test_tool_list_all_includes_private(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.list_tools.return_value = [
        Tool(name="_private", description="hidden", inputSchema={"type": "object"}),
    ]
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "list", "--all"], rt)
    assert result.exit_code == 0
    assert "_private" in result.output


def test_tool_list_daemon_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonClientError("nope")),
    ):
        result = _invoke(["tool", "list"], rt)
    assert result.exit_code == 2


def test_tool_list_startup_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonStartupTimeoutError("slow")),
    ):
        result = _invoke(["tool", "list"], rt)
    assert result.exit_code == 2


def test_tool_list_mcp_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("boom")
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "list"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# tool show
# ---------------------------------------------------------------------------


def test_tool_show_returns_metadata_in_json(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.list_tools.return_value = [
        Tool(name="foo", description="bar", inputSchema={"type": "object"}),
    ]
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["-o", "json", "tool", "show", "foo"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["name"] == "foo"


def test_tool_show_human_output(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.list_tools.return_value = [
        Tool(
            name="foo",
            description="bar",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "show", "foo"], rt)
    assert result.exit_code == 0
    assert "foo" in result.output
    assert "Input schema:" in result.output


def test_tool_show_unknown_name(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.list_tools.return_value = [
        Tool(name="other", description="x", inputSchema={"type": "object"}),
    ]
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "show", "missing"], rt)
    assert result.exit_code == 2


def test_tool_show_daemon_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonClientError("nope")),
    ):
        result = _invoke(["tool", "show", "foo"], rt)
    assert result.exit_code == 2


def test_tool_show_startup_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonStartupTimeoutError("slow")),
    ):
        result = _invoke(["tool", "show", "foo"], rt)
    assert result.exit_code == 2


def test_tool_show_mcp_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("boom")
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "show", "foo"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# tool call
# ---------------------------------------------------------------------------


def test_tool_call_success(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.call_tool.return_value = CallToolResult(
        content=[TextContent(type="text", text="ok")]
    )
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "call", "foo", "--arg", "k=42"], rt)
    assert result.exit_code == 0
    fake.call_tool.assert_awaited_once()
    args, _ = fake.call_tool.await_args
    assert args[0] == "foo"
    assert args[1] == {"k": 42}


def test_tool_call_returns_3_on_tool_error(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.call_tool.return_value = CallToolResult(
        content=[TextContent(type="text", text="bad")], isError=True
    )
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "call", "foo"], rt)
    assert result.exit_code == 3


def test_tool_call_bad_arg(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result = _invoke(["tool", "call", "foo", "--arg", "nokey"], rt)
    assert result.exit_code == 2


def test_tool_call_daemon_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonClientError("nope")),
    ):
        result = _invoke(["tool", "call", "foo"], rt)
    assert result.exit_code == 2


def test_tool_call_startup_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonStartupTimeoutError("slow")),
    ):
        result = _invoke(["tool", "call", "foo"], rt)
    assert result.exit_code == 2


def test_tool_call_mcp_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("boom")
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(tool_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["tool", "call", "foo"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# error-code accuracy
# ---------------------------------------------------------------------------


def test_tool_list_registry_corrupt(tmp_path: Path) -> None:
    """The corrupt-registry path is reachable from a tool verb.

    Confirms the previously-undocumented daemon_registry_corrupt path is
    real: a RegistryCorruptError from the shared acquire helper surfaces as
    an exit-2 CliError carrying the corrupt-registry recovery hint. (CliRunner
    runs standalone_mode, so the structured error_code JSON is not rendered;
    the recovery message is the observable signature of this path.)
    """
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=RegistryCorruptError("bad json")),
    ):
        result = _invoke(["tool", "list"], rt)
    assert result.exit_code == 2
    assert "to recover" in result.output.lower()
    assert "dh-mcp tool list" in result.output


@pytest.mark.parametrize(
    "verb,extra_codes",
    [
        ("list", ()),
        ("show", ("tool_not_found",)),
        ("call", ("arg_parse_error", "tool_returned_error")),
    ],
)
def test_tool_help_documents_full_error_code_set(
    verb: str, extra_codes: tuple[str, ...]
) -> None:
    """Every tool verb documents the full shared _acquire error set plus its own.

    Regression test for the drift where daemon_registry_corrupt (and, for
    show/call, daemon_startup_timeout) were omitted despite being reachable
    through the shared _acquire path.
    """
    help_text = tool_mod.tool.commands[verb].help or ""
    for code in (
        "daemon_not_running",
        "daemon_startup_timeout",
        "daemon_registry_corrupt",
        "mcp_request_failed",
        *extra_codes,
    ):
        assert code in help_text, f"tool {verb}: missing error code {code!r}"
