"""Tests for ``deephaven_mcp.cli._commands.session``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, ImageContent, TextContent

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import session as session_mod
from deephaven_mcp.cli._commands import shared as shared_mod
from deephaven_mcp.cli._commands.session import _payload
from deephaven_mcp.cli._daemon import (
    DaemonClientError,
    DaemonStartupTimeoutError,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._mcp_client import McpClientError
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import RegistryCorruptError

from .._helpers import fake_load_runtime, make_entry, make_runtime

_SESSION_ID = "community:community:my-session"

_SUCCESS_PAYLOAD = {
    "success": True,
    "auth_type": "PSK",
    "auth_token": "tok-123",
    "connection_url": "http://localhost:45123",
    "connection_url_with_auth": "http://localhost:45123/?psk=tok-123",
}


def _invoke(args: list[str], runtime: Runtime):
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args)


def _fake_client(result: CallToolResult) -> AsyncMock:
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.call_tool.return_value = result
    return fake


def _success_result() -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(_SUCCESS_PAYLOAD))],
        structuredContent=_SUCCESS_PAYLOAD,
    )


# ---------------------------------------------------------------------------
# _payload
# ---------------------------------------------------------------------------


def test_payload_prefers_structured_content() -> None:
    result = CallToolResult(
        content=[TextContent(type="text", text="ignored")],
        structuredContent={"success": True, "auth_token": "abc"},
    )
    assert _payload(result) == {"success": True, "auth_token": "abc"}


def test_payload_falls_back_to_json_text_block() -> None:
    result = CallToolResult(
        content=[TextContent(type="text", text=json.dumps({"success": False}))],
    )
    assert _payload(result) == {"success": False}


def test_payload_skips_non_json_and_non_dict_text_blocks() -> None:
    result = CallToolResult(
        content=[
            TextContent(type="text", text="not json"),
            TextContent(type="text", text="[1, 2, 3]"),
            TextContent(type="text", text=json.dumps({"ok": 1})),
        ],
    )
    assert _payload(result) == {"ok": 1}


def test_payload_ignores_non_text_blocks() -> None:
    result = CallToolResult(
        content=[
            ImageContent(type="image", data="Zg==", mimeType="image/png"),
            TextContent(type="text", text=json.dumps({"ok": 2})),
        ],
    )
    assert _payload(result) == {"ok": 2}


def test_payload_raises_when_no_dict_payload() -> None:
    result = CallToolResult(content=[])
    with pytest.raises(CliError) as excinfo:
        _payload(result)
    assert excinfo.value.code is ErrorCode.MCP_REQUEST_FAILED


# ---------------------------------------------------------------------------
# session credentials — success
# ---------------------------------------------------------------------------


def test_credentials_success_human(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = _fake_client(_success_result())
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 0
    assert "tok-123" in result.output
    assert "connection_url_with_auth" in result.output
    fake.call_tool.assert_awaited_once()
    args, _ = fake.call_tool.await_args
    assert args[0] == "session_community_credentials"
    assert args[1] == {"session_id": _SESSION_ID}


def test_credentials_success_json_excludes_success_key(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = _fake_client(_success_result())
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["-o", "json", "session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {
        "auth_type": "PSK",
        "auth_token": "tok-123",
        "connection_url": "http://localhost:45123",
        "connection_url_with_auth": "http://localhost:45123/?psk=tok-123",
    }
    assert "success" not in payload


def test_credentials_success_yaml(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = _fake_client(_success_result())
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["-o", "yaml", "session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 0
    assert "auth_token: tok-123" in result.output


# ---------------------------------------------------------------------------
# session credentials — tool-level failure (exit 3)
# ---------------------------------------------------------------------------


def test_credentials_gate_disabled_exits_3(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    disabled = {
        "success": False,
        "error": "Credential retrieval is disabled (mode='none').",
        "isError": True,
    }
    fake = _fake_client(
        CallToolResult(
            content=[TextContent(type="text", text=json.dumps(disabled))],
            structuredContent=disabled,
        )
    )
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 3


def test_credentials_failure_without_error_message_exits_3(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = _fake_client(
        CallToolResult(
            content=[TextContent(type="text", text=json.dumps({"success": False}))],
            structuredContent={"success": False},
        )
    )
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 3


def test_credentials_no_structured_payload_exits_2(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = _fake_client(CallToolResult(content=[]))
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# session credentials — daemon / transport failures (exit 2)
# ---------------------------------------------------------------------------


def test_credentials_daemon_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonClientError("nope")),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 2


def test_credentials_startup_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonStartupTimeoutError("slow")),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 2


def test_credentials_registry_corrupt(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        shared_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=RegistryCorruptError("bad json")),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 2
    # The corrupt-registry path renders this command's own recovery hint.
    assert "dh-mcp daemon reset" in result.output


def test_credentials_mcp_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("boom")
    with (
        patch.object(
            shared_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
        ),
        patch.object(session_mod, "McpClient", return_value=fake),
    ):
        result = _invoke(["session", "credentials", _SESSION_ID], rt)
    assert result.exit_code == 2


def test_credentials_requires_session_id(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result = _invoke(["session", "credentials"], rt)
    assert result.exit_code == 2
