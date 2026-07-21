"""Tests for ``deephaven_mcp.cli._commands.docs``."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent, Tool

from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import docs as docs_mod
from deephaven_mcp.cli._commands.docs import _parse_history
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._mcp_client import McpClientError, McpRequestTimeoutError
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.schema import DocsConfig

from .._helpers import fake_load_runtime, make_runtime

_DOCS_URL = DocsConfig().url
"""Effective docs endpoint for a default-config runtime (the prod default)."""


def _invoke(args: list[str], runtime: Runtime, *, standalone_mode: bool = True):
    runner = CliRunner()
    with patch.object(runtime_mod, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args, standalone_mode=standalone_mode)


def _error_code(result) -> str:
    """Return the raised ``CliError``'s stable code.

    The structured JSON error payload is rendered in ``main()``
    (``standalone_mode=False``), which ``CliRunner`` does not exercise;
    invoke with ``standalone_mode=False`` so the ``CliError`` propagates
    into ``result.exception`` and assert its code here.
    """
    return result.exception.code.value


def _fake_client(payload: dict | None = None) -> AsyncMock:
    """Return an entered-client mock whose call_tool returns ``payload``."""
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    if payload is not None:
        fake.call_tool.return_value = CallToolResult(
            content=[TextContent(type="text", text=json.dumps(payload))],
            structuredContent=payload,
        )
    return fake


# ---------------------------------------------------------------------------
# _parse_history
# ---------------------------------------------------------------------------


def test_parse_history_valid() -> None:
    raw = json.dumps(
        [
            {"role": "user", "content": "How do I aggregate?"},
            {"role": "assistant", "content": "Use agg_by."},
        ]
    )
    assert _parse_history(raw) == [
        {"role": "user", "content": "How do I aggregate?"},
        {"role": "assistant", "content": "Use agg_by."},
    ]


@pytest.mark.parametrize(
    "entry",
    [
        # 'system' role: the terminal validator accepts it.
        {"role": "system", "content": "x"},
        # Extra string-valued key: within the declared wire type.
        {"role": "user", "content": "x", "extra": "y"},
        # Missing 'role': wire-type-valid; requiredness is the docs
        # server's semantic check, not the CLI's.
        {"content": "x"},
    ],
)
def test_parse_history_forwards_semantics_to_server(entry: dict[str, str]) -> None:
    """Entry semantics pass through; only the wire type is checked here."""
    assert _parse_history(json.dumps([entry])) == [entry]


@pytest.mark.parametrize(
    "entry",
    [
        1,
        "not an object",
        ["role", "content"],
        {"role": "user", "content": 42},
        {"role": "user", "content": "x", "extra": 1},
    ],
)
def test_parse_history_rejects_wire_type_violations(entry: object) -> None:
    """Entries outside the declared list[dict[str, str]] fail fast, exit 2."""
    with pytest.raises(CliError) as exc:
        _parse_history(json.dumps([entry]))
    assert exc.value.code is ErrorCode.ARG_PARSE_ERROR
    assert "entry 0" in str(exc.value)


def test_parse_history_names_offending_index() -> None:
    """The error points at the first bad entry, not just the array."""
    raw = json.dumps([{"role": "user", "content": "ok"}, 7])
    with pytest.raises(CliError, match="entry 1"):
        _parse_history(raw)


def test_parse_history_invalid_json() -> None:
    with pytest.raises(CliError) as exc:
        _parse_history("not json")
    assert exc.value.code is ErrorCode.ARG_PARSE_ERROR


def test_parse_history_not_an_array() -> None:
    with pytest.raises(CliError) as exc:
        _parse_history('{"role": "user"}')
    assert exc.value.code is ErrorCode.ARG_PARSE_ERROR


# ---------------------------------------------------------------------------
# docs ask
# ---------------------------------------------------------------------------


def test_docs_ask_success(tmp_path: Path) -> None:
    """The wrapper calls docs_chat with the prompt and prints the response."""
    rt = make_runtime(tmp_path)
    fake = _fake_client({"success": True, "response": "Use natural_join()."})
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(["docs", "ask", "How do I join tables?"], rt)
    assert result.exit_code == 0
    fake.call_tool.assert_awaited_once_with(
        "docs_chat", {"prompt": "How do I join tables?"}
    )
    payload = json.loads(result.output)
    assert payload == {"response": "Use natural_join()."}


def test_docs_ask_forwards_optional_arguments(tmp_path: Path) -> None:
    """Every optional flag maps to its tool parameter by name."""
    rt = make_runtime(tmp_path)
    fake = _fake_client({"success": True, "response": "ok"})
    history = [{"role": "user", "content": "hi"}]
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(
            [
                "docs",
                "ask",
                "q",
                "--language",
                "python",
                "--core-version",
                "0.39.0",
                "--enterprise-version",
                "1.20240517.344",
                "--history",
                json.dumps(history),
            ],
            rt,
        )
    assert result.exit_code == 0
    fake.call_tool.assert_awaited_once_with(
        "docs_chat",
        {
            "prompt": "q",
            "programming_language": "python",
            "deephaven_core_version": "0.39.0",
            "deephaven_enterprise_version": "1.20240517.344",
            "history": history,
        },
    )


def test_docs_ask_tool_failure_exits_3(tmp_path: Path) -> None:
    """A success=False payload maps to tool_returned_error (exit 3)."""
    rt = make_runtime(tmp_path)
    fake = _fake_client(
        {"success": False, "error": "OpenAIClientError: boom", "isError": True}
    )
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(["docs", "ask", "q"], rt, standalone_mode=False)
    assert _error_code(result) == "tool_returned_error"
    assert result.exception.code.exit_code == 3
    assert "OpenAIClientError" in str(result.exception)


def test_docs_ask_unreachable_exits_2(tmp_path: Path) -> None:
    """A transport failure maps to mcp_request_failed and names the URL."""
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("connect refused")
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(["docs", "ask", "q"], rt, standalone_mode=False)
    assert _error_code(result) == "mcp_request_failed"
    assert result.exception.code.exit_code == 2
    assert _DOCS_URL in str(result.exception)


def test_docs_ask_timeout_exits_2(tmp_path: Path) -> None:
    """A request timeout maps to mcp_request_timeout."""
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.call_tool.side_effect = McpRequestTimeoutError("timed out after 120 seconds")
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(["docs", "ask", "q"], rt, standalone_mode=False)
    assert _error_code(result) == "mcp_request_timeout"
    assert result.exception.code.exit_code == 2


def test_docs_ask_stalled_connect_times_out_exits_2(tmp_path: Path) -> None:
    """The budget bounds the whole operation, not just the tool call.

    Regression: the per-request timeout starts inside ``call_tool``, so
    a server that accepted the connection but stalled during MCP
    initialization used to hang ``docs ask`` past the configured budget.
    """
    rt = make_runtime(tmp_path)
    fake = AsyncMock()

    async def _stall() -> None:
        await asyncio.sleep(30)

    fake.__aenter__.side_effect = _stall
    fake.__aexit__.return_value = None
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(
            ["--timeout", "1", "docs", "ask", "q"], rt, standalone_mode=False
        )
    assert _error_code(result) == "mcp_request_timeout"
    assert result.exception.code.exit_code == 2
    assert "including connect and initialize" in str(result.exception)
    assert "docs.timeouts.request_seconds" in str(result.exception)


def test_docs_ask_bad_history_exits_2(tmp_path: Path) -> None:
    """Malformed --history fails before any connection is attempted."""
    rt = make_runtime(tmp_path)
    with patch.object(docs_mod, "_docs_client") as mock_client:
        result = _invoke(
            ["docs", "ask", "q", "--history", "not json"],
            rt,
            standalone_mode=False,
        )
    assert _error_code(result) == "arg_parse_error"
    mock_client.assert_not_called()


def test_docs_ask_rejects_unknown_language(tmp_path: Path) -> None:
    """--language is a closed choice; click rejects other values."""
    rt = make_runtime(tmp_path)
    result = _invoke(["docs", "ask", "q", "--language", "javascript"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# docs status
# ---------------------------------------------------------------------------


def test_docs_status_success(tmp_path: Path) -> None:
    """Reports the effective URL, reachability, tool names, and latency."""
    rt = make_runtime(tmp_path)
    fake = _fake_client()
    fake.list_tools.return_value = [
        Tool(name="docs_chat", description="d", inputSchema={"type": "object"}),
    ]
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(["docs", "status"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["url"] == _DOCS_URL
    assert payload["reachable"] is True
    assert payload["tools"] == ["docs_chat"]
    assert isinstance(payload["latency_ms"], int)


def test_docs_status_unreachable_exits_2(tmp_path: Path) -> None:
    """An unreachable server maps to mcp_request_failed and names the URL."""
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("dns failure")
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(["docs", "status"], rt, standalone_mode=False)
    assert _error_code(result) == "mcp_request_failed"
    assert result.exception.code.exit_code == 2
    assert _DOCS_URL in str(result.exception)


def test_docs_status_stalled_probe_times_out_exits_2(tmp_path: Path) -> None:
    """A server that connects but stalls is bounded by the request timeout.

    Regression: ``list_tools`` applies no read timeout, so the probe
    used to hang past the configured budget; the whole probe is now
    bounded and expiry maps to ``mcp_request_timeout``.
    """
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None

    async def _stall() -> None:
        await asyncio.sleep(30)

    fake.list_tools = AsyncMock(side_effect=_stall)
    with patch.object(docs_mod, "_docs_client", return_value=fake):
        result = _invoke(
            ["--timeout", "1", "docs", "status"], rt, standalone_mode=False
        )
    assert _error_code(result) == "mcp_request_timeout"
    assert result.exception.code.exit_code == 2
    assert "docs.timeouts.request_seconds" in str(result.exception)


# ---------------------------------------------------------------------------
# _docs_client wiring
# ---------------------------------------------------------------------------


def test_docs_client_reads_url_and_timeout_from_config(tmp_path: Path) -> None:
    """The client is built from docs.url and docs.timeouts.request_seconds."""
    rt = make_runtime(tmp_path)
    client = docs_mod._docs_client(rt)
    assert client._url == _DOCS_URL  # noqa: SLF001
    assert client._timeout.total_seconds() == 120  # noqa: SLF001
    assert client._headers == {}  # noqa: SLF001
    assert client._timeout_setting == "docs.timeouts.request_seconds"  # noqa: SLF001
