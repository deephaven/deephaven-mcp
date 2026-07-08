"""Tests for ``deephaven_mcp.cli._commands._wrapping``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from mcp.types import CallToolResult, ImageContent, TextContent

from deephaven_mcp.cli._commands import _wrapping
from deephaven_mcp.cli._commands._wrapping import (
    acquire,
    call_and_echo,
    call_and_echo_field,
    call_and_echo_table,
    call_for_payload,
    call_tool,
    echo_payload,
    parse_key_value,
    require_success,
    tool_payload,
    wrapper_error_codes,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._mcp_client import McpClientError, McpRequestTimeoutError
from deephaven_mcp.daemon_registry import RegistryCorruptError

from .._helpers import make_entry, make_runtime

# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_forwards_config_and_recovery_hint(tmp_path: Path) -> None:
    """acquire delegates to acquire_daemon with this command's wiring.

    The error-to-ErrorCode mapping itself lives in (and is tested via)
    ``shared.acquire_daemon``; here we confirm ``acquire`` passes the
    CLI's ``auto_start`` config, the ``DAEMON_NOT_RUNNING`` client code,
    and an ``on_registry_corrupt`` callback that renders this command's
    own recovery hint.
    """
    rt = make_runtime(tmp_path)
    captured: dict[str, object] = {}

    async def fake_acquire_daemon(
        runtime, *, auto_start, client_error_code, on_registry_corrupt
    ):
        captured["auto_start"] = auto_start
        captured["code"] = client_error_code
        captured["on_corrupt"] = on_registry_corrupt
        return make_entry()

    with patch.object(_wrapping, "acquire_daemon", fake_acquire_daemon):
        entry = await acquire(rt, retry_command="dh-mcp system list")

    assert entry.port == 9999
    assert captured["auto_start"] is True
    assert captured["code"] is ErrorCode.DAEMON_NOT_RUNNING
    err = captured["on_corrupt"](RegistryCorruptError("bad json"))  # type: ignore[operator]
    assert err.code is ErrorCode.DAEMON_REGISTRY_CORRUPT
    assert "dh-mcp daemon repair" in err.message
    assert "dh-mcp system list" in err.message


# ---------------------------------------------------------------------------
# call_tool
# ---------------------------------------------------------------------------


def _fake_client(result: CallToolResult) -> AsyncMock:
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    fake.call_tool.return_value = result
    return fake


@pytest.mark.asyncio
async def test_call_tool_success(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    expected = CallToolResult(content=[])
    fake = _fake_client(expected)
    with patch.object(_wrapping, "McpClient", return_value=fake):
        result = await call_tool(make_entry(), rt, "list_systems", {})
    assert result is expected
    fake.call_tool.assert_awaited_once_with("list_systems", {})


@pytest.mark.asyncio
async def test_call_tool_transport_error_maps_to_mcp_request_failed(
    tmp_path: Path,
) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpClientError("boom")
    with patch.object(_wrapping, "McpClient", return_value=fake):
        with pytest.raises(CliError) as exc:
            await call_tool(make_entry(), rt, "list_systems", {})
    assert exc.value.code is ErrorCode.MCP_REQUEST_FAILED


@pytest.mark.asyncio
async def test_call_tool_timeout_maps_to_mcp_request_timeout(
    tmp_path: Path,
) -> None:
    rt = make_runtime(tmp_path)
    fake = AsyncMock()
    fake.__aenter__.side_effect = McpRequestTimeoutError("timed out after 60 seconds")
    with patch.object(_wrapping, "McpClient", return_value=fake):
        with pytest.raises(CliError) as exc:
            await call_tool(make_entry(), rt, "pq_delete", {})
    assert exc.value.code is ErrorCode.MCP_REQUEST_TIMEOUT
    assert "timed out" in exc.value.message


# ---------------------------------------------------------------------------
# tool_payload
# ---------------------------------------------------------------------------


def test_tool_payload_prefers_structured_content() -> None:
    result = CallToolResult(
        content=[TextContent(type="text", text="ignored")],
        structuredContent={"success": True, "systems": []},
    )
    assert tool_payload(result) == {"success": True, "systems": []}


def test_tool_payload_falls_back_to_json_text_block() -> None:
    result = CallToolResult(
        content=[TextContent(type="text", text=json.dumps({"success": False}))],
    )
    assert tool_payload(result) == {"success": False}


def test_tool_payload_skips_non_json_and_non_dict_blocks() -> None:
    result = CallToolResult(
        content=[
            TextContent(type="text", text="not json"),
            TextContent(type="text", text="[1, 2, 3]"),
            ImageContent(type="image", data="Zg==", mimeType="image/png"),
            TextContent(type="text", text=json.dumps({"ok": 1})),
        ],
    )
    assert tool_payload(result) == {"ok": 1}


def test_tool_payload_raises_when_no_dict_payload() -> None:
    with pytest.raises(CliError) as exc:
        tool_payload(CallToolResult(content=[]))
    assert exc.value.code is ErrorCode.MCP_REQUEST_FAILED


# ---------------------------------------------------------------------------
# require_success
# ---------------------------------------------------------------------------


def test_require_success_strips_bookkeeping_keys() -> None:
    payload = {"success": True, "isError": False, "systems": [1, 2]}
    assert require_success(payload, tool="list_systems") == {"systems": [1, 2]}


def test_require_success_failure_with_error_message() -> None:
    with pytest.raises(CliError) as exc:
        require_success({"success": False, "error": "disabled"}, tool="t")
    assert exc.value.code is ErrorCode.TOOL_RETURNED_ERROR
    assert "disabled" in exc.value.message


def test_require_success_failure_without_error_message() -> None:
    with pytest.raises(CliError) as exc:
        require_success({"success": False}, tool="some_tool")
    assert exc.value.code is ErrorCode.TOOL_RETURNED_ERROR
    assert "some_tool" in exc.value.message


# ---------------------------------------------------------------------------
# parse_key_value
# ---------------------------------------------------------------------------


def test_parse_key_value_string_mode_keeps_raw_value() -> None:
    assert parse_key_value("LOG=42", decode_json=False) == ("LOG", "42")


def test_parse_key_value_json_mode_decodes() -> None:
    assert parse_key_value("n=42", decode_json=True) == ("n", 42)


def test_parse_key_value_json_mode_falls_back_to_string() -> None:
    assert parse_key_value("s=hi", decode_json=True) == ("s", "hi")


def test_parse_key_value_value_may_contain_equals() -> None:
    assert parse_key_value("k=a=b", decode_json=False) == ("k", "a=b")


def test_parse_key_value_no_equals_raises() -> None:
    with pytest.raises(CliError) as exc:
        parse_key_value("noeq", decode_json=False)
    assert exc.value.code is ErrorCode.ARG_PARSE_ERROR


def test_parse_key_value_empty_key_raises() -> None:
    with pytest.raises(CliError) as exc:
        parse_key_value("=v", decode_json=False)
    assert exc.value.code is ErrorCode.ARG_PARSE_ERROR


# ---------------------------------------------------------------------------
# wrapper_error_codes
# ---------------------------------------------------------------------------


def test_wrapper_error_codes_prepends_tool_returned_error_by_default() -> None:
    """The default set is the no-tool-error set with TOOL_RETURNED_ERROR in front."""
    base = wrapper_error_codes(tool_error=False)
    assert wrapper_error_codes() == (ErrorCode.TOOL_RETURNED_ERROR, *base)


def test_wrapper_error_codes_base_set_excludes_tool_returned_error() -> None:
    assert ErrorCode.TOOL_RETURNED_ERROR not in wrapper_error_codes(tool_error=False)


def test_wrapper_error_codes_includes_timeout_by_default() -> None:
    assert ErrorCode.MCP_REQUEST_TIMEOUT in wrapper_error_codes()


def test_wrapper_error_codes_request_timeout_false_excludes_timeout() -> None:
    """list_tools-only verbs (tool list/show) cannot hit the request timeout."""
    codes = wrapper_error_codes(tool_error=False, request_timeout=False)
    assert ErrorCode.MCP_REQUEST_TIMEOUT not in codes
    assert ErrorCode.MCP_REQUEST_FAILED in codes


# ---------------------------------------------------------------------------
# call_for_payload / echo_payload / call_and_echo
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_for_payload_composes_fetch_and_returns_payload(
    tmp_path: Path,
) -> None:
    """The fetch half: acquire + call_tool + require_success, returning payload."""
    rt = make_runtime(tmp_path)
    result = CallToolResult(
        content=[], structuredContent={"success": True, "systems": [1, 2]}
    )
    with (
        patch.object(_wrapping, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(_wrapping, "call_tool", AsyncMock(return_value=result)) as call,
    ):
        payload = await call_for_payload(
            rt, "list_systems", retry_command="dh-mcp system list", arguments={"x": 1}
        )
    assert payload == {"systems": [1, 2]}
    assert call.await_args.args[2] == "list_systems"
    assert call.await_args.args[3] == {"x": 1}


@pytest.mark.asyncio
async def test_call_for_payload_failure_exits_3(tmp_path: Path) -> None:
    """A ``success=False`` payload raises the exit-3 tool error."""
    rt = make_runtime(tmp_path)
    result = CallToolResult(
        content=[], structuredContent={"success": False, "error": "nope"}
    )
    with (
        patch.object(_wrapping, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(_wrapping, "call_tool", AsyncMock(return_value=result)),
    ):
        with pytest.raises(CliError) as exc:
            await call_for_payload(rt, "t", retry_command="dh-mcp x", arguments={})
    assert exc.value.code is ErrorCode.TOOL_RETURNED_ERROR


def test_echo_payload_renders_in_configured_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """echo_payload prints the value via format_output in the runtime's mode."""
    echo_payload(make_runtime(tmp_path, output_format="human"), {"a": 1, "b": 2})
    out = capsys.readouterr().out
    assert "a: 1" in out
    assert "b: 2" in out


def test_echo_payload_forwards_empty_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """echo_payload forwards empty_message to format_output for an empty list."""
    echo_payload(
        make_runtime(tmp_path, output_format="human"),
        [],
        empty_message="(nothing here)",
    )
    assert capsys.readouterr().out.strip() == "(nothing here)"


def test_echo_payload_forwards_sort_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """echo_payload forwards sort_keys to format_output.

    The default sorts object keys alphabetically; ``sort_keys=False`` preserves
    insertion order, which the daemon-reporting commands rely on.
    """
    rt = make_runtime(tmp_path, output_format="json")

    echo_payload(rt, {"b": 1, "a": 2})
    sorted_out = capsys.readouterr().out
    assert sorted_out.index('"a"') < sorted_out.index('"b"')

    echo_payload(rt, {"b": 1, "a": 2}, sort_keys=False)
    insertion_out = capsys.readouterr().out
    assert insertion_out.index('"b"') < insertion_out.index('"a"')


def test_echo_payload_human_exclude_drops_keys_in_human_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human_exclude drops the named dict keys in human mode."""
    echo_payload(
        make_runtime(tmp_path, output_format="human"),
        {"keep": 1, "drop": 2},
        human_exclude=("drop",),
    )
    out = capsys.readouterr().out
    assert "keep: 1" in out
    assert "drop" not in out


def test_echo_payload_human_exclude_ignored_in_json_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human_exclude is a no-op outside human mode; json keeps every key."""
    echo_payload(
        make_runtime(tmp_path, output_format="json"),
        {"keep": 1, "drop": 2},
        human_exclude=("drop",),
    )
    assert json.loads(capsys.readouterr().out) == {"keep": 1, "drop": 2}


def test_echo_payload_human_exclude_ignored_for_non_dict(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human_exclude only filters dicts; a list value passes through unchanged."""
    echo_payload(
        make_runtime(tmp_path, output_format="human"),
        ["drop", "keep"],
        human_exclude=("drop",),
    )
    out = capsys.readouterr().out
    assert "drop" in out
    assert "keep" in out


# ---------------------------------------------------------------------------
# call_and_echo_table
# ---------------------------------------------------------------------------

# The tools emit their envelope keys in reading order (identity, summary,
# format, schema, data); these payloads mirror that so the tests exercise
# call_and_echo_table's job: preserve that order and drop ``format`` in human.
_ORDERED_TABLE_PAYLOAD = {
    "success": True,
    "namespace": "Correlation",
    "table_name": "EndOfDay",
    "row_count": 1,
    "is_complete": True,
    "format": "json-row",
    "schema": [{"name": "c", "type": "int"}],
    "data": [{"c": 1}],
}


@pytest.mark.asyncio
async def test_call_and_echo_table_drops_format_and_keeps_order_in_human_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human mode drops the constant format field and preserves reading order."""
    rt = make_runtime(tmp_path, output_format="human")
    result = CallToolResult(content=[], structuredContent=dict(_ORDERED_TABLE_PAYLOAD))
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_table(
            rt,
            "catalog_table_sample",
            retry_command="dh-mcp catalog sample",
            arguments={},
        )
    out = capsys.readouterr().out
    assert "format" not in out
    assert out.index("namespace") < out.index("row_count") < out.index("data")


@pytest.mark.asyncio
async def test_call_and_echo_table_keeps_format_and_order_in_json_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """json mode keeps format and preserves the reading order (sort_keys=False)."""
    rt = make_runtime(tmp_path, output_format="json")
    result = CallToolResult(content=[], structuredContent=dict(_ORDERED_TABLE_PAYLOAD))
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_table(
            rt, "session_table_data", retry_command="dh-mcp table data", arguments={}
        )
    raw = capsys.readouterr().out
    # require_success strips success/isError; format and all data survive.
    emitted = json.loads(raw)
    assert emitted["format"] == "json-row"
    assert emitted["data"] == [{"c": 1}]
    # sort_keys=False carries the reading order into json, not alphabetical.
    assert raw.index('"namespace"') < raw.index('"row_count"') < raw.index('"data"')


# The list tools (catalog tables/namespaces) emit a ``columns`` key instead of
# ``schema``; ``columns`` merely restates the rendered data table's headers.
_LIST_TABLE_PAYLOAD = {
    "success": True,
    "id": "enterprise:dev:1",
    "row_count": 2,
    "is_complete": True,
    "format": "json-row",
    "columns": [{"name": "Namespace", "type": "string"}],
    "data": [{"Namespace": "Correlation"}, {"Namespace": "Chip"}],
}


@pytest.mark.asyncio
async def test_call_and_echo_table_drops_columns_in_human_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human mode drops the redundant columns block (and format) for list tools."""
    rt = make_runtime(tmp_path, output_format="human")
    result = CallToolResult(content=[], structuredContent=dict(_LIST_TABLE_PAYLOAD))
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_table(
            rt,
            "catalog_namespaces_list",
            retry_command="dh-mcp catalog namespaces",
            arguments={},
        )
    out = capsys.readouterr().out
    assert "columns" not in out
    assert "format" not in out
    # The row-count summary and the data itself remain.
    assert "row_count" in out
    assert "Correlation" in out


@pytest.mark.asyncio
async def test_call_and_echo_table_keeps_columns_in_json_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """json mode keeps columns for machine consumers."""
    rt = make_runtime(tmp_path, output_format="json")
    result = CallToolResult(content=[], structuredContent=dict(_LIST_TABLE_PAYLOAD))
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_table(
            rt,
            "catalog_namespaces_list",
            retry_command="dh-mcp catalog namespaces",
            arguments={},
        )
    emitted = json.loads(capsys.readouterr().out)
    assert emitted["columns"] == [{"name": "Namespace", "type": "string"}]
    assert emitted["format"] == "json-row"


@pytest.mark.asyncio
async def test_call_and_echo_fetches_then_prints_whole_payload(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """call_and_echo is call_for_payload composed with echo_payload."""
    rt = make_runtime(tmp_path, output_format="human")
    result = CallToolResult(content=[], structuredContent={"success": True, "count": 1})
    with (
        patch.object(_wrapping, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(_wrapping, "call_tool", AsyncMock(return_value=result)) as call,
    ):
        await call_and_echo(
            rt, "list_systems", retry_command="dh-mcp system list", arguments={}
        )
    assert "count: 1" in capsys.readouterr().out
    assert call.await_args.args[2] == "list_systems"


@pytest.mark.asyncio
async def test_call_and_echo_forwards_sort_keys_and_human_exclude(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """call_and_echo passes sort_keys and human_exclude through to echo_payload."""
    rt = make_runtime(tmp_path, output_format="human")
    result = CallToolResult(
        content=[], structuredContent={"success": True, "keep": 1, "drop": 2}
    )
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo(
            rt,
            "t",
            retry_command="dh-mcp x",
            arguments={},
            sort_keys=False,
            human_exclude=("drop",),
        )
    out = capsys.readouterr().out
    assert "keep: 1" in out
    assert "drop" not in out


def _patched_call(result: CallToolResult):
    return (
        patch.object(_wrapping, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(_wrapping, "call_tool", AsyncMock(return_value=result)),
    )


@pytest.mark.asyncio
async def test_call_and_echo_field_emits_field_and_asserts_call(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The shaping helper echoes one field on stdout; no diagnostics → no stderr."""
    rt = make_runtime(tmp_path)
    result = CallToolResult(
        content=[], structuredContent={"success": True, "systems": [{"name": "prod"}]}
    )
    acq, call = _patched_call(result)
    with acq, call as call_mock:
        await call_and_echo_field(
            rt,
            "list_systems",
            retry_command="dh-mcp system list",
            arguments={"x": 1},
            field="systems",
            default=[],
        )
    captured = capsys.readouterr()
    assert call_mock.await_args.args[2] == "list_systems"
    assert call_mock.await_args.args[3] == {"x": 1}
    assert "prod" in captured.out
    assert captured.err == ""


@pytest.mark.asyncio
async def test_call_and_echo_field_uses_default_when_field_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rt = make_runtime(tmp_path, output_format="human")
    result = CallToolResult(content=[], structuredContent={"success": True})
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_field(
            rt, "t", retry_command="dh-mcp x", arguments={}, field="missing", default=[]
        )
    assert capsys.readouterr().out.strip() == "(none)"


@pytest.mark.asyncio
async def test_call_and_echo_field_surfaces_partial_result_with_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A success payload's `partial_result` errors go to stderr, not stdout."""
    rt = make_runtime(tmp_path, output_format="human")
    payload = {
        "success": True,
        "sessions": [{"id": "community:community:dev"}],
        "partial_result": {
            "phase": "completed",
            "detail": "Some enterprise systems had connection issues during discovery.",
            "errors": {"prod": "connection refused"},
        },
    }
    result = CallToolResult(content=[], structuredContent=payload)
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_field(
            rt,
            "sessions_list",
            retry_command="dh-mcp session list",
            arguments={},
            field="sessions",
            default=[],
        )
    captured = capsys.readouterr()
    assert "community:community:dev" in captured.out
    assert "partial_result" not in captured.out
    assert "connection issues" in captured.err
    assert "prod: connection refused" in captured.err


@pytest.mark.asyncio
async def test_call_and_echo_field_surfaces_partial_result_detail_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """An in-progress `partial_result` (detail, no errors) still warns on stderr."""
    rt = make_runtime(tmp_path)
    payload = {
        "success": True,
        "sessions": [],
        "partial_result": {
            "phase": "loading",
            "detail": "Enterprise session discovery is actively running.",
        },
    }
    result = CallToolResult(content=[], structuredContent=payload)
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_field(
            rt,
            "sessions_list",
            retry_command="dh-mcp session list",
            arguments={},
            field="sessions",
            default=[],
        )
    err = capsys.readouterr().err
    assert "actively running" in err


@pytest.mark.asyncio
async def test_call_and_echo_field_suppresses_completed_partial_result_when_opted_out(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """With ``reasons_in_rows=True`` a ``phase=='completed'``
    ``partial_result`` is suppressed entirely — the banner ("had connection
    issues") would only restate the per-row reasons in ``liveness_detail``."""
    rt = make_runtime(tmp_path)
    payload = {
        "success": True,
        "systems": [{"name": "prod", "liveness_detail": "DeephavenConnectionError"}],
        "partial_result": {
            "phase": "completed",
            "detail": "Some enterprise systems had connection issues during discovery.",
            "errors": {"prod": "DeephavenConnectionError: Network is unreachable"},
        },
    }
    result = CallToolResult(content=[], structuredContent=payload)
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_field(
            rt,
            "enterprise_systems_status",
            retry_command="dh-mcp system status",
            arguments={},
            field="systems",
            default=[],
            reasons_in_rows=True,
        )
    captured = capsys.readouterr()
    # Nothing on stderr — the per-row reasons fully cover the COMPLETED case.
    assert captured.err == ""


@pytest.mark.asyncio
async def test_call_and_echo_field_loading_still_warns_when_opted_out(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``phase=='loading'`` carries a timing signal ("retry in a moment") that
    rows cannot convey, so the warning still appears even with
    ``reasons_in_rows=True``."""
    rt = make_runtime(tmp_path)
    payload = {
        "success": True,
        "systems": [],
        "partial_result": {
            "phase": "loading",
            "detail": "Enterprise session discovery is actively running.",
        },
    }
    result = CallToolResult(content=[], structuredContent=payload)
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_field(
            rt,
            "enterprise_systems_status",
            retry_command="dh-mcp system status",
            arguments={},
            field="systems",
            default=[],
            reasons_in_rows=True,
        )
    captured = capsys.readouterr()
    assert "actively running" in captured.err


@pytest.mark.asyncio
async def test_call_and_echo_field_failed_surfaces_errors_when_reasons_in_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A non-``completed`` phase still surfaces the per-system ``errors`` map
    even with ``reasons_in_rows=True`` — the row's ``liveness_detail`` carries
    only the short reason, so the full message would otherwise be unreachable."""
    rt = make_runtime(tmp_path)
    payload = {
        "success": True,
        "systems": [{"name": "prod", "liveness_detail": "DeephavenConnectionError"}],
        "partial_result": {
            "phase": "failed",
            "detail": "Enterprise session discovery failed critically.",
            "errors": {"prod": "DeephavenConnectionError: Network is unreachable"},
        },
    }
    result = CallToolResult(content=[], structuredContent=payload)
    acq, call = _patched_call(result)
    with acq, call:
        await call_and_echo_field(
            rt,
            "enterprise_systems_status",
            retry_command="dh-mcp system status",
            arguments={},
            field="systems",
            default=[],
            reasons_in_rows=True,
        )
    captured = capsys.readouterr()
    # The full per-system message must reach stderr — not just the short
    # exception type the table cell shows.
    assert "Network is unreachable" in captured.err
