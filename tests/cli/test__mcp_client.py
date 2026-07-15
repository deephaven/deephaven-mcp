"""Tests for ``deephaven_mcp.cli._mcp_client``."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import NoReturn
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from mcp.shared.exceptions import McpError
from mcp.types import CallToolResult, ErrorData, TextContent, Tool
from pydantic import SecretStr

from deephaven_mcp.cli import _mcp_client as mc
from deephaven_mcp.cli._mcp_client import (
    McpClient,
    McpClientError,
    McpRequestTimeoutError,
)
from deephaven_mcp.daemon_registry import DaemonRegistryEntry

_PSK_PLAINTEXT = "shhhhhhhhhhhhhhhh"


def _handle() -> DaemonRegistryEntry:
    return DaemonRegistryEntry.model_validate(
        {
            "pid": 1,
            "create_time_ns": 1_700_000_000_000_000_000,
            "process_name": "python",
            "host": "127.0.0.1",
            "port": 12345,
            "psk": SecretStr(_PSK_PLAINTEXT),
            "started_at": "2024-01-01T00:00:00Z",
            "config_dir": Path("/tmp/cfg"),
            "server_name": "dh-test",
            "build_identity": {
                "version": "1.2.3",
                "venv": "/venv/x",
                "fingerprint": "f" * 64,
            },
        }
    )


def test_constructor_sets_url_headers_and_timeout() -> None:
    client = McpClient(
        "https://docs.example.test/mcp",
        headers={"X-Test": "v"},
        request_timeout_seconds=30,
    )
    assert client._url == "https://docs.example.test/mcp"  # noqa: SLF001
    assert client._headers == {"X-Test": "v"}  # noqa: SLF001
    assert client._timeout == timedelta(seconds=30)  # noqa: SLF001


def test_constructor_defaults_to_no_headers() -> None:
    client = McpClient("https://docs.example.test/mcp")
    assert client._headers == {}  # noqa: SLF001


def test_for_daemon_sets_url_psk_header_and_timeout() -> None:
    client = McpClient.for_daemon(_handle(), request_timeout_seconds=30)
    assert client._url == "http://127.0.0.1:12345/mcp"  # noqa: SLF001
    assert client._headers == {"X-Deephaven-PSK": _PSK_PLAINTEXT}  # noqa: SLF001
    assert client._timeout == timedelta(seconds=30)  # noqa: SLF001


def test_for_daemon_url_path_override() -> None:
    client = McpClient.for_daemon(_handle(), url_path="/alt")
    assert client._url == "http://127.0.0.1:12345/alt"  # noqa: SLF001


@pytest.mark.asyncio
async def test_list_tools_before_aenter_raises() -> None:
    client = McpClient.for_daemon(_handle())
    with pytest.raises(McpClientError, match="before entering"):
        await client.list_tools()


@pytest.mark.asyncio
async def test_call_tool_before_aenter_raises() -> None:
    client = McpClient.for_daemon(_handle())
    with pytest.raises(McpClientError, match="before entering"):
        await client.call_tool("foo")


@pytest.mark.asyncio
async def test_aenter_failure_closes_stack_and_wraps() -> None:
    """A failure during ``__aenter__`` closes the stack and wraps in McpClientError.

    Covers the ``except BaseException`` cleanup branch in ``__aenter__``.
    """
    with patch.object(mc, "create_mcp_http_client") as mock_http:
        # Entering the HTTP-client context raises before transport/session
        # are entered, so any partially-entered stack must be unwound.
        mock_http.return_value.__aenter__ = AsyncMock(side_effect=RuntimeError("boom"))
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient.for_daemon(_handle())
        with pytest.raises(McpClientError, match="RuntimeError: boom") as exc_info:
            await client.__aenter__()
        assert "Could not connect to http://127.0.0.1:12345/mcp" in str(exc_info.value)
        # Stack closed and session not assigned.
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_unreachable_server_wraps_exception_group() -> None:
    """The anyio task-group failure shape maps to a McpClientError naming the cause.

    A real unreachable server surfaces as
    ``ExceptionGroup([httpx.ConnectError])`` from the streamable-HTTP
    transport; the wrapper must flatten it so 'docs status' /
    'docs ask' can report ``mcp_request_failed`` instead of a raw
    traceback.
    """
    group = ExceptionGroup(
        "unhandled errors in a TaskGroup",
        [httpx.ConnectError("All connection attempts failed")],
    )
    with patch.object(mc, "create_mcp_http_client") as mock_http:
        mock_http.return_value.__aenter__ = AsyncMock(side_effect=group)
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(
            McpClientError, match="ConnectError: All connection attempts failed"
        ) as exc_info:
            await client.__aenter__()
        assert "Could not connect to https://docs.example.test/mcp" in str(
            exc_info.value
        )
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_unwind_failure_is_preferred_and_wrapped() -> None:
    """A failure raised while closing the stack names the root cause.

    Against a real unreachable server the ``async with`` entry fails
    with a generic error and the anyio task group re-raises the
    underlying ``ConnectError`` (as a group) only when the stack is
    unwound; the wrapper must report that root cause.
    """
    unwind_group = ExceptionGroup(
        "unhandled errors in a TaskGroup",
        [httpx.ConnectError("All connection attempts failed")],
    )
    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        # Unwinding the HTTP-client context surfaces the transport failure.
        mock_http.return_value.__aexit__ = AsyncMock(side_effect=unwind_group)
        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=RuntimeError("generic entry failure")
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(
            McpClientError, match="ConnectError: All connection attempts failed"
        ):
            await client.__aenter__()
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_typed_unwind_failure_not_rewrapped() -> None:
    """A McpClientError surfacing at unwind propagates typed, without re-wrapping."""
    typed = McpClientError("typed at unwind")
    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_http.return_value.__aexit__ = AsyncMock(side_effect=typed)
        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=RuntimeError("generic entry failure")
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(McpClientError) as exc_info:
            await client.__aenter__()
        assert exc_info.value is typed


@pytest.mark.asyncio
async def test_aenter_cancellation_propagates_unwrapped() -> None:
    """Cancellation is not an McpClientError; it must propagate raw."""
    with patch.object(mc, "create_mcp_http_client") as mock_http:
        mock_http.return_value.__aenter__ = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(asyncio.CancelledError):
            await client.__aenter__()
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_initialize_failure_clears_session() -> None:
    """A failed ``initialize()`` leaves the client unusable, not half-open.

    Regression: the session was assigned before ``initialize()`` ran, so
    an initialization failure used to leave ``_session`` pointing at a
    closed session that ``list_tools`` / ``call_tool`` would accept.
    """
    fake_session = AsyncMock()
    fake_session.initialize = AsyncMock(side_effect=RuntimeError("init boom"))
    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
        patch.object(mc, "ClientSession") as mock_session_cls,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_transport.return_value.__aenter__ = AsyncMock(
            return_value=(MagicMock(), MagicMock(), None)
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=fake_session)
        mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(McpClientError, match="RuntimeError: init boom"):
            await client.__aenter__()
        assert client._session is None  # noqa: SLF001
        with pytest.raises(McpClientError, match="before entering"):
            await client.list_tools()


@pytest.mark.asyncio
async def test_aenter_external_cancellation_not_displaced_by_unwind_error() -> None:
    """An external cancel propagates raw even when unwinding raises.

    Regression: an ordinary ``Exception`` from ``aclose()`` used to
    displace the entry failure unconditionally, wrapping the cleanup
    error as ``McpClientError`` and swallowing the cancellation.
    """
    entered = asyncio.Event()

    async def _hang(*_args: object) -> NoReturn:
        # Instance magic-method mocks receive the mock as first arg.
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        # Unwinding the entered HTTP-client context raises an ordinary
        # Exception; it must not displace the external cancellation.
        mock_http.return_value.__aexit__ = AsyncMock(
            side_effect=RuntimeError("cleanup bug")
        )
        mock_transport.return_value.__aenter__ = AsyncMock(side_effect=_hang)
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        task = asyncio.create_task(client.__aenter__())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_internal_cancellation_prefers_unwind_error() -> None:
    """The anyio peer-cancellation pattern still surfaces the root cause.

    A bare ``CancelledError`` with no pending external cancel request is
    the transport task group canceling its peers; the real failure
    raised at unwind time is preferred and wrapped.
    """
    unwind_group = ExceptionGroup(
        "unhandled errors in a TaskGroup",
        [httpx.ConnectError("All connection attempts failed")],
    )
    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_http.return_value.__aexit__ = AsyncMock(side_effect=unwind_group)
        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(
            McpClientError, match="ConnectError: All connection attempts failed"
        ):
            await client.__aenter__()
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_keyboard_interrupt_not_displaced_by_unwind_error() -> None:
    """A non-Exception entry failure propagates raw despite an unwind error."""
    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_http.return_value.__aexit__ = AsyncMock(
            side_effect=RuntimeError("cleanup bug")
        )
        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=KeyboardInterrupt()
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(KeyboardInterrupt):
            await client.__aenter__()
        assert client._session is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_aenter_mcp_client_error_not_double_wrapped() -> None:
    """An already-typed McpClientError propagates without a second wrapper."""
    original = McpClientError("already typed")
    with patch.object(mc, "create_mcp_http_client") as mock_http:
        mock_http.return_value.__aenter__ = AsyncMock(side_effect=original)
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient("https://docs.example.test/mcp")
        with pytest.raises(McpClientError) as exc_info:
            await client.__aenter__()
        assert exc_info.value is original


@pytest.mark.asyncio
async def test_aenter_aexit_drives_session() -> None:
    """Happy path: ``__aenter__`` initializes; ``__aexit__`` closes."""
    fake_session = AsyncMock()
    fake_session.initialize = AsyncMock()

    async def _fake_session_ctx(read, write):  # noqa: ANN001
        return fake_session

    with (
        patch.object(mc, "create_mcp_http_client") as mock_http,
        patch.object(mc, "streamable_http_client") as mock_transport,
        patch.object(mc, "ClientSession") as mock_session_cls,
    ):
        mock_http.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_transport.return_value.__aenter__ = AsyncMock(
            return_value=(MagicMock(), MagicMock(), None)
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=fake_session)
        mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=None)

        async with McpClient.for_daemon(_handle()) as client:
            assert client._session is fake_session  # noqa: SLF001
            fake_session.initialize.assert_awaited_once()
        # The HTTP transport receives the PSK *plaintext* (extracted
        # via ``SecretStr.get_secret_value()``); the daemon's
        # ``PSKMiddleware`` compares with ``hmac.compare_digest`` and
        # cannot accept a SecretStr object.
        headers = mock_http.call_args.kwargs["headers"]
        assert "X-Deephaven-PSK" in headers
        assert headers["X-Deephaven-PSK"] == _PSK_PLAINTEXT


@pytest.mark.asyncio
async def test_list_tools_returns_list() -> None:
    fake_session = AsyncMock()
    expected = [Tool(name="t", description="d", inputSchema={"type": "object"})]
    fake_session.list_tools = AsyncMock(return_value=MagicMock(tools=expected))

    client = McpClient.for_daemon(_handle())
    client._session = fake_session  # noqa: SLF001
    out = await client.list_tools()
    assert out == expected


@pytest.mark.asyncio
async def test_list_tools_wraps_exception() -> None:
    fake_session = AsyncMock()
    fake_session.list_tools = AsyncMock(side_effect=RuntimeError("boom"))
    client = McpClient.for_daemon(_handle())
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpClientError, match="list_tools failed"):
        await client.list_tools()


@pytest.mark.asyncio
async def test_call_tool_uses_default_timeout() -> None:
    fake_session = AsyncMock()
    expected = CallToolResult(content=[TextContent(type="text", text="ok")])
    fake_session.call_tool = AsyncMock(return_value=expected)
    client = McpClient.for_daemon(_handle(), request_timeout_seconds=42)
    client._session = fake_session  # noqa: SLF001
    out = await client.call_tool("foo", {"a": 1})
    assert out is expected
    _, kwargs = fake_session.call_tool.await_args
    assert kwargs["read_timeout_seconds"] == timedelta(seconds=42)


@pytest.mark.asyncio
async def test_call_tool_uses_per_call_timeout() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=CallToolResult(content=[]))
    client = McpClient.for_daemon(_handle(), request_timeout_seconds=42)
    client._session = fake_session  # noqa: SLF001
    await client.call_tool("foo", timeout_seconds=5)
    _, kwargs = fake_session.call_tool.await_args
    assert kwargs["read_timeout_seconds"] == timedelta(seconds=5)


@pytest.mark.asyncio
async def test_call_tool_default_arguments_to_empty_dict() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=CallToolResult(content=[]))
    client = McpClient.for_daemon(_handle())
    client._session = fake_session  # noqa: SLF001
    await client.call_tool("foo")
    args, _ = fake_session.call_tool.await_args
    assert args[1] == {}


@pytest.mark.asyncio
async def test_call_tool_wraps_exception() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=ConnectionError("nope"))
    client = McpClient.for_daemon(_handle())
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpClientError, match="call_tool"):
        await client.call_tool("foo")


def _mcp_timeout_error() -> McpError:
    return McpError(
        ErrorData(
            code=httpx.codes.REQUEST_TIMEOUT,
            message="Timed out while waiting for response to ClientRequest. Waited 60.0 seconds.",
        )
    )


@pytest.mark.asyncio
async def test_call_tool_timeout_gets_dedicated_message() -> None:
    """A request timeout notes possible server-side completion and how to allow more time."""
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=_mcp_timeout_error())
    client = McpClient.for_daemon(_handle(), request_timeout_seconds=60)
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpRequestTimeoutError) as exc_info:
        await client.call_tool("pq_delete")
    msg = str(exc_info.value)
    assert "timed out after 60 seconds" in msg
    assert "may still complete the operation server-side" in msg
    assert "--timeout" in msg
    assert "request.timeouts.default_seconds" in msg


@pytest.mark.asyncio
async def test_call_tool_timeout_message_uses_custom_timeout_setting() -> None:
    """The recovery hint names the cli.json key the constructor was given."""
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=_mcp_timeout_error())
    client = McpClient(
        "https://docs.example.test/mcp",
        request_timeout_seconds=60,
        timeout_setting="docs.timeouts.request_seconds",
    )
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpRequestTimeoutError, match="docs.timeouts.request_seconds"):
        await client.call_tool("docs_chat")


@pytest.mark.asyncio
async def test_call_tool_timeout_message_uses_per_call_override() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=_mcp_timeout_error())
    client = McpClient.for_daemon(_handle(), request_timeout_seconds=60)
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpRequestTimeoutError, match="timed out after 5 seconds"):
        await client.call_tool("pq_delete", timeout_seconds=5)


@pytest.mark.asyncio
async def test_call_tool_non_timeout_mcp_error_keeps_generic_message() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(
        side_effect=McpError(
            ErrorData(code=httpx.codes.INTERNAL_SERVER_ERROR, message="boom")
        )
    )
    client = McpClient.for_daemon(_handle())
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpClientError, match=r"call_tool\('foo'\) failed: boom") as ei:
        await client.call_tool("foo")
    assert not isinstance(ei.value, McpRequestTimeoutError)
