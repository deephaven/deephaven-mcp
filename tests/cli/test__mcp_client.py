"""Tests for ``deephaven_mcp.cli._mcp_client``."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.types import CallToolResult, TextContent, Tool
from pydantic import SecretStr

from deephaven_mcp.cli import _mcp_client as mc
from deephaven_mcp.cli._mcp_client import McpClient, McpClientError
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
        }
    )


def test_constructor_sets_url_and_timeout() -> None:
    client = McpClient(_handle(), request_timeout_seconds=30)
    assert client._url == "http://127.0.0.1:12345/mcp"  # noqa: SLF001
    assert client._timeout == timedelta(seconds=30)  # noqa: SLF001


def test_constructor_url_path_override() -> None:
    client = McpClient(_handle(), url_path="/alt")
    assert client._url == "http://127.0.0.1:12345/alt"  # noqa: SLF001


@pytest.mark.asyncio
async def test_list_tools_before_aenter_raises() -> None:
    client = McpClient(_handle())
    with pytest.raises(McpClientError, match="before entering"):
        await client.list_tools()


@pytest.mark.asyncio
async def test_call_tool_before_aenter_raises() -> None:
    client = McpClient(_handle())
    with pytest.raises(McpClientError, match="before entering"):
        await client.call_tool("foo")


@pytest.mark.asyncio
async def test_aenter_failure_closes_stack_and_reraises() -> None:
    """A failure during ``__aenter__`` must close the AsyncExitStack and re-raise.

    Covers the ``except BaseException`` cleanup branch in ``__aenter__``.
    """
    with patch.object(mc, "create_mcp_http_client") as mock_http:
        # Entering the HTTP-client context raises before transport/session
        # are entered, so any partially-entered stack must be unwound.
        mock_http.return_value.__aenter__ = AsyncMock(side_effect=RuntimeError("boom"))
        mock_http.return_value.__aexit__ = AsyncMock(return_value=None)
        client = McpClient(_handle())
        with pytest.raises(RuntimeError, match="boom"):
            await client.__aenter__()
        # Stack closed and session not assigned.
        assert client._session is None  # noqa: SLF001


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

        async with McpClient(_handle()) as client:
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

    client = McpClient(_handle())
    client._session = fake_session  # noqa: SLF001
    out = await client.list_tools()
    assert out == expected


@pytest.mark.asyncio
async def test_list_tools_wraps_exception() -> None:
    fake_session = AsyncMock()
    fake_session.list_tools = AsyncMock(side_effect=RuntimeError("boom"))
    client = McpClient(_handle())
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpClientError, match="list_tools failed"):
        await client.list_tools()


@pytest.mark.asyncio
async def test_call_tool_uses_default_timeout() -> None:
    fake_session = AsyncMock()
    expected = CallToolResult(content=[TextContent(type="text", text="ok")])
    fake_session.call_tool = AsyncMock(return_value=expected)
    client = McpClient(_handle(), request_timeout_seconds=42)
    client._session = fake_session  # noqa: SLF001
    out = await client.call_tool("foo", {"a": 1})
    assert out is expected
    _, kwargs = fake_session.call_tool.await_args
    assert kwargs["read_timeout_seconds"] == timedelta(seconds=42)


@pytest.mark.asyncio
async def test_call_tool_uses_per_call_timeout() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=CallToolResult(content=[]))
    client = McpClient(_handle(), request_timeout_seconds=42)
    client._session = fake_session  # noqa: SLF001
    await client.call_tool("foo", timeout_seconds=5)
    _, kwargs = fake_session.call_tool.await_args
    assert kwargs["read_timeout_seconds"] == timedelta(seconds=5)


@pytest.mark.asyncio
async def test_call_tool_default_arguments_to_empty_dict() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=CallToolResult(content=[]))
    client = McpClient(_handle())
    client._session = fake_session  # noqa: SLF001
    await client.call_tool("foo")
    args, _ = fake_session.call_tool.await_args
    assert args[1] == {}


@pytest.mark.asyncio
async def test_call_tool_wraps_exception() -> None:
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=ConnectionError("nope"))
    client = McpClient(_handle())
    client._session = fake_session  # noqa: SLF001
    with pytest.raises(McpClientError, match="call_tool"):
        await client.call_tool("foo")
