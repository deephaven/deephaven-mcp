"""Thin async wrapper around an MCP streamable-HTTP session.

The CLI's subcommands need a small, deterministic surface for two
operations: list tools, and call tools. Constructing the underlying
:class:`mcp.ClientSession` directly requires juggling three nested
async context managers (HTTP client → transport → session); this
module bundles that into a single :class:`McpClient` async context
manager so handler code stays focused on UX, not boilerplate.

The wrapper also injects the ``X-Deephaven-PSK`` header on every
request and applies a per-call timeout that defaults to
:attr:`RequestTimeouts.default_seconds` but can be overridden per
``call_tool`` invocation.
"""

from __future__ import annotations

__all__ = ["McpClient", "McpClientError"]

import logging
from contextlib import AsyncExitStack
from datetime import timedelta
from types import TracebackType
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.types import CallToolResult, Tool

from deephaven_mcp._exceptions import McpClientError
from deephaven_mcp.auth.middleware._psk import PSK_HEADER_NAME
from deephaven_mcp.daemon_registry import DaemonRegistryEntry

_LOGGER = logging.getLogger(__name__)


class McpClient:
    """Async-context-manager wrapper over a streamable-HTTP MCP session.

    Usage::

        async with McpClient(handle, request_timeout_seconds=60) as client:
            tools = await client.list_tools()
            result = await client.call_tool("sessions_list", {})

    The class is **not** reentrant: a single instance must be opened
    and closed exactly once. Construct a new instance per CLI
    subcommand invocation.
    """

    def __init__(
        self,
        handle: DaemonRegistryEntry,
        *,
        request_timeout_seconds: int = 60,
        url_path: str = "/mcp",
    ) -> None:
        """Capture the connection parameters; no I/O yet.

        Args:
            handle (DaemonRegistryEntry): The validated registry
                entry produced by :func:`get_or_start_daemon`.
                Read for ``host``, ``port``, ``psk``; the rest of
                the fields are operator telemetry and not
                consulted here.
            request_timeout_seconds (int): Default per-call timeout
                applied to ``call_tool`` when no override is
                supplied. Defaults to ``60``.
            url_path (str): Path the MCP server is mounted at on
                the daemon. The systems server uses ``/mcp`` (the
                default for ``streamable_http_app``); other servers
                may differ.
        """
        self._handle = handle
        self._timeout = timedelta(seconds=request_timeout_seconds)
        self._url = f"http://{handle.host}:{handle.port}{url_path}"
        self._stack = AsyncExitStack()
        self._session: ClientSession | None = None

    async def __aenter__(self) -> McpClient:
        """Open the HTTP transport and initialize the MCP session."""
        try:
            http_client = await self._stack.enter_async_context(
                # The PSK is a pydantic SecretStr; this is the
                # single place in the CLI that needs the plaintext
                # (the daemon's PSKMiddleware compares the header
                # value with hmac.compare_digest).
                create_mcp_http_client(
                    headers={PSK_HEADER_NAME: self._handle.psk.get_secret_value()}
                )
            )
            read, write, _ = await self._stack.enter_async_context(
                streamable_http_client(self._url, http_client=http_client)
            )
            self._session = await self._stack.enter_async_context(
                ClientSession(read, write)
            )
            await self._session.initialize()
            _LOGGER.debug(
                f"[_mcp_client:McpClient.__aenter__] Connected to {self._url}"
            )
            return self
        except BaseException:
            await self._stack.aclose()
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the session and underlying transport unconditionally."""
        await self._stack.aclose()
        self._session = None

    async def list_tools(self) -> list[Tool]:
        """Return the daemon's registered MCP tools.

        Returns:
            list[Tool]: The fully-typed list of tools as reported by
                the server. Empty when the daemon registers nothing.

        Raises:
            McpClientError: If the wrapper has not been entered yet
                or the request fails.
        """
        if self._session is None:
            raise McpClientError(
                "McpClient.list_tools() called before entering the async context."
            )
        try:
            response = await self._session.list_tools()
        except Exception as exc:
            raise McpClientError(f"list_tools failed: {exc}") from exc
        return list(response.tools)

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> CallToolResult:
        """Invoke a single MCP tool and return its result.

        Args:
            name (str): The registered tool name.
            arguments (dict[str, Any] | None): Tool argument
                dictionary; ``None`` is treated as ``{}``.
            timeout_seconds (int | None): Per-call timeout
                override. When ``None``, the value passed to the
                constructor is used.

        Returns:
            CallToolResult: The fully-typed MCP result.

        Raises:
            McpClientError: When the call fails for any reason
                (network, timeout, server-reported error). The
                exception preserves the original cause via
                ``__cause__``.
        """
        if self._session is None:
            raise McpClientError(
                "McpClient.call_tool() called before entering the async context."
            )
        timeout = (
            timedelta(seconds=timeout_seconds)
            if timeout_seconds is not None
            else self._timeout
        )
        try:
            return await self._session.call_tool(
                name, arguments or {}, read_timeout_seconds=timeout
            )
        except Exception as exc:
            raise McpClientError(f"call_tool({name!r}) failed: {exc}") from exc
