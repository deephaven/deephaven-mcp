"""Thin async wrapper around an MCP streamable-HTTP session.

The CLI's subcommands need a small, deterministic surface for two
operations: list tools, and call tools. Constructing the underlying
:class:`mcp.ClientSession` directly requires juggling three nested
async context managers (HTTP client → transport → session); this
module bundles that into a single :class:`McpClient` async context
manager so handler code stays focused on UX, not boilerplate.

Two construction paths cover the CLI's two server surfaces:
:meth:`McpClient.for_daemon` connects to the local daemon and injects
the ``X-Deephaven-PSK`` header on every request; the plain constructor
takes any streamable-HTTP endpoint URL (e.g. the docs MCP server) with
optional headers. Both apply a per-call timeout that can be overridden
per ``call_tool`` invocation.
"""

from __future__ import annotations

__all__ = ["McpClient", "McpClientError", "McpRequestTimeoutError"]

import asyncio
import logging
from contextlib import AsyncExitStack
from datetime import timedelta
from types import TracebackType
from typing import Any

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.exceptions import McpError
from mcp.types import CallToolResult, Tool

from deephaven_mcp._exception_utils import describe_exception
from deephaven_mcp._exceptions import McpClientError, McpRequestTimeoutError
from deephaven_mcp.auth.middleware._psk import PSK_HEADER_NAME
from deephaven_mcp.daemon_registry import DaemonRegistryEntry

_LOGGER = logging.getLogger(__name__)


def _is_internal_cancellation(exc: BaseException) -> bool:
    """Report whether ``exc`` is a cancellation not requested from outside.

    Distinguishes the anyio task-group pattern — a peer failure cancels
    the host task and the cancel scope absorbs the cancellation (calling
    ``Task.uncancel``) before the real failure is re-raised at unwind —
    from an external ``Task.cancel``, whose request is still pending on
    the current task.

    Args:
        exc (BaseException): The exception caught during ``__aenter__``.

    Returns:
        bool: ``True`` when ``exc`` is a :class:`asyncio.CancelledError`
            and the current task has no pending external cancel request.
    """
    if not isinstance(exc, asyncio.CancelledError):
        return False
    task = asyncio.current_task()
    return task is not None and task.cancelling() == 0  # codespell:ignore cancelling


class McpClient:
    """Async-context-manager wrapper over a streamable-HTTP MCP session.

    Usage::

        async with McpClient.for_daemon(handle, request_timeout_seconds=60) as client:
            tools = await client.list_tools()
            result = await client.call_tool("sessions_list", {})

    The class is **not** reentrant: a single instance must be opened
    and closed exactly once. Construct a new instance per CLI
    subcommand invocation.
    """

    def __init__(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        request_timeout_seconds: int = 60,
        timeout_setting: str = "cli.request.timeouts.default_seconds",
    ) -> None:
        """Capture the connection parameters; no I/O yet.

        Args:
            url (str): Full streamable-HTTP endpoint URL of the MCP
                server (e.g. ``http://127.0.0.1:8000/mcp`` or a remote
                ``https://...`` docs server endpoint).
            headers (dict[str, str] | None): Extra HTTP headers sent
                with every request, or ``None`` for none.
            request_timeout_seconds (int): Default per-call timeout
                applied to ``call_tool`` when no override is
                supplied. Defaults to ``60``.
            timeout_setting (str): Logical config path named in the
                timeout error's recovery hint. Defaults to the daemon
                request timeout path.
        """
        self._url = url
        self._headers = dict(headers) if headers else {}
        self._timeout = timedelta(seconds=request_timeout_seconds)
        self._timeout_setting = timeout_setting
        self._stack = AsyncExitStack()
        self._session: ClientSession | None = None

    @classmethod
    def for_daemon(
        cls,
        handle: DaemonRegistryEntry,
        *,
        request_timeout_seconds: int = 60,
        url_path: str = "/mcp",
    ) -> McpClient:
        """Build a client for the local daemon's MCP endpoint.

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

        Returns:
            McpClient: An unopened client whose requests carry the
                daemon's ``X-Deephaven-PSK`` header.
        """
        # The PSK is a pydantic SecretStr; this is the single place
        # in the CLI that needs the plaintext (the daemon's
        # PSKMiddleware compares the header value with
        # hmac.compare_digest).
        return cls(
            f"http://{handle.host}:{handle.port}{url_path}",
            headers={PSK_HEADER_NAME: handle.psk.get_secret_value()},
            request_timeout_seconds=request_timeout_seconds,
        )

    async def __aenter__(self) -> McpClient:
        """Open the HTTP transport and initialize the MCP session.

        Returns:
            McpClient: This instance, with the session ready for
                :meth:`list_tools` / :meth:`call_tool`.

        Raises:
            McpClientError: When the transport cannot be opened or the
                MCP session fails to initialize (e.g. the server is
                unreachable). The partially-entered stack is closed
                first. External cancellation and other non-``Exception``
                failures propagate unwrapped; an internal cancellation
                (the transport task group canceling its peers on a
                connect failure) is replaced by the real failure raised
                at unwind time.
        """
        try:
            http_client = await self._stack.enter_async_context(
                create_mcp_http_client(headers=self._headers or None)
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
        except BaseException as exc:
            self._session = None
            failure: BaseException = exc
            try:
                await self._stack.aclose()
            except Exception as close_exc:
                # On a connect failure the transport's anyio task group
                # cancels its peers (so ``initialize`` sees a bare
                # CancelledError) and re-raises the real failure (e.g.
                # ExceptionGroup([ConnectError])) only at unwind time;
                # prefer it — it names the root cause. Never let an
                # unwind error displace an *external* cancellation or
                # another non-Exception entry failure (KeyboardInterrupt,
                # SystemExit): those must propagate raw per the contract
                # below.
                if isinstance(exc, Exception) or _is_internal_cancellation(exc):
                    failure = close_exc
                else:
                    _LOGGER.debug(
                        f"[_mcp_client:McpClient.__aenter__] Suppressing "
                        f"unwind error after {type(exc).__name__}: "
                        f"{describe_exception(close_exc)}"
                    )
            # Re-raise already-typed client errors and non-Exception
            # failures (CancelledError, KeyboardInterrupt — and any
            # BaseExceptionGroup carrying one, which is not an
            # Exception) untouched; wrap everything else so callers
            # can catch a connect failure as McpClientError.
            if isinstance(failure, McpClientError) or not isinstance(
                failure, Exception
            ):
                if failure is exc:
                    raise
                raise failure from exc
            raise McpClientError(
                f"Could not connect to {self._url}: {describe_exception(failure)}"
            ) from failure

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
            raise McpClientError(
                f"list_tools failed: {describe_exception(exc)}"
            ) from exc
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
            McpRequestTimeoutError: When the server does not respond
                within the timeout. The server may still finish
                processing the request; the message says so and how to
                allow more time.
            McpClientError: When the call fails for any other reason
                (network, server-reported error). The exception
                preserves the original cause via ``__cause__``.
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
            # The MCP SDK signals its client-side read timeout as an
            # McpError carrying httpx.codes.REQUEST_TIMEOUT (see
            # mcp.shared.session); surface it as the dedicated subtype.
            if (
                isinstance(exc, McpError)
                and exc.error.code == httpx.codes.REQUEST_TIMEOUT
            ):
                raise McpRequestTimeoutError(
                    f"call_tool({name!r}) timed out after "
                    f"{timeout.total_seconds():g} seconds. The server may still "
                    f"finish processing the request — if the operation changes "
                    f"state, verify the result before retrying. To allow more "
                    f"time, pass --timeout or raise {self._timeout_setting}."
                ) from exc
            raise McpClientError(
                f"call_tool({name!r}) failed: {describe_exception(exc)}"
            ) from exc
