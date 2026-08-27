"""WebClientData widget protocol for fetching Enterprise per-user tables.

Deephaven Enterprise serves web-client state from a well-known persistent
query named ``WebClientData``, which exports a table-factory widget. This
module drives that widget's bidirectional object-message stream to obtain a
named per-user table.

Why this exists rather than the direct ``db`` calls: a client-side
``session.catalog_table()`` is a *ticket* fetch, and the server's ticket
resolver admits only users who administer the worker being queried. Reading
the catalog off a shared worker therefore fails for ordinary users. The
widget instead builds each table for a named user with that user's ACLs
applied, which is how the web client reads the catalog.

Tables obtained this way are ordinary Deephaven tables on the
``WebClientData`` worker, so callers may apply Deephaven Query Language
``where`` clauses to them and have the engine evaluate the filter
server-side.

The scope field the widget is exported under, the request shape, and the
per-user table names are all defined server-side; this module is the only
place in the codebase that encodes them.
"""

from __future__ import annotations

__all__ = [
    "WEB_CLIENT_DATA_PQ",
    "WebClientDataTable",
    "fetch_web_client_data_table",
]

import asyncio
import enum
import json
import logging
import threading
import time
import uuid

from deephaven_enterprise.client.session_manager import DndSession
from pydeephaven.experimental.plugin_client import PluginClient
from pydeephaven.table import Table

from deephaven_mcp._exceptions import WebClientDataError

from ._base import describe_exception_chain
from ._session import CorePlusSession

_LOGGER = logging.getLogger(__name__)

WEB_CLIENT_DATA_PQ = "WebClientData"
"""Name of the Enterprise persistent query that serves web-client state."""

_TABLE_FACTORY_FIELD = "WebClientTableFactory"
"""Scope field the WebClientData worker exports the table-factory widget under."""


class WebClientDataTable(enum.StrEnum):
    """A per-user table the WebClientData table factory can produce."""

    CATALOG = "catalog"
    """The data catalog, built with the ACLs of the identity named in the
    request (``operate_as``).
    Columns: ``Namespace``, ``NamespaceSet``, ``TableName``."""

    QUERY_INFO = "QueryInfo"
    """Persistent-query inventory: one row per PQ visible to the requesting
    user, with columns including Serial, Name, Owner, Status, and HeapSize.
    ``Serial`` is a string column here, not an integer."""


def _request_payload(table: WebClientDataTable, operate_as: str) -> bytes:
    """Build the JSON request the table-factory widget expects.

    Args:
        table (WebClientDataTable): The per-user table to request.
        operate_as (str): Identity whose ACLs the table is built with. The
            server rejects a null or empty value.

    Returns:
        bytes: The UTF-8 encoded request payload.
    """
    return json.dumps(
        {
            "id": uuid.uuid4().hex,
            "user": operate_as,
            "tableNames": [str(table)],
        }
    ).encode("utf-8")


def _refusal_reason(payload: bytes) -> str:
    """Render the widget's refusal text from a response payload.

    The widget answers with an ``{"id": ..., "error": ...}`` object whose
    ``error`` is null on success. A refusal arrives as a payload carrying no
    exported objects.

    Args:
        payload (bytes): The raw response payload from the widget stream.

    Returns:
        str: The ``error`` field when the payload has the expected shape,
            otherwise the payload rendered for diagnosis.
    """
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return repr(payload)
    if isinstance(decoded, dict) and decoded.get("error"):
        return str(decoded["error"])
    return repr(decoded)


def _open_plugin(session: DndSession) -> PluginClient:
    """Open a message stream to the table-factory widget.

    Args:
        session (DndSession): A session connected to the ``WebClientData``
            persistent query.

    Returns:
        PluginClient: The connected widget client. The caller owns it and must
            close it.

    Raises:
        WebClientDataError: If the widget is not exported under the expected
            scope field.
    """
    try:
        factory_obj = session.exportable_objects[_TABLE_FACTORY_FIELD]
    except KeyError as e:
        raise WebClientDataError(
            f"The '{WEB_CLIENT_DATA_PQ}' worker does not export a table factory "
            f"under scope field '{_TABLE_FACTORY_FIELD}'; the server may run a "
            f"version that exports it under a different name."
        ) from e

    # PluginClient sends the initial ConnectRequest.
    return PluginClient(session, factory_obj)


def _request_table(
    plugin: PluginClient,
    table: WebClientDataTable,
    operate_as: str,
    deadline_seconds: float,
) -> Table:
    """Write the widget request and read back the exported table.

    Blocks on the response stream. Closing ``plugin`` from another thread is
    what interrupts that read; the deadline below only advances once the
    server has sent something.

    Args:
        plugin (PluginClient): An open widget client from :func:`_open_plugin`.
        table (WebClientDataTable): The per-user table to request.
        operate_as (str): Identity whose ACLs the table is built with.
        deadline_seconds (float): Wall-clock budget for a table to arrive on
            the response stream.

    Returns:
        Table: The requested table on the ``WebClientData`` worker.

    Raises:
        WebClientDataError: If the widget refuses the request, returns a
            non-table object, or produces no table before ``deadline_seconds``
            elapses.
    """
    plugin.req_stream.write(_request_payload(table, operate_as), references=[])

    deadline = time.monotonic() + deadline_seconds
    for payload, exported in plugin.resp_stream:
        if exported:
            fetched = exported[0].fetch()
            if not isinstance(fetched, Table):
                raise WebClientDataError(
                    f"The '{_TABLE_FACTORY_FIELD}' widget returned a "
                    f"{type(fetched).__name__} for table '{table}'; expected a table."
                )
            return fetched
        # A refusal carries a payload and exports nothing. Reading on would
        # block until the stream closes, so the deadline below never fires.
        if payload:
            raise WebClientDataError(
                f"The '{_TABLE_FACTORY_FIELD}' widget refused the request for "
                f"'{table}' as user '{operate_as}': {_refusal_reason(payload)}"
            )
        if time.monotonic() > deadline:
            break

    raise WebClientDataError(
        f"The '{_TABLE_FACTORY_FIELD}' widget returned no table for "
        f"'{table}' within {deadline_seconds:.0f}s."
    )


class _PluginHandoff:
    """Thread-safe ownership transfer of a plugin from the worker thread.

    The worker can finish opening the stream after ``asyncio.wait_for`` has
    given up, so whoever gets there second closes it. Without this the caller
    would have no handle to a plugin created by an abandoned worker.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._plugin: PluginClient | None = None
        self._released = False

    def offer(self, plugin: PluginClient) -> bool:
        """Hand ``plugin`` to the caller; False if the caller already gave up."""
        with self._lock:
            if self._released:
                return False
            self._plugin = plugin
            return True

    def release(self) -> PluginClient | None:
        """Take the plugin, if any, and refuse all later offers."""
        with self._lock:
            self._released = True
            plugin, self._plugin = self._plugin, None
            return plugin


def _open_and_request(
    handoff: _PluginHandoff,
    session: DndSession,
    table: WebClientDataTable,
    operate_as: str,
    deadline_seconds: float,
) -> Table:
    """Open the widget stream and read the requested table, in one thread.

    Args:
        handoff (_PluginHandoff): Receives the plugin as soon as it exists.
        session (DndSession): A session connected to the ``WebClientData``
            persistent query.
        table (WebClientDataTable): The per-user table to request.
        operate_as (str): Identity whose ACLs the table is built with.
        deadline_seconds (float): Wall-clock budget for a table to arrive on
            the response stream.

    Returns:
        Table: The requested table on the ``WebClientData`` worker.

    Raises:
        WebClientDataError: If the widget is unavailable, refuses the request,
            returns a non-table object, produces no table in time, or the
            caller abandoned this request while the stream was opening.
    """
    plugin = _open_plugin(session)
    if not handoff.offer(plugin):
        plugin.close()
        raise WebClientDataError(
            f"The request for '{table}' was abandoned while the "
            f"'{_TABLE_FACTORY_FIELD}' stream was opening."
        )
    return _request_table(plugin, table, operate_as, deadline_seconds)


def _release_plugin(handoff: _PluginHandoff) -> None:
    """Close the handed-off plugin, if any, without raising.

    A close failure must not replace the call's real result or error.

    Args:
        handoff (_PluginHandoff): The handoff to drain.
    """
    plugin = handoff.release()
    if plugin is None:
        return
    try:
        plugin.close()
    except Exception as e:
        _LOGGER.warning(
            f"[fetch_web_client_data_table] Failed to close the "
            f"'{_TABLE_FACTORY_FIELD}' stream: {e!r}"
        )


async def fetch_web_client_data_table(
    session: CorePlusSession,
    table: WebClientDataTable,
    *,
    operate_as: str,
    timeout_seconds: float,
) -> Table:
    """Fetch a per-user table from the WebClientData table-factory widget.

    Args:
        session (CorePlusSession): A session connected to the
            ``WebClientData`` persistent query.
        table (WebClientDataTable): The per-user table to request.
        operate_as (str): Identity whose ACLs the table is built with; the
            result shows only what this identity may see. Required — the
            server rejects a null or empty value.
        timeout_seconds (float): Budget for the widget request/response
            round-trip.

    Returns:
        Table: The requested table on the ``WebClientData`` worker. Callers
            may apply Deephaven Query Language filters to it.

    Raises:
        WebClientDataError: If the widget cannot produce the table, refuses
            the request, or the round-trip exceeds ``timeout_seconds``.
    """
    _LOGGER.debug(
        f"[fetch_web_client_data_table] Requesting table: table={str(table)!r}, "
        f"operate_as={operate_as!r}, timeout_seconds={timeout_seconds}"
    )
    handoff = _PluginHandoff()

    try:
        # Opening the stream is inside the deadline: a stalled widget connect
        # must fail on the same budget as a stalled read.
        result = await asyncio.wait_for(
            asyncio.to_thread(
                _open_and_request,
                handoff,
                session.wrapped,
                table,
                operate_as,
                timeout_seconds,
            ),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        # wait_for abandons the worker thread mid-read; only the plugin close
        # in the finally below ends that read, so the thread is not stranded.
        _LOGGER.error(
            f"[fetch_web_client_data_table] Timed out after {timeout_seconds}s "
            f"waiting for table {str(table)!r}"
        )
        raise WebClientDataError(
            f"Timed out after {timeout_seconds}s fetching '{table}' from "
            f"'{WEB_CLIENT_DATA_PQ}'. To allow more time, increase "
            f"enterprise/settings.json: timeouts.client.web_client_data_timeout_seconds."
        ) from None
    except WebClientDataError:
        raise
    except Exception as e:
        _LOGGER.error(
            f"[fetch_web_client_data_table] Failed to fetch table {str(table)!r}: {e!r}",
            exc_info=True,
        )
        raise WebClientDataError(
            f"Failed to fetch '{table}' from '{WEB_CLIENT_DATA_PQ}': "
            f"{describe_exception_chain(e)}"
        ) from e
    finally:
        await asyncio.to_thread(_release_plugin, handoff)

    _LOGGER.debug(f"[fetch_web_client_data_table] Received table: table={str(table)!r}")
    return result
