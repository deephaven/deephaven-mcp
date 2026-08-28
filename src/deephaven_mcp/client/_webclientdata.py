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

import enum
import json
import logging
import time
import uuid

from deephaven_enterprise.client.session_manager import DndSession
from pydeephaven.experimental.plugin_client import PluginClient
from pydeephaven.table import Table

from deephaven_mcp._blocking import BlockingResource
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


def _close_plugin(plugin: PluginClient) -> None:
    """Cancel the widget's underlying call, then close the client.

    ``PluginResponseStream.__next__`` holds a lock for the whole of a blocked
    read and ``close()`` needs that same lock, so closing alone cannot end the
    read. Canceling the gRPC call is what unblocks it; ``stream_resp`` is a
    pydeephaven internal, so a version that renames it degrades to the plain
    close rather than failing.

    Args:
        plugin (PluginClient): The widget client to tear down.
    """
    try:
        plugin.resp_stream.stream_resp.cancel()
    except Exception as e:
        _LOGGER.debug(
            f"[_close_plugin] Could not cancel the '{_TABLE_FACTORY_FIELD}' "
            f"call before closing: {e!r}"
        )
    plugin.close()


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
    widget = BlockingResource(
        lambda: _open_plugin(session.wrapped),
        _close_plugin,
    )

    try:
        # Opening the stream is inside the deadline: a stalled widget connect
        # must fail on the same budget as a stalled read.
        result = await widget.run(
            lambda plugin: _request_table(plugin, table, operate_as, timeout_seconds),
            timeout_seconds=timeout_seconds,
        )
    except TimeoutError:
        # The abandoned worker is blocked on the response stream; the close
        # BlockingResource performs is what ends that read.
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

    _LOGGER.debug(f"[fetch_web_client_data_table] Received table: table={str(table)!r}")
    return result
