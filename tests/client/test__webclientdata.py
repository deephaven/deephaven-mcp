"""Tests for deephaven_mcp.client._webclientdata."""

import json
import threading
from unittest.mock import MagicMock, patch

import pytest

from deephaven_mcp._exceptions import WebClientDataError

# Table is bound from the module under test: tests/client/test__session.py
# replaces sys.modules["pydeephaven.table"], so importing it directly here can
# yield a different class than the one _request_table's isinstance check uses.
from deephaven_mcp.client._webclientdata import (
    WEB_CLIENT_DATA_PQ,
    Table,
    WebClientDataTable,
    _open_plugin,
    _refusal_reason,
    _request_payload,
    _request_table,
    fetch_web_client_data_table,
)


class DummyReqStream:
    """Records what the widget request stream was asked to write."""

    def __init__(self):
        self.writes = []

    def write(self, payload, references):
        self.writes.append((payload, references))


class DummyPluginClient:
    """Stands in for pydeephaven's PluginClient over the widget stream."""

    def __init__(self, responses):
        self.req_stream = DummyReqStream()
        self.resp_stream = responses
        self.closed = False

    def close(self):
        self.closed = True


def _session_with_factory():
    session = MagicMock()
    session.exportable_objects = {"WebClientTableFactory": MagicMock()}
    return session


def _exported(table):
    """Build a one-element exported-objects list whose fetch() yields ``table``."""
    exported = MagicMock()
    exported.fetch.return_value = table
    return [exported]


# ===== table names =====


def test_table_names_match_the_server_literals():
    """The enum values are the server's per-user table names."""
    assert str(WebClientDataTable.CATALOG) == "catalog"
    assert str(WebClientDataTable.QUERY_INFO) == "QueryInfo"
    assert WEB_CLIENT_DATA_PQ == "WebClientData"


# ===== _request_payload =====


def test_request_payload_names_the_table_and_user():
    payload = json.loads(_request_payload(WebClientDataTable.CATALOG, "iris"))
    assert payload["tableNames"] == ["catalog"]
    assert payload["user"] == "iris"
    assert "id" in payload


def test_request_payload_ids_are_unique():
    first = json.loads(_request_payload(WebClientDataTable.CATALOG, "iris"))
    second = json.loads(_request_payload(WebClientDataTable.CATALOG, "iris"))
    assert first["id"] != second["id"]


# ===== _refusal_reason =====


def test_refusal_reason_extracts_the_error_field():
    payload = json.dumps({"id": "abc", "error": "no such table"}).encode("utf-8")
    assert _refusal_reason(payload) == "no such table"


def test_refusal_reason_renders_non_error_json():
    payload = json.dumps({"id": "abc", "error": None}).encode("utf-8")
    assert "abc" in _refusal_reason(payload)


def test_refusal_reason_renders_undecodable_payload():
    assert "xff" in _refusal_reason(b"\xff\xfe")


def test_refusal_reason_renders_non_json_payload():
    assert "not json" in _refusal_reason(b"not json")


# ===== _open_plugin =====


def test_open_plugin_missing_factory_field():
    session = MagicMock()
    session.exportable_objects = {}
    with pytest.raises(WebClientDataError, match="does not export a table factory"):
        _open_plugin(session)


def test_open_plugin_returns_connected_client():
    plugin = DummyPluginClient([])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        assert _open_plugin(_session_with_factory()) is plugin


# ===== _request_table =====


def test_request_table_returns_exported_table():
    table = MagicMock(spec=Table)
    plugin = DummyPluginClient([(b"", _exported(table))])
    result = _request_table(plugin, WebClientDataTable.CATALOG, "iris", 30.0)
    assert result is table
    payload = json.loads(plugin.req_stream.writes[0][0])
    assert payload["tableNames"] == ["catalog"]


def test_request_table_skips_responses_without_payload_or_exports():
    table = MagicMock(spec=Table)
    plugin = DummyPluginClient([(b"", []), (b"", _exported(table))])
    result = _request_table(plugin, WebClientDataTable.CATALOG, "iris", 30.0)
    assert result is table


def test_request_table_surfaces_a_refusal():
    """A payload with no exports is the server refusing; it must not hang."""
    refusal = json.dumps({"id": "x", "error": "not allowed"}).encode("utf-8")
    plugin = DummyPluginClient([(refusal, [])])
    with pytest.raises(WebClientDataError, match="not allowed"):
        _request_table(plugin, WebClientDataTable.CATALOG, "iris", 30.0)


def test_request_table_non_table_object():
    plugin = DummyPluginClient([(b"", _exported("not-a-table"))])
    with pytest.raises(WebClientDataError, match="expected a table"):
        _request_table(plugin, WebClientDataTable.CATALOG, "iris", 30.0)


def test_request_table_stream_exhausted_without_table():
    plugin = DummyPluginClient([])
    with pytest.raises(WebClientDataError, match="returned no table"):
        _request_table(plugin, WebClientDataTable.CATALOG, "iris", 30.0)


def test_request_table_breaks_on_deadline():
    """A zero budget stops the loop after the first empty response."""
    plugin = DummyPluginClient([(b"", []), (b"", [])])
    with pytest.raises(WebClientDataError, match="returned no table"):
        _request_table(plugin, WebClientDataTable.CATALOG, "iris", 0.0)


# ===== fetch_web_client_data_table =====


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_success():
    table = MagicMock(spec=Table)
    plugin = DummyPluginClient([])
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with (
        patch("deephaven_mcp.client._webclientdata._open_plugin", return_value=plugin),
        patch(
            "deephaven_mcp.client._webclientdata._request_table", return_value=table
        ) as request,
    ):
        result = await fetch_web_client_data_table(
            session,
            WebClientDataTable.CATALOG,
            operate_as="iris",
            timeout_seconds=30.0,
        )
    assert result is table
    assert request.call_args[0][0] is plugin
    assert request.call_args[0][1] is WebClientDataTable.CATALOG
    assert request.call_args[0][2] == "iris"
    assert plugin.closed


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_propagates_open_failure():
    """A missing widget fails before any plugin exists to close."""
    session = MagicMock()
    session.wrapped = MagicMock()
    session.wrapped.exportable_objects = {}
    with pytest.raises(WebClientDataError, match="does not export a table factory"):
        await fetch_web_client_data_table(
            session,
            WebClientDataTable.CATALOG,
            operate_as="iris",
            timeout_seconds=30.0,
        )


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_wraps_a_vendor_error_from_open():
    """Opening the stream is guarded too, so PluginClient errors are wrapped."""
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with patch(
        "deephaven_mcp.client._webclientdata._open_plugin",
        side_effect=RuntimeError("connect refused"),
    ):
        with pytest.raises(WebClientDataError, match="Failed to fetch"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=30.0,
            )


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_times_out_on_a_stalled_open():
    """A widget connect that never returns is bounded by the same deadline."""
    session = MagicMock()
    session.wrapped = _session_with_factory()
    release = threading.Event()

    def stalled_open(*_args, **_kwargs):
        release.wait(timeout=10)
        return DummyPluginClient([])

    with patch("deephaven_mcp.client._webclientdata._open_plugin", stalled_open):
        with pytest.raises(WebClientDataError, match="Timed out"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=0.05,
            )
    release.set()


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_propagates_web_client_data_error():
    plugin = DummyPluginClient([])
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with (
        patch("deephaven_mcp.client._webclientdata._open_plugin", return_value=plugin),
        patch(
            "deephaven_mcp.client._webclientdata._request_table",
            side_effect=WebClientDataError("widget said no"),
        ),
    ):
        with pytest.raises(WebClientDataError, match="widget said no"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=30.0,
            )
    assert plugin.closed


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_wraps_unexpected_error():
    plugin = DummyPluginClient([])
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with (
        patch("deephaven_mcp.client._webclientdata._open_plugin", return_value=plugin),
        patch(
            "deephaven_mcp.client._webclientdata._request_table",
            side_effect=RuntimeError("grpc exploded"),
        ),
    ):
        with pytest.raises(WebClientDataError, match="Failed to fetch"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=30.0,
            )
    assert plugin.closed


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_timeout_releases_the_worker():
    """A timeout must close the stream so the blocked worker thread returns.

    ``asyncio.wait_for`` abandons the thread but cannot interrupt it, so the
    close in the ``finally`` is the only thing that ends the read. Uses a
    genuinely blocking worker rather than a coroutine stand-in.
    """
    stream_closed = threading.Event()
    worker_returned = threading.Event()

    class BlockingPlugin:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True
            stream_closed.set()

    plugin = BlockingPlugin()

    def blocking_request(*_args, **_kwargs):
        # Stands in for a read on resp_stream: returns only once closed.
        assert stream_closed.wait(timeout=10)
        worker_returned.set()
        return MagicMock(spec=Table)

    session = MagicMock()
    session.wrapped = _session_with_factory()
    with (
        patch("deephaven_mcp.client._webclientdata._open_plugin", return_value=plugin),
        patch("deephaven_mcp.client._webclientdata._request_table", blocking_request),
    ):
        with pytest.raises(WebClientDataError, match="Timed out"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=0.05,
            )

    assert plugin.closed
    assert worker_returned.wait(timeout=10), "worker thread was left stranded"
