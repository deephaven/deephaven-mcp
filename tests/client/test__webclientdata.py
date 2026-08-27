"""Tests for deephaven_mcp.client._webclientdata."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from deephaven_mcp._exceptions import WebClientDataError

# Table is bound from the module under test: tests/client/test__session.py
# replaces sys.modules["pydeephaven.table"], so importing it directly here can
# yield a different class than the one _fetch_blocking's isinstance check uses.
from deephaven_mcp.client._webclientdata import (
    WEB_CLIENT_DATA_PQ,
    Table,
    WebClientDataTable,
    _fetch_blocking,
    _refusal_reason,
    _request_payload,
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


# ===== _fetch_blocking =====


def test_fetch_blocking_returns_exported_table():
    table = MagicMock(spec=Table)
    plugin = DummyPluginClient([(b"", _exported(table))])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        result = _fetch_blocking(
            _session_with_factory(), WebClientDataTable.CATALOG, "iris", 30.0
        )
    assert result is table
    assert plugin.closed
    payload = json.loads(plugin.req_stream.writes[0][0])
    assert payload["tableNames"] == ["catalog"]


def test_fetch_blocking_skips_responses_without_payload_or_exports():
    table = MagicMock(spec=Table)
    plugin = DummyPluginClient([(b"", []), (b"", _exported(table))])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        result = _fetch_blocking(
            _session_with_factory(), WebClientDataTable.CATALOG, "iris", 30.0
        )
    assert result is table


def test_fetch_blocking_missing_factory_field():
    session = MagicMock()
    session.exportable_objects = {}
    with pytest.raises(WebClientDataError, match="does not export a table factory"):
        _fetch_blocking(session, WebClientDataTable.CATALOG, "iris", 30.0)


def test_fetch_blocking_surfaces_a_refusal():
    """A payload with no exports is the server refusing; it must not hang."""
    refusal = json.dumps({"id": "x", "error": "not allowed"}).encode("utf-8")
    plugin = DummyPluginClient([(refusal, [])])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        with pytest.raises(WebClientDataError, match="not allowed"):
            _fetch_blocking(
                _session_with_factory(), WebClientDataTable.CATALOG, "iris", 30.0
            )
    assert plugin.closed


def test_fetch_blocking_non_table_object():
    plugin = DummyPluginClient([(b"", _exported("not-a-table"))])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        with pytest.raises(WebClientDataError, match="expected a table"):
            _fetch_blocking(
                _session_with_factory(), WebClientDataTable.CATALOG, "iris", 30.0
            )
    assert plugin.closed


def test_fetch_blocking_stream_exhausted_without_table():
    plugin = DummyPluginClient([])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        with pytest.raises(WebClientDataError, match="returned no table"):
            _fetch_blocking(
                _session_with_factory(), WebClientDataTable.CATALOG, "iris", 30.0
            )


def test_fetch_blocking_breaks_on_deadline():
    """A zero budget stops the loop after the first empty response."""
    plugin = DummyPluginClient([(b"", []), (b"", [])])
    with patch("deephaven_mcp.client._webclientdata.PluginClient", return_value=plugin):
        with pytest.raises(WebClientDataError, match="returned no table"):
            _fetch_blocking(
                _session_with_factory(), WebClientDataTable.CATALOG, "iris", 0.0
            )


# ===== fetch_web_client_data_table =====


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_success():
    table = MagicMock(spec=Table)
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with patch(
        "deephaven_mcp.client._webclientdata._fetch_blocking", return_value=table
    ) as blocking:
        result = await fetch_web_client_data_table(
            session,
            WebClientDataTable.CATALOG,
            operate_as="iris",
            timeout_seconds=30.0,
        )
    assert result is table
    assert blocking.call_args[0][1] is WebClientDataTable.CATALOG
    assert blocking.call_args[0][2] == "iris"


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_propagates_web_client_data_error():
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with patch(
        "deephaven_mcp.client._webclientdata._fetch_blocking",
        side_effect=WebClientDataError("widget said no"),
    ):
        with pytest.raises(WebClientDataError, match="widget said no"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=30.0,
            )


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_wraps_unexpected_error():
    session = MagicMock()
    session.wrapped = _session_with_factory()
    with patch(
        "deephaven_mcp.client._webclientdata._fetch_blocking",
        side_effect=RuntimeError("grpc exploded"),
    ):
        with pytest.raises(WebClientDataError, match="Failed to fetch"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=30.0,
            )


@pytest.mark.asyncio
async def test_fetch_web_client_data_table_timeout():
    session = MagicMock()
    session.wrapped = _session_with_factory()

    async def never(*_args, **_kwargs):
        await asyncio.sleep(10)

    with patch("deephaven_mcp.client._webclientdata.asyncio.to_thread", never):
        with pytest.raises(WebClientDataError, match="Timed out"):
            await fetch_web_client_data_table(
                session,
                WebClientDataTable.CATALOG,
                operate_as="iris",
                timeout_seconds=0.01,
            )
