import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from unittest.mock import AsyncMock, MagicMock

import pyarrow
import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp.community._sessions import (
    SessionCreationError,
    SessionManager,
    _load_bytes,
    get_table,
)


# --- Fixtures and helpers ---
@pytest.fixture
def mock_config_manager():
    # Create a MagicMock for ConfigManager, with async methods
    mock = MagicMock()
    mock.get_worker_config = AsyncMock(return_value={"host": "localhost"})
    mock.get_config = AsyncMock(
        return_value={"workers": {"local": {"host": "localhost"}}}
    )
    return mock


@pytest.fixture
def session_manager(mock_config_manager):
    return SessionManager(mock_config_manager)


@pytest.fixture
def fake_session():
    s = MagicMock(spec=Session)
    s.is_alive = True
    return s


# --- Tests for _load_bytes ---
@pytest.mark.asyncio
async def test_load_bytes_reads_file(tmp_path):
    file_path = tmp_path / "cert.pem"
    content = b"test-bytes"
    file_path.write_bytes(content)
    result = await _load_bytes(str(file_path))
    assert result == content


@pytest.mark.asyncio
async def test_load_bytes_none():
    assert await _load_bytes(None) is None


@pytest.mark.asyncio
async def test_load_bytes_error(tmp_path, caplog):
    with pytest.raises(Exception):
        await _load_bytes("/nonexistent/path/to/file")


# --- Tests for SessionManager._redact_sensitive_session_fields ---
def test_redact_sensitive_session_fields_comprehensive(session_manager):
    # All sensitive keys, string and binary
    config = {
        "auth_token": "secret",
        "tls_root_certs": b"bytes",
        "client_cert_chain": b"chain-bytes",
        "client_private_key": b"key-bytes",
        "foo": "bar",
    }
    # Default: redact all sensitive fields
    redacted = session_manager._redact_sensitive_session_fields(config)
    assert redacted["auth_token"] == "REDACTED"
    assert redacted["tls_root_certs"] == "REDACTED"
    assert redacted["client_cert_chain"] == "REDACTED"
    assert redacted["client_private_key"] == "REDACTED"
    assert redacted["foo"] == "bar"
    # redact_binary_values=False: only auth_token is redacted, binary fields are not
    config = {"auth_token": "tok", "tls_root_certs": b"binary"}
    redacted = session_manager._redact_sensitive_session_fields(
        config, redact_binary_values=False
    )
    assert redacted["auth_token"] == "REDACTED"
    assert redacted["tls_root_certs"] == b"binary"


# --- Tests for SessionManager._close_session_safely ---
@pytest.mark.asyncio
async def test_close_session_safely_closes_alive(session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close = MagicMock()
    await session_manager._close_session_safely("worker1", session)
    session.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_session_safely_already_closed(session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = False
    session.close = MagicMock()
    await session_manager._close_session_safely("worker1", session)
    session.close.assert_not_called()


@pytest.mark.asyncio
async def test_close_session_safely_raises(session_manager, caplog):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close.side_effect = Exception("fail-close")
    await session_manager._close_session_safely("worker1", session)
    assert any("Failed to close session" in r for r in caplog.text.splitlines())


# --- Tests for SessionManager context manager ---
@pytest.mark.asyncio
async def test_async_context_manager_clears_sessions():
    mgr = SessionManager(mock_config_manager)
    mgr._cache["foo"] = MagicMock(spec=Session)
    mgr._cache["foo"].is_alive = False
    async with mgr:
        assert "foo" in mgr._cache
    assert mgr._cache == {}


# --- Tests for clear_all_sessions ---
@pytest.mark.asyncio
async def test_clear_all_sessions_calls_close(session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close = MagicMock()
    session_manager._cache["worker1"] = session
    await session_manager.clear_all_sessions()
    session.close.assert_called_once()


@pytest.mark.asyncio
async def test_get_session_parameters_with_and_without_files(monkeypatch):
    mgr = SessionManager(mock_config_manager)
    # Patch _load_bytes to simulate file reading
    monkeypatch.setattr(
        "deephaven_mcp.community._sessions._load_bytes",
        AsyncMock(return_value=b"binary"),
    )
    # All fields present (as file paths)
    cfg = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "token",
        "auth_token": "tok",
        "never_timeout": True,
        "session_type": "python",
        "use_tls": True,
        "tls_root_certs": "/tmp/root.pem",
        "client_cert_chain": "/tmp/chain.pem",
        "client_private_key": "/tmp/key.pem",
    }
    params = await mgr._get_session_parameters(cfg)
    assert params["tls_root_certs"] == b"binary"
    assert params["client_cert_chain"] == b"binary"
    assert params["client_private_key"] == b"binary"
    # No files present
    cfg = {"host": "localhost"}
    params = await mgr._get_session_parameters(cfg)
    assert params["host"] == "localhost"


@pytest.mark.asyncio
async def test_get_session_parameters_file_error(monkeypatch):
    mgr = SessionManager(mock_config_manager)

    # Patch _load_bytes to raise
    async def raise_io(path):
        raise IOError("fail")

    monkeypatch.setattr("deephaven_mcp.community._sessions._load_bytes", raise_io)
    cfg = {"tls_root_certs": "/bad/path"}
    with pytest.raises(IOError):
        await mgr._get_session_parameters(cfg)


def test_redact_sensitive_session_fields_empty():
    mgr = SessionManager(mock_config_manager)
    # No sensitive values
    cfg = {"foo": "bar"}
    assert mgr._redact_sensitive_session_fields(cfg) == cfg


@pytest.mark.asyncio
async def test_create_session_error(monkeypatch):
    mgr = SessionManager(mock_config_manager)
    # Patch Session to raise
    monkeypatch.setattr(
        "deephaven_mcp.community._sessions.Session",
        MagicMock(side_effect=RuntimeError("fail")),
    )
    with pytest.raises(SessionCreationError) as exc_info:
        await mgr._create_session(host="localhost")
    assert "fail" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_or_create_session_liveness_exception(
    monkeypatch, session_manager, caplog
):
    # Simulate exception in session.is_alive
    bad_session = MagicMock(spec=Session)
    type(bad_session).is_alive = property(
        lambda self: (_ for _ in ()).throw(Exception("fail"))
    )
    session_manager._cache["foo"] = bad_session
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value={"host": "localhost"}
    )
    monkeypatch.setattr("deephaven_mcp.community._sessions.Session", MagicMock())
    await session_manager.get_or_create_session("foo")
    assert any("Error checking session liveness" in r for r in caplog.text.splitlines())
    assert "foo" in session_manager._cache


# --- Tests for get_table ---


@pytest.mark.asyncio
async def test_get_table_success():
    table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    table_mock.to_arrow = MagicMock(return_value=arrow_mock)
    session_mock = MagicMock()
    session_mock.open_table = MagicMock(return_value=table_mock)
    result = await get_table(session_mock, "foo")
    assert result is arrow_mock
    session_mock.open_table.assert_called_once_with("foo")
    table_mock.to_arrow.assert_called_once()


@pytest.mark.asyncio
async def test_get_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table = MagicMock(side_effect=RuntimeError("fail open"))
    with pytest.raises(RuntimeError, match="fail open"):
        await get_table(session_mock, "foo")


@pytest.mark.asyncio
async def test_get_table_to_arrow_error():
    table_mock = MagicMock()
    table_mock.to_arrow = MagicMock(side_effect=RuntimeError("fail arrow"))
    session_mock = MagicMock()
    session_mock.open_table = MagicMock(return_value=table_mock)
    with pytest.raises(RuntimeError, match="fail arrow"):
        await get_table(session_mock, "foo")


# --- Tests for get_or_create_session ---
@pytest.mark.asyncio
async def test_get_or_create_session_reuses_alive(monkeypatch, session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.host = "localhost"
    session.port = 10000
    session_manager._cache["foo"] = session
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value={"host": "localhost"}
    )
    monkeypatch.setattr("deephaven_mcp.community._sessions.Session", MagicMock())
    result = await session_manager.get_or_create_session("foo")
    assert result is session


@pytest.mark.asyncio
async def test_get_or_create_session_creates_new(monkeypatch, session_manager):
    session_manager._cache.clear()
    fake_config = {"host": "localhost"}
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value=fake_config
    )
    monkeypatch.setattr(
        SessionManager,
        "_get_session_parameters",
        AsyncMock(return_value={"host": "localhost"}),
    )
    monkeypatch.setattr(
        SessionManager, "_create_session", AsyncMock(return_value="SESSION")
    )
    result = await session_manager.get_or_create_session("foo")
    assert result == "SESSION"
    assert session_manager._cache["foo"] == "SESSION"


@pytest.mark.asyncio
async def test_get_or_create_session_handles_dead(monkeypatch, session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = False
    session_manager._cache["foo"] = session
    fake_config = {"host": "localhost"}
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value=fake_config
    )
    monkeypatch.setattr(
        SessionManager,
        "_get_session_parameters",
        AsyncMock(return_value={"host": "localhost"}),
    )
    monkeypatch.setattr(
        SessionManager, "_create_session", AsyncMock(return_value="SESSION")
    )
    result = await session_manager.get_or_create_session("foo")
    assert result == "SESSION"
    assert session_manager._cache["foo"] == "SESSION"
