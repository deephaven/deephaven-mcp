import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import types
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp.systems import _sessions
from deephaven_mcp.systems._sessions import (
    SessionCreationError,
    SessionManager,
    _load_bytes,
    get_dh_versions,
    get_meta_table,
    get_table,
)


# --- Fixtures and helpers ---
@pytest.fixture
def mock_config_manager():
    # Create a MagicMock for ConfigManager, with async methods
    mock = MagicMock()
    mock.get_system_session_config = AsyncMock(return_value={"host": "localhost"})
    mock.get_config = AsyncMock(
        return_value={"community_sessions": {"local": {"host": "localhost"}}}
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
        "deephaven_mcp.systems._sessions._load_bytes",
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

    monkeypatch.setattr("deephaven_mcp.systems._sessions._load_bytes", raise_io)
    cfg = {"tls_root_certs": "/bad/path"}
    with pytest.raises(IOError):
        await mgr._get_session_parameters(cfg)


@pytest.mark.asyncio
async def test_get_session_parameters_auth_token_from_env_var(
    session_manager, monkeypatch
):
    """Test auth_token is sourced from environment variable when auth_token_env_var is set."""
    env_var_name = "MY_TEST_TOKEN_VAR"
    expected_token = "token_from_environment"
    monkeypatch.setenv(env_var_name, expected_token)

    worker_cfg = {
        "auth_token_env_var": env_var_name,
        # As per config validation, auth_token should not be present if auth_token_env_var is.
    }
    params = await session_manager._get_session_parameters(worker_cfg)
    assert params["auth_token"] == expected_token
    monkeypatch.delenv(env_var_name)  # Clean up


@pytest.mark.asyncio
async def test_get_session_parameters_auth_token_env_var_not_set(
    session_manager, monkeypatch, caplog
):
    """Test auth_token is empty and warning logged if auth_token_env_var is set but env var is not."""
    env_var_name = "MY_MISSING_TOKEN_VAR"
    monkeypatch.delenv(env_var_name, raising=False)  # Ensure it's not set

    worker_cfg = {
        "auth_token_env_var": env_var_name,
    }
    params = await session_manager._get_session_parameters(worker_cfg)
    assert params["auth_token"] == ""
    assert (
        f"Environment variable {env_var_name} specified for auth_token but not found. Using empty token."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_get_session_parameters_auth_token_from_config(session_manager):
    """Test auth_token is sourced from config when auth_token_env_var is not set."""
    expected_token = "token_from_config_direct"
    worker_cfg = {
        "auth_token": expected_token,
    }
    params = await session_manager._get_session_parameters(worker_cfg)
    assert params["auth_token"] == expected_token


@pytest.mark.asyncio
async def test_get_session_parameters_no_auth_token_provided(session_manager):
    """Test auth_token is empty if neither auth_token nor auth_token_env_var is provided."""
    worker_cfg = {"host": "localhost"}  # Some other config, but no auth token fields
    params = await session_manager._get_session_parameters(worker_cfg)
    assert params["auth_token"] == ""


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
        "deephaven_mcp.systems._sessions.Session",
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
    session_manager._config_manager.get_config = AsyncMock(
        return_value={"community_sessions": {"foo": {"host": "localhost"}}}
    )
    monkeypatch.setattr("deephaven_mcp.systems._sessions.Session", MagicMock())
    await session_manager.get_or_create_session("foo")
    assert any("Error checking session liveness" in r for r in caplog.text.splitlines())
    assert "foo" in session_manager._cache


# --- Tests for get_meta_table ---


@pytest.mark.asyncio
async def test_get_meta_table_success():
    session_mock = MagicMock()
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    arrow_mock = object()
    session_mock.open_table.return_value = table_mock
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        return arrow_mock

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.systems._sessions.asyncio.to_thread", new=fake_to_thread):
        result = await get_meta_table(session_mock, "foo")
        assert result is arrow_mock
        session_mock.open_table.assert_called_once_with("foo")


@pytest.mark.asyncio
async def test_get_meta_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table.side_effect = RuntimeError("fail-open")
    with pytest.raises(RuntimeError) as excinfo:
        await get_meta_table(session_mock, "foo")
    assert "fail-open" in str(excinfo.value)
    session_mock.open_table.assert_called_once_with("foo")


@pytest.mark.asyncio
async def test_get_meta_table_to_arrow_error():
    session_mock = MagicMock()
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    session_mock.open_table.return_value = table_mock
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        raise RuntimeError("fail-arrow")

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.systems._sessions.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError) as excinfo:
            await get_meta_table(session_mock, "foo")
        assert "fail-arrow" in str(excinfo.value)
        session_mock.open_table.assert_called_once_with("foo")


# --- Tests for get_pip_packages_table ---

import logging


@pytest.mark.asyncio
async def test_get_pip_packages_table_success(monkeypatch, caplog):
    session_mock = MagicMock()

    # Patch asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(_sessions.asyncio, "to_thread", fake_to_thread)
    # Patch get_table to return a sentinel
    arrow_mock = MagicMock()
    monkeypatch.setattr(_sessions, "get_table", AsyncMock(return_value=arrow_mock))
    # Should log all steps
    with caplog.at_level(logging.INFO):
        result = await _sessions.get_pip_packages_table(session_mock)
    assert result is arrow_mock
    assert "Running pip packages script in session..." in caplog.text
    assert "Script executed successfully." in caplog.text
    assert "Table retrieved successfully." in caplog.text
    session_mock.run_script.assert_called_once()
    _sessions.get_table.assert_awaited_once_with(session_mock, "_pip_packages_table")


@pytest.mark.asyncio
async def test_get_pip_packages_table_script_failure(monkeypatch):
    session_mock = MagicMock()

    # Patch asyncio.to_thread to raise when running script
    async def fake_to_thread(fn, *args, **kwargs):
        if fn == session_mock.run_script:
            raise RuntimeError("fail-script")
        return fn(*args, **kwargs)

    monkeypatch.setattr(_sessions.asyncio, "to_thread", fake_to_thread)
    with pytest.raises(RuntimeError, match="fail-script"):
        await _sessions.get_pip_packages_table(session_mock)
    # No assertion on run_script: exception is raised before call


@pytest.mark.asyncio
async def test_get_pip_packages_table_table_failure(monkeypatch):
    session_mock = MagicMock()

    # Patch asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(_sessions.asyncio, "to_thread", fake_to_thread)
    # Patch get_table to raise
    monkeypatch.setattr(
        _sessions, "get_table", AsyncMock(side_effect=RuntimeError("fail-table"))
    )
    with pytest.raises(RuntimeError, match="fail-table"):
        await _sessions.get_pip_packages_table(session_mock)
    session_mock.run_script.assert_called_once()
    _sessions.get_table.assert_awaited_once_with(session_mock, "_pip_packages_table")


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
    session_manager._config_manager.get_config = AsyncMock(
        return_value={"community_sessions": {"foo": {"host": "localhost"}}}
    )
    monkeypatch.setattr("deephaven_mcp.systems._sessions.Session", MagicMock())
    result = await session_manager.get_or_create_session("foo")
    assert result is session


@pytest.mark.asyncio
async def test_get_or_create_session_creates_new(monkeypatch, session_manager):
    session_manager._cache.clear()
    fake_config = {"host": "localhost"}
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value=fake_config
    )
    session_manager._config_manager.get_config = AsyncMock(
        return_value={"community_sessions": {"foo": {"host": "localhost"}}}
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
    # Patch get_config to include 'community_sessions: {"foo": ...}'
    session_manager._config_manager.get_config = AsyncMock(
        return_value={"community_sessions": {"foo": {"host": "localhost"}}}
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


# --- Tests for get_dh_versions ---


@pytest.mark.asyncio
async def test_get_dh_versions_both_versions(monkeypatch):
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven-core", "Version": "1.2.3"},
        {"Package": "deephaven_coreplus_worker", "Version": "4.5.6"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    )
    core, coreplus = await get_dh_versions(session)
    assert core == "1.2.3"
    assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_only_core(monkeypatch):
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven-core", "Version": "1.2.3"},
        {"Package": "numpy", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    )
    core, coreplus = await get_dh_versions(session)
    assert core == "1.2.3"
    assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_only_coreplus(monkeypatch):
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven_coreplus_worker", "Version": "4.5.6"},
        {"Package": "pandas", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    )
    core, coreplus = await get_dh_versions(session)
    assert core is None
    assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_neither(monkeypatch):
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "numpy", "Version": "2.0.0"},
        {"Package": "pandas", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    )
    core, coreplus = await get_dh_versions(session)
    assert core is None
    assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_malformed(monkeypatch):
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [{"NotPackage": "foo", "NotVersion": "bar"}]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    )
    core, coreplus = await get_dh_versions(session)
    assert core is None
    assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_arrow_none(monkeypatch):
    session = MagicMock()
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(return_value=None),
    )
    core, coreplus = await get_dh_versions(session)
    assert core is None
    assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_raises(monkeypatch):
    session = MagicMock()
    monkeypatch.setattr(
        "deephaven_mcp.systems._sessions.get_pip_packages_table",
        AsyncMock(side_effect=RuntimeError("fail!")),
    )
    with pytest.raises(RuntimeError, match="fail!"):
        await get_dh_versions(session)
