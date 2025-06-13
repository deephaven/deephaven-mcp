import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import asyncio
import logging
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp.config._community_session import redact_community_session_config
from deephaven_mcp.sessions import _sessions
from deephaven_mcp.sessions._queries import get_dh_versions
from deephaven_mcp.sessions._sessions import (
    SessionCreationError,
    SessionManager,
    close_session_safely,
    create_session,
    get_session_parameters,
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


# --- Additional Robustness Tests ---


@pytest.mark.asyncio
async def test_get_session_parameters_missing_fields():
    # Should not raise if only host provided
    params = await get_session_parameters({"host": "localhost"})
    assert params["host"] == "localhost"
    # Should fill defaults
    assert params["auth_type"] == "Anonymous"
    assert params["auth_token"] == ""
    assert params["never_timeout"] is False
    assert params["session_type"] == "python"
    assert params["use_tls"] is False


@pytest.mark.asyncio
async def test_create_session_error_handling():
    # Should raise SessionCreationError on failure
    with patch(
        "deephaven_mcp.sessions._sessions.Session",
        new=MagicMock(side_effect=RuntimeError("fail-create")),
    ):
        with pytest.raises(SessionCreationError) as exc_info:
            await create_session(host="localhost")
        assert "fail-create" in str(exc_info.value)


@pytest.mark.asyncio
async def test_close_session_safely_handles_exceptions(caplog):
    # Should log and not raise on error (close raises)
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close.side_effect = Exception("fail-close")
    await close_session_safely("worker1", session)
    assert any("Failed to close session" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_static_close_session_safely_already_closed(caplog):
    caplog.set_level("DEBUG")
    session = MagicMock(spec=Session)
    session.is_alive = False
    await SessionManager._close_session_safely("workerX", session)
    assert any("is already closed" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_static_close_session_safely_close_raises(caplog):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close.side_effect = Exception("fail-close")
    await SessionManager._close_session_safely("workerY", session)
    assert any("Failed to close session" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_static_close_session_safely_close_success(caplog):
    caplog.set_level("INFO")
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close.side_effect = None  # No error
    await SessionManager._close_session_safely("workerZ", session)
    assert any("Successfully closed session" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_close_session_safely_already_closed(caplog):
    # Should log debug if session is already closed (is_alive is False)
    session = MagicMock(spec=Session)
    session.is_alive = False
    await close_session_safely("worker2", session)
    assert any("already closed" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_close_session_safely_is_alive_raises(caplog):
    # Should log error if is_alive raises (raise once, then return False)
    session = MagicMock(spec=Session)
    calls = {"count": 0}

    def is_alive_side_effect(self):
        if calls["count"] == 0:
            calls["count"] += 1
            raise Exception("fail-attr")
        return False

    type(session).is_alive = property(is_alive_side_effect)
    await close_session_safely("worker3", session)
    assert any(
        "Failed to close session" in r or "Error" in r for r in caplog.text.splitlines()
    )


@pytest.mark.asyncio
async def test_session_manager_concurrent_get_or_create_session():
    # Use an AsyncMock for config manager so awaited calls work
    mock_cfg_mgr = AsyncMock()
    mock_cfg_mgr.get_worker_config = AsyncMock(return_value={"host": "localhost"})
    mock_cfg_mgr.get_config = AsyncMock(
        return_value={"community_sessions": {"workerX": {"host": "localhost"}}}
    )
    mgr = SessionManager(mock_cfg_mgr)
    # Patch helpers to simulate slow creation and track calls
    call_count = 0
    # Use a single mock session with is_alive = True
    mock_session = MagicMock()
    mock_session.is_alive = True

    async def fake_get_params(cfg):
        await asyncio.sleep(0.05)
        return {"host": "localhost"}

    async def fake_create_session(**kwargs):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return mock_session

    with (
        patch(
            "deephaven_mcp.sessions._sessions.get_session_parameters",
            new=fake_get_params,
        ),
        patch(
            "deephaven_mcp.sessions._sessions.create_session", new=fake_create_session
        ),
    ):
        # Fire off multiple concurrent requests for the same worker
        results = await asyncio.gather(
            *[mgr.get_or_create_session("workerX") for _ in range(5)]
        )
    # All should return the same session object (cached)
    assert all(r is mock_session for r in results)
    # Only one session should have been created
    assert call_count == 1


@pytest.mark.asyncio
async def test_session_manager_delegates_to_helpers():
    mock_cfg_mgr = AsyncMock()
    mock_cfg_mgr.get_worker_config = AsyncMock(return_value={"host": "localhost"})
    mock_cfg_mgr.get_config = AsyncMock(
        return_value={"community_sessions": {"workerY": {"host": "localhost"}}}
    )
    mgr = SessionManager(mock_cfg_mgr)
    with (
        patch(
            "deephaven_mcp.sessions._sessions.get_session_parameters",
            new=AsyncMock(return_value={"host": "localhost"}),
        ) as mock_params,
        patch(
            "deephaven_mcp.sessions._sessions.create_session",
            new=AsyncMock(return_value="SESSION"),
        ) as mock_create,
    ):
        result = await mgr.get_or_create_session("workerY")
    mock_params.assert_called_once()
    mock_create.assert_called_once()
    assert result == "SESSION"


@pytest.fixture
def fake_session():
    s = MagicMock(spec=Session)
    s.is_alive = True
    return s


# --- Tests for SessionManager._close_session_safely ---
@pytest.mark.asyncio
async def test_close_session_safely_closes_alive(session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close = MagicMock()
    await close_session_safely("worker1", session)
    session.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_session_safely_already_closed(session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = False
    session.close = MagicMock()
    await close_session_safely("worker1", session)
    session.close.assert_not_called()


@pytest.mark.asyncio
async def test_close_session_safely_raises(session_manager, caplog):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close.side_effect = Exception("fail-close")
    await close_session_safely("worker1", session)
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
async def test_get_session_parameters_with_and_without_files():
    mgr = SessionManager(mock_config_manager)
    from unittest.mock import AsyncMock, patch

    with patch(
        "deephaven_mcp.sessions._sessions.load_bytes",
        new=AsyncMock(return_value=b"binary"),
    ):
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
        params = await get_session_parameters(cfg)
        assert params["tls_root_certs"] == b"binary"
        assert params["client_cert_chain"] == b"binary"
        assert params["client_private_key"] == b"binary"
        # No files present
        cfg = {"host": "localhost"}
        params = await get_session_parameters(cfg)
        assert params["host"] == "localhost"


@pytest.mark.asyncio
async def test_get_session_parameters_file_error():
    # Patch load_bytes to raise
    async def raise_io(path):
        raise IOError("fail")

    with patch("deephaven_mcp.sessions._sessions.load_bytes", new=raise_io):
        cfg = {"tls_root_certs": "/bad/path"}
        with pytest.raises(IOError):
            await get_session_parameters(cfg)


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
    params = await get_session_parameters(worker_cfg)
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
    params = await get_session_parameters(worker_cfg)
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
    params = await get_session_parameters(worker_cfg)
    assert params["auth_token"] == expected_token


@pytest.mark.asyncio
async def test_get_session_parameters_no_auth_token_provided(session_manager):
    """Test auth_token is empty if neither auth_token nor auth_token_env_var is provided."""
    worker_cfg = {"host": "localhost"}  # Some other config, but no auth token fields
    params = await get_session_parameters(worker_cfg)
    assert params["auth_token"] == ""


@pytest.mark.asyncio
async def test_create_session_error():
    # Patch Session to raise
    with patch(
        "deephaven_mcp.sessions._sessions.Session",
        new=MagicMock(side_effect=RuntimeError("fail")),
    ):
        with pytest.raises(SessionCreationError) as exc_info:
            await create_session(host="localhost")
        assert "fail" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_or_create_session_liveness_exception(session_manager, caplog):
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
    with patch("deephaven_mcp.sessions._sessions.Session", new=MagicMock()):
        await session_manager.get_or_create_session("foo")
        assert any(
            "Error checking session liveness" in r for r in caplog.text.splitlines()
        )
        assert "foo" in session_manager._cache


# --- Tests for get_or_create_session ---
@pytest.mark.asyncio
async def test_get_or_create_session_reuses_alive(session_manager):
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
    with patch("deephaven_mcp.sessions._sessions.Session", new=MagicMock()):
        result = await session_manager.get_or_create_session("foo")
        assert result is session


@pytest.mark.asyncio
async def test_get_or_create_session_creates_new(session_manager):
    session_manager._cache.clear()
    fake_config = {"host": "localhost"}
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value=fake_config
    )
    session_manager._config_manager.get_config = AsyncMock(
        return_value={"community_sessions": {"foo": {"host": "localhost"}}}
    )
    with (
        patch(
            "deephaven_mcp.sessions._sessions.get_session_parameters",
            new=AsyncMock(return_value={"host": "localhost"}),
        ),
        patch(
            "deephaven_mcp.sessions._sessions.create_session",
            new=AsyncMock(return_value="SESSION"),
        ),
    ):
        result = await session_manager.get_or_create_session("foo")
        assert result == "SESSION"
        assert session_manager._cache["foo"] == "SESSION"


@pytest.mark.asyncio
async def test_get_or_create_session_handles_dead(session_manager):
    session = MagicMock(spec=Session)
    session.is_alive = False
    session_manager._cache["foo"] = session
    fake_config = {"host": "localhost"}
    session_manager._config_manager.get_worker_config = AsyncMock(
        return_value=fake_config
    )
    session_manager._config_manager.get_config = AsyncMock(
        return_value={"community_sessions": {"foo": {"host": "localhost"}}}
    )
    with (
        patch(
            "deephaven_mcp.sessions._sessions.get_session_parameters",
            new=AsyncMock(return_value={"host": "localhost"}),
        ),
        patch(
            "deephaven_mcp.sessions._sessions.create_session",
            new=AsyncMock(return_value="SESSION"),
        ),
    ):
        result = await session_manager.get_or_create_session("foo")
        assert result == "SESSION"
        assert session_manager._cache["foo"] == "SESSION"
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven_coreplus_worker", "Version": "4.5.6"},
        {"Package": "pandas", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_neither():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "numpy", "Version": "2.0.0"},
        {"Package": "pandas", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_malformed():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [{"NotPackage": "foo", "NotVersion": "bar"}]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_arrow_none():
    session = MagicMock()
    with patch(
        "deephaven_mcp.sessions._queries.get_pip_packages_table",
        new=AsyncMock(return_value=None),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_raises():
    session = MagicMock()
    with patch(
        "deephaven_mcp.sessions._queries.get_pip_packages_table",
        new=AsyncMock(side_effect=RuntimeError("fail!")),
    ):
        with pytest.raises(RuntimeError, match="fail!"):
            await get_dh_versions(session)
