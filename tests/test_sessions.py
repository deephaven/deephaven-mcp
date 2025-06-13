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
    create_session,
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
async def test_create_session_error_handling():
    # Should raise SessionCreationError on failure
    with patch(
        "deephaven_mcp.sessions._sessions.Session",
        new=MagicMock(side_effect=RuntimeError("fail-create")),
    ):
        with pytest.raises(SessionCreationError) as exc_info:
            await create_session(host="localhost")
        # Check error message contains context and original error
        assert "Failed to create Deephaven Community (Core) Session" in str(
            exc_info.value
        )
        assert "fail" in str(exc_info.value) or (
            exc_info.value.__cause__ and "fail" in str(exc_info.value.__cause__)
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

    # Success case: patch Session in _lifecycle_community to return mock_session
    with patch(
        "deephaven_mcp.sessions._lifecycle.community.Session",
        new=MagicMock(return_value=mock_session),
    ):
        # Fire off multiple concurrent requests for the same worker
        results = await asyncio.gather(
            *[mgr.get_or_create_session("workerX") for _ in range(5)]
        )
    # All should return the same session object (cached)
    assert all(r is mock_session for r in results)
    # Only one session should have been created (simulate by call_count if using fake_create_session)


@pytest.mark.asyncio
async def test_session_manager_concurrent_get_or_create_session_failure():
    mock_cfg_mgr = AsyncMock()
    mock_cfg_mgr.get_worker_config = AsyncMock(return_value={"host": "localhost"})
    mock_cfg_mgr.get_config = AsyncMock(
        return_value={"community_sessions": {"workerX": {"host": "localhost"}}}
    )
    mgr = SessionManager(mock_cfg_mgr)
    with patch(
        "deephaven_mcp.sessions._lifecycle.community.Session",
        new=MagicMock(side_effect=RuntimeError("fail")),
    ):
        with pytest.raises(SessionCreationError) as exc_info:
            await mgr.get_or_create_session("workerX")
        assert "Failed to create Deephaven Community (Core) Session" in str(
            exc_info.value
        )
        assert "fail" in str(exc_info.value) or (
            exc_info.value.__cause__ and "fail" in str(exc_info.value.__cause__)
        )


@pytest.mark.asyncio
async def test_session_manager_delegates_to_helpers():
    mock_cfg_mgr = AsyncMock()
    mock_cfg_mgr.get_worker_config = AsyncMock(return_value={"host": "localhost"})
    mock_cfg_mgr.get_config = AsyncMock(
        return_value={"community_sessions": {"workerY": {"host": "localhost"}}}
    )
    mgr = SessionManager(mock_cfg_mgr)
    with patch(
        "deephaven_mcp.sessions._sessions.create_session_for_worker",
        new=AsyncMock(return_value="SESSION"),
    ) as mock_create_worker:
        result = await mgr.get_or_create_session("workerY")
    mock_create_worker.assert_called_once_with(mock_cfg_mgr, "workerY")
    assert result == "SESSION"


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
    session_manager._cache["session1"] = session
    await session_manager.clear_all_sessions()
    session.close.assert_called_once()


@pytest.mark.asyncio
async def test_create_session_error():
    # Patch Session to raise
    with patch(
        "deephaven_mcp.sessions._sessions.Session",
        new=MagicMock(side_effect=RuntimeError("fail")),
    ):
        with pytest.raises(SessionCreationError) as exc_info:
            await create_session(host="localhost")
        # Check error message contains context and original error
        assert "Failed to create Deephaven Community (Core) Session" in str(
            exc_info.value
        )
        assert "fail" in str(exc_info.value) or (
            exc_info.value.__cause__ and "fail" in str(exc_info.value.__cause__)
        )


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
    with patch("deephaven_mcp.sessions._lifecycle.community.Session", new=MagicMock()):
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
    mock_session = MagicMock()
    with (
        patch(
            "deephaven_mcp.sessions._lifecycle.community._get_session_parameters",
            new=AsyncMock(return_value={"host": "localhost"}),
        ),
        patch(
            "deephaven_mcp.sessions._lifecycle.community.Session",
            new=MagicMock(return_value=mock_session),
        ),
    ):
        result = await session_manager.get_or_create_session("foo")
        assert result == mock_session
        assert session_manager._cache["foo"] == mock_session


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
    mock_session = MagicMock()
    with (
        patch(
            "deephaven_mcp.sessions._lifecycle.community._get_session_parameters",
            new=AsyncMock(return_value={"host": "localhost"}),
        ),
        patch(
            "deephaven_mcp.sessions._lifecycle.community.Session",
            new=MagicMock(return_value=mock_session),
        ),
    ):
        result = await session_manager.get_or_create_session("foo")
        assert result == mock_session
        assert session_manager._cache["foo"] == mock_session
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
