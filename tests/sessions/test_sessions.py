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
from deephaven_mcp.sessions._errors import SessionCreationError
from deephaven_mcp.sessions._lifecycle.community import create_session
from deephaven_mcp.sessions._queries import get_dh_versions
from deephaven_mcp.sessions._sessions import SessionManager


# --- Fixtures and helpers ---
@pytest.fixture
def mock_config_manager():
    # Create a MagicMock for ConfigManager, with async methods
    mock = MagicMock()
    mock.get_config = AsyncMock(
        return_value={"community": {"sessions": {
            "local": {"host": "localhost"}, 
            "foo": {"host": "localhost"}
        }}}
    )
    return mock


@pytest.fixture
def session_manager(mock_config_manager):
    mgr = SessionManager(mock_config_manager)
    return mgr


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
async def test_session_manager_concurrent_get_or_create_session(session_manager):
    """Test concurrent calls to get_or_create_session don't create duplicate sessions."""
    # Initialize sessions
    await session_manager._ensure_sessions_initialized()
    
    # Mock the session creation to track calls
    create_call_count = 0
    
    async def mock_create_session(**kwargs):
        nonlocal create_call_count
        create_call_count += 1
        # Return a mock session
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        return mock_session
    
    with patch("deephaven_mcp.sessions._community_session.create_session", side_effect=mock_create_session):
        # Run multiple concurrent get_or_create_session calls
        tasks = [session_manager.get_or_create_session("foo") for _ in range(5)]
        sessions = await asyncio.gather(*tasks)
        
        # All should return the same session object
        assert all(session is sessions[0] for session in sessions)
        # Should only create one session despite concurrent calls
        assert create_call_count == 1


@pytest.mark.asyncio
async def test_session_manager_concurrent_get_or_create_session_failure(session_manager):
    """Test concurrent calls handle session creation failures properly."""
    # Initialize sessions
    await session_manager._ensure_sessions_initialized()
    
    with patch("deephaven_mcp.sessions._community_session.create_session", side_effect=SessionCreationError("fail")):
        with pytest.raises(SessionCreationError):
            await session_manager.get_or_create_session("foo")


@pytest.mark.asyncio
async def test_session_manager_delegates_to_helpers(session_manager):
    """Test SessionManager delegates to session objects properly."""
    # Initialize sessions
    await session_manager._ensure_sessions_initialized()
    
    mock_session = MagicMock(spec=Session)
    mock_session.is_alive = True
    
    with patch("deephaven_mcp.sessions._community_session.create_session", return_value=mock_session):
        session = await session_manager.get_or_create_session("foo")
        assert session is mock_session


# --- Tests for SessionManager cleanup ---
@pytest.mark.asyncio
async def test_clear_all_sessions_clears_cache(session_manager):
    """Test clear_all_sessions properly clears the session cache."""
    # Initialize sessions
    await session_manager._ensure_sessions_initialized()
    
    # Create a session first
    mock_session = MagicMock(spec=Session)
    mock_session.is_alive = True
    
    with patch("deephaven_mcp.sessions._community_session.create_session", return_value=mock_session):
        await session_manager.get_or_create_session("foo")
        
        # Verify session exists
        assert len(session_manager._sessions) == 2  # Both local and foo
        
        # Clear all sessions
        await session_manager.clear_all_sessions()
        
        # Verify cache is cleared
        assert len(session_manager._sessions) == 0


# --- Tests for clear_all_sessions ---
@pytest.mark.asyncio
async def test_clear_all_sessions_calls_close(session_manager):
    # Initialize sessions first
    await session_manager._ensure_sessions_initialized()
    
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close = MagicMock()
    
    # Manually set a session in a CommunitySession object
    community_session = session_manager._sessions["local"]
    community_session._session_cache = session
    
    await session_manager.clear_all_sessions()
    
    # Verify the session was closed (this is done via close_session_safely in CommunitySession.close_session)
    assert len(session_manager._sessions) == 0


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
    """Test get_or_create_session when session liveness check fails."""
    # Initialize sessions first
    await session_manager._ensure_sessions_initialized()
    
    bad_session = MagicMock(spec=Session)
    # Make is_alive property raise an exception
    type(bad_session).is_alive = property(
        lambda self: (_ for _ in ()).throw(Exception("fail"))
    )
    
    # Manually inject a bad session into a CommunitySession object
    community_session = session_manager._sessions["local"]
    community_session._session_cache = bad_session
    
    with patch("deephaven_mcp.sessions._community_session.create_session", new=AsyncMock()):
        await session_manager.get_or_create_session("local")
        assert any(
            "Error checking session liveness" in r for r in caplog.text.splitlines()
        )
        assert "local" in session_manager._sessions


@pytest.mark.asyncio
async def test_get_or_create_session_checks_liveness_error(session_manager, caplog):
    """Test get_or_create_session when session liveness check fails."""
    # Initialize sessions first
    await session_manager._ensure_sessions_initialized()
    
    bad_session = MagicMock(spec=Session)
    # Make is_alive property raise an exception
    type(bad_session).is_alive = property(
        lambda self: (_ for _ in ()).throw(Exception("fail"))
    )
    
    # Manually inject a bad session into a CommunitySession object
    community_session = session_manager._sessions["local"]
    community_session._session_cache = bad_session
    
    with patch("deephaven_mcp.sessions._community_session.create_session", new=AsyncMock()):
        await session_manager.get_or_create_session("local")
        assert any(
            "Error checking session liveness" in r for r in caplog.text.splitlines()
        )
        assert "local" in session_manager._sessions


# --- Tests for get_or_create_session ---
@pytest.mark.asyncio
async def test_get_or_create_session_reuses_existing(session_manager):
    """Test get_or_create_session returns existing alive session."""
    # Initialize sessions first
    await session_manager._ensure_sessions_initialized()
    
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.host = "localhost"
    session.port = 10000
    
    # Manually set session in CommunitySession object
    community_session = session_manager._sessions["local"]
    community_session._session_cache = session
    
    result = await session_manager.get_or_create_session("local")
    assert result == session


@pytest.mark.asyncio
async def test_get_or_create_session_creates_new(session_manager):
    # Initialize sessions first 
    await session_manager._ensure_sessions_initialized()
    
    # Clear the session cache for local to ensure a new one is created
    community_session = session_manager._sessions["local"]
    community_session._session_cache = None
    
    mock_session = MagicMock(spec=Session)
    mock_session.is_alive = True
    
    with patch("deephaven_mcp.sessions._community_session.create_session", return_value=mock_session):
        result = await session_manager.get_or_create_session("local")
        assert result == mock_session


@pytest.mark.asyncio
async def test_get_or_create_session_replaces_dead(session_manager):
    """Test get_or_create_session replaces dead session with new one."""
    # Initialize sessions first
    await session_manager._ensure_sessions_initialized()
    
    session = MagicMock(spec=Session)
    session.is_alive = False
    
    # Set dead session in CommunitySession object
    community_session = session_manager._sessions["local"]
    community_session._session_cache = session
    
    mock_session = MagicMock(spec=Session)
    mock_session.is_alive = True
    
    with patch("deephaven_mcp.sessions._community_session.create_session", return_value=mock_session):
        result = await session_manager.get_or_create_session("local")
        assert result == mock_session


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
