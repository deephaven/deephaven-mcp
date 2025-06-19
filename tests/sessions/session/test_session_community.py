"""Unit tests for the SessionCommunity class."""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from pydeephaven import Session

from deephaven_mcp.sessions._session._session_base import SessionType
from deephaven_mcp.sessions._session._session_community import SessionCommunity


class TestSessionCommunity:
    """Tests for the SessionCommunity class."""
    
    def test_init(self):
        """Test initialization of SessionCommunity."""
        config = {"host": "testhost", "port": 12345}
        session = SessionCommunity("test-name", config)
        
        assert session.name == "test-name"
        assert session.source == "community"
        assert session.session_type == SessionType.COMMUNITY
        assert session.full_name == "community:community:test-name"
        assert session._config == config
        assert session._session_cache is None
        assert isinstance(session._lock, asyncio.Lock)
    
    @pytest.mark.asyncio
    async def test_is_alive_no_session(self):
        """Test is_alive when no session is cached."""
        session = SessionCommunity("test-name", {})
        
        result = await session.is_alive
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_is_alive_with_session_alive(self):
        """Test is_alive with a live cached session."""
        session = SessionCommunity("test-name", {})
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        session._session_cache = mock_session
        
        result = await session.is_alive
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_is_alive_with_session_not_alive(self):
        """Test is_alive with a dead cached session."""
        session = SessionCommunity("test-name", {})
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = False
        session._session_cache = mock_session
        
        result = await session.is_alive
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_is_alive_with_exception(self):
        """Test is_alive when checking liveness raises an exception."""
        session = SessionCommunity("test-name", {})
        mock_session = MagicMock(spec=Session)
        # Create a property mock that raises an exception when accessed
        mock_property = MagicMock()
        mock_property.__get__ = MagicMock(side_effect=RuntimeError("Test error"))
        type(mock_session).is_alive = mock_property
        session._session_cache = mock_session
        
        result = await session.is_alive
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_session_new(self):
        """Test get_session when creating a new session."""
        config = {"host": "testhost", "port": 12345}
        session = SessionCommunity("test-name", config)
        mock_session = MagicMock(spec=Session)
        
        with patch("deephaven_mcp.sessions._session._session_community._get_session_parameters",
                  new=AsyncMock(return_value={"host": "testhost", "port": 12345})) as mock_params:
            with patch("deephaven_mcp.sessions._session._session_community.create_session",
                      new=AsyncMock(return_value=mock_session)) as mock_create:
                result = await session.get_session()
        mock_create.assert_called_once_with({"host": "testhost", "port": 12345})
        assert result is mock_session
        assert session._session_cache is mock_session
    
    @pytest.mark.asyncio
    async def test_get_session_cached_alive(self):
        """Test get_session with a cached session that is alive."""
        session = SessionCommunity("test-name", {})
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        session._session_cache = mock_session
        
        result = await session.get_session()
        
        assert result is mock_session
    
    @pytest.mark.asyncio
    async def test_get_session_cached_not_alive(self):
        """Test get_session with a cached session that is not alive."""
        config = {"host": "testhost", "port": 12345}
        session = SessionCommunity("test-name", config)
        mock_session_old = MagicMock(spec=Session)
        mock_session_old.is_alive = False
        mock_session_new = MagicMock(spec=Session)
        session._session_cache = mock_session_old
        
        with patch("deephaven_mcp.sessions._session._session_community._get_session_parameters",
                  new=AsyncMock(return_value={"host": "testhost", "port": 12345})) as mock_params:
            with patch("deephaven_mcp.sessions._session._session_community.create_session",
                      new=AsyncMock(return_value=mock_session_new)) as mock_create:
                
                result = await session.get_session()
                
        mock_create.assert_called_once_with({"host": "testhost", "port": 12345})
        assert result is mock_session_new
        assert session._session_cache is mock_session_new
    
    @pytest.mark.asyncio
    async def test_get_session_cached_error(self):
        """Test get_session when checking cached session liveness raises an exception."""
        config = {"host": "testhost", "port": 12345}
        session = SessionCommunity("test-name", config)
        mock_session_old = MagicMock(spec=Session)
        # Create a property mock that raises an exception when accessed
        mock_property = MagicMock()
        mock_property.__get__ = MagicMock(side_effect=RuntimeError("Test error"))
        type(mock_session_old).is_alive = mock_property
        mock_session_new = MagicMock(spec=Session)
        session._session_cache = mock_session_old
        
        with patch("deephaven_mcp.sessions._session._session_community._get_session_parameters",
                  new=AsyncMock(return_value={"host": "testhost", "port": 12345})) as mock_params:
            with patch("deephaven_mcp.sessions._session._session_community.create_session",
                      new=AsyncMock(return_value=mock_session_new)) as mock_create:
                
                result = await session.get_session()
                
        mock_create.assert_called_once_with({"host": "testhost", "port": 12345})
        assert result is mock_session_new
        assert session._session_cache is mock_session_new
    
    