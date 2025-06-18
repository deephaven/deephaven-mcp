"""
Unit tests for the _community_session module.
"""

import asyncio
import logging
import sys
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp.sessions._session._community_session import (
    SessionBase,
    SessionCommunity,
    SessionType,
)
from deephaven_mcp.sessions._errors import SessionCreationError


class TestSessionType:
    """Tests for the SessionType enum."""
    
    def test_session_type_values(self):
        """Test the values of the SessionType enum."""
        assert SessionType.COMMUNITY.value == "community"
        assert SessionType.ENTERPRISE.value == "enterprise"
        assert len(list(SessionType)) == 2


class StubSessionBase(SessionBase):
    """Stub implementation of SessionBase for testing."""
    
    @property
    def is_alive(self) -> bool:
        return True
    
    async def get_session(self) -> Session:
        return MagicMock(spec=Session)


class TestSessionBase:
    """Tests for the SessionBase abstract base class."""
    
    def test_init(self):
        """Test SessionBase initialization and attributes."""
        session = StubSessionBase(SessionType.ENTERPRISE, "test-source", "test-name")
        
        assert session._type == SessionType.ENTERPRISE
        assert session._source == "test-source"
        assert session._name == "test-name"
        assert session._session_cache is None
        assert isinstance(session._lock, asyncio.Lock)
    
    def test_properties(self):
        """Test SessionBase property getters."""
        session = StubSessionBase(SessionType.ENTERPRISE, "test-source", "test-name")
        
        assert session.session_type == SessionType.ENTERPRISE
        assert session.source == "test-source"
        assert session.name == "test-name"
    
    def test_full_name(self):
        """Test the full_name property."""
        session = StubSessionBase(SessionType.ENTERPRISE, "test-source", "test-name")
        
        assert session.full_name == "enterprise:test-source:test-name"
    
    @pytest.mark.asyncio
    async def test_close_session_no_cache(self):
        """Test close_session with no cached session."""
        session = StubSessionBase(SessionType.ENTERPRISE, "test-source", "test-name")
        
        await session.close_session()  # Should not raise exceptions
        assert session._session_cache is None
    
    @pytest.mark.asyncio
    async def test_close_session_with_cache(self):
        """Test close_session with a cached session."""
        session = StubSessionBase(SessionType.ENTERPRISE, "test-source", "test-name")
        mock_session = MagicMock(spec=Session)
        session._session_cache = mock_session
        
        with patch("deephaven_mcp.sessions._session._community_session.close_session_safely", 
                  new=AsyncMock()) as mock_close:
            await session.close_session()
            
            mock_close.assert_called_once_with(mock_session, "test-name")
            assert session._session_cache is None


class TestSessionCommunity:
    """Tests for the SessionCommunity class."""
    
    @pytest.fixture
    def config(self):
        """A minimal valid configuration for a community session."""
        return {
            "host": "localhost",
            "port": 10000,
        }
    
    @pytest.mark.asyncio
    async def test_init_attributes(self, config):
        """Test that SessionCommunity properly initializes attributes."""
        session = SessionCommunity("test_name", config)
        
        assert session.name == "test_name"
        assert session.session_type == SessionType.COMMUNITY
        assert session.source == "community"
        assert session.full_name == "community:community:test_name"
        assert session._config == config
        assert session._session_cache is None
        assert isinstance(session._lock, asyncio.Lock)
    
    @pytest.mark.asyncio
    async def test_is_alive_no_session(self, config):
        """Test is_alive property when there's no cached session."""
        session = SessionCommunity("test", config)
        assert await session.is_alive() is False
    
    @pytest.mark.asyncio
    async def test_is_alive_with_alive_session(self, config):
        """Test is_alive property when the cached session is alive."""
        session = SessionCommunity("test", config)
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        session._session_cache = mock_session
        
        assert await session.is_alive() is True
    
    @pytest.mark.asyncio
    async def test_is_alive_with_dead_session(self, config):
        """Test is_alive property when the cached session is not alive."""
        session = SessionCommunity("test", config)
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = False
        session._session_cache = mock_session
        
        assert await session.is_alive() is False
    
    @pytest.mark.asyncio
    async def test_is_alive_error_handling(self, config):
        """Test is_alive handles errors when checking session liveness."""
        session = SessionCommunity("test", config)
        mock_session = MagicMock(spec=Session)
        # Make is_alive property raise an exception
        type(mock_session).is_alive = PropertyMock(side_effect=Exception("fail"))
        session._session_cache = mock_session
        
        assert await session.is_alive() is False
    
    @pytest.mark.asyncio
    async def test_get_session_creates_new_session(self, config):
        """Test get_session creates a new session when none exists."""
        session = SessionCommunity("test", config)
        
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        
        with patch("deephaven_mcp.sessions._session._community_session._get_session_parameters", 
                  return_value={"host": "localhost"}) as mock_params:
            with patch("deephaven_mcp.sessions._session._community_session.create_session",
                      return_value=mock_session) as mock_create:
                result = await session.get_session()
                
                mock_params.assert_called_once_with(config)
                mock_create.assert_called_once()
                assert result == mock_session
                assert session._session_cache == mock_session
    
    @pytest.mark.asyncio
    async def test_get_session_reuses_alive_session(self, config):
        """Test get_session reuses an existing alive session."""
        session = SessionCommunity("test", config)
        
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        session._session_cache = mock_session
        
        with patch("deephaven_mcp.sessions._session._community_session.create_session") as mock_create:
            result = await session.get_session()
            
            mock_create.assert_not_called()
            assert result == mock_session
            assert session._session_cache == mock_session
    
    @pytest.mark.asyncio
    async def test_get_session_replaces_dead_session(self, config):
        """Test get_session replaces a dead session with a new one."""
        session = SessionCommunity("test", config)
        
        # Dead session
        dead_session = MagicMock(spec=Session)
        dead_session.is_alive = False
        session._session_cache = dead_session
        
        # New alive session
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        
        with patch("deephaven_mcp.sessions._session._community_session._get_session_parameters", 
                  return_value={"host": "localhost"}):
            with patch("deephaven_mcp.sessions._session._community_session.create_session",
                      return_value=mock_session) as mock_create:
                result = await session.get_session()
                
                mock_create.assert_called_once()
                assert result == mock_session
                assert session._session_cache == mock_session
                assert session._session_cache != dead_session

    @pytest.mark.asyncio
    async def test_get_session_liveness_exception(self, config, caplog):
        """Test get_session when session liveness check raises an exception."""
        session = SessionCommunity("test", config)
        
        # Session with problematic is_alive property
        bad_session = MagicMock(spec=Session)
        type(bad_session).is_alive = PropertyMock(side_effect=Exception("fail"))
        session._session_cache = bad_session
        
        # New alive session
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        
        with patch("deephaven_mcp.sessions._session._community_session._get_session_parameters", 
                  return_value={"host": "localhost"}):
            with patch("deephaven_mcp.sessions._session._community_session.create_session",
                      return_value=mock_session) as mock_create:
                with caplog.at_level(logging.WARNING):
                    result = await session.get_session()
                
                mock_create.assert_called_once()
                assert result == mock_session
                assert session._session_cache == mock_session
                assert "Error checking session liveness" in caplog.text

    @pytest.mark.asyncio
    async def test_get_session_creation_error(self, config):
        """Test get_session when session creation fails."""
        session = SessionCommunity("test", config)
        
        with patch("deephaven_mcp.sessions._session._community_session._get_session_parameters", 
                  return_value={"host": "localhost"}):
            with patch("deephaven_mcp.sessions._session._community_session.create_session",
                      side_effect=SessionCreationError("Failed to create session")):
                with pytest.raises(SessionCreationError):
                    await session.get_session()
                
                # Session cache should remain None
                assert session._session_cache is None

    @pytest.mark.asyncio
    async def test_close_session_no_session(self, config):
        """Test close_session when there's no cached session."""
        session = SessionCommunity("test", config)
        
        # No exception should be raised
        await session.close_session()
        assert session._session_cache is None

    @pytest.mark.asyncio
    async def test_close_session_with_session(self, config):
        """Test close_session properly closes a cached session."""
        session = SessionCommunity("test", config)
        
        mock_session = MagicMock(spec=Session)
        session._session_cache = mock_session
        
        with patch("deephaven_mcp.sessions._session._community_session.close_session_safely", 
                  new=AsyncMock()) as mock_close:
            await session.close_session()
            
            mock_close.assert_called_once_with(mock_session, "test")
            assert session._session_cache is None
