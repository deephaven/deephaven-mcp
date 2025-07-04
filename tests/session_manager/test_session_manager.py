"""
Unit tests for the BaseSessionRegistry class.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp.session_manager import CommunitySessionManager, SessionManagerType
from deephaven_mcp.session_manager._session_manager import BaseSessionManager


class TestSessionManagerType:
    """Tests for the SessionManagerType enum."""

    def test_session_type_values(self):
        """Test the values of the SessionManagerType enum."""
        assert SessionManagerType.COMMUNITY.value == "community"
        assert SessionManagerType.ENTERPRISE.value == "enterprise"
        assert len(list(SessionManagerType)) == 2


class StubBaseSessionManager(BaseSessionManager):
    """Stub implementation of BaseSessionRegistry for testing."""

    @property
    def is_alive(self) -> bool:
        return True

    async def get_session(self) -> Session:
        return MagicMock(spec=Session)


class TestBaseSessionRegistry:
    """Tests for the BaseSessionRegistry abstract base class."""

    def test_init(self):
        """Test BaseSessionRegistry initialization and attributes."""
        session = StubBaseSessionManager(SessionManagerType.ENTERPRISE, "test-source", "test-name")

        assert session._type == SessionManagerType.ENTERPRISE
        assert session._source == "test-source"
        assert session._name == "test-name"
        assert session._session_cache is None
        assert isinstance(session._lock, asyncio.Lock)

    def test_properties(self):
        """Test BaseSessionRegistry property getters."""
        session = StubBaseSessionManager(SessionManagerType.ENTERPRISE, "test-source", "test-name")

        assert session.session_type == SessionManagerType.ENTERPRISE
        assert session.source == "test-source"
        assert session.name == "test-name"

    def test_full_name(self):
        """Test the full_name property."""
        session = StubBaseSessionManager(SessionManagerType.ENTERPRISE, "test-source", "test-name")

        assert session.full_name == "enterprise:test-source:test-name"

    @pytest.mark.asyncio
    async def test_close_session_no_cache(self):
        """Test close_session with no cached session."""
        session = StubBaseSessionManager(SessionManagerType.ENTERPRISE, "test-source", "test-name")

        await session.close_session()  # Should not raise exceptions
        assert session._session_cache is None

    @pytest.mark.asyncio
    async def test_close_session_with_cache(self):
        """Test close_session with a cached session."""
        session = StubBaseSessionManager(SessionManagerType.ENTERPRISE, "test-source", "test-name")
        mock_session = MagicMock(spec=Session)
        session._session_cache = mock_session

        mock_session.close = AsyncMock()
        await session.close_session()
        mock_session.close.assert_awaited_once()
        assert session._session_cache is None





from deephaven_mcp.session_manager._session_manager import EnterpriseSessionManager

class TestEnterpriseSessionManager:
    """Tests for the EnterpriseSessionManager class."""

    def test_init(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-1")
        assert session._type.value == "enterprise"
        assert session._source == "enterprise-source"
        assert session._name == "worker-1"
        assert session._session_cache is None
        assert isinstance(session._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_is_alive_no_cache(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-2")
        result = await session.is_alive
        assert result is False

    @pytest.mark.asyncio
    async def test_is_alive_alive(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-3")
        mock_session = MagicMock()
        mock_session.is_alive = True
        session._session_cache = mock_session
        result = await session.is_alive
        assert result is True

    @pytest.mark.asyncio
    async def test_is_alive_dead(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-4")
        mock_session = MagicMock()
        mock_session.is_alive = False
        session._session_cache = mock_session
        result = await session.is_alive
        assert result is False

    @pytest.mark.asyncio
    async def test_is_alive_exception(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-5")
        mock_session = MagicMock()
        # Simulate exception on is_alive property
        type(mock_session).is_alive = property(lambda self: (_ for _ in ()).throw(Exception("fail")))
        session._session_cache = mock_session
        result = await session.is_alive
        assert result is False

    @pytest.mark.asyncio
    async def test_get_session_cached_alive(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-6")
        mock_session = MagicMock()
        mock_session.is_alive = True
        session._session_cache = mock_session
        result = await session.get_session()
        # Should return CorePlusSession(mock_session)
        assert hasattr(result, '__class__')  # Just check it's a wrapper, not the raw mock
        assert session._session_cache is mock_session

    @pytest.mark.asyncio
    async def test_get_session_cached_dead(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-7")
        mock_session_old = MagicMock()
        mock_session_old.is_alive = False
        mock_session_new = MagicMock()
        session._session_cache = mock_session_old
        with pytest.raises(Exception) as exc_info:
            await session.get_session()
        # Should raise InternalError until implemented
        assert "not implemented" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_session_cached_exception(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-8")
        mock_session_old = MagicMock()
        # Simulate exception on is_alive property
        type(mock_session_old).is_alive = property(lambda self: (_ for _ in ()).throw(Exception("fail")))
        mock_session_new = MagicMock()
        session._session_cache = mock_session_old
        with pytest.raises(Exception) as exc_info:
            await session.get_session()
        # Should raise InternalError until implemented
        assert "not implemented" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_session_new(self):
        session = EnterpriseSessionManager("enterprise-source", "worker-9")
        mock_session = MagicMock()
        with pytest.raises(Exception) as exc_info:
            await session.get_session()
        # Should raise InternalError until implemented
        assert "not implemented" in str(exc_info.value)


class TestCommunitySessionManager:
    """Tests for the CommunitySessionManager class."""

    def test_init(self):
        """Test initialization of CommunitySessionManager."""
        config = {"host": "testhost", "port": 12345}
        session = CommunitySessionManager("test-name", config)

        assert session.name == "test-name"
        assert session.source == "community"
        assert session.session_type == SessionManagerType.COMMUNITY
        assert session.full_name == "community:community:test-name"
        assert session._config == config
        assert session._session_cache is None
        assert isinstance(session._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_is_alive_no_session(self):
        """Test is_alive when no session is cached."""
        session = CommunitySessionManager("test-name", {})

        result = await session.is_alive

        assert result is False

    @pytest.mark.asyncio
    async def test_is_alive_with_session_alive(self):
        """Test is_alive with a live cached session."""
        session = CommunitySessionManager("test-name", {})
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        session._session_cache = mock_session

        result = await session.is_alive

        assert result is True

    @pytest.mark.asyncio
    async def test_is_alive_with_session_not_alive(self):
        """Test is_alive with a dead cached session."""
        session = CommunitySessionManager("test-name", {})
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = False
        session._session_cache = mock_session

        result = await session.is_alive

        assert result is False

    @pytest.mark.asyncio
    async def test_is_alive_with_exception(self):
        """Test is_alive when checking liveness raises an exception."""
        session = CommunitySessionManager("test-name", {})
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
        session = CommunitySessionManager("test-name", config)
        mock_session = MagicMock(spec=Session)

        with patch(
            "deephaven_mcp.client._session.CoreSession.from_config",
            new=AsyncMock(return_value=mock_session),
        ) as mock_from_config:
            result = await session.get_session()
        mock_from_config.assert_called_once_with(config)
        assert result is mock_session
        assert session._session_cache is mock_session

    @pytest.mark.asyncio
    async def test_get_session_cached_alive(self):
        """Test get_session with a cached session that is alive."""
        session = CommunitySessionManager("test-name", {})
        mock_session = MagicMock(spec=Session)
        mock_session.is_alive = True
        session._session_cache = mock_session

        result = await session.get_session()

        assert result is mock_session

    @pytest.mark.asyncio
    async def test_get_session_cached_not_alive(self):
        """Test get_session with a cached session that is not alive."""
        config = {"host": "testhost", "port": 12345}
        session = CommunitySessionManager("test-name", config)
        mock_session_old = MagicMock(spec=Session)
        mock_session_old.is_alive = False
        mock_session_new = MagicMock(spec=Session)
        session._session_cache = mock_session_old

        with patch(
            "deephaven_mcp.client._session.CoreSession.from_config",
            new=AsyncMock(return_value=mock_session_new),
        ) as mock_from_config:
            result = await session.get_session()
        mock_from_config.assert_called_once_with(config)
        assert result is mock_session_new
        assert session._session_cache is mock_session_new

    @pytest.mark.asyncio
    async def test_get_session_cached_error(self):
        """Test get_session when checking cached session liveness raises an exception."""
        config = {"host": "testhost", "port": 12345}
        session = CommunitySessionManager("test-name", config)
        mock_session_old = MagicMock(spec=Session)
        # Create a property mock that raises an exception when accessed
        mock_property = MagicMock()
        mock_property.__get__ = MagicMock(side_effect=RuntimeError("Test error"))
        type(mock_session_old).is_alive = mock_property
        mock_session_new = MagicMock(spec=Session)
        session._session_cache = mock_session_old

        with patch(
            "deephaven_mcp.client._session.CoreSession.from_config",
            new=AsyncMock(return_value=mock_session_new),
        ) as mock_from_config:
            result = await session.get_session()
        mock_from_config.assert_called_once_with(config)
        assert result is mock_session_new
        assert session._session_cache is mock_session_new
