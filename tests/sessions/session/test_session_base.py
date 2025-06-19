"""
Unit tests for the SessionBase class.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp.sessions._session._session_base import SessionBase, SessionType


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

        with patch(
            "deephaven_mcp.sessions._session._session_base.close_session_safely",
            new=AsyncMock(),
        ) as mock_close:
            await session.close_session()

            mock_close.assert_called_once_with(mock_session, "test-name")
            assert session._session_cache is None
