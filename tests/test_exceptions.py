import pytest

from deephaven_mcp._exceptions import (
    AuthenticationError,
    DeephavenConnectionError,
    InternalError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)


class TestSessionExceptions:
    """Tests for session-related exceptions."""

    def test_session_error(self):
        """Test that SessionError can be raised and caught properly."""
        message = "general session error"
        with pytest.raises(SessionError) as exc_info:
            raise SessionError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, Exception)

    def test_session_creation_error(self):
        """Test that SessionCreationError can be raised and caught properly."""
        message = "session creation error"
        with pytest.raises(SessionCreationError) as exc_info:
            raise SessionCreationError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, SessionError)
        assert isinstance(exc_info.value, Exception)


class TestAuthenticationExceptions:
    """Tests for authentication-related exceptions."""

    def test_authentication_error(self):
        """Test that AuthenticationError can be raised and caught properly."""
        message = "authentication error"
        with pytest.raises(AuthenticationError) as exc_info:
            raise AuthenticationError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, Exception)


class TestQueryExceptions:
    """Tests for query-related exceptions."""

    def test_query_error(self):
        """Test that QueryError can be raised and caught properly."""
        message = "query error"
        with pytest.raises(QueryError) as exc_info:
            raise QueryError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, Exception)


class TestConnectionExceptions:
    """Tests for connection-related exceptions."""

    def test_deephaven_connection_error(self):
        """Test that DeephavenConnectionError can be raised and caught properly."""
        message = "connection error"
        with pytest.raises(DeephavenConnectionError) as exc_info:
            raise DeephavenConnectionError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, Exception)


class TestResourceExceptions:
    """Tests for resource-related exceptions."""

    def test_resource_error(self):
        """Test that ResourceError can be raised and caught properly."""
        message = "resource error"
        with pytest.raises(ResourceError) as exc_info:
            raise ResourceError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, Exception)


class TestInternalExceptions:
    """Tests for internal exceptions."""

    def test_internal_error(self):
        """Test that InternalError can be raised and caught properly."""
        message = "internal error"
        with pytest.raises(InternalError) as exc_info:
            raise InternalError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, RuntimeError)
        assert isinstance(exc_info.value, Exception)
