import pytest

from deephaven_mcp._exceptions import (
    AuthenticationError,
    CommunitySessionConfigurationError,
    ConfigurationError,
    DeephavenConnectionError,
    EnterpriseSystemConfigurationError,
    InternalError,
    McpError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)


class TestBaseExceptions:
    """Tests for base exceptions."""

    def test_mcp_error(self):
        """Test that McpError can be raised and caught properly."""
        message = "base MCP error"
        with pytest.raises(McpError) as exc_info:
            raise McpError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, Exception)

    def test_internal_error(self):
        """Test that InternalError can be raised and caught properly."""
        message = "internal MCP error"
        with pytest.raises(InternalError) as exc_info:
            raise InternalError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, McpError)
        assert isinstance(exc_info.value, RuntimeError)
        assert isinstance(exc_info.value, Exception)

    def test_internal_error_inheritance(self):
        """Test that InternalError inherits from both McpError and RuntimeError."""
        message = "internal error with multiple inheritance"
        # Can be caught as McpError
        with pytest.raises(McpError) as exc_info:
            raise InternalError(message)
        assert str(exc_info.value) == message

        # Can be caught as RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            raise InternalError(message)
        assert str(exc_info.value) == message


class TestExceptionParameterized:
    """Parameterized tests for common exception behaviors."""

    @pytest.mark.parametrize(
        "exception_class,parent_classes,message",
        [
            # Basic exceptions that inherit directly from McpError
            (SessionError, [McpError], "session error"),
            (AuthenticationError, [McpError], "authentication error"),
            (QueryError, [McpError], "query error"),
            (DeephavenConnectionError, [McpError], "connection error"),
            (ResourceError, [McpError], "resource error"),
            (ConfigurationError, [McpError], "configuration error"),
            # Specialized exceptions with additional inheritance
            (SessionCreationError, [SessionError, McpError], "session creation error"),
            (
                CommunitySessionConfigurationError,
                [ConfigurationError, McpError],
                "community session configuration error",
            ),
            (
                EnterpriseSystemConfigurationError,
                [ConfigurationError, McpError],
                "enterprise system configuration error",
            ),
        ],
    )
    def test_exception_basics(self, exception_class, parent_classes, message):
        """Test that exceptions can be raised and caught properly with correct inheritance."""
        # Test raising and catching the exception
        with pytest.raises(exception_class) as exc_info:
            raise exception_class(message)
        assert str(exc_info.value) == message

        # Test inheritance
        for parent_class in parent_classes:
            assert isinstance(exc_info.value, parent_class)

        # All exceptions should inherit from Exception
        assert isinstance(exc_info.value, Exception)


# Exception-specific tests can be added here if needed in the future


class TestConfigurationExceptions:
    """Additional tests for configuration-related exceptions."""

    # Any configuration-specific tests that aren't covered by the parameterized tests
    pass


class TestExceptionModule:
    """Tests for module-level functionality of the exceptions module."""

    def test_all_exceptions_exported(self):
        """Test that all exceptions are properly exported in __all__."""
        from deephaven_mcp import _exceptions

        exported = set(_exceptions.__all__)
        expected_exceptions = {
            # Base exceptions
            "McpError",
            "InternalError",
            # Session exceptions
            "SessionError",
            "SessionCreationError",
            # Authentication exceptions
            "AuthenticationError",
            # Query exceptions
            "QueryError",
            # Connection exceptions
            "DeephavenConnectionError",
            # Resource exceptions
            "ResourceError",
            # Configuration exceptions
            "ConfigurationError",
            "CommunitySessionConfigurationError",
            "EnterpriseSystemConfigurationError",
        }
        # Check that the exported set exactly matches the expected exceptions
        assert (
            exported == expected_exceptions
        ), f"Exported exceptions don't match expected. Missing: {expected_exceptions - exported}, Extra: {exported - expected_exceptions}"
