import pytest

from deephaven_mcp._exceptions import (
    AuthenticationError,
    CommunityNotConfiguredError,
    ConfigurationError,
    DaemonAlreadyPublishedError,
    DaemonClientError,
    DaemonRegistryError,
    DaemonReuseRefusedError,
    DaemonStartupTimeoutError,
    DeephavenConnectionError,
    EnterpriseNotConfiguredError,
    FileLockTimeoutError,
    InternalError,
    McpClientError,
    McpError,
    QueryError,
    RegistryCorruptError,
    ResourceError,
    SessionCreationError,
    SessionError,
    SessionLaunchError,
    SpawnError,
    SystemNotConfiguredError,
    UnsupportedOperationError,
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

    def test_unsupported_operation_error(self):
        """Test that UnsupportedOperationError can be raised and caught properly."""
        message = "operation not supported"
        with pytest.raises(UnsupportedOperationError) as exc_info:
            raise UnsupportedOperationError(message)
        assert str(exc_info.value) == message
        assert isinstance(exc_info.value, McpError)
        assert isinstance(exc_info.value, Exception)


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
            (
                SystemNotConfiguredError,
                [ConfigurationError, McpError],
                "system not configured",
            ),
            (
                EnterpriseNotConfiguredError,
                [SystemNotConfiguredError, ConfigurationError, McpError],
                "no enterprise configured",
            ),
            (
                CommunityNotConfiguredError,
                [SystemNotConfiguredError, ConfigurationError, McpError],
                "no community configured",
            ),
            (UnsupportedOperationError, [McpError], "unsupported operation error"),
            # Specialized exceptions with additional inheritance
            (SessionCreationError, [SessionError, McpError], "session creation error"),
            (
                SessionLaunchError,
                [SessionCreationError, SessionError, McpError],
                "session launch error",
            ),
            # File-lock, daemon-registry, and CLI exceptions
            (FileLockTimeoutError, [McpError], "lock timeout"),
            (DaemonRegistryError, [McpError], "registry error"),
            (
                DaemonAlreadyPublishedError,
                [DaemonRegistryError, McpError],
                "already published",
            ),
            (
                RegistryCorruptError,
                [DaemonRegistryError, McpError],
                "registry corrupt",
            ),
            (DaemonClientError, [McpError], "client error"),
            (DaemonReuseRefusedError, [McpError], "reuse refused"),
            (SpawnError, [McpError], "spawn error"),
            (
                DaemonStartupTimeoutError,
                [SpawnError, McpError],
                "startup timeout",
            ),
            (McpClientError, [McpError], "mcp client error"),
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


class TestDaemonReuseRefusedError:
    """Tests for ``DaemonReuseRefusedError``'s ``differing`` attribute.

    The only exception in the module carrying custom ``__init__`` state, so it
    gets a focused test beyond the parameterized inheritance check.
    """

    def test_records_differing_fields(self):
        """The ``differing`` keyword is stored verbatim on the instance."""
        exc = DaemonReuseRefusedError("different build", differing=("version", "venv"))
        assert str(exc) == "different build"
        assert exc.differing == ("version", "venv")

    def test_differing_defaults_to_empty_tuple(self):
        """``differing`` defaults to an empty tuple when omitted."""
        exc = DaemonReuseRefusedError("different build")
        assert exc.differing == ()


# Exception-specific tests can be added here if needed in the future


class TestConfigurationExceptions:
    """Additional tests for configuration-related exceptions."""

    # Any configuration-specific tests that aren't covered by the parameterized tests
    pass


class TestExceptionModule:
    """Tests for module-level functionality of the exceptions module."""

    def test_all_exceptions_exported(self):
        """Test that __all__ lists exactly the McpError subclasses defined in the module.

        The expected set is derived from the module's actual contents rather than
        hardcoded, so a newly added exception that is omitted from __all__ fails
        this test instead of silently passing.
        """
        import inspect

        from deephaven_mcp import _exceptions

        exported = set(_exceptions.__all__)
        defined = {
            name
            for name, obj in vars(_exceptions).items()
            if inspect.isclass(obj)
            and issubclass(obj, McpError)
            and obj.__module__ == _exceptions.__name__
        }
        assert exported == defined, (
            "__all__ does not match the module's McpError subclasses. "
            f"Missing from __all__: {defined - exported}, "
            f"Extra in __all__: {exported - defined}"
        )
