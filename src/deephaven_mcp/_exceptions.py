"""Custom exception types for Deephaven MCP.

Defines specialized exception hierarchies related to various subsystems including session
management, client operations, authentication, and resource handling. These exceptions provide
fine-grained error reporting and enable more specific exception handling strategies.

All exception classes in this module should be used consistently throughout the Deephaven MCP
system to signal recoverable or expected problems, allowing callers to implement appropriate
recovery or reporting strategies.

Exception Hierarchy:
    - Base exceptions: McpError (base for all MCP exceptions), InternalError (extends McpError and RuntimeError)
    - Session exceptions: SessionError (extends McpError), SessionCreationError (extends SessionError)
    - Authentication exceptions: AuthenticationError (extends McpError)
    - Query exceptions: QueryError (extends McpError)
    - Connection exceptions: DeephavenConnectionError (extends McpError)
    - Resource exceptions: ResourceError (extends McpError)
    - Configuration exceptions: ConfigurationError (extends McpError), CommunitySessionConfigurationError, EnterpriseSystemConfigurationError

Usage Example:
    ```python
    from deephaven_mcp._exceptions import SessionError, DeephavenConnectionError

    def connect_to_session(config):
        try:
            # Attempt to connect to session
            return create_session(config)
        except DeephavenConnectionError as e:
            # Network or connection problems
            logger.error(f"Connection failed: {e}")
            raise
        except SessionError as e:
            # Other session-related problems
            logger.error(f"Session creation failed: {e}")
            raise
    ```
"""

__all__ = [
    # Base exceptions
    "McpError",
    "InternalError",
    # Session exceptions
    "SessionCreationError",
    "SessionError",
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
]


# Base Exceptions


class McpError(Exception):
    """Base exception for all Deephaven MCP errors.

    This serves as the common base class for all MCP-related exceptions,
    allowing callers to catch all MCP errors with a single except clause
    while still maintaining specific exception types for detailed error handling.

    All MCP exceptions should inherit from this class either directly or
    through one of the more specific base classes (SessionError, ConfigurationError, etc.).

    Examples:
        ```python
        try:
            # MCP operations
            pass
        except McpError as e:
            # Handle any MCP-related error
            logger.error(f"MCP operation failed: {e}")
        ```
    """

    pass


class InternalError(McpError, RuntimeError):
    """Internal errors indicating bugs in the MCP implementation.

    This exception inherits from both McpError (for unified MCP error handling)
    and RuntimeError (to emphasize that this represents a programming error,
    not a user configuration or usage error).

    InternalError should be raised when:
    - Unexpected internal state is encountered
    - Programming assumptions are violated
    - System invariants are broken
    - Unrecoverable implementation bugs occur



    Examples:
        ```python
        if unexpected_internal_state:
            raise InternalError("Unexpected state in registry: {state}")
        ```
    """

    pass


# Session Exceptions


class SessionError(McpError):
    """Base exception for all session-related errors.

    This exception serves as a base class for more specific session-related exceptions
    and can be used directly for general session errors that don't fit specific categories.
    SessionError is typically raised when operations on an existing session fail, such as
    when closing a session, checking session status, or performing operations with an
    invalid session state.

    Examples:
        - Session connections cannot be closed properly
        - Session enters an invalid or unexpected state
        - Session operations timeout
        - Session resource allocation fails after initialization

    Note:
        If the error occurs specifically during session creation, use SessionCreationError instead.
    """

    pass


class SessionCreationError(SessionError):
    """
    Exception raised when a Deephaven Session cannot be created.

    Raised by session management code when a new session cannot be instantiated due to
    configuration errors, resource exhaustion, authentication failures, or other recoverable
    problems. This error is intended to be caught by callers that can handle or report
    session creation failures gracefully.

    This exception is a subclass of SessionError, providing a more specific error type
    for initialization and creation phase issues, as opposed to problems with existing sessions.

    Examples:
        - Failed to create a new worker for a session
        - Unable to connect to a persistent query
        - Failed to establish a session connection
        - Missing required session parameters
        - Session initialization script failed

    Usage:
        ```python
        try:
            session = session_manager.connect_to_new_worker()
        except SessionCreationError as e:
            logger.error(f"Failed to create session: {e}")
            # Implement fallback or retry logic
        ```
    """

    pass


# Authentication Exceptions


class AuthenticationError(McpError):
    """Exception raised when authentication fails.

    This exception represents failures during authentication operations, including
    incorrect credentials, expired tokens, authentication service issues, or insufficient
    permissions. It can be subclassed for more specific authentication error cases.

    This exception is raised by authentication-related methods in various client modules,
    particularly CorePlusAuthClient and CorePlusSessionManager when authentication operations fail.

    Examples:
        - Invalid username or password
        - Expired authentication token
        - Invalid or corrupted private key
        - Authentication service unavailable
        - Insufficient permissions for requested operation
        - Failed SAML authentication

    Usage:
        ```python
        try:
            await session_manager.password("username", "password")
        except AuthenticationError as e:
            logger.error(f"Authentication failed: {e}")
            # Implement authentication retry or fallback
        ```
    """

    pass


# Query Exceptions


class QueryError(McpError):
    """Exception raised when a query operation fails.

    This exception represents failures during query creation, execution, or management,
    such as syntax errors, execution failures, resource constraints, or query timeouts.

    QueryError is commonly raised by both standard and enterprise session operations
    that involve tables, queries, or data operations. It indicates a logical or operational
    failure rather than a connection or resource issue.

    Examples:
        - Query syntax errors
        - Failed table creation or manipulation
        - Invalid query parameters
        - Query execution timeout
        - Script execution failures
        - Table binding errors

    Usage:
        ```python
        try:
            result = await session.query(table).update_view(["Value = x + 1"]).to_table()
        except QueryError as e:
            logger.error(f"Query failed: {e}")
            # Handle the query failure
        ```
    """

    pass


# Connection Exceptions


class DeephavenConnectionError(McpError):
    """Exception raised when connection to a Deephaven service fails.

    This exception represents failures to establish or maintain connections to
    Deephaven services, such as network issues, service unavailability, or
    connection timeouts. Note that this is distinct from Python's built-in
    ConnectionError to avoid naming conflicts.

    This exception wraps lower-level connection errors from Python's standard library
    and networking packages. It provides a consistent interface for connection-related
    failures across the Deephaven MCP codebase.

    Examples:
        - Network connectivity issues
        - Server not responding
        - Connection timeout
        - Server unreachable
        - Connection reset or terminated
        - TLS/SSL connection failures

    Usage:
        ```python
        try:
            manager = CorePlusSessionManager.from_url("https://example.com/iris/connection.json")
            await manager.ping()
        except DeephavenConnectionError as e:
            logger.error(f"Cannot connect to Deephaven server: {e}")
            # Implement connection retry or fallback logic
        ```

    Note:
        Always catch this exception before other more specific exceptions in try/except
        chains, as connection failures typically prevent other operations from succeeding.
    """

    pass


# Resource Exceptions


class ResourceError(McpError):
    """Exception raised when resource management operations fail.

    This exception represents failures related to resource allocation, deallocation,
    or limitations, such as out-of-memory conditions, resource contention, or
    exceeding resource quotas.

    ResourceError is typically raised when an operation cannot be completed because
    a required resource (table, worker, memory, etc.) is not available, cannot be
    allocated, or has been exhausted.

    Examples:
        - Table not found
        - Key not found or cannot be deleted
        - Insufficient server resources to create a worker
        - Memory allocation limits exceeded
        - Resource quota exceeded
        - Historical or live table not found in namespace

    Usage:
        ```python
        try:
            table = await session.open_table("non_existent_table")
        except ResourceError as e:
            logger.warning(f"Resource not found: {e}")
            # Create resource or use alternative
        ```
    """

    pass


# Configuration Exceptions


class ConfigurationError(McpError):
    """Base class for all Deephaven MCP configuration errors.

    This exception serves as a base class for configuration-related errors that occur
    when loading, parsing, or validating configuration data for the Deephaven MCP system.
    It represents user configuration mistakes or invalid configuration states that prevent
    the system from operating correctly.

    Unlike InternalError, which indicates bugs in the code, ConfigurationError indicates
    problems with user-provided configuration data that can be corrected by updating
    the configuration files or environment variables.

    Examples:
        - Invalid JSON syntax in configuration files
        - Missing required configuration fields
        - Invalid configuration field values
        - Conflicting configuration settings
        - Configuration referencing unavailable features

    Usage:
        ```python
        try:
            config = load_configuration(config_file)
        except ConfigurationError as e:
            logger.error(f"Configuration error: {e}")
            # Provide guidance to user on fixing configuration
        ```
    """

    pass


class CommunitySessionConfigurationError(ConfigurationError):
    """Raised when a community session's configuration cannot be retrieved or is invalid.

    This exception is raised when there are problems with community session configuration
    data, such as invalid session parameters, missing required fields, or conflicting
    configuration values in the community sessions section of the configuration file.

    Examples:
        - Invalid host or port values
        - Missing required authentication parameters
        - Conflicting authentication methods specified
        - Invalid session timeout values
        - Malformed session configuration objects

    Usage:
        ```python
        try:
            session_config = validate_community_session_config(config_data)
        except CommunitySessionConfigurationError as e:
            logger.error(f"Community session configuration error: {e}")
            # Guide user to fix community session configuration
        ```
    """

    pass


class EnterpriseSystemConfigurationError(ConfigurationError):
    """Custom exception for errors in enterprise system configuration.

    This exception is raised when there are problems with enterprise system configuration
    data, such as invalid connection parameters, authentication configuration errors,
    or missing required enterprise-specific settings.

    Examples:
        - Invalid connection URLs
        - Missing or invalid authentication credentials
        - Conflicting authentication methods
        - Invalid TLS/SSL configuration
        - Missing required enterprise system parameters

    Usage:
        ```python
        try:
            enterprise_config = validate_enterprise_system_config(system_name, config_data)
        except EnterpriseSystemConfigurationError as e:
            logger.error(f"Enterprise system configuration error for {system_name}: {e}")
            # Guide user to fix enterprise system configuration
        ```
    """

    pass
