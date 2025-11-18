"""Custom exception types for Deephaven MCP.

Defines specialized exception hierarchies related to various subsystems including session
management, client operations, authentication, and resource handling. These exceptions provide
fine-grained error reporting and enable more specific exception handling strategies.

All exception classes in this module should be used consistently throughout the Deephaven MCP
system to signal recoverable or expected problems, allowing callers to implement appropriate
recovery or reporting strategies.

Exception Hierarchy:
    - Base exceptions: McpError (base for all MCP exceptions), InternalError (extends McpError and RuntimeError), UnsupportedOperationError (extends McpError), MissingEnterprisePackageError (extends InternalError)
    - Session exceptions: SessionError (extends McpError), SessionCreationError (extends SessionError), SessionLaunchError (extends SessionCreationError), InvalidSessionNameError (extends SessionError and ValueError)
    - Authentication exceptions: AuthenticationError (extends McpError)
    - Query exceptions: QueryError (extends McpError)
    - Connection exceptions: DeephavenConnectionError (extends McpError)
    - Resource exceptions: ResourceError (extends McpError), RegistryItemNotFoundError (extends ResourceError and KeyError)
    - Configuration exceptions: ConfigurationError (extends McpError), CommunitySessionConfigurationError (extends ConfigurationError), EnterpriseSystemConfigurationError (extends ConfigurationError)

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
    "UnsupportedOperationError",
    "MissingEnterprisePackageError",
    # Session exceptions
    "SessionCreationError",
    "SessionError",
    "SessionLaunchError",
    "InvalidSessionNameError",
    # Authentication exceptions
    "AuthenticationError",
    # Query exceptions
    "QueryError",
    # Connection exceptions
    "DeephavenConnectionError",
    # Resource exceptions
    "ResourceError",
    "RegistryItemNotFoundError",
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


class MissingEnterprisePackageError(InternalError):
    """Exception raised when deephaven-coreplus-client package is not installed.

    This exception provides prominent error messaging to help users quickly identify
    and resolve the missing package issue when attempting to use Deephaven Enterprise
    (DHE) features.

    The exception formats the error message to be highly visible and actionable,
    with clear instructions on how to resolve the issue.

    Examples:
        ```python
        if not is_enterprise_available:
            raise MissingEnterprisePackageError()
        ```
    """

    def __init__(self, message: str | None = None):
        """Initialize the exception with an optional custom message.

        Args:
            message (str | None): Optional custom message. If not provided, uses a default
                message about the missing deephaven-coreplus-client package.
        """
        if message is None:
            message = "Core+ features are not available (deephaven-coreplus-client Python package not installed)"

        self.package_message = message
        super().__init__(message)

    def __str__(self) -> str:
        """Return a prominently formatted error message.

        Returns:
            str: A formatted error message with clear visual separation and
                actionable instructions for resolving the issue.
        """
        separator = "=" * 80
        return f"""
{separator}
ERROR: Core+ features are not available
{separator}

The Python package 'deephaven-coreplus-client' is not installed.

This package is required to use Deephaven Enterprise (DHE) features.

To resolve this issue:
  1. Obtain the deephaven-coreplus-client wheel file from your 
     Deephaven Enterprise administrator
  2. Install it using pip:
     
     pip install /path/to/deephaven_coreplus_client-X.Y.Z-py3-none-any.whl

For more information, see the installation documentation.

{separator}
"""


# Session Exceptions


class SessionError(McpError):
    """Base exception for all session-related errors.

    This exception serves as a base class for more specific session-related exceptions
    and can be used directly for general session errors that don't fit specific categories.
    
    Use SessionError for errors with existing, already-initialized sessions, such as:
    - Session connections cannot be closed properly
    - Session enters an invalid or unexpected state
    - Session operations timeout
    - Session resource allocation fails after initialization

    For session initialization and creation failures, use SessionCreationError instead.
    For session name parsing failures, use InvalidSessionNameError instead.

    Usage:
        ```python
        try:
            await session.close()
        except SessionError as e:
            logger.error(f"Session operation failed: {e}")
            # Implement cleanup or recovery logic
        ```
    """

    pass


class SessionCreationError(SessionError):
    """Exception raised when a Deephaven session cannot be created or initialized.

    This exception is raised during the session creation and initialization phase, before
    the session is fully operational. It indicates that a new session could not be instantiated
    due to configuration errors, resource issues, authentication failures, or other problems
    that prevent successful session startup.

    This is distinct from SessionError, which applies to failures with already-running sessions.
    Use SessionCreationError for problems that occur during the creation process itself.

    Common causes include:
    - Failed to create a new worker for a session
    - Unable to connect to a persistent query
    - Failed to establish initial session connection
    - Missing required session parameters
    - Session initialization script failed
    - Authentication failed during session startup

    For dynamic session launch failures (Docker/Python process startup), use the more
    specific SessionLaunchError subclass instead.

    Usage:
        ```python
        try:
            session = await session_manager.connect_to_new_worker()
        except SessionCreationError as e:
            logger.error(f"Failed to create session: {e}")
            # Implement fallback or retry logic
        ```
    """

    pass


class SessionLaunchError(SessionCreationError):
    """Exception raised when launching a Deephaven Community session fails.

    This exception is raised during the launch phase of dynamically created community sessions
    (via Docker or Python). It represents failures in the actual process/container startup,
    port allocation, health checking, or session readiness verification.

    This is a subclass of SessionCreationError, specifically for launch-related failures
    during dynamic session creation, as opposed to configuration or connection issues.

    Examples:
        - Docker container failed to start
        - Python-launched Deephaven process failed to start
        - Unable to find available port for session
        - Session health check failed or timed out
        - Container/process startup returned non-zero exit code
        - Failed to stop running container/process

    Usage:
        ```python
        try:
            session = await launcher.launch(session_name, port, config)
        except SessionLaunchError as e:
            logger.error(f"Failed to launch session: {e}")
            # Implement cleanup or retry logic
        ```
    """

    pass


class InvalidSessionNameError(SessionError, ValueError):
    """Exception raised when a session name cannot be parsed or is malformed.

    This exception is raised when attempting to parse a session name that does not
    follow the expected format: `system_type:source:name` (e.g., "enterprise:factory1:session1"
    or "community:local:worker1").

    Multiple Inheritance:
        Inherits from both SessionError (for MCP-specific error handling) and ValueError
        (to indicate invalid input format), allowing callers to catch it as either type
        depending on their error handling strategy.

    This is an expected exception that should be caught when:
    - Session name parsing is optional or may fail gracefully
    - Handling user-provided session names that might be incorrectly formatted
    - Validating session identifiers from external sources

    Common causes include:
    - Session name missing required colons (separators)
    - Session name with too few or too many components
    - Session name with empty components (e.g., "enterprise::session1")
    - Session name with invalid system type

    Usage:
        ```python
        try:
            system_type, source, name = BaseItemManager.parse_full_name(session_id)
        except InvalidSessionNameError as e:
            logger.warning(f"Invalid session name format: {e}")
            # Handle malformed session name gracefully
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
    Deephaven services. It wraps lower-level network errors and provides a consistent
    interface for connection-related failures across the Deephaven MCP codebase.

    This is distinct from Python's built-in ConnectionError to avoid naming conflicts
    and to provide MCP-specific error handling capabilities.

    Common causes include:
    - Network connectivity issues
    - Server not responding or unreachable
    - Connection timeout
    - Connection reset or terminated unexpectedly
    - TLS/SSL connection failures
    - DNS resolution failures

    Error Handling Strategy:
        DeephavenConnectionError is often treated as an expected, recoverable error
        in production environments where services may be temporarily unavailable.
        Many MCP operations handle this exception internally and log warnings rather
        than propagating it, treating offline services as gracefully degraded state.

    Usage:
        ```python
        try:
            manager = CorePlusSessionManager.from_url("https://example.com/iris/connection.json")
            await manager.ping()
        except DeephavenConnectionError as e:
            logger.warning(f"Cannot connect to Deephaven server: {e}")
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


class RegistryItemNotFoundError(ResourceError, KeyError):
    """Exception raised when an item is not found in a registry.

    This exception is raised by registry `get()` methods when attempting to retrieve
    an item by name that does not exist in the registry. This can occur for session
    factories, community sessions, or other registry-managed resources.

    Multiple Inheritance:
        Inherits from both ResourceError (for MCP resource handling) and KeyError
        (to indicate missing key/identifier), allowing callers to catch it as either
        type depending on their error handling strategy.

    This is an expected exception that should be caught when:
    - Registry lookups may legitimately fail (e.g., optional resources)
    - Services may be temporarily unavailable or offline
    - Configuration changes may have removed items
    - Names may come from untrusted or user-provided sources

    This distinguishes expected "item not found" scenarios from actual coding bugs
    that might also raise generic KeyError elsewhere in the codebase.

    Common causes include:
    - Item removed from configuration file
    - Item name misspelled or incorrectly formatted
    - Item not yet discovered during initialization
    - Factory or session offline and removed from active registry
    - Stale reference to previously-existing item

    Usage:
        ```python
        try:
            factory = await registry.get(factory_name)
        except RegistryItemNotFoundError as e:
            logger.warning(f"Registry item not available: {e}")
            # Handle missing item gracefully (e.g., skip, use default, retry)
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

    Key Distinction:
        ConfigurationError indicates problems with user-provided configuration data
        (files, environment variables) that can be corrected by the user. This is
        distinct from InternalError, which indicates bugs in the MCP code itself.

    Use more specific subclasses when possible:
        - CommunitySessionConfigurationError: For community session configuration issues
        - EnterpriseSystemConfigurationError: For enterprise system configuration issues

    Common causes include:
        - Invalid JSON syntax in configuration files
        - Missing required configuration fields
        - Invalid configuration field values or types
        - Conflicting configuration settings
        - Configuration referencing unavailable features
        - Environment variables not set or incorrectly formatted

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
    """Raised when community session configuration is invalid.

    This exception is raised during validation of community session configuration
    data from the `deephaven_mcp.json` configuration file. It indicates that
    session parameters are missing, invalid, or conflicting in the `community_sessions`
    section of the configuration.

    Community sessions are statically configured Deephaven Community (Core) instances
    that the MCP server connects to. Configuration errors prevent these sessions from
    being initialized and registered.

    Common causes include:
        - Invalid host or port values (wrong type, out of range)
        - Missing required authentication parameters
        - Conflicting authentication methods specified (e.g., both PSK and anonymous)
        - Invalid session timeout or connection parameter values
        - Malformed session configuration objects (wrong structure)
        - Session names that conflict with reserved keywords

    Usage:
        ```python
        try:
            session_config = validate_community_session_config(config_data)
        except CommunitySessionConfigurationError as e:
            logger.error(f"Community session configuration error: {e}")
            # Guide user to fix community session configuration in deephaven_mcp.json
        ```
    """

    pass


class EnterpriseSystemConfigurationError(ConfigurationError):
    """Raised when enterprise system configuration is invalid.

    This exception is raised during validation of enterprise system (Deephaven Core+)
    configuration data from the `deephaven_mcp.json` configuration file. It indicates
    that connection parameters, authentication settings, or other enterprise-specific
    configuration is missing, invalid, or conflicting in the `enterprise.systems` section.

    Enterprise systems are Deephaven Core+ (CorePlus) deployments with controller
    clients that the MCP server connects to. Configuration errors prevent these systems
    from being initialized and their session factories from being registered.

    Common causes include:
        - Invalid or malformed connection_json_url
        - Missing or invalid authentication credentials (username, password, private_key)
        - Conflicting authentication methods (e.g., both password and private_key)
        - Invalid TLS/SSL configuration
        - Missing required enterprise system parameters (auth_type, connection_json_url)
        - Invalid worker creation parameters (max_concurrent_workers)
        - Malformed session creation configuration objects

    Usage:
        ```python
        try:
            enterprise_config = validate_enterprise_system_config(system_name, config_data)
        except EnterpriseSystemConfigurationError as e:
            logger.error(f"Enterprise system configuration error for {system_name}: {e}")
            # Guide user to fix enterprise system configuration in deephaven_mcp.json
        ```
    """

    pass


class UnsupportedOperationError(McpError):
    """Exception raised when an operation is not supported in the current context.

    This exception is raised when a method or operation is called in a context where
    it cannot be executed. This typically occurs when attempting to use features that
    require specific session types, programming languages, or environments that are
    not available in the current context.

    Common scenarios include:
        - Python-specific operations attempted on Groovy sessions
        - Enterprise (Core+) features attempted on Community sessions
        - Language-specific operations (e.g., pip packages) on non-Python sessions
        - Operations requiring specific capabilities not available in current environment
        - Features not yet implemented for certain session types
        - Platform-specific operations attempted on unsupported platforms

    Usage:
        ```python
        if session.programming_language != "python":
            raise UnsupportedOperationError(
                f"This operation requires a Python session, but session uses {session.programming_language}"
            )
        ```

    Note:
        This is distinct from NotImplementedError, which indicates planned but unimplemented
        features. UnsupportedOperationError indicates operations that are fundamentally
        incompatible with the current context.
    """

    pass
