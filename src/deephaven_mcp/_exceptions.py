"""Custom exception types for Deephaven MCP.

Defines specialized exception hierarchies related to various subsystems including session
management, client operations, authentication, and resource handling. These exceptions provide
fine-grained error reporting and enable more specific exception handling strategies.

All exception classes in this module should be used consistently throughout the Deephaven MCP
system to signal recoverable or expected problems, allowing callers to implement appropriate
recovery or reporting strategies.

Exception Hierarchy:
    - Session exceptions: SessionError (base), SessionCreationError
    - Authentication exceptions: AuthenticationError
    - Query exceptions: QueryError
    - Connection exceptions: DeephavenConnectionError
    - Resource exceptions: ResourceError
    - Internal exceptions: InternalError (extends RuntimeError)

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
    # Internal exceptions
    "InternalError",
]


# Session Exceptions


class SessionError(Exception):
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


class AuthenticationError(Exception):
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


class QueryError(Exception):
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


class DeephavenConnectionError(Exception):
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


class ResourceError(Exception):
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


# Internal Exceptions


class InternalError(RuntimeError):
    """Exception raised for errors that are due to internal inconsistencies in the code.

    This exception is used when there is a logical contradiction or unexpected state
    in the internal implementation, rather than an error in user input or external resources.

    Unlike other exceptions in this module that represent expected or recoverable error
    conditions, InternalError typically indicates a bug or logical flaw in the MCP
    implementation that requires developer attention. It inherits from RuntimeError rather
    than Exception to emphasize its different nature.

    Examples:
        - Unexpected object types in internal methods
        - Invalid state transitions
        - Unhandled conditions in control flow
        - Assertion failures in internal logic
        - Implementation assumptions violated

    Usage:
        ```python
        def process_session_object(session):
            if not isinstance(session, (CoreSession, CorePlusSession)):
                raise InternalError(f"Expected CoreSession or CorePlusSession, got {type(session)}")
            # Continue with processing
        ```

    Note:
        This exception should generally not be caught by client code except at the highest
        levels for general error handling and reporting. When encountered, it typically
        requires code fixes rather than runtime recovery strategies.
    """

    pass
