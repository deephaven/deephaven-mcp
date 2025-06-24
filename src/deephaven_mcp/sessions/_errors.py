"""
Custom exception types for Deephaven MCP session management.

Defines errors related to session creation and lifecycle failures. Use these exceptions to signal
recoverable or expected problems during session instantiation or management.
"""

__all__ = ["SessionCreationError"]


class SessionCreationError(Exception):
    """
    Exception raised when a Deephaven Session cannot be created.

    Raised by session management code when a new session cannot be instantiated due to
    configuration errors, resource exhaustion, authentication failures, or other recoverable
    problems. This error is intended to be caught by callers that can handle or report
    session creation failures gracefully.
    """

    pass
