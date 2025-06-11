"""
Custom exceptions for Deephaven MCP session management.
"""

__all__ = ["SessionCreationError"]


class SessionCreationError(Exception):
    """Raised when a Deephaven Session cannot be created."""

    pass
