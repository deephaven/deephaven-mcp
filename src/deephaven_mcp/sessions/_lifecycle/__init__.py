"""
Session lifecycle helpers package for Deephaven MCP.

This package provides coroutine-compatible helpers for managing session lifecycles in Deephaven MCP.

Submodules:
- community: Lifecycle helpers for Deephaven Community (Core) sessions.
- shared: General/shared helpers for session lifecycle management.

This package is designed for extensibility, allowing additional submodules (e.g., 'enterprise') for other session types in the future.
"""

from . import community, shared

__all__ = ["community", "shared"]
