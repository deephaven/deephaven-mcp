"""
Async session management for Deephaven workers.

This package provides asyncio-compatible, coroutine-safe creation, caching, and lifecycle management of Deephaven Session objects.

Features:
    - Coroutine-safe session cache keyed by worker name, protected by an asyncio.Lock.
    - Automatic session reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for cache clearing and atomic reloads.
    - Designed for use by other MCP modules and MCP tools.

Async Safety:
    All public functions are async and use an instance-level asyncio.Lock (self._lock) for coroutine safety.
    Each SessionManager instance encapsulates its own session cache and lock.

Error Handling:
    - All certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Session creation failures are logged and raised to the caller.
    - Session closure failures are logged but do not prevent other operations.

Dependencies:
    - Requires aiofiles for async file I/O.
"""

from ._errors import SessionCreationError
from ._session._session_base import SessionType
from ._session._queries import (
    get_dh_versions,
    get_meta_table,
    get_pip_packages_table,
    get_table,
)
from ._session_manager import (
    Session,
    SessionManager,
)

__all__ = [
    "SessionManager",
    "SessionType",
    "get_dh_versions",
    "get_table",
    "get_pip_packages_table",
    "get_meta_table",
    "SessionCreationError",
    "Session",
]
