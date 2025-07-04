"""
Deephaven MCP Session Management Public API.

This module defines the public API for session management in Deephaven MCP. It re-exports the core session manager types, session registry, and related enums from submodules to provide a unified interface for session creation, caching, and lifecycle management.

Exports:
    - BaseSessionManager: Abstract base class for all session managers.
    - CommunitySessionManager: Async/thread-safe manager for community sessions.
    - EnterpriseSessionManager: Async/thread-safe manager for enterprise sessions.
    - SessionManagerType: Enum representing available session manager types.
    - SessionRegistry: Async/thread-safe registry for session lifecycle and caching.

Features:
    - Coroutine-safe session cache keyed by worker name, protected by an asyncio.Lock.
    - Automatic session reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for cache clearing and atomic reloads.

Async Safety:
    - All public functions are async and use an instance-level asyncio.Lock for coroutine safety.
    - Each session manager instance encapsulates its own session cache and lock.

Error Handling:
    - Certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Session creation failures are logged and raised to the caller.
    - Session closure failures are logged but do not prevent other operations.

Dependencies:
    - Requires aiofiles for async file I/O.
"""

from ._session_manager import (
    BaseSessionManager,
    CommunitySessionManager,
    EnterpriseSessionManager,
    SessionManagerType,
)
from ._session_registry import SessionRegistry

__all__ = [
    "BaseSessionManager",
    "CommunitySessionManager",
    "EnterpriseSessionManager",
    "SessionRegistry",
    "SessionManagerType",
]
