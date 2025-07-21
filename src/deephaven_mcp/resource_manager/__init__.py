"""
Deephaven MCP Resource Management Public API.

This module defines the public API for resource management in Deephaven MCP. It re-exports the core resource manager types, registries, and related enums from submodules to provide a unified interface for resource creation, caching, and lifecycle management.

Exports:
    - SystemType: Enum for backend system type (COMMUNITY, ENTERPRISE).
    - BaseItemManager: Abstract base class for managing lazily-initialized items.
    - CommunitySessionManager: Manages the lifecycle of community sessions.
    - EnterpriseSessionManager: Manages the lifecycle of enterprise sessions.
    - CorePlusSessionFactoryManager: Manages the lifecycle of CorePlusSessionFactory objects.

    - CommunitySessionRegistry: A registry for all configured CommunitySessionManager instances.
    - CorePlusSessionFactoryRegistry: A registry for all configured CorePlusSessionFactoryManager instances.

Features:
    - Coroutine-safe item cache keyed by name, protected by an asyncio.Lock.
    - Automatic item reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for cache clearing and atomic reloads.

Async Safety:
    - All public functions are async and use an instance-level asyncio.Lock for coroutine safety.
    - Each manager instance encapsulates its own item cache and lock.

Error Handling:
    - Certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Resource creation failures are logged and raised to the caller.
    - Resource closure failures are logged but do not prevent other operations.

Dependencies:
    - Requires aiofiles for async file I/O.
"""

from ._manager import (
    BaseItemManager,
    CommunitySessionManager,
    CorePlusSessionFactoryManager,
    EnterpriseSessionManager,
    ResourceLivenessStatus,
    SystemType,
)
from ._registry import CommunitySessionRegistry, CorePlusSessionFactoryRegistry

__all__ = [
    "SystemType",
    "ResourceLivenessStatus",
    "BaseItemManager",
    "CommunitySessionManager",
    "EnterpriseSessionManager",
    "CorePlusSessionFactoryManager",
    "CommunitySessionRegistry",
    "CorePlusSessionFactoryRegistry",
]
