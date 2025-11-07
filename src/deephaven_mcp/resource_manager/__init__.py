"""
Deephaven MCP Resource Management Public API.

This module defines the public API for resource management in Deephaven MCP. It re-exports
core resource manager types, registries, session launchers, and utility functions from
submodules to provide a unified interface for resource creation, caching, and lifecycle
management.

Overview:
    The resource_manager package provides two primary patterns for working with Deephaven:
    
    1. **Static Sessions** (pre-configured): Connect to existing Deephaven servers using
       CommunitySessionManager or EnterpriseSessionManager. Sessions are loaded from
       configuration and cached for reuse.
    
    2. **Dynamic Sessions** (on-demand): Launch new Deephaven servers on-demand using
       DynamicCommunitySessionManager with Docker or pip. Automatically allocates ports,
       manages lifecycle, and handles cleanup.

Exports - Session Managers:
    - CommunitySessionManager: Manages lifecycle of pre-configured Deephaven Community sessions.
      Connects to existing servers specified in configuration. Used for static deployments
      where servers are already running.
      
    - DynamicCommunitySessionManager: Manages lifecycle of dynamically launched Deephaven
      Community sessions. Launches sessions on-demand via Docker or pip, automatically handles
      port allocation, authentication tokens, and cleanup. Used for ephemeral or test sessions.
      
    - EnterpriseSessionManager: Manages lifecycle of Deephaven Enterprise (Core+) sessions.
      Connects to existing enterprise servers with support for authentication, TLS, and
      CorePlusSessionFactory management.
      
    - CorePlusSessionFactoryManager: Manages lifecycle of CorePlusSessionFactory objects
      used for creating enterprise sessions. Handles worker creation configuration and
      authentication.
      
    - BaseItemManager: Abstract base class for all manager types. Provides common interface
      for item caching, liveness checking, and async-safe resource management.

Exports - Registries:
    - CommunitySessionRegistry: Registry for all configured CommunitySessionManager instances.
      Provides centralized access to all community sessions loaded from configuration.
      
    - CorePlusSessionFactoryRegistry: Registry for all configured CorePlusSessionFactoryManager
      instances. Provides centralized access to all enterprise factory configurations.
      
    - CombinedSessionRegistry: Combined registry that provides unified access to both
      community and enterprise sessions. Simplifies code that needs to work with either type.

Exports - Session Launchers:
    - LaunchedSession: Abstract base class for launched sessions. Defines interface for
      sessions that own their lifecycle (launch + stop).
      
    - DockerLaunchedSession: Deephaven session launched via Docker container. Uses host
      networking, supports resource limits (memory/CPU), volume mounts, and custom JVM args.
      
    - PipLaunchedSession: Deephaven session launched via pip-installed deephaven. Uses
      local process with subprocess management. Requires deephaven-server package installed.
      
    - launch_session: Convenience function to launch sessions via docker or pip. Delegates
      to appropriate launcher based on method parameter.

Exports - Utility Functions:
    - find_available_port: Find an available TCP port for session binding. Uses OS to
      assign from ephemeral port range. Useful for dynamic session creation.
      
    - generate_auth_token: Generate cryptographically secure PSK authentication token.
      Creates 32-character hex string with 128 bits of entropy. Used for session security.

Exports - Enums:
    - SystemType: Backend system type enum with values COMMUNITY and ENTERPRISE. Used to
      distinguish between Deephaven Community and Enterprise (Core+) deployments.
      
    - ResourceLivenessStatus: Resource health status enum with values ALIVE, DEAD, and UNKNOWN.
      Used by managers to track connection health and determine when to recreate resources.

Features:
    - Coroutine-safe item cache keyed by name, protected by asyncio.Lock
    - Automatic item reuse with liveness checking and stale resource cleanup
    - Dynamic session launching via Docker containers or pip-installed processes
    - Native async file I/O for secure certificate loading (TLS, client certs/keys)
    - Health checking for dynamically launched sessions with configurable timeouts
    - Utility functions for port allocation and cryptographic token generation
    - Idempotent cleanup operations allowing safe repeated calls

Async Safety:
    - All public functions are async and use instance-level asyncio.Lock for coroutine safety
    - Each manager instance encapsulates its own item cache and lock
    - Session launchers use async subprocess management (asyncio.create_subprocess_exec)
    - Registries use async initialization to load all configured items in parallel

Error Handling:
    - Certificate loading operations wrapped in try-except with aiofiles for async I/O
    - Resource creation failures are logged and raised to caller (propagates exceptions)
    - Resource closure failures are logged but do not prevent other operations
    - Session launch failures raise SessionLaunchError with detailed context and logging
    - Invalid configuration raises appropriate errors during manager/registry initialization

Dependencies:
    - aiofiles: For async file I/O (certificate and key file loading)
    - aiohttp: For session health checking (HTTP endpoint polling)
    - Docker (optional): Required for dynamic session launching via docker
    - deephaven-server (optional): Required for dynamic session launching via pip

Usage Example - Static Sessions:
    >>> from deephaven_mcp.resource_manager import CommunitySessionRegistry
    >>> 
    >>> # Initialize registry from configuration
    >>> registry = CommunitySessionRegistry()
    >>> await registry.initialize(config_manager)
    >>> 
    >>> # Get a pre-configured session manager
    >>> manager = await registry.get("my-session")
    >>> session = await manager.get_session()
    >>> # Use session...
    >>> await session.close()

Usage Example - Dynamic Sessions:
    >>> from deephaven_mcp.resource_manager import (
    ...     launch_session,
    ...     find_available_port,
    ...     generate_auth_token,
    ... )
    >>> 
    >>> # Launch a session dynamically
    >>> port = find_available_port()
    >>> token = generate_auth_token()
    >>> 
    >>> launched = await launch_session(
    ...     launch_method="docker",
    ...     session_name="test-session",
    ...     port=port,
    ...     auth_token=token,
    ...     heap_size_gb=4.0,
    ...     extra_jvm_args=[],
    ...     environment_vars={},
    ...     docker_image="ghcr.io/deephaven/server:latest",
    ... )
    >>> 
    >>> if await launched.wait_until_ready():
    ...     print(f"Session ready at {launched.connection_url}")
    >>> 
    >>> # Clean up
    >>> await launched.stop()
"""

from ._launcher import (
    DockerLaunchedSession,
    LaunchedSession,
    PipLaunchedSession,
    launch_session,
)
from ._manager import (
    BaseItemManager,
    CommunitySessionManager,
    CorePlusSessionFactoryManager,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    ResourceLivenessStatus,
    SystemType,
)
from ._registry import CommunitySessionRegistry, CorePlusSessionFactoryRegistry
from ._registry_combined import CombinedSessionRegistry
from ._utils import find_available_port, generate_auth_token

__all__ = [
    "SystemType",
    "ResourceLivenessStatus",
    "BaseItemManager",
    "CommunitySessionManager",
    "DynamicCommunitySessionManager",
    "EnterpriseSessionManager",
    "CorePlusSessionFactoryManager",
    "CommunitySessionRegistry",
    "CorePlusSessionFactoryRegistry",
    "CombinedSessionRegistry",
    "LaunchedSession",
    "DockerLaunchedSession",
    "PipLaunchedSession",
    "launch_session",
    "find_available_port",
    "generate_auth_token",
]
