"""
Async session management for Deephaven workers.

This module provides asyncio-compatible, coroutine-safe creation, caching, and lifecycle management of Deephaven SessionManager objects.

A SessionRegistry manages the lifecycle of connections to Deephaven servers ("SessionManagers"), enabling safe reuse, cleanup, and liveness checking of sessions for multiple workers. Configuration is loaded via a ConfigManager and validated for each worker. All operations are fully async and safe for concurrent use.

Main Features:
    - Coroutine-safe session manager cache keyed by worker name, protected by an asyncio.Lock.
    - Automatic session manager reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for atomic cache clearing and reloads.
    - Designed for robust integration with other MCP modules and tools.

Async Safety:
    All public methods are async and protected by an instance-level asyncio.Lock (self._lock) for coroutine safety.
    Each SessionRegistry instance encapsulates its own session manager cache and lock.

Error Handling:
    - All certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Session manager creation failures are logged and raised to the caller.
    - Session manager closure failures are logged but do not prevent other operations.
    - InternalError is raised if methods are called before explicit initialization.

Public API:
    - SessionRegistry: Main entry point for session manager creation, caching, and lifecycle management.
    - initialize(): Explicitly initialize all session managers from config (must be awaited after construction).
    - get(session_name): Retrieve an initialized Deephaven SessionManager for a worker.
    - close(): Close all active session managers without clearing the cache.

Usage Example:
    cfg_mgr = ...  # Your ConfigManager instance
    registry = SessionRegistry()
    await registry.initialize(cfg_mgr)  # Must be called before use
    session_mgr = await registry.get('worker1')
    await registry.close()

Dependencies:
    - Requires aiofiles for async file I/O.
"""

import asyncio
import logging
import time

from deephaven_mcp import config
from ._session_manager import BaseSessionManager, CommunitySessionManager
from deephaven_mcp._exceptions import InternalError

_LOGGER = logging.getLogger(__name__)


class SessionRegistry:
    """
    Async/thread-safe registry for managing Deephaven SessionManager objects for multiple workers.

    The SessionRegistry coordinates the lifecycle, caching, and concurrency of session managers for all configured workers. It ensures that session managers are created from configuration, cached, and reused if alive, and provides atomic cleanup of all sessions. All methods are fully async and protected by an internal lock for coroutine safety.

    Initialization:
        After construction, you MUST explicitly call and await `initialize()` before using any other methods. Initialization loads all session manager configurations and populates the cache. If methods are called before initialization, an InternalError is raised.

    Usage Example:
        cfg_mgr = ...  # Your ConfigManager instance
        registry = SessionRegistry()
        await registry.initialize(cfg_mgr)  # <-- Required before use!
        session_mgr = await registry.get('worker1')
        await registry.close()

    Async Safety:
        All public methods are coroutine-safe and protected by an instance asyncio.Lock.

    Error Handling:
        - InternalError is raised if methods are called before initialization.
        - Session manager creation and closure errors are logged and raised as appropriate.

    WARNING:
        You MUST explicitly call and await `initialize()` after construction before using this object.
        Initialization is NOT automatic and is required for correct operation.
    """

    def __init__(self):
        """
        Construct a new SessionRegistry for managing Deephaven session managers for multiple workers.

        Sets up the internal session manager cache (mapping worker names to BaseSessionManager objects)
        and an asyncio.Lock for coroutine safety. Does NOT initialize session managers; you must call and await `initialize()` before use.

        WARNING:
            You MUST explicitly call and await `initialize()` after construction before using this object.
            Initialization is NOT automatic and is required for correct operation.

        Example:
            cfg_mgr = ...  # Your ConfigManager instance
            registry = SessionRegistry()
            await registry.initialize(cfg_mgr)  # <-- Required before use!
        """
        #TODO change the type to CommunitySessionManager?
        self._sessions: dict[str, BaseSessionManager] = {}
        self._lock = asyncio.Lock()
        self._initialized = False

        _LOGGER.info(
            "[SessionRegistry] created (must call and await initialize() after construction)"
        )

    async def initialize(self, config_manager: config.ConfigManager) -> None:
        """
        Asynchronously initialize all session managers from configuration.

        This method must be called and awaited after construction and before using any other methods. It loads the configuration, creates CommunitySessionManager objects for each configured session, and populates the internal cache. If called multiple times, it is idempotent and has no effect after the first successful call.

        Args:
            config_manager (ConfigManager): The configuration manager to use for worker config lookup.

        Async Safety:
            This method acquires the internal lock for coroutine safety.
        Error Handling:
            Any errors during configuration loading or session manager creation are propagated to the caller.

        Example:
            cfg_mgr = ...  # Your ConfigManager instance
            registry = SessionRegistry()
            await registry.initialize(cfg_mgr)
        """
        async with self._lock:
            if self._initialized:
                return  # Already initialized

            _LOGGER.info("[SessionRegistry] initializing...")

            # Load configuration and create CommunitySession objects
            config_data = await config_manager.get_config()
            community_sessions_config = config_data.get("community", {}).get("sessions", {})

            for session_name, session_config in community_sessions_config.items():
                self._sessions[session_name] = CommunitySessionManager(
                    session_name, session_config
                )

            self._initialized = True
            _LOGGER.info(f"SessionRegistry initialized with {len(self._sessions)} sessions")

    async def close(self) -> None:
        """
        Close all active Deephaven session managers.

        This method iterates through all initialized session managers and calls their respective `close` methods.
        It does not clear the session cache or reset the registry's initialized state.
        Operations are performed under a lock for coroutine safety.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError("SessionRegistry not initialized. Call 'await initialize()' after construction.")

            start_time = time.time()
            _LOGGER.info("Closing all active Deephaven sessions...")
            num_sessions = len(self._sessions)
            _LOGGER.info(f"Processing {num_sessions} cached sessions...")

            for _session_name, session_obj in list(self._sessions.items()):
                await session_obj.close()

            _LOGGER.info(
                f"Close command sent to all session managers. Processed {num_sessions} sessions in {time.time() - start_time:.2f}s"
            )

    async def get(self, session_name: str) -> "BaseSessionManager":
        """
        Retrieve an initialized session manager for the given session name.

        This is the main entry point for obtaining a session manager (e.g., CommunitySessionManager)
        that was created and cached during the 'initialize()' call.

        Args:
            session_name: The name of the session to retrieve the manager for.

        Returns:
            The corresponding session manager instance.

        Raises:
            InternalError: If the SessionRegistry has not been initialized.
            KeyError: If no session configuration is found for the given session_name.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError("SessionRegistry not initialized. Call 'await initialize()' after construction.")
            
            if session_name not in self._sessions:
                raise KeyError(f"No session configuration found for worker: {session_name}")

            return self._sessions[session_name]
