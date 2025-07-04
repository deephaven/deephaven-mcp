"""
Async session management for Deephaven workers.

This module provides asyncio-compatible, coroutine-safe creation, caching, and lifecycle management of Deephaven SessionManager objects.
A "SessionManager" manages the lifecycle of a connection to a Deephaven server, used to execute queries and manage resources. Sessions are configured using validated worker configuration from _config.py. Session reuse is automatic: if a cached session manager is alive, it is returned; otherwise, a new one is created and cached.

Features:
    - Coroutine-safe session manager cache keyed by worker name, protected by an asyncio.Lock.
    - Automatic session manager reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for cache clearing and atomic reloads.
    - Designed for use by other MCP modules and tools.

Async Safety:
    All public methods are async and use an instance-level asyncio.Lock (self._lock) for coroutine safety.
    Each SessionRegistry instance encapsulates its own session manager cache and lock.

Error Handling:
    - All certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Session manager creation failures are logged and raised to the caller.
    - Session manager closure failures are logged but do not prevent other operations.

Public API:
    - SessionRegistry: Main entry point for session manager creation, caching, and lifecycle management.
    - get(session_name): Obtain or create an alive Deephaven SessionManager for a worker.
    - clear_all_sessions(): Atomically clear all session managers and release resources.

Dependencies:
    - Requires aiofiles for async file I/O.
"""

import asyncio
import logging
import time

from deephaven_mcp import config
from ._session_manager import BaseSessionManager, CommunitySessionManager

_LOGGER = logging.getLogger(__name__)


class SessionRegistry:
    """
    Async/thread-safe manager for Deephaven SessionManager objects, including creation, caching, and lifecycle management.

    Each SessionRegistry instance is fully isolated and requires a ConfigManager for worker configuration lookup. All operations are coroutine-safe.
    Use this class to obtain, reuse, and clean up Deephaven SessionManager objects for workers.

    Example:
        cfg_mgr = ...  # Your ConfigManager instance
        registry = SessionRegistry(cfg_mgr)
        session_mgr = await registry.get('worker1')
        await registry.clear_all_sessions()
    """

    def __init__(self, config_manager: config.ConfigManager):
        """
        Initialize a new SessionRegistry instance.

        Args:
            config_manager (ConfigManager): The configuration manager to use for worker config lookup.

        Sets up the internal session manager cache (mapping worker names to BaseSessionManager objects)
        and an asyncio.Lock for coroutine safety. Session managers are created lazily on first use.

        Example:
            cfg_mgr = ...  # Your ConfigManager instance
            registry = SessionRegistry(cfg_mgr)
        """
        #TODO change the type to CommunitySessionManager?
        self._sessions: dict[str, BaseSessionManager] = {}
        self._lock = asyncio.Lock()
        self._config_manager = config_manager

        _LOGGER.info(
            "[SessionRegistry] initialized (sessions will be created on first access)"
        )

    async def _ensure_sessions_initialized(self) -> None:
        """
        Lazily initialize session manager objects from configuration if not already present.

        Called on first access to avoid requiring an async constructor. Populates the session manager cache
        with CommunitySessionManager objects for all configured community sessions.

        Ensures that each community session is managed by a CommunitySessionManager, providing
        async/thread-safe session creation, caching, and liveness checking for each worker.
        """
        if self._sessions:
            return  # Already initialized

        # Load configuration and create CommunitySession objects
        config_data = await self._config_manager.get_config()
        community_sessions_config = config_data.get("community", {}).get("sessions", {})

        for session_name, session_config in community_sessions_config.items():
            self._sessions[session_name] = CommunitySessionManager(
                session_name, session_config
            )

        _LOGGER.info(f"SessionManager initialized with {len(self._sessions)} sessions")

    async def clear_all_sessions(self) -> None:
        """
        Atomically clear all Deephaven session managers and their cache (async).

        Only clears and closes session managers that have already been initialized. Does not initialize new session managers.
        Acquires the session manager cache lock for coroutine safety, closes all cached session managers, and clears the cache.
        Any exceptions during session manager closure are logged but do not prevent other session managers from being closed.
        The cache is always cleared regardless of errors. Intended for both production and test cleanup.
        """
        start_time = time.time()
        _LOGGER.info("Clearing Deephaven session cache...")

        _LOGGER.info(f"Current session cache size: {len(self._sessions)}")

        async with self._lock:
            num_sessions = len(self._sessions)
            _LOGGER.info(f"Processing {num_sessions} cached sessions...")
            for _session_name, session_obj in list(self._sessions.items()):
                await session_obj.close_session()
            self._sessions.clear()
            _LOGGER.info(
                f"Session cache cleared. Processed {num_sessions} sessions in {time.time() - start_time:.2f}s"
            )

    #TODO: change the type to CommunitySessionManager?
    #TODO: support DHE?
    async def get(self, session_name: str) -> BaseSessionManager:
        """
        Retrieve a cached or newly created session manager for the given session name.

        Main entry point for obtaining a live Deephaven session manager (such as CommunitySessionManager) for a configured session.
        If a session manager for the given name is cached and alive, it is reused; otherwise, a new one is created and cached. All operations are coroutine-safe.

        Args:
            session_name (str): The name of the session as specified in the configuration (e.g., from deephaven_mcp.json).

        Returns:
            BaseSessionManager: An alive session manager instance (e.g., CommunitySessionManager) for the requested session.

        Raises:
            SessionCreationError: If the session manager could not be created for any reason.
            FileNotFoundError: If configuration or certificate files are missing.
            KeyError: If no configuration exists for the given session name.
            OSError: If there are file I/O errors when loading certificates/keys.
            RuntimeError: If configuration loading fails for other reasons.

        Notes:
            - Exceptions during session manager creation raise SessionCreationError with details.
            - Exceptions during config loading are logged and propagated to the caller.
            - If the cached session manager is not alive or liveness check fails, a new one is created.
            - This method is coroutine-safe and can be used concurrently in async workflows.

        Example:
            session_mgr = await registry.get('community_prod')
        """
        _LOGGER.info(f"Getting session manager for worker: {session_name}")
        _LOGGER.info(f"Session cache size: {len(self._sessions)}")

        await self._ensure_sessions_initialized()

        async with self._lock:
            session_obj = self._sessions.get(session_name)
            if session_obj is None:
                raise KeyError(
                    f"No session configuration found for worker: {session_name}"
                )

            # Delegate to the session object to get or create the actual Session
            return await session_obj.get_session()
