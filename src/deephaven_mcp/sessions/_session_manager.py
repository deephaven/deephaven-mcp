"""
Async session management for Deephaven workers.

This module provides asyncio-compatible, coroutine-safe creation, caching, and lifecycle management of Deephaven Session objects.
Sessions are configured using validated worker configuration from _config.py. Session reuse is automatic:
if a cached session is alive, it is returned; otherwise, a new session is created and cached.

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

import asyncio
import logging
import time

from pydeephaven import Session

from deephaven_mcp import config

from deephaven_mcp.sessions._session._community_session import SessionCommunity, SessionBase

_LOGGER = logging.getLogger(__name__)


class SessionManager:
    """
    Manages Deephaven Session objects, including creation, caching, and lifecycle.

    Usage:
        - Instantiate with a ConfigManager instance:
            cfg_mgr = ...  # Your ConfigManager
            mgr = SessionManager(cfg_mgr)
        - Use clear_all_sessions() for explicit cleanup when needed:
            await mgr.clear_all_sessions()

    Notes:
        - Each SessionManager instance is fully isolated and must be provided a ConfigManager.
    """

    def __init__(self, config_manager: config.ConfigManager):
        """
        Initialize a new SessionManager instance.

        Args:
            config_manager (ConfigManager): The configuration manager to use for worker config lookup.

        This constructor sets up the internal session cache (mapping worker names to SessionBase objects)
        and an asyncio.Lock to ensure coroutine safety for all session management operations.
        It also creates CommunitySession objects for all configured community sessions.

        Example:
            cfg_mgr = ...  # Your ConfigManager instance
            mgr = SessionManager(cfg_mgr)
        """
        self._sessions: dict[str, SessionBase] = {}
        self._lock = asyncio.Lock()
        self._config_manager = config_manager
        
        _LOGGER.info("SessionManager initialized (sessions will be created on first access)")

    async def _ensure_sessions_initialized(self) -> None:
        """
        Ensure that session objects have been created from the configuration.
        
        This is called lazily on first access to avoid requiring the constructor to be async.
        """
        if self._sessions:
            return  # Already initialized
            
        # Load configuration and create CommunitySession objects
        config_data = await self._config_manager.get_config()
        community_sessions_config = config_data.get("community", {}).get("sessions", {})
        
        for session_name, session_config in community_sessions_config.items():
            self._sessions[session_name] = SessionCommunity(session_name, session_config)
        
        _LOGGER.info(f"SessionManager initialized with {len(self._sessions)} sessions")

    async def clear_all_sessions(self) -> None:
        """
        Atomically clear all Deephaven sessions and their cache (async).

        This method:
        1. Acquires the async session cache lock for coroutine safety.
        2. Iterates through all session objects.
        3. Attempts to close each session's cached Session (if any).
        4. Clears the session objects after all sessions are processed.

        Args:
            None

        Returns:
            None

        Error Handling:
            - Any exceptions during session closure are logged but do not prevent other sessions from being closed.
            - The cache is always cleared regardless of errors.

        Async Safety:
            This method is coroutine-safe and uses an asyncio.Lock to prevent race conditions.

        Notes:
            Intended for both production and test cleanup. Should be preferred over forcibly clearing the cache to ensure all resources are released.
        """
        start_time = time.time()
        _LOGGER.info("Clearing Deephaven session cache...")
        
        await self._ensure_sessions_initialized()
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

    async def get_or_create_session(self, session_name: str) -> Session:
        """
        Retrieve a cached Deephaven session for the specified worker, or create and cache a new one if needed.

        This is the main entry point for obtaining a Deephaven Session for a given worker. Sessions are reused if possible;
        if the cached session is not alive, a new one is created and cached. All session creation and configuration is coroutine-safe.

        Args:
            session_name (str): The name of the worker to retrieve a session for. This argument is required.

        Returns:
            Session: An alive Deephaven Session instance for the worker.

        Error Handling:
            - Any exceptions during session creation will raise SessionCreationError with details.
            - Any exceptions during config loading are logged and propagated to the caller.
            - If the cached session is not alive or liveness check fails, a new session is created.

        Raises:
            SessionCreationError: If the session could not be created for any reason.
            FileNotFoundError: If configuration or certificate files are missing.
            ValueError: If configuration is invalid or session not found.
            OSError: If there are file I/O errors when loading certificates/keys.
            RuntimeError: If configuration loading fails for other reasons.

        Usage:
            This method is coroutine-safe and can be used concurrently in async workflows.

        Example:
            session = await mgr.get_or_create_session('worker1')
        """
        _LOGGER.info(f"Getting or creating session for worker: {session_name}")
        _LOGGER.info(f"Session cache size: {len(self._sessions)}")

        await self._ensure_sessions_initialized()

        async with self._lock:
            session_obj = self._sessions.get(session_name)
            if session_obj is None:
                raise ValueError(f"No session configuration found for worker: {session_name}")
            
            # Delegate to the session object to get or create the actual Session
            return await session_obj.get_session()
