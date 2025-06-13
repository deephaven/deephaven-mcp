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
from types import TracebackType

from pydeephaven import Session

from deephaven_mcp import config

from ._lifecycle.community import (
    create_session_for_worker,
)
from ._lifecycle.shared import close_session_safely

_LOGGER = logging.getLogger(__name__)


class SessionManager:
    """
    Manages Deephaven Session objects, including creation, caching, and lifecycle.

    Usage:
        - Instantiate with a ConfigManager instance:
            cfg_mgr = ...  # Your ConfigManager
            mgr = SessionManager(cfg_mgr)
        - Use in async context for deterministic cleanup:
            async with SessionManager(cfg_mgr) as mgr:
                ...
            # Sessions are automatically cleared on exit

    Notes:
        - Each SessionManager instance is fully isolated and must be provided a ConfigManager.
    """

    def __init__(self, config_manager: config.ConfigManager):
        """
        Initialize a new SessionManager instance.

        Args:
            config_manager (ConfigManager): The configuration manager to use for worker config lookup.

        This constructor sets up the internal session cache (mapping worker names to Session objects)
        and an asyncio.Lock to ensure coroutine safety for all session management operations.

        Example:
            cfg_mgr = ...  # Your ConfigManager instance
            mgr = SessionManager(cfg_mgr)
        """
        self._cache: dict[str, Session] = {}
        self._lock = asyncio.Lock()
        self._config_manager = config_manager

    async def __aenter__(self) -> "SessionManager":
        """
        Enter the async context manager for SessionManager.

        Returns:
            SessionManager: The current instance (self).

        Usage:
            async with SessionManager() as mgr:
                # Use mgr to create, cache, and reuse sessions
                ...
            # On exit, all sessions are automatically cleaned up.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Exit the async context manager for SessionManager, ensuring resource cleanup.

        On exit, all cached sessions are cleared via clear_all_sessions().
        This guarantees no lingering sessions after the context block, which is useful for tests,
        scripts, and advanced workflows that require deterministic resource management.

        Args:
            exc_type (type): Exception type if raised in the context, else None.
            exc (Exception): Exception instance if raised, else None.
            tb (traceback): Traceback if exception was raised, else None.

        Example:
            async with SessionManager() as mgr:
                ...
            # Sessions are cleaned up here
        """
        await self.clear_all_sessions()

    async def clear_all_sessions(self) -> None:
        """
        Atomically clear all Deephaven sessions and their cache (async).

        This method:
        1. Acquires the async session cache lock for coroutine safety.
        2. Iterates through all cached sessions.
        3. Attempts to close each alive session (using await asyncio.to_thread).
        4. Clears the session cache after all sessions are processed.

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
        _LOGGER.info(f"Current session cache size: {len(self._cache)}")

        async with self._lock:
            num_sessions = len(self._cache)
            _LOGGER.info(f"Processing {num_sessions} cached sessions...")
            for session_name, session in list(self._cache.items()):
                await close_session_safely(session, session_name)
            self._cache.clear()
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
            ValueError: If configuration is invalid.
            OSError: If there are file I/O errors when loading certificates/keys.
            RuntimeError: If configuration loading fails for other reasons.

        Usage:
            This method is coroutine-safe and can be used concurrently in async workflows.

        Example:
            session = await mgr.get_or_create_session('worker1')
        """
        _LOGGER.info(f"Getting or creating session for worker: {session_name}")
        _LOGGER.info(f"Session cache size: {len(self._cache)}")

        async with self._lock:
            session = self._cache.get(session_name)
            if session is not None:
                try:
                    if session.is_alive:
                        _LOGGER.info(
                            f"Found and returning cached session for worker: {session_name}"
                        )
                        return session
                    else:
                        _LOGGER.info(
                            f"Cached session for worker '{session_name}' is not alive. Recreating."
                        )
                except Exception as e:
                    _LOGGER.warning(
                        f"Error checking session liveness for worker '{session_name}': {e}. Recreating session."
                    )

            # At this point, we need to create a new session
            session = await create_session_for_worker(
                self._config_manager, session_name
            )
            self._cache[session_name] = session
            _LOGGER.info(
                f"Session cached for worker: {session_name}. Returning session."
            )
            return session
