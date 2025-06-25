"""
Async session management for Deephaven workers.

This module provides asyncio-compatible, coroutine-safe management of Deephaven Session objects.
A "Session" is a connection to a Deephaven server, used to execute queries and manage resources.
Sessions are configured using session configuration passed directly to the SessionManager.

Features:
    - Coroutine-safe session cache keyed by session name, protected by an asyncio.Lock.
    - Automatic session reuse and resource cleanup.
    - Tools for cache clearing and atomic operations.
    - Designed for use by other MCP modules and MCP tools.

Async Safety:
    All public functions are async and use an instance-level asyncio.Lock (self._lock) for coroutine safety.
    Each SessionManager instance encapsulates its own session cache and lock.

Error Handling:
    - Session failures are logged and raised to the caller.
    - Session closure failures are logged but do not prevent other operations.

Public API:
    - SessionManager: Main entry point for session management.
    - get_by_name(session_name): Retrieve a SessionBase object for a session by name.
    - close_all_sessions(): Atomically close all sessions and release resources.
"""

import asyncio
import logging
import time
from typing import Any

from deephaven_mcp.sessions._session._session_base import SessionBase
from deephaven_mcp.sessions._session._session_community import SessionCommunity

_LOGGER = logging.getLogger(__name__)


class SessionManager:
    """
    Async/thread-safe manager for Deephaven Session objects, including retrieval and lifecycle management.

    Each SessionManager instance is fully isolated and initialized with configuration data. All operations are coroutine-safe.
    Use this class to retrieve and clean up Deephaven Session objects.

    Usage:
        config_data = {"community": {"sessions": {"local": {"host": "localhost"}}}}
        mgr = SessionManager(config_data)
        session_obj = await mgr.get_by_name('local')
        session = await session_obj.get_session()
        await mgr.close_all_sessions()
    """

    def __init__(self, config_data: dict[str, Any]):
        """
        Initialize a new SessionManager instance.

        Args:
            config_data (dict[str, Any]): The configuration data containing session configurations.
                The expected format is a dictionary with community.sessions mapping to session configurations.

        Sets up the internal session cache and an asyncio.Lock for coroutine safety.

        Example:
            config = {"community": {"sessions": {"local": {"host": "localhost"}}}}
            mgr = SessionManager(config)
        """
        _LOGGER.info("[SessionManager] Initializing session manager")
        self._lock = asyncio.Lock()

        _LOGGER.info("[SessionManager] Loading session configuration")
        community_sessions_config = config_data.get("community", {}).get("sessions", {})
        self._sessions_community = [
            SessionCommunity(session_name, session_config)
            for session_name, session_config in community_sessions_config.items()
        ]

        _LOGGER.info(
            "[SessionManager] Initialization complete: %d community sessions loaded",
            len(self._sessions_community),
        )

    async def close_all_sessions(self) -> None:
        """
        Atomically close all Deephaven sessions and release resources (async).

        Acquires the session cache lock for coroutine safety and closes all cached sessions.
        Any exceptions during session closure are logged but do not prevent other sessions from being closed.
        Intended for both production and test cleanup.
        """
        start_time = time.time()
        _LOGGER.info("[SessionManager.close_all_sessions] Closing all sessions")

        async with self._lock:
            num_sessions = len(self._sessions_community)
            _LOGGER.info(
                "[SessionManager.close_all_sessions] Processing %d sessions",
                num_sessions,
            )
            for session_obj in self._sessions_community:
                await session_obj.close_session()
            _LOGGER.info(
                "[SessionManager.close_all_sessions] All sessions closed: processed %d sessions in %.2fs",
                num_sessions,
                time.time() - start_time,
            )

    async def get_by_name(self, session_name: str) -> SessionBase:
        """
        Retrieve a SessionBase object for the specified session by name.

        Main entry point for obtaining a session object by name. The session object
        provides access to the underlying Deephaven session and additional metadata/management methods.

        Args:
            session_name (str): The name of the session to retrieve.

        Returns:
            SessionBase: A session object for the specified session name.

        Raises:
            ValueError: If the session name is not found in the configuration.

        Notes:
            - This method is coroutine-safe and can be used concurrently in async workflows.
            - To get the actual Deephaven Session instance, use the returned object's get_session() method.
            - The method does not create sessions; it only retrieves existing ones configured at initialization.

        Example:
            session_obj = await mgr.get_by_name('local')
            session = await session_obj.get_session()  # Get the actual Deephaven Session
        """
        _LOGGER.info(
            "[SessionManager.get_by_name] Retrieving session '%s'", session_name
        )

        async with self._lock:
            # Find the session with the matching name in the list
            session_obj = next(
                (s for s in self._sessions_community if s.name == session_name), None
            )
            if session_obj is None:
                _LOGGER.error(
                    "[SessionManager.get_by_name] No session found with name '%s'",
                    session_name,
                )
                raise ValueError(
                    f"No session configuration found for session: {session_name}"
                )

            # Return the session object directly
            return session_obj
