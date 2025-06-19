"""
Async community session implementation for Deephaven MCP.

This module provides an async/thread-safe session manager for Deephaven community environments.
Sessions are created on demand, cached for reuse, and checked for liveness before reuse.
Handles error propagation and resource cleanup.
"""
import asyncio
import logging
from typing import Dict, Any, Optional

from pydeephaven import Session

from deephaven_mcp.sessions._session._session_base import SessionBase, SessionType
from deephaven_mcp.sessions._errors import SessionCreationError
from deephaven_mcp.sessions._lifecycle.community import _get_session_parameters, create_session
from deephaven_mcp.sessions._lifecycle.shared import close_session_safely

_LOGGER = logging.getLogger(__name__)


class SessionCommunity(SessionBase):
    """
    Async/thread-safe session manager for Deephaven community environments.

    Manages a single cached Deephaven session, creating it on demand and reusing it if alive.
    Ensures thread safety, proper liveness checks, and error propagation for session operations.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize a community session.
        
        Args:
            name: Identifier for this session.
            config: Session configuration dictionary.
        """
        super().__init__(SessionType.COMMUNITY, "community", name)
        self._config = config
    
    @property
    async def is_alive(self) -> bool:
        """
        Asynchronously check if the cached session is alive and usable.

        This method acquires a lock for thread safety. Returns True if a cached session exists and is alive
        (connection open, authenticated, not expired). Any error in checking liveness is treated as not alive.

        Returns:
            bool: True if the cached session is alive and usable, False otherwise.
        """
        async with self._lock:
            if self._session_cache is None:
                return False
                
            try:
                return self._session_cache.is_alive
            except Exception:
                # Any error in checking liveness means the session is not usable
                return False
    
    async def get_session(self) -> Session:
        """
        Retrieve an alive Deephaven Session for this community worker.

        This method is async and thread-safe. If a cached session exists and is alive,
        it is returned. Otherwise, a new session is created and cached. If liveness
        cannot be determined or the cached session is dead, the cache is cleared and a
        new session is created.

        Returns:
            Session: An alive Deephaven Session instance for this worker.

        Raises:
            SessionCreationError: If the session could not be created or initialized.
            Exception: If an unexpected error occurs during liveness check or session creation.
        """
        async with self._lock:
            # Check if we have a cached session that's still alive
            if self._session_cache is not None:
                try:
                    if self._session_cache.is_alive:
                        _LOGGER.debug(f"Returning cached session for community worker: {self._name}")
                        return self._session_cache
                    else:
                        _LOGGER.info(f"Cached session for community worker '{self._name}' is not alive. Recreating.")
                        self._session_cache = None
                except Exception as e:
                    _LOGGER.warning(
                        f"Error checking session liveness for community worker '{self._name}': {e}. Recreating session."
                    )
                    self._session_cache = None
            
            # Create a new session
            _LOGGER.info(f"Creating new session for community worker: {self._name}")
            session = await create_session(self._config)
            
            self._session_cache = session
            _LOGGER.info(f"Session created and cached for community worker: {self._name}")
            return session
    
