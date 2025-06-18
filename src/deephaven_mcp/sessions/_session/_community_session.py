"""
Community session implementation for Deephaven MCP.

This module provides the CommunitySession class which encapsulates configuration
and lifecycle management for individual community sessions.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from pydeephaven import Session

from deephaven_mcp.sessions._lifecycle.community import (
    _get_session_parameters,
    create_session,
)
from deephaven_mcp.sessions._lifecycle.shared import close_session_safely

_LOGGER = logging.getLogger(__name__)


class SessionType(Enum):
    """Enum for different types of Deephaven sessions."""
    COMMUNITY = "community"
    ENTERPRISE = "enterprise"


class SessionBase(ABC):
    """
    Abstract base class for all Deephaven session types.
    
    This defines the interface that all session implementations must follow.
    """
    
    def __init__(self, session_type: SessionType, source: str, name: str):
        """Initialize the session with a type, source, and name."""
        self._type = session_type
        self._source = source
        self._name = name
        self._session_cache: Session | None = None
        self._lock = asyncio.Lock()
    
    @property
    def session_type(self) -> SessionType:
        """Return the type of this session."""
        return self._type

    @property
    def source(self) -> str:
        """Get the session source."""
        return self._source

    @property
    def name(self) -> str:
        """Get the session name."""
        return self._name
        
    @property
    def full_name(self) -> str:
        """Get the full name of this session, including type, source, and name."""
        return f"{self._type.value}:{self._source}:{self._name}"
    
    @property
    @abstractmethod
    def is_alive(self) -> bool:
        """
        Return True if the session is currently alive/usable, False otherwise.
        Implementations should check the underlying session state as appropriate.
        """
        pass  # pragma: no cover
    
    @abstractmethod
    async def get_session(self) -> Session:
        """
        Get or create a session for this configuration.
        
        Returns:
            Session: An alive Deephaven Session instance.
        """
        pass  # pragma: no cover
    
    async def close_session(self) -> None:
        """
        Close the cached session if it exists.
        """
        async with self._lock:
            if self._session_cache is not None:
                await close_session_safely(self._session_cache, self._name)
                self._session_cache = None


class SessionCommunity(SessionBase):
    """
    Manages a single community session configuration and its cached Session.
    
    This class encapsulates:
    - The parsed community session configuration
    - A cached pydeephaven.Session instance 
    - Session lifecycle management (creation, liveness checking, cleanup)
    - Thread-safe access via asyncio.Lock
    """
    
    def __init__(self, name: str, config: dict[str, Any]):
        """
        Initialize a community session.
        
        Args:
            name: The session name/identifier
            config: The validated community session configuration dictionary
        """
        super().__init__(SessionType.COMMUNITY, "community", name)
        self._config = config

    async def is_alive(self) -> bool:
        """
        Return True if the cached session exists and is alive, False otherwise.
        This method acquires the lock to ensure thread/coroutine safety.
        """
        async with self._lock:
            if self._session_cache is not None:
                try:
                    return bool(self._session_cache.is_alive)
                except Exception:
                    return False
            return False

    async def get_session(self) -> Session:
        """
        Get or create a session for this community configuration.
        
        This method:
        1. Checks if there's a cached session that's still alive
        2. If not, creates a new session using the configuration
        3. Caches and returns the session
        
        Returns:
            Session: An alive Deephaven Session instance.
            
        Raises:
            SessionCreationError: If the session could not be created.
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
            session_params = await _get_session_parameters(self._config)
            session = await create_session(**session_params)
            
            self._session_cache = session
            _LOGGER.info(f"Session created and cached for community worker: {self._name}")
            return session
