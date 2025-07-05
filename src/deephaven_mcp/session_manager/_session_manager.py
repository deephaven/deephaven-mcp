"""
Base classes and enums for Deephaven MCP session management.

This module defines the abstract interfaces and base implementations for all session manager types
in Deephaven MCP. It provides async/thread-safe session management, session caching, liveness contracts,
and resource cleanup. Subclasses must implement the required interface for concrete session types,
such as community and enterprise session managers.
"""

import asyncio
import enum
import logging
from abc import ABC, abstractmethod
from typing import override

from deephaven_mcp.client import BaseSession

import logging

from deephaven_mcp.client import CorePlusSession
from typing import Any
from deephaven_mcp.client import CoreSession

_LOGGER = logging.getLogger(__name__)


class SessionManagerType(enum.Enum):
    """
    Enum representing the types of Deephaven session managers.

    Attributes:
        COMMUNITY (str): Community session manager type, suitable for community environments.
        ENTERPRISE (str): Enterprise session manager type, suitable for enterprise environments.

    Notes:
        This enum is used to identify the type of session manager being used.
    """
    COMMUNITY = "community"
    ENTERPRISE = "enterprise"

class BaseSessionManager(ABC):
    """
    Abstract base class for all Deephaven session managers.

    This class provides the foundation for managing cached Deephaven sessions in a thread-safe and asynchronous manner.
    It defines the interface for session creation, liveness checking, and resource cleanup. Subclasses must implement
    This class defines the async/thread-safe interface for managing cached Deephaven sessions,
    including liveness checking, session creation, and resource cleanup. Subclasses must implement
    all abstract methods and ensure correct resource management and cache semantics.
    """

    def __init__(self, session_type: SessionManagerType, source: str, name: str):
        """
        Initialize the session manager.

        Args:
            session_type (SessionManagerType): The type of session manager (community or enterprise).
            source (str): The source identifier for the session manager.
            name (str): The name of the session manager instance.
        """
        self._type = session_type
        self._source = source
        self._name = name
        self._session_cache: BaseSession | None = None
        self._lock = asyncio.Lock()

    @property
    def session_type(self) -> SessionManagerType:
        """
        Get the type of this session manager.

        Returns:
            SessionManagerType: The type of the session manager (community or enterprise).
        """
        return self._type

    @property
    def source(self) -> str:
        """
        Get the session source.

        Returns:
            str: The source identifier for this session manager.
        """
        return self._source

    @property
    def name(self) -> str:
        """
        Get the session name.

        Returns:
            str: The name of this session manager instance.
        """
        return self._name

    @property
    def full_name(self) -> str:
        """
        Get the full name of this session, including type, source, and name.

        Returns:
            str: The full session identifier in the format 'type:source:name'.
        """
        return f"{self._type.value}:{self._source}:{self._name}"

    @property
    @abstractmethod
    async def is_alive(self) -> bool:
        """
        Asynchronously check if the session is currently alive and usable.

        Implementations must check the underlying session state (e.g., connection open, authenticated, not expired).
        This property should never block for long periods and should be safe to call frequently. Any exceptions
        should be handled gracefully (e.g., by returning False).

        Returns:
            bool: True if the session is alive and usable, False otherwise.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get(self) -> BaseSession:
        """
        Asynchronously get or create an alive Deephaven session for this configuration.

        This method is async and thread-safe. If a cached session exists and is alive, it is returned.
        Otherwise, a new session is created, cached, and returned. The returned session is guaranteed
        to be alive and usable. Subclasses must propagate or wrap errors if session creation or liveness
        checks fail.

        Returns:
            BaseSession: An alive Deephaven Session instance.

        Raises:
            Exception: If session creation fails or the session cannot be made alive.
        """
        pass  # pragma: no cover

    async def close(self) -> None:
        """
        Asynchronously close and clean up the cached session if it exists.

        This method closes and cleans up the cached session resource (if any), removes it from the cache,
        and is safe to call multiple times (idempotent). If no session is cached, this is a no-op. Any exceptions
        during close are propagated to the caller.

        Raises:
            Exception: If closing the session fails for any reason.
        """
        async with self._lock:
            if self._session_cache is not None:
                await self._session_cache.close()
                self._session_cache = None

class CommunitySessionManager(BaseSessionManager):
    """
    Async/thread-safe session manager for Deephaven community environments.

    This class manages a single cached Deephaven community session, creating it on demand and reusing it if alive.
    Ensures thread safety, proper liveness checks, and error propagation for session operations.
    """

    @override
    def __init__(self, name: str, config: dict[str, Any]):
        """
        Initialize a community session manager.

        Args:
            name (str): Identifier for this session manager instance.
            config (dict[str, Any]): Session configuration dictionary.
        """
        super().__init__(SessionManagerType.COMMUNITY, "community", name)
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

    @override
    async def get(self) -> CoreSession:
        """
        Retrieve an alive Deephaven Session for this community worker.

        This method is async and thread-safe. If a cached session exists and is alive,
        it is returned. Otherwise, a new session is created and cached. If liveness
        cannot be determined or the cached session is dead, the cache is cleared and a
        new session is created.

        Returns:
            CoreSession: An alive Deephaven CoreSession instance for this worker.

        Raises:
            SessionCreationError: If the session could not be created or initialized.
            Exception: If an unexpected error occurs during liveness check or session creation.
        """
        async with self._lock:
            # Check if we have a cached session that's still alive
            if self._session_cache is not None:
                try:
                    if self._session_cache.is_alive:
                        _LOGGER.debug(
                            f"[CommunitySessionManager] Returning cached session for community worker: {self._name}"
                        )
                        return self._session_cache
                    else:
                        _LOGGER.info(
                            f"[CommunitySessionManager] Cached session for community worker '{self._name}' is not alive. Recreating."
                        )
                        self._session_cache = None
                except Exception as e:
                    _LOGGER.warning(
                        f"[CommunitySessionManager] Error checking session liveness for community worker '{self._name}': {e}. Recreating session."
                    )
                    self._session_cache = None

            # Create a new session
            _LOGGER.info(f"[CommunitySessionManager] Creating new session for community worker: {self._name}")
            session = await CoreSession.from_config(self._config)

            self._session_cache = session
            _LOGGER.info(
                f"[CommunitySessionManager] Session created and cached for community worker: {self._name}"
            )
            return session

class EnterpriseSessionManager(BaseSessionManager):
    """
    Async/thread-safe session manager for Deephaven enterprise environments.

    This class manages a single cached Deephaven enterprise session, creating it on demand and reusing it if alive.
    Ensures thread safety, proper liveness checks, and error propagation for session operations.
    """

    @override
    def __init__(self, source: str, name: str):
        """
        Initialize an enterprise session manager.

        Args:
            source (str): Source identifier for this session manager instance.
            name (str): Identifier for this session manager instance.
        """
        super().__init__(SessionManagerType.ENTERPRISE, source, name)

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

    @override
    async def get(self) -> CorePlusSession:
        """
        Retrieve an alive Deephaven CorePlusSession for this enterprise worker.

        This method is async and thread-safe. If a cached session exists and is alive,
        it is returned. Otherwise, a new session is created and cached. If liveness
        cannot be determined or the cached session is dead, the cache is cleared and a
        new session is created.

        Returns:
            CorePlusSession: An alive Deephaven CorePlusSession instance for this worker.

        Raises:
            SessionCreationError: If the session could not be created or initialized.
            Exception: If an unexpected error occurs during liveness check or session creation.
        """
        async with self._lock:
            # Check if we have a cached session that's still alive
            if self._session_cache is not None:
                try:
                    if self._session_cache.is_alive:
                        _LOGGER.debug(
                            f"[EnterpriseSessionManager] Returning cached session for enterprise worker: {self._name}"
                        )
                        return self._session_cache
                    else:
                        _LOGGER.info(
                            f"[EnterpriseSessionManager] Cached session for enterprise worker '{self._name}' is not alive. Recreating."
                        )
                        self._session_cache = None
                except Exception as e:
                    _LOGGER.warning(
                        f"[EnterpriseSessionManager] Error checking session liveness for enterprise worker '{self._name}': {e}. Recreating session."
                    )
                    self._session_cache = None

            # Create a new session
            _LOGGER.info(f"[EnterpriseSessionManager] Creating new session for enterprise worker: {self._name}")

            #TODO: Implement enterprise session creation
            # Note: Enterprise session creation would need to be implemented without config
            # and use source/name information from the session object
            # session = await create_session({"source": self._source, "name": self._name})
            session = None
            from deephaven_mcp._exceptions import InternalError
            raise InternalError("Enterprise session creation not implemented.")

            # self._session_cache = session
            # _LOGGER.info(
            #     f"[EnterpriseSessionManager] Session created and cached for enterprise worker: {self._name}"
            # )
            # return CorePlusSession(session)
