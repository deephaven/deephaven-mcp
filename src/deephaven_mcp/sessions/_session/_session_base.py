"""
Base classes and enums for Deephaven MCP session management.

Defines the abstract interface for all session types, including async/thread-safety, caching,
and liveness contract. Subclasses must implement the required interface for concrete session types.
"""

import asyncio
import enum
import logging
from abc import ABC, abstractmethod

from pydeephaven import Session

from deephaven_mcp.sessions._lifecycle.shared import close_session_safely

_LOGGER = logging.getLogger(__name__)


class SessionType(enum.Enum):
    """Enum for different types of Deephaven sessions."""

    COMMUNITY = "community"
    ENTERPRISE = "enterprise"


class SessionBase(ABC):
    """
    Abstract base class for all Deephaven session types.

    Defines the async/thread-safe interface for managing cached Deephaven sessions, including
    liveness checking, creation, and cleanup. Subclasses must implement all abstract methods and
    ensure correct resource management and cache semantics.
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
    async def is_alive(self) -> bool:
        """
        Asynchronously check if the session is currently alive and usable.

        Implementations must check the underlying session state (e.g., connection open, authenticated, not expired).
        This property should never block for long periods and should be safe to call frequently. Any exceptions
        should be handled gracefully (e.g., by returning False).

        Returns:
            bool: True if the session is alive/usable, False otherwise.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_session(self) -> Session:
        """
        Asynchronously get or create an alive Deephaven session for this configuration.

        This method is async and thread-safe. If a cached session exists and is alive, it is returned.
        Otherwise, a new session is created, cached, and returned. The returned session is guaranteed
        to be alive/usable. Subclasses must propagate or wrap errors if session creation or liveness
        checks fail.

        Returns:
            Session: An alive Deephaven Session instance.

        Raises:
            Exception: If session creation fails or the session cannot be made alive.
        """
        pass  # pragma: no cover

    async def close_session(self) -> None:
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
                await close_session_safely(self._session_cache, self._name)
                self._session_cache = None
