"""Registry for Deephaven Community sessions.

Provides :class:`CommunitySessionRegistry`, which loads its static sessions
from a pre-resolved mapping of :class:`~deephaven_mcp.sessions.CommunitySessionConfig`
instances supplied at construction time, and supports dynamic session
mutation (add / remove / count) via its
:class:`~deephaven_mcp.resource_manager._registry.MutableSessionRegistry` base.

Both static and dynamic managers are constructed inside the registry,
which is the single owner of :class:`~deephaven_mcp.client.CommunityClientTimeouts`
for the community side. Tool callers spawning dynamic sessions use
:meth:`CommunitySessionRegistry.add_dynamic_session` and never fetch the
timeouts themselves.
"""

import logging
from typing import override

from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.client import CommunityClientTimeouts
from deephaven_mcp.sessions import CommunitySessionConfig

from ._launcher import DockerLaunchedSession, PythonLaunchedSession
from ._manager import (
    DynamicCommunitySessionManager,
    StaticCommunitySessionManager,
)
from ._registry import MutableSessionRegistry
from ._session_id import QualifiedSessionId, SessionId

_LOGGER = logging.getLogger(__name__)


class CommunitySessionRegistry(MutableSessionRegistry):
    """Registry for community sessions — both static (from config) and dynamically created.

    Static sessions are loaded from the per-session configuration mapping
    passed to the constructor at startup. Dynamic sessions added later
    (via :meth:`add_dynamic_session`) are tracked separately so they
    count toward ``max_concurrent_sessions`` quotas.
    """

    def __init__(
        self,
        sessions: dict[str, CommunitySessionConfig],
        timeouts: CommunityClientTimeouts,
    ) -> None:
        """Capture the validated per-session configurations.

        Args:
            sessions (dict[str, CommunitySessionConfig]): Validated
                community session configurations keyed by session name
                (filename stem). The registry stores a shallow copy; the
                caller may discard its own reference once construction
                returns.
            timeouts (CommunityClientTimeouts): Community client-layer timeout
                configuration forwarded to every
                :class:`StaticCommunitySessionManager` and
                :class:`DynamicCommunitySessionManager` this registry
                constructs.
        """
        super().__init__()
        self._session_configs: dict[str, CommunitySessionConfig] = dict(sessions)
        self._timeouts = timeouts

    @override
    async def _load_items(self) -> None:
        """Materialize a :class:`StaticCommunitySessionManager` for every configured session."""
        _LOGGER.info(
            f"[{self.__class__.__name__}] Found "
            f"{len(self._session_configs)} community session "
            f"configurations to load."
        )
        for session_name, session_config in self._session_configs.items():
            _LOGGER.info(
                f"[{self.__class__.__name__}] Loading session configuration "
                f"for '{session_name}'..."
            )
            session_id = SessionId(session_name)
            mgr = StaticCommunitySessionManager(
                name=session_name,
                session_id=session_id,
                session_config=session_config,
                timeouts=self._timeouts,
            )
            self._items[mgr.qualified_session_id] = mgr

    async def add_dynamic_session(
        self,
        name: str,
        session_config: CommunitySessionConfig,
        launched_session: DockerLaunchedSession | PythonLaunchedSession,
    ) -> DynamicCommunitySessionManager:
        """Construct and register a :class:`DynamicCommunitySessionManager`.

        The registry owns :class:`CommunityClientTimeouts` and applies its own
        instance when constructing the manager, so tool-layer callers do
        not fetch them out-of-band. This is the only sanctioned path for
        adding a runtime-launched community session.

        Args:
            name (str): Simple session name (used to build the
                ``community:community:{name}`` full name).
            session_config (CommunitySessionConfig): Validated session
                declaration describing how to connect to
                ``launched_session``.
            launched_session (DockerLaunchedSession | PythonLaunchedSession):
                The already-started worker the manager will wrap.

        Returns:
            DynamicCommunitySessionManager: The newly-constructed
                manager after it has been registered. Callers typically
                read ``.qualified_session_id`` for downstream identification.

        Raises:
            ValueError: If a session with the same display ``name``
                already exists in the registry. The check and the
                registration both happen under a single lock so two
                concurrent calls with the same name cannot both succeed.
        """
        async with self._lock:
            # The community SessionId is just the name itself, so the
            # full identifier ``session_id`` is deterministic from
            # ``name``. A direct ``in self._items`` check is the
            # duplicate guard.
            session_id = QualifiedSessionId(
                SystemType.COMMUNITY, SystemType.COMMUNITY.value, SessionId(name)
            )
            if session_id in self._items:
                raise ValueError(f"A community session named {name!r} already exists")
            manager = DynamicCommunitySessionManager(
                session_id=SessionId(name),
                name=name,
                session_config=session_config,
                launched_session=launched_session,
                timeouts=self._timeouts,
            )
            self._add_session_locked(manager)
            return manager
