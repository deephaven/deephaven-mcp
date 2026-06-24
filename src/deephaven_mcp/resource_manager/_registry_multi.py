"""Composite registry that fans out to community + enterprise child registries.

The multiplexed MCP server hosts at most one
:class:`~deephaven_mcp.resource_manager.CommunitySessionRegistry`
(for the umbrella ``community`` system) plus one
:class:`~deephaven_mcp.resource_manager.EnterpriseSessionRegistry` per
configured enterprise system. :class:`MultiSystemRegistry` owns those
children and exposes a small router surface for the operations that
must span both sections (string-id lookup and merged snapshot);
section-specific operations stay on the children themselves.

Routing rules
-------------

A fully qualified session identifier has the form ``"<type>:<system>:<name>"``
(see :class:`~deephaven_mcp.resource_manager.QualifiedSessionId`):

- ``community:community:<name>`` -> community child registry.
- ``enterprise:<system_name>:<name>`` -> enterprise child registry whose
  ``system_name`` equals the ``<system>`` segment.

Any other shape raises :class:`InvalidSessionNameError`.

Lifecycle
---------

:meth:`initialize` calls each child's :meth:`initialize` concurrently;
:meth:`close` does the same. Children are constructed eagerly from
already-validated per-section ingredients (community sessions +
client timeouts; enterprise systems + client timeouts), so a
configuration directory with no community sessions and no enterprise
systems is rejected by the config layer before this class ever sees it.
"""

from __future__ import annotations

import asyncio
import logging

from deephaven_mcp._exceptions import InternalError, InvalidSessionNameError
from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.client._timeouts import (
    CommunityClientTimeouts,
    EnterpriseClientTimeouts,
)
from deephaven_mcp.sessions import CommunitySessionConfig, EnterpriseSystemConfig

from ._manager import SessionManager
from ._registry import (
    InitializationPhase,
    MutableSessionRegistry,
    RegistrySnapshot,
)
from ._registry_community import CommunitySessionRegistry
from ._registry_enterprise import EnterpriseSessionRegistry
from ._session_id import QualifiedSessionId

__all__ = ["MultiSystemRegistry", "least_advanced_phase"]

_LOGGER = logging.getLogger(__name__)


class MultiSystemRegistry:
    """Router over one community + many enterprise child registries.

    Owns one optional :class:`CommunitySessionRegistry` and one
    :class:`EnterpriseSessionRegistry` per configured enterprise system.
    Exposes only the cross-system operations a caller cannot do with a
    typed reference to a single child:

    - :meth:`initialize` and :meth:`close` fan out across every child
      so the lifespan can manage them as a unit.
    - :meth:`get` parses a fully qualified session identifier
      (``"<type>:<system>:<name>"``) and dispatches to the owning child
      — the only routing surface that needs to live here, because
      string-keyed lookup spans both sections.
    - :meth:`get_all` produces a single merged snapshot across every
      child, with the per-section initialization phase folded into one.
    - :attr:`community` and :attr:`enterprise_systems` expose the
      children directly so per-section tools can call them without
      going through the router.

    Section-specific operations (``add_session``, ``remove``,
    ``count_added_sessions``) deliberately do **not** live here.
    Callers already hold a typed reference to the child they want
    (via :attr:`community` or :attr:`enterprise_systems[name]`) and
    call those methods on the child directly — the router has no
    routing decision to make.

    This class does not extend :class:`MutableSessionRegistry`: it
    owns no ``_items`` storage and no ``_added_session_ids`` tracking
    set.  All managed sessions live in the children.
    """

    def __init__(
        self,
        *,
        community_sessions: dict[str, CommunitySessionConfig] | None,
        community_client_timeouts: CommunityClientTimeouts | None,
        enterprise_systems: dict[str, EnterpriseSystemConfig] | None,
        enterprise_client_timeouts: EnterpriseClientTimeouts | None,
    ) -> None:
        """Create child registries from per-section ingredients.

        The community pair and the enterprise pair are each "all or
        nothing": either both members of a pair are non-``None`` (the
        section is configured) or both are ``None`` (the section is
        absent). Mixing them is an :class:`InternalError`.

        Args:
            community_sessions (dict[str, CommunitySessionConfig] | None):
                Validated static community session configs keyed by
                session name. Pass ``None`` when no community section
                is configured.
            community_client_timeouts (CommunityClientTimeouts | None):
                Client-layer timeouts to apply to every community
                session. Pass ``None`` iff ``community_sessions`` is
                also ``None``.
            enterprise_systems (dict[str, EnterpriseSystemConfig] | None):
                Validated enterprise system configs keyed by system
                name. Pass ``None`` when no enterprise section is
                configured.
            enterprise_client_timeouts (EnterpriseClientTimeouts | None):
                Client-layer timeouts shared by every enterprise
                system. Pass ``None`` iff ``enterprise_systems`` is
                also ``None``.

        Raises:
            InternalError: If exactly one of a pair is ``None`` (the
                pair must be passed together), or if neither section
                is configured at all.
        """
        if (community_sessions is None) != (community_client_timeouts is None):
            raise InternalError(
                "MultiSystemRegistry: community_sessions and "
                "community_client_timeouts must both be provided or both be None."
            )
        if (enterprise_systems is None) != (enterprise_client_timeouts is None):
            raise InternalError(
                "MultiSystemRegistry: enterprise_systems and "
                "enterprise_client_timeouts must both be provided or both be None."
            )
        if community_sessions is None and enterprise_systems is None:
            raise InternalError(
                "MultiSystemRegistry: at least one of the community or "
                "enterprise sections must be configured; the config layer "
                "should have rejected an empty configuration."
            )

        # The paired-None invariants checked above let the combined
        # ``and`` conditions below be exhaustive for mypy narrowing.
        self._community: CommunitySessionRegistry | None = None
        if community_sessions is not None and community_client_timeouts is not None:
            self._community = CommunitySessionRegistry(
                community_sessions,
                community_client_timeouts,
            )
        self._enterprise: dict[str, EnterpriseSessionRegistry] = {}
        if enterprise_systems is not None and enterprise_client_timeouts is not None:
            for name, system_cfg in enterprise_systems.items():
                self._enterprise[name] = EnterpriseSessionRegistry(
                    system_cfg, enterprise_client_timeouts
                )
        self._initialized = False
        _LOGGER.info(
            f"[MultiSystemRegistry] created with "
            f"community={'yes' if self._community is not None else 'no'}, "
            f"enterprise={list(self._enterprise.keys())}"
        )

    # ------------------------------------------------------------------
    # Initialization state
    # ------------------------------------------------------------------

    def _check_initialized(self) -> None:
        """Raise :class:`InternalError` when the registry has not been initialized.

        Raises:
            InternalError: If :meth:`initialize` has not yet been awaited.
        """
        if not self._initialized:
            raise InternalError(
                "MultiSystemRegistry not initialized. Call 'await initialize()' "
                "after construction."
            )

    # ------------------------------------------------------------------
    # Read-only accessors for the multiplexed server tools
    # ------------------------------------------------------------------

    @property
    def community(self) -> CommunitySessionRegistry | None:
        """Return the community child registry, or ``None`` if not configured."""
        return self._community

    @property
    def enterprise_systems(self) -> dict[str, EnterpriseSessionRegistry]:
        """Return a snapshot of enterprise child registries keyed by system name.

        Returns:
            dict[str, EnterpriseSessionRegistry]: A shallow copy of the
                internal mapping; safe for callers to iterate without
                holding any lock. Modifying the returned dict does not
                affect the registry.
        """
        return dict(self._enterprise)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Initialize every child registry concurrently.

        Idempotent — subsequent calls return immediately if already initialized.
        The constructor guarantees at least one child is configured, so the
        gather always has work to do.

        All children's :meth:`initialize` are awaited via
        ``asyncio.gather(..., return_exceptions=True)`` so a failure in one
        child does not cancel the others. After every child has been
        awaited, if any failed, every child that *did* succeed is closed
        (best-effort) before a single :class:`InternalError` is raised
        with one line per failing child (``ClassName: error``). The
        registry is left in the uninitialized state in that case, with
        no surviving child resources or background tasks (e.g. an
        :class:`EnterpriseSessionRegistry` whose ``_load_items`` already
        spawned a discovery task) leaking past the failure.

        Raises:
            InternalError: If one or more children raised an exception during
                :meth:`initialize`. The message lists every failing child's
                class name and error.
        """
        if self._initialized:
            return
        children = self._iter_children()
        results = await asyncio.gather(
            *(child.initialize() for child in children),
            return_exceptions=True,
        )
        failures: list[tuple[type, BaseException]] = [
            (type(child), result)
            for child, result in zip(children, results, strict=True)
            if isinstance(result, BaseException)
        ]
        if failures:
            # Roll back: close any children that initialized successfully
            # so partial-init never leaves background tasks or network
            # resources alive. ``close()`` on the multi-registry itself
            # short-circuits while ``_initialized`` is False, so the
            # lifespan's outer cleanup cannot do this for us.
            succeeded = [
                child
                for child, result in zip(children, results, strict=True)
                if not isinstance(result, BaseException)
            ]
            if succeeded:
                noun = "registry" if len(succeeded) == 1 else "registries"
                _LOGGER.error(
                    f"[MultiSystemRegistry] partial-init failure; rolling back "
                    f"{len(succeeded)} already-initialized child {noun}"
                )
                close_results = await asyncio.gather(
                    *(child.close() for child in succeeded),
                    return_exceptions=True,
                )
                for child, close_result in zip(succeeded, close_results, strict=True):
                    if isinstance(close_result, BaseException):
                        _LOGGER.error(
                            f"[MultiSystemRegistry] error closing "
                            f"{child.__class__.__name__} during partial-init "
                            f"rollback: {close_result!r}",
                            exc_info=close_result,
                        )
            details = "; ".join(
                f"{child_cls.__name__}: {err!r}" for child_cls, err in failures
            )
            label = "registries" if len(failures) != 1 else "registry"
            raise InternalError(
                f"MultiSystemRegistry: {len(failures)} child {label} "
                f"failed to initialize: {details}"
            )
        self._initialized = True

    async def close(self) -> None:
        """Close every child registry concurrently.

        Errors raised by individual children are logged but do not abort
        the close of the remaining children.

        Raises:
            InternalError: If this registry has not been initialized.
        """
        self._check_initialized()
        self._initialized = False

        children = self._iter_children()
        results = await asyncio.gather(
            *(child.close() for child in children),
            return_exceptions=True,
        )
        for child, result in zip(children, results, strict=True):
            if isinstance(result, BaseException):
                _LOGGER.error(
                    f"[MultiSystemRegistry] error closing "
                    f"{child.__class__.__name__}: {result}"
                )
        _LOGGER.info("[MultiSystemRegistry] closed all child registries")

    # ------------------------------------------------------------------
    # Cross-system reads
    # ------------------------------------------------------------------

    async def get(self, name: QualifiedSessionId) -> SessionManager:
        """Route a session lookup to the child that owns ``name``.

        Args:
            name (str): Fully qualified session identifier in
                ``"<type>:<source>:<sub_name>"`` form.

        Returns:
            SessionManager: The session manager registered under
                ``name`` in the routed child.

        Raises:
            InternalError: If this registry has not been initialized.
            InvalidSessionNameError: If ``name`` is not a valid session
                identifier or its ``<type>`` / ``<source>`` does not
                correspond to any configured child.
            RegistryItemNotFoundError: If the routed child does not have
                ``name``.
        """
        self._check_initialized()
        return await self._route(name).get(name)

    async def get_all(self) -> RegistrySnapshot[SessionManager]:
        """Return a merged snapshot across every child registry.

        Returns:
            RegistrySnapshot[SessionManager]: Items from every child
                merged into one mapping. The merged
                ``initialization_phase`` is the *least advanced* phase
                across children (e.g. one child still in ``LOADING``
                yields ``LOADING`` overall). Per-source errors are
                aggregated into ``initialization_errors`` keyed by the
                child registry name (``"community"`` or the enterprise
                ``system_name``).

        Raises:
            InternalError: If this registry has not been initialized.
        """
        self._check_initialized()
        merged_items: dict[QualifiedSessionId, SessionManager] = {}
        merged_errors: dict[str, str] = {}
        phases: list[InitializationPhase] = []

        if self._community is not None:
            snap = await self._community.get_all()
            merged_items.update(snap.items)
            phases.append(snap.initialization_phase)
            for source, err in snap.initialization_errors.items():
                merged_errors[f"community:{source}"] = err

        for system_name, registry in self._enterprise.items():
            snap = await registry.get_all()
            merged_items.update(snap.items)
            phases.append(snap.initialization_phase)
            for source, err in snap.initialization_errors.items():
                merged_errors[f"enterprise:{system_name}:{source}"] = err

        return RegistrySnapshot.with_initialization(
            items=merged_items,
            phase=least_advanced_phase(phases),
            errors=merged_errors,
        )

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def _iter_children(self) -> list[MutableSessionRegistry]:
        """Return all configured child registries in startup order.

        Returns:
            list[MutableSessionRegistry]: Community first (when
                configured), followed by enterprise children in
                declaration order.
        """
        children: list[MutableSessionRegistry] = []
        if self._community is not None:
            children.append(self._community)
        children.extend(self._enterprise.values())
        return children

    def _route(
        self, qualified_session_id: QualifiedSessionId
    ) -> MutableSessionRegistry:
        """Return the child registry that owns ``qualified_session_id``.

        Args:
            qualified_session_id (QualifiedSessionId): An already-parsed,
                fully qualified session identifier.

        Returns:
            MutableSessionRegistry: The community or enterprise child
                that ``qualified_session_id`` routes to.

        Raises:
            InvalidSessionNameError: If no community child is configured
                (for community ids), or no enterprise child has the named
                system (for enterprise ids).
            InternalError: If ``qualified_session_id.system_type`` is a
                :class:`SystemType` member this dispatch does not handle.
        """
        system = qualified_session_id.system_name

        match qualified_session_id.system_type:
            case SystemType.COMMUNITY:
                if self._community is None:
                    raise InvalidSessionNameError(
                        f"Session id {qualified_session_id!r} requests the community system, "
                        "but no community sessions are configured."
                    )
                return self._community
            case SystemType.ENTERPRISE:
                registry = self._enterprise.get(system)
                if registry is None:
                    known = sorted(self._enterprise.keys())
                    raise InvalidSessionNameError(
                        f"Session id {qualified_session_id!r} names enterprise system "
                        f"{system!r}, which is not configured. Known enterprise "
                        f"systems: {known}."
                    )
                return registry
            case _ as unexpected:
                # Statically unreachable: ``SystemType(...)`` above
                # narrowed to a known member. mypy flags any future
                # :class:`SystemType` member added without updating
                # this dispatch; the runtime :class:`InternalError`
                # is the safety net for that in-project drift.
                raise InternalError(
                    f"MultiSystemRegistry._route reached an unhandled "
                    f"SystemType {unexpected!r} for qualified_session_id "
                    f"{qualified_session_id!r}; the match dispatch is out of sync "
                    f"with :class:`SystemType`."
                )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_PHASE_ORDER: dict[InitializationPhase, int] = {
    InitializationPhase.FAILED: 0,
    InitializationPhase.NOT_STARTED: 1,
    InitializationPhase.PARTIAL: 2,
    InitializationPhase.LOADING: 3,
    InitializationPhase.COMPLETED: 4,
}
"""Ordering used by :func:`least_advanced_phase` to fold phases.

Lower-ordered phases are considered "less advanced". ``FAILED`` ranks
below every other phase so a single failed child always surfaces in
the merged snapshot, even when sibling registries are still loading
or have already completed successfully.
"""


def least_advanced_phase(
    phases: list[InitializationPhase],
) -> InitializationPhase:
    """Return the most pessimistic phase across child registries.

    Args:
        phases (list[InitializationPhase]): One phase per child.

    Returns:
        InitializationPhase: The phase with the lowest order in
            :data:`_PHASE_ORDER`. When ``phases`` is empty (no children)
            returns :attr:`InitializationPhase.COMPLETED` so that
            ``get_all`` on a configurationally empty registry does not
            falsely advertise pending work.
    """
    if not phases:
        return InitializationPhase.COMPLETED
    return min(phases, key=lambda p: _PHASE_ORDER[p])
