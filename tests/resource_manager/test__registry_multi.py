"""Unit tests for ``deephaven_mcp.resource_manager._registry_multi``.

The :class:`MultiSystemRegistry` is a thin composite over one community
child registry and one enterprise child registry per configured system.
The tests in this module exercise the routing and lifecycle logic in
isolation by injecting fake child registries via :mod:`unittest.mock`,
so neither the real config layer nor the real Core+ stack are required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import InternalError, InvalidSessionNameError
from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.config.schema import (
    CommunityTimeouts,
    EnterpriseTimeouts,
)
from deephaven_mcp.resource_manager import QualifiedSessionId
from deephaven_mcp.resource_manager._registry import (
    InitializationPhase,
    MutableSessionRegistry,
    RegistrySnapshot,
)
from deephaven_mcp.resource_manager._registry_multi import (
    MultiSystemRegistry,
    least_advanced_phase,
)


def _qsid(s: str) -> QualifiedSessionId:
    return QualifiedSessionId.from_str(s)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_child(name: str) -> MagicMock:
    """Return a fake child registry that mimics the surface used here."""
    child = MagicMock(spec=MutableSessionRegistry)
    child.__class__ = MutableSessionRegistry  # for name in error log
    child.initialize = AsyncMock()
    child.close = AsyncMock()
    child.get = AsyncMock()
    child.get_all = AsyncMock()
    child.add_session = AsyncMock()
    child.remove = AsyncMock()
    # Mark the mock with the system name for assertion convenience.
    child._fake_name = name
    return child


def _build_registry(
    *,
    community_child: MagicMock | None,
    enterprise_children: dict[str, MagicMock] | None,
) -> MultiSystemRegistry:
    """Construct a :class:`MultiSystemRegistry` with patched child classes."""
    enterprise_children = enterprise_children or {}

    # Patch the class symbols inside ``_registry_multi`` so the
    # constructor builds our mocks instead of the real children.
    def _community_factory(_sessions: dict[str, Any], timeouts: Any) -> MagicMock:
        if community_child is None:  # pragma: no cover - defensive
            raise AssertionError(
                "Community factory invoked but no community_child provided"
            )
        return community_child

    def _enterprise_factory(system_cfg: Any, timeouts: Any) -> MagicMock:
        return enterprise_children[system_cfg.name]

    enterprise_cfgs: dict[str, Any] = {}
    for name in enterprise_children:
        cfg = MagicMock()
        cfg.name = name
        enterprise_cfgs[name] = cfg

    if community_child is not None:
        community_sessions: dict[str, Any] | None = {"placeholder": MagicMock()}
        community_client_timeouts: Any | None = CommunityTimeouts().client
    else:
        community_sessions = None
        community_client_timeouts = None
    if enterprise_cfgs:
        enterprise_systems: dict[str, Any] | None = enterprise_cfgs
        enterprise_client_timeouts: Any | None = EnterpriseTimeouts().client
    else:
        enterprise_systems = None
        enterprise_client_timeouts = None

    with (
        patch(
            "deephaven_mcp.resource_manager._registry_multi.CommunitySessionRegistry",
            side_effect=_community_factory,
        ),
        patch(
            "deephaven_mcp.resource_manager._registry_multi.EnterpriseSessionRegistry",
            side_effect=_enterprise_factory,
        ),
    ):
        return MultiSystemRegistry(
            community_sessions=community_sessions,
            community_client_timeouts=community_client_timeouts,
            enterprise_systems=enterprise_systems,
            enterprise_client_timeouts=enterprise_client_timeouts,
        )


# ---------------------------------------------------------------------------
# Construction / accessors
# ---------------------------------------------------------------------------


def test_construct_rejects_partial_community_pair() -> None:
    """Passing community_sessions without timeouts (or vice versa) is an InternalError."""
    with pytest.raises(InternalError, match="community_sessions"):
        MultiSystemRegistry(
            community_sessions={"x": MagicMock()},
            community_client_timeouts=None,
            enterprise_systems=None,
            enterprise_client_timeouts=None,
        )
    with pytest.raises(InternalError, match="community_sessions"):
        MultiSystemRegistry(
            community_sessions=None,
            community_client_timeouts=MagicMock(),
            enterprise_systems=None,
            enterprise_client_timeouts=None,
        )


def test_construct_rejects_partial_enterprise_pair() -> None:
    """Passing enterprise_systems without timeouts (or vice versa) is an InternalError."""
    with pytest.raises(InternalError, match="enterprise_systems"):
        MultiSystemRegistry(
            community_sessions=None,
            community_client_timeouts=None,
            enterprise_systems={"x": MagicMock()},
            enterprise_client_timeouts=None,
        )
    with pytest.raises(InternalError, match="enterprise_systems"):
        MultiSystemRegistry(
            community_sessions=None,
            community_client_timeouts=None,
            enterprise_systems=None,
            enterprise_client_timeouts=MagicMock(),
        )


def test_construct_rejects_completely_empty_configuration() -> None:
    """At least one section (community or enterprise) must be configured."""
    with pytest.raises(InternalError, match="at least one"):
        MultiSystemRegistry(
            community_sessions=None,
            community_client_timeouts=None,
            enterprise_systems=None,
            enterprise_client_timeouts=None,
        )


def test_construct_with_community_only_records_no_enterprise() -> None:
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    assert registry.community is community
    assert registry.enterprise_systems == {}


def test_construct_with_enterprise_only() -> None:
    prod = _make_child("prod")
    staging = _make_child("staging")
    registry = _build_registry(
        community_child=None,
        enterprise_children={"prod": prod, "staging": staging},
    )
    assert registry.community is None
    assert registry.enterprise_systems == {"prod": prod, "staging": staging}


def test_construct_with_community_and_enterprise() -> None:
    community = _make_child("community")
    prod = _make_child("prod")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod},
    )
    assert registry.community is community
    assert registry.enterprise_systems == {"prod": prod}
    # ``enterprise_systems`` returns a *copy*; mutations are isolated.
    snapshot = registry.enterprise_systems
    snapshot["bogus"] = MagicMock()
    assert "bogus" not in registry.enterprise_systems


# ---------------------------------------------------------------------------
# initialize / _load_items
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initialize_calls_every_child() -> None:
    community = _make_child("community")
    prod = _make_child("prod")
    staging = _make_child("staging")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod, "staging": staging},
    )

    await registry.initialize()

    community.initialize.assert_awaited_once()
    prod.initialize.assert_awaited_once()
    staging.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_initialize_is_idempotent() -> None:
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)

    await registry.initialize()
    await registry.initialize()

    community.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_initialize_awaits_every_child_even_when_one_fails() -> None:
    """A failure in one child must not cancel sibling initializations.

    With ``return_exceptions=True`` on the underlying gather, every child's
    ``initialize`` is awaited before the aggregated failure is raised.
    """
    community = _make_child("community")
    prod = _make_child("prod")
    staging = _make_child("staging")
    prod.initialize.side_effect = RuntimeError("prod boom")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod, "staging": staging},
    )

    with pytest.raises(InternalError) as exc_info:
        await registry.initialize()

    # Every child was awaited even though prod raised.
    community.initialize.assert_awaited_once()
    prod.initialize.assert_awaited_once()
    staging.initialize.assert_awaited_once()
    # Aggregated error names the failing child class and its error.
    assert "prod boom" in str(exc_info.value)
    assert "1 child" in str(exc_info.value)
    # Registry stays uninitialized so a retry is possible.
    assert registry._initialized is False


@pytest.mark.asyncio
async def test_initialize_aggregates_multiple_child_failures() -> None:
    """Multiple child failures are aggregated into a single error message."""
    prod = _make_child("prod")
    staging = _make_child("staging")
    prod.initialize.side_effect = RuntimeError("prod boom")
    staging.initialize.side_effect = ValueError("staging boom")
    registry = _build_registry(
        community_child=None,
        enterprise_children={"prod": prod, "staging": staging},
    )

    with pytest.raises(InternalError) as exc_info:
        await registry.initialize()

    message = str(exc_info.value)
    assert "2 child registries" in message
    assert "prod boom" in message
    assert "staging boom" in message


@pytest.mark.asyncio
async def test_initialize_closes_successful_children_on_partial_failure() -> None:
    """Partial-init failure must close children that already succeeded.

    Otherwise a successfully initialized child (e.g. an
    ``EnterpriseSessionRegistry`` whose ``_load_items`` already spawned a
    discovery task) would leak resources past the raised ``InternalError``,
    because the lifespan's outer ``registry.close()`` short-circuits while
    ``_initialized`` is still False.
    """
    community = _make_child("community")
    prod = _make_child("prod")
    staging = _make_child("staging")
    staging.initialize.side_effect = RuntimeError("staging boom")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod, "staging": staging},
    )

    with pytest.raises(InternalError):
        await registry.initialize()

    # The two children that initialized successfully were closed as
    # part of the rollback; the failed one was not (it never got
    # past initialize).
    community.close.assert_awaited_once()
    prod.close.assert_awaited_once()
    staging.close.assert_not_awaited()
    assert registry._initialized is False


@pytest.mark.asyncio
async def test_initialize_rollback_errors_are_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Errors raised by rollback ``close()`` calls must not mask the original failure."""
    import logging

    community = _make_child("community")
    community.close.side_effect = RuntimeError("close boom")
    prod = _make_child("prod")
    prod.initialize.side_effect = RuntimeError("prod boom")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod},
    )

    with caplog.at_level(logging.ERROR), pytest.raises(InternalError) as exc_info:
        await registry.initialize()

    # The original failure surfaces, not the rollback failure.
    assert "prod boom" in str(exc_info.value)
    assert "close boom" not in str(exc_info.value)
    # The rollback error is logged for operators.
    assert any("close boom" in record.getMessage() for record in caplog.records)
    community.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_initialize_no_rollback_when_no_children_succeeded() -> None:
    """When every child fails to initialize there is nothing to roll back."""
    prod = _make_child("prod")
    staging = _make_child("staging")
    prod.initialize.side_effect = RuntimeError("prod boom")
    staging.initialize.side_effect = RuntimeError("staging boom")
    registry = _build_registry(
        community_child=None,
        enterprise_children={"prod": prod, "staging": staging},
    )

    with pytest.raises(InternalError):
        await registry.initialize()

    prod.close.assert_not_awaited()
    staging.close.assert_not_awaited()


def test_multi_system_registry_is_not_a_mutable_session_registry() -> None:
    """:class:`MultiSystemRegistry` no longer inherits ``MutableSessionRegistry``.

    Sessions live in the child registries; the composite owns no
    ``_items`` storage. This test pins that change so future authors
    do not accidentally reintroduce the inheritance.
    """
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    assert not isinstance(registry, MutableSessionRegistry)
    # The composite must not maintain its own session storage.
    assert not hasattr(registry, "_items")
    assert not hasattr(registry, "_added_session_ids")


# ---------------------------------------------------------------------------
# get / add_session / remove routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_routes_community() -> None:
    community = _make_child("community")
    sentinel = MagicMock(name="manager")
    community.get.return_value = sentinel
    registry = _build_registry(community_child=community, enterprise_children=None)
    await registry.initialize()

    result = await registry.get(_qsid("community:community:42"))

    community.get.assert_awaited_once_with(_qsid("community:community:42"))
    assert result is sentinel


@pytest.mark.asyncio
async def test_get_routes_enterprise_by_system_name() -> None:
    prod = _make_child("prod")
    staging = _make_child("staging")
    sentinel = MagicMock(name="manager")
    staging.get.return_value = sentinel
    registry = _build_registry(
        community_child=None,
        enterprise_children={"prod": prod, "staging": staging},
    )
    await registry.initialize()

    result = await registry.get(_qsid("enterprise:staging:1"))

    staging.get.assert_awaited_once_with(_qsid("enterprise:staging:1"))
    prod.get.assert_not_called()
    assert result is sentinel


@pytest.mark.asyncio
async def test_get_before_initialize_raises_internal_error() -> None:
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    with pytest.raises(InternalError):
        await registry.get(_qsid("community:community:42"))


@pytest.mark.asyncio
async def test_get_for_unconfigured_community_raises() -> None:
    prod = _make_child("prod")
    registry = _build_registry(
        community_child=None,
        enterprise_children={"prod": prod},
    )
    await registry.initialize()
    with pytest.raises(InvalidSessionNameError) as exc_info:
        await registry.get(_qsid("community:community:42"))
    assert "community system" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_for_unknown_enterprise_system_raises() -> None:
    prod = _make_child("prod")
    registry = _build_registry(
        community_child=None,
        enterprise_children={"prod": prod},
    )
    await registry.initialize()
    with pytest.raises(InvalidSessionNameError) as exc_info:
        await registry.get(_qsid("enterprise:staging:1"))
    assert "'staging'" in str(exc_info.value)
    assert "['prod']" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_with_unsupported_type_raises() -> None:
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    await registry.initialize()
    with pytest.raises(InvalidSessionNameError) as exc_info:
        await registry.get(_qsid("docs:central:1"))
    # QualifiedSessionId.from_str validates the system_type segment up front,
    # so the error names the offending segment and the allowed values.
    msg = str(exc_info.value)
    assert "docs" in msg
    assert "system_type" in msg


@pytest.mark.asyncio
async def test_route_unhandled_system_type_raises_internal_error() -> None:
    """``_route``'s defensive ``case _`` branch raises ``InternalError``.

    Required by ``feedback_no_asserts_in_production``: every defensive
    raise in production code must have a unit test that triggers it. A
    real :class:`QualifiedSessionId` cannot carry an unknown
    ``system_type`` (its constructors validate the enum), so we
    hand-build a valid instance and overwrite its slot to exercise the
    branch.
    """
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    await registry.initialize()

    qsid = _qsid("community:community:1")
    object.__setattr__(qsid, "system_type", "unhandled-future-type")

    with pytest.raises(InternalError, match="unhandled"):
        await registry.get(qsid)


# ---------------------------------------------------------------------------
# get_all merging
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_merges_items_and_namespaces_errors() -> None:
    community = _make_child("community")
    prod = _make_child("prod")
    staging = _make_child("staging")

    community_mgr = MagicMock(name="community-mgr")
    prod_mgr = MagicMock(name="prod-mgr")

    community.get_all.return_value = RegistrySnapshot.with_initialization(
        items={"community:community:7": community_mgr},
        phase=InitializationPhase.COMPLETED,
        errors={"config": "community-error"},
    )
    prod.get_all.return_value = RegistrySnapshot.with_initialization(
        items={"enterprise:prod:11": prod_mgr},
        phase=InitializationPhase.LOADING,
        errors={"prod": "prod-error"},
    )
    staging.get_all.return_value = RegistrySnapshot.with_initialization(
        items={},
        phase=InitializationPhase.FAILED,
        errors={},
    )

    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod, "staging": staging},
    )
    await registry.initialize()

    snapshot = await registry.get_all()

    assert snapshot.items == {
        "community:community:7": community_mgr,
        "enterprise:prod:11": prod_mgr,
    }
    # FAILED has the lowest configured order so a single failed child wins
    # over any sibling that is still loading or already completed.
    assert snapshot.initialization_phase is InitializationPhase.FAILED
    assert snapshot.initialization_errors == {
        "community:config": "community-error",
        "enterprise:prod:prod": "prod-error",
    }


@pytest.mark.asyncio
async def test_get_all_before_initialize_raises() -> None:
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    with pytest.raises(InternalError):
        await registry.get_all()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_invokes_every_child_concurrently() -> None:
    community = _make_child("community")
    prod = _make_child("prod")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod},
    )
    await registry.initialize()
    await registry.close()
    community.close.assert_awaited_once()
    prod.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_swallows_child_errors_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    community = _make_child("community")
    prod = _make_child("prod")
    prod.close.side_effect = RuntimeError("kaboom")
    registry = _build_registry(
        community_child=community,
        enterprise_children={"prod": prod},
    )
    await registry.initialize()

    with caplog.at_level(logging.ERROR):
        await registry.close()

    community.close.assert_awaited_once()
    prod.close.assert_awaited_once()
    assert any("kaboom" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_close_before_initialize_raises() -> None:
    community = _make_child("community")
    registry = _build_registry(community_child=community, enterprise_children=None)
    with pytest.raises(InternalError):
        await registry.close()


# ---------------------------------------------------------------------------
# least_advanced_phase helper
# ---------------------------------------------------------------------------


def test_least_advanced_phase_picks_minimum_order() -> None:
    assert (
        least_advanced_phase(
            [
                InitializationPhase.COMPLETED,
                InitializationPhase.PARTIAL,
                InitializationPhase.LOADING,
            ]
        )
        is InitializationPhase.PARTIAL
    )


def test_least_advanced_phase_failed_outranks_completed() -> None:
    assert (
        least_advanced_phase(
            [InitializationPhase.FAILED, InitializationPhase.COMPLETED]
        )
        is InitializationPhase.FAILED
    )


def test_least_advanced_phase_failed_outranks_loading() -> None:
    """FAILED beats LOADING so a single failure is never masked by an in-flight peer."""
    assert (
        least_advanced_phase([InitializationPhase.LOADING, InitializationPhase.FAILED])
        is InitializationPhase.FAILED
    )


def test_least_advanced_phase_empty_returns_completed() -> None:
    assert least_advanced_phase([]) is InitializationPhase.COMPLETED
