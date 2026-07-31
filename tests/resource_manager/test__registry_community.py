"""Tests for CommunitySessionRegistry in the resource manager module.

The community registry now takes a pre-resolved mapping of
:class:`~deephaven_mcp.config.CommunitySessionConfig` instances at
construction time (no config manager, no top-level config dict).
"""

from unittest.mock import AsyncMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp._exceptions import (
    InternalError,
    RegistryItemNotFoundError,
    SessionCreationError,
)
from deephaven_mcp.client import CommunityClientTimeouts
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    QualifiedSessionId,
    SessionId,
    StaticCommunitySessionManager,
    SystemType,
)
from deephaven_mcp.resource_manager._registry import MutableSessionRegistry
from deephaven_mcp.sessions import CommunitySessionConfig


def _full(name: str) -> QualifiedSessionId:
    """Return the full registry key for a community session named ``name``.

    The community :class:`SessionId` is the session name itself, so the
    full identifier is ``community:community:<name>``.
    """
    return QualifiedSessionId(SystemType.COMMUNITY, "community", SessionId(name))


def _session_configs() -> dict[str, CommunitySessionConfig]:
    """Build a couple of resolved community session configs for tests."""
    anon_auth = {"auth": {"credentials": {"type": "anonymous"}}}
    return {
        "worker1": CommunitySessionConfig.model_validate(
            {"name": "worker1", "host": "localhost", "port": 10001, **anon_auth}
        ),
        "worker2": CommunitySessionConfig.model_validate(
            {"name": "worker2", "host": "localhost", "port": 10002, **anon_auth}
        ),
    }


@pytest.fixture
def registry() -> CommunitySessionRegistry:
    """A registry pre-loaded with two static session configs."""
    return CommunitySessionRegistry(
        _session_configs(), timeouts=CommunityClientTimeouts()
    )


# --- Construction and isinstance ---


def test_construction_with_no_sessions() -> None:
    """The registry constructs cleanly with an empty session mapping."""
    reg = CommunitySessionRegistry({}, timeouts=CommunityClientTimeouts())
    assert isinstance(reg, CommunitySessionRegistry)
    assert isinstance(reg, MutableSessionRegistry)
    assert not reg._initialized


def test_construction_copies_mapping() -> None:
    """The registry must not retain the caller's mapping by reference."""
    cfg = _session_configs()
    reg = CommunitySessionRegistry(cfg, timeouts=CommunityClientTimeouts())
    cfg.clear()
    assert len(reg._session_configs) == 2


# --- initialize / _load_items ---


@pytest.mark.asyncio
async def test_initialize_populates_managers(
    registry: CommunitySessionRegistry,
) -> None:
    """initialize() materializes one StaticCommunitySessionManager per config entry."""
    await registry.initialize()
    assert registry._initialized
    assert len(registry._items) == 2
    assert _full("worker1") in registry._items
    assert _full("worker2") in registry._items
    assert isinstance(registry._items[_full("worker1")], StaticCommunitySessionManager)


@pytest.mark.asyncio
async def test_initialize_is_idempotent(registry: CommunitySessionRegistry) -> None:
    """A second initialize() call is a no-op."""
    await registry.initialize()
    await registry.initialize()
    assert len(registry._items) == 2


@pytest.mark.asyncio
async def test_initialize_empty_sessions() -> None:
    """An empty session mapping yields an initialized but empty registry."""
    reg = CommunitySessionRegistry({}, timeouts=CommunityClientTimeouts())
    await reg.initialize()
    assert reg._initialized
    assert reg._items == {}


# --- Methods raise before initialize ---


@pytest.mark.asyncio
async def test_methods_raise_before_initialize(
    registry: CommunitySessionRegistry,
) -> None:
    """get() and close() raise InternalError when called before initialize()."""
    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await registry.get(_full("worker1"))
    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await registry.close()


# --- get ---


@pytest.mark.asyncio
async def test_get_returns_manager(registry: CommunitySessionRegistry) -> None:
    """get() returns the StaticCommunitySessionManager for a known full name."""
    await registry.initialize()
    mgr = await registry.get(_full("worker1"))
    assert isinstance(mgr, StaticCommunitySessionManager)
    assert mgr._name == "worker1"


@pytest.mark.asyncio
async def test_get_unknown_raises(registry: CommunitySessionRegistry) -> None:
    """Unknown names produce a typed RegistryItemNotFoundError."""
    await registry.initialize()
    with pytest.raises(
        RegistryItemNotFoundError,
        match="No item with name 'unknown_worker' found in CommunitySessionRegistry",
    ):
        await registry.get("unknown_worker")


# --- close ---


@pytest.mark.asyncio
async def test_close_invokes_each_manager(registry: CommunitySessionRegistry) -> None:
    """close() awaits close() on every managed item and resets state."""
    await registry.initialize()

    mgr1 = registry._items[_full("worker1")]
    mgr2 = registry._items[_full("worker2")]
    mgr1.close = AsyncMock()
    mgr2.close = AsyncMock()

    await registry.close()

    mgr1.close.assert_awaited_once()
    mgr2.close.assert_awaited_once()
    assert not registry._initialized
    assert registry._items == {}


# --- end-to-end session retrieval ---


@pytest.mark.asyncio
async def test_get_session_from_manager(registry: CommunitySessionRegistry) -> None:
    """Full flow: registry.get(...) then manager.get() yields the underlying Session."""
    await registry.initialize()

    mock_session = AsyncMock(spec=Session)
    mock_session.is_alive = True

    with patch(
        "deephaven_mcp.resource_manager._manager.CommunitySessionManager.get",
        new=AsyncMock(return_value=mock_session),
    ) as mock_manager_get:
        mgr = await registry.get(_full("worker1"))
        session = await mgr.get()

    assert session is mock_session
    mock_manager_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_session_propagates_creation_error(
    registry: CommunitySessionRegistry,
) -> None:
    """SessionCreationError raised by the manager propagates to the caller."""
    await registry.initialize()

    with patch(
        "deephaven_mcp.resource_manager._manager.CommunitySessionManager.get",
        side_effect=SessionCreationError("Failed to connect"),
    ):
        mgr = await registry.get(_full("worker1"))
        with pytest.raises(SessionCreationError, match="Failed to connect"):
            await mgr.get()


# --- add_dynamic_session ---


@pytest.mark.asyncio
async def test_add_dynamic_session_uses_registry_timeouts() -> None:
    """add_dynamic_session constructs the manager with the registry's own timeouts.

    Identity check (``is``, not just equality) guarantees no future
    contributor accidentally introduces a separate timeouts source for
    the dynamic path.
    """
    from unittest.mock import MagicMock

    from deephaven_mcp.resource_manager import (
        DockerLaunchedSession,
        DynamicCommunitySessionManager,
    )

    registry_timeouts = CommunityClientTimeouts(session_connect_timeout_seconds=42.0)
    reg = CommunitySessionRegistry({}, timeouts=registry_timeouts)
    await reg.initialize()

    session_config = CommunitySessionConfig.model_validate(
        {
            "name": "dyn",
            "host": "localhost",
            "port": 10000,
            "auth": {"credentials": {"type": "anonymous"}},
        }
    )
    launched = MagicMock(spec=DockerLaunchedSession)
    launched.port = 10000
    launched.launch_method = "docker"

    manager = await reg.add_dynamic_session(
        name="dyn",
        session_config=session_config,
        launched_session=launched,
    )

    assert isinstance(manager, DynamicCommunitySessionManager)
    snapshot = await reg.get_all()
    assert manager.qualified_session_id in snapshot.items
    # The dynamic manager carries the registry's exact timeouts instance,
    # not a separately-fetched copy.
    assert manager._timeouts is registry_timeouts


@pytest.mark.asyncio
async def test_add_dynamic_session_rejects_duplicate_display_name() -> None:
    """A second add with the same display name must be rejected atomically.

    The community :class:`SessionId` is the session name itself, so two
    same-named adds produce the same ``qualified_session_id`` and the second is
    rejected by the registry's duplicate-name guard.
    """
    from unittest.mock import MagicMock

    from deephaven_mcp.resource_manager import DockerLaunchedSession

    reg = CommunitySessionRegistry({}, timeouts=CommunityClientTimeouts())
    await reg.initialize()

    session_config = CommunitySessionConfig.model_validate(
        {
            "name": "dup",
            "host": "localhost",
            "port": 10000,
            "auth": {"credentials": {"type": "anonymous"}},
        }
    )
    launched = MagicMock(spec=DockerLaunchedSession)
    launched.port = 10000
    launched.launch_method = "docker"

    first = await reg.add_dynamic_session(
        name="dup",
        session_config=session_config,
        launched_session=launched,
    )

    with pytest.raises(ValueError, match="already exists"):
        await reg.add_dynamic_session(
            name="dup",
            session_config=session_config,
            launched_session=launched,
        )

    # First session is still registered; no partial-failure damage.
    snapshot = await reg.get_all()
    assert first.qualified_session_id in snapshot.items
    assert len(snapshot.items) == 1
