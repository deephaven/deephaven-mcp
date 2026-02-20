"""
Tests for CombinedSessionRegistry and _fetch_factory_pqs.

Structure:
  TestConstruction               — __init__ state
  TestLoadItems                  — _load_items (called by super().initialize)
  TestInitialize                 — initialize() lifecycle + background task
  TestClose                      — close() lifecycle
  TestGet                        — get() with refresh logic
  TestGetAll                     — get_all() with refresh logic
  TestAddSession                 — add_session()
  TestRemoveSession              — remove_session()
  TestCountAddedSessions         — count_added_sessions()
  TestIsAddedSession             — is_added_session()
  TestSyncEnterpriseSessions     — _sync_enterprise_sessions() orchestration
  TestSnapshotFactoryState       — _snapshot_factory_state()
  TestGetFactoryKeys             — _get_factory_keys()
  TestRemoveFactorySessionsByKeys — _remove_factory_sessions_by_keys()
  TestRemoveFactorySessions      — _remove_factory_sessions()
  TestApplyFactorySuccess        — _apply_factory_success()
  TestApplyFactoryError          — _apply_factory_error()
  TestApplyResults               — _apply_results() dispatch
  TestDiscoverEnterpriseSessions — _discover_enterprise_sessions()
  TestBuildNotFoundMessage       — _build_not_found_message()
  TestMakeEnterpriseSessionManager — static helper
  TestQueryFactory               — module-level _fetch_factory_pqs()
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import InternalError, RegistryItemNotFoundError
from deephaven_mcp.client import CorePlusControllerClient
from deephaven_mcp.config import ConfigManager
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CombinedSessionRegistry,
    CommunitySessionRegistry,
    CorePlusSessionFactoryManager,
    CorePlusSessionFactoryRegistry,
    EnterpriseSessionManager,
    InitializationPhase,
    RegistrySnapshot,
    SystemType,
)
from deephaven_mcp.resource_manager._registry_combined import (
    _FactoryQueryError,
    _FactoryQueryResult,
    _FactorySnapshot,
    _fetch_factory_pqs,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry():
    """Uninitialized CombinedSessionRegistry."""
    return CombinedSessionRegistry()


@pytest.fixture
def mock_community_registry():
    m = MagicMock(spec=CommunitySessionRegistry)
    m.initialize = AsyncMock()
    m.close = AsyncMock()
    m.get_all = AsyncMock(return_value=RegistrySnapshot.simple(items={}))
    return m


@pytest.fixture
def mock_enterprise_registry():
    m = MagicMock(spec=CorePlusSessionFactoryRegistry)
    m.initialize = AsyncMock()
    m.close = AsyncMock()
    m.get_all = AsyncMock(return_value=RegistrySnapshot.simple(items={}))
    return m


@pytest.fixture
def initialized_registry(registry, mock_community_registry, mock_enterprise_registry):
    """Registry with _initialized=True and COMPLETED phase, no background task."""
    registry._community_registry = mock_community_registry
    registry._enterprise_registry = mock_enterprise_registry
    registry._initialized = True
    registry._phase = InitializationPhase.COMPLETED
    return registry


def _make_mock_factory_manager() -> MagicMock:
    m = MagicMock(spec=CorePlusSessionFactoryManager)
    m.get = AsyncMock()
    return m


def _make_mock_controller_client() -> MagicMock:
    m = MagicMock(spec=CorePlusControllerClient)
    m.ping = AsyncMock(return_value=True)
    m.map = AsyncMock(return_value={})
    return m


# ---------------------------------------------------------------------------
# TestConstruction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_initial_state(self, registry):
        assert registry._community_registry is None
        assert registry._enterprise_registry is None
        assert registry._controller_clients == {}
        assert registry._added_session_ids == set()
        assert registry._phase == InitializationPhase.NOT_STARTED
        assert registry._errors == {}
        assert registry._discovery_task is None
        assert not registry._initialized

    def test_inherits_base_registry(self, registry):
        from deephaven_mcp.resource_manager._registry import BaseRegistry

        assert isinstance(registry, BaseRegistry)
        assert hasattr(registry, "_lock")
        assert hasattr(registry, "_items")


# ---------------------------------------------------------------------------
# TestLoadItems
# ---------------------------------------------------------------------------


class TestLoadItems:
    @pytest.mark.asyncio
    async def test_loads_community_sessions(
        self, registry, mock_community_registry, mock_enterprise_registry
    ):
        """_load_items copies community sessions into _items and sets PARTIAL phase."""
        mock_mgr = MagicMock(spec=BaseItemManager)
        mock_mgr.full_name = "community:src:s1"
        mock_community_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"community:src:s1": mock_mgr})
        )

        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry",
                return_value=mock_community_registry,
            ),
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry",
                return_value=mock_enterprise_registry,
            ),
        ):
            await registry._load_items(MagicMock(spec=ConfigManager))

        assert "community:src:s1" in registry._items
        assert registry._items["community:src:s1"] is mock_mgr
        assert registry._phase == InitializationPhase.PARTIAL

    @pytest.mark.asyncio
    async def test_raises_on_unexpected_community_phase(
        self, registry, mock_community_registry, mock_enterprise_registry
    ):
        mock_community_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.LOADING,
                errors={},
            )
        )
        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry",
                return_value=mock_community_registry,
            ),
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry",
                return_value=mock_enterprise_registry,
            ),
            pytest.raises(InternalError, match="unexpected phase"),
        ):
            await registry._load_items(MagicMock(spec=ConfigManager))

    @pytest.mark.asyncio
    async def test_raises_on_community_errors(
        self, registry, mock_community_registry, mock_enterprise_registry
    ):
        mock_community_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.SIMPLE,
                errors={"src": "boom"},
            )
        )
        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry",
                return_value=mock_community_registry,
            ),
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry",
                return_value=mock_enterprise_registry,
            ),
            pytest.raises(InternalError, match="unexpected errors"),
        ):
            await registry._load_items(MagicMock(spec=ConfigManager))


# ---------------------------------------------------------------------------
# TestInitialize
# ---------------------------------------------------------------------------


class TestInitialize:
    @pytest.mark.asyncio
    async def test_initialize_creates_discovery_task(
        self, registry, mock_community_registry, mock_enterprise_registry
    ):
        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry",
                return_value=mock_community_registry,
            ),
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry",
                return_value=mock_enterprise_registry,
            ),
            patch.object(
                registry,
                "_discover_enterprise_sessions",
                AsyncMock(),
            ),
        ):
            await registry.initialize(MagicMock(spec=ConfigManager))
            assert registry._initialized is True
            assert registry._discovery_task is not None
            # Let the task finish
            await registry._discovery_task

    @pytest.mark.asyncio
    async def test_initialize_idempotent(
        self, registry, mock_community_registry, mock_enterprise_registry
    ):
        """Second call to initialize() is a no-op."""
        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry",
                return_value=mock_community_registry,
            ),
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry",
                return_value=mock_enterprise_registry,
            ),
            patch.object(registry, "_discover_enterprise_sessions", AsyncMock()),
        ):
            cfg = MagicMock(spec=ConfigManager)
            await registry.initialize(cfg)
            task1 = registry._discovery_task
            await registry.initialize(cfg)
            assert registry._discovery_task is task1  # same task, not replaced

    @pytest.mark.asyncio
    async def test_initialize_propagates_load_error(
        self, registry, mock_community_registry, mock_enterprise_registry
    ):
        mock_community_registry.initialize = AsyncMock(
            side_effect=RuntimeError("load failed")
        )
        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry",
                return_value=mock_community_registry,
            ),
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry",
                return_value=mock_enterprise_registry,
            ),
            pytest.raises(RuntimeError, match="load failed"),
        ):
            await registry.initialize(MagicMock(spec=ConfigManager))
        assert not registry._initialized


# ---------------------------------------------------------------------------
# TestClose
# ---------------------------------------------------------------------------


class TestClose:
    @pytest.mark.asyncio
    async def test_close_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.close()

    @pytest.mark.asyncio
    async def test_close_cancels_discovery_task(self, initialized_registry):
        """close() cancels a running discovery task."""
        never_done = asyncio.create_task(asyncio.sleep(9999))
        initialized_registry._discovery_task = never_done
        # Add a dummy item so super().close() has something to close
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.close = AsyncMock()
        initialized_registry._items["community:x:y"] = mock_item

        await initialized_registry.close()

        assert never_done.cancelled()
        assert not initialized_registry._initialized

    @pytest.mark.asyncio
    async def test_close_closes_sub_registries(self, initialized_registry):
        mock_community = initialized_registry._community_registry
        mock_enterprise = initialized_registry._enterprise_registry
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.close = AsyncMock()
        initialized_registry._items["community:x:y"] = mock_item

        await initialized_registry.close()

        mock_community.close.assert_awaited_once()
        mock_enterprise.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_handles_sub_registry_errors(self, initialized_registry):
        """Errors from sub-registry close() are logged, not propagated."""
        initialized_registry._community_registry.close = AsyncMock(
            side_effect=RuntimeError("community boom")
        )
        initialized_registry._enterprise_registry.close = AsyncMock(
            side_effect=RuntimeError("enterprise boom")
        )
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.close = AsyncMock()
        initialized_registry._items["community:x:y"] = mock_item

        # Should not raise
        await initialized_registry.close()
        assert not initialized_registry._initialized

    @pytest.mark.asyncio
    async def test_close_resets_state(self, initialized_registry):
        """After close(), extra state is cleared."""
        initialized_registry._controller_clients["f1"] = MagicMock()
        initialized_registry._added_session_ids.add("enterprise:f1:s1")
        initialized_registry._errors["f1"] = "err"
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.close = AsyncMock()
        initialized_registry._items["community:x:y"] = mock_item

        await initialized_registry.close()

        assert initialized_registry._controller_clients == {}
        assert initialized_registry._added_session_ids == set()
        assert initialized_registry._errors == {}
        assert initialized_registry._phase == InitializationPhase.NOT_STARTED
        assert initialized_registry._items == {}
        assert not initialized_registry._initialized

    @pytest.mark.asyncio
    async def test_close_closes_items(self, initialized_registry):
        """Items in _items are closed (outside the lock — close() may do I/O)."""
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.close = AsyncMock()
        initialized_registry._items["community:x:y"] = mock_item

        await initialized_registry.close()

        mock_item.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_items_outside_lock(self, initialized_registry):
        """Items are closed after _items is cleared from the lock, not while holding it."""
        close_called_while_locked = []

        async def check_lock_during_close():
            # If the lock is held during close(), acquiring it here would deadlock.
            # Use try_acquire (non-blocking) to detect if lock is held.
            acquired = initialized_registry._lock.locked()
            close_called_while_locked.append(acquired)

        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.full_name = "community:x:y"
        mock_item.close = AsyncMock(side_effect=check_lock_during_close)
        initialized_registry._items["community:x:y"] = mock_item

        await initialized_registry.close()

        assert close_called_while_locked == [
            False
        ], "item.close() was called while self._lock was held"

    @pytest.mark.asyncio
    async def test_close_item_error_does_not_propagate(self, initialized_registry):
        """Errors from item.close() are logged, not propagated."""
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.full_name = "community:x:y"
        mock_item.close = AsyncMock(side_effect=RuntimeError("item close boom"))
        initialized_registry._items["community:x:y"] = mock_item

        # Should not raise
        await initialized_registry.close()
        assert not initialized_registry._initialized

    @pytest.mark.asyncio
    async def test_close_gates_concurrent_caller(self, initialized_registry):
        """A second concurrent close() call raises InternalError immediately."""
        mock_item = MagicMock(spec=BaseItemManager)
        mock_item.close = AsyncMock()
        initialized_registry._items["community:x:y"] = mock_item

        # First close succeeds; second should raise because _initialized=False.
        await initialized_registry.close()
        with pytest.raises(InternalError):
            await initialized_registry.close()

    @pytest.mark.asyncio
    async def test_close_waits_for_in_flight_sync(self, initialized_registry):
        """close() waits for an in-flight _sync_enterprise_sessions via _refresh_lock."""
        sync_started = asyncio.Event()
        sync_may_finish = asyncio.Event()

        async def slow_sync(*_args, **_kwargs):
            sync_started.set()
            await sync_may_finish.wait()

        with patch(
            "deephaven_mcp.resource_manager._registry_combined.CombinedSessionRegistry._sync_enterprise_sessions",
            side_effect=slow_sync,
        ):
            # Simulate a sync holding _refresh_lock
            async with initialized_registry._refresh_lock:
                sync_started.set()
                close_task = asyncio.create_task(initialized_registry.close())
                # Give close() a chance to reach the _refresh_lock barrier
                await asyncio.sleep(0)
                assert not close_task.done()
            # Now release the lock — close() should proceed
            await close_task

        assert not initialized_registry._initialized


# ---------------------------------------------------------------------------
# TestGet
# ---------------------------------------------------------------------------


class TestGet:
    @pytest.mark.asyncio
    async def test_get_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.get("community:src:name")

    @pytest.mark.asyncio
    async def test_get_community_session_no_refresh(self, initialized_registry):
        """Community sessions are returned without triggering a refresh."""
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["community:src:name"] = mock_item

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            result = await initialized_registry.get("community:src:name")

        assert result is mock_item
        mock_sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_enterprise_session_triggers_refresh(self, initialized_registry):
        """Enterprise sessions trigger _sync_enterprise_sessions for that factory."""
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["enterprise:factory1:session1"] = mock_item

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            result = await initialized_registry.get("enterprise:factory1:session1")

        assert result is mock_item
        mock_sync.assert_awaited_once_with(["factory1"])

    @pytest.mark.asyncio
    async def test_get_enterprise_no_refresh_during_loading(self, initialized_registry):
        """No refresh during LOADING phase — background task is sole writer."""
        initialized_registry._phase = InitializationPhase.LOADING
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["enterprise:factory1:session1"] = mock_item

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            result = await initialized_registry.get("enterprise:factory1:session1")

        assert result is mock_item
        mock_sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_not_found_raises(self, initialized_registry):
        with pytest.raises(RegistryItemNotFoundError, match="No item with name"):
            await initialized_registry.get("community:src:nonexistent")

    @pytest.mark.asyncio
    async def test_get_malformed_name_raises(self, initialized_registry):
        """Malformed session name raises InvalidSessionNameError, no refresh triggered."""
        from deephaven_mcp._exceptions import InvalidSessionNameError

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            with pytest.raises(InvalidSessionNameError):
                await initialized_registry.get("bad-name")
        mock_sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_not_found_message_during_loading(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.LOADING
        with pytest.raises(RegistryItemNotFoundError) as exc_info:
            await initialized_registry.get("enterprise:factory1:session1")
        assert "still in progress" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_not_found_message_with_matching_factory_error(
        self, initialized_registry
    ):
        initialized_registry._errors = {"factory1": "Connection refused"}
        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ):
            with pytest.raises(RegistryItemNotFoundError) as exc_info:
                await initialized_registry.get("enterprise:factory1:session1")
        assert "factory1" in str(exc_info.value)
        assert "Connection refused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_not_found_message_with_other_factory_error(
        self, initialized_registry
    ):
        initialized_registry._errors = {"other_factory": "Timeout"}
        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ):
            with pytest.raises(RegistryItemNotFoundError) as exc_info:
                await initialized_registry.get("enterprise:factory1:session1")
        assert "1 factory" in str(exc_info.value)
        assert "other_factory" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TestGetAll
# ---------------------------------------------------------------------------


class TestGetAll:
    @pytest.mark.asyncio
    async def test_get_all_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.get_all()

    @pytest.mark.asyncio
    async def test_get_all_returns_snapshot(self, initialized_registry):
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["community:src:s1"] = mock_item

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ):
            snapshot = await initialized_registry.get_all()

        assert "community:src:s1" in snapshot.items
        assert snapshot.items["community:src:s1"] is mock_item
        assert snapshot.initialization_phase == InitializationPhase.COMPLETED
        assert snapshot.initialization_errors == {}

    @pytest.mark.asyncio
    async def test_get_all_returns_copy(self, initialized_registry):
        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ):
            snapshot = await initialized_registry.get_all()
        assert snapshot.items is not initialized_registry._items

    @pytest.mark.asyncio
    async def test_get_all_triggers_refresh_when_completed(self, initialized_registry):
        mock_factory = _make_mock_factory_manager()
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"f1": mock_factory})
        )

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            await initialized_registry.get_all()

        mock_sync.assert_awaited_once_with(["f1"])

    @pytest.mark.asyncio
    async def test_get_all_no_refresh_during_loading(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.LOADING

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            await initialized_registry.get_all()

        mock_sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_all_includes_errors(self, initialized_registry):
        initialized_registry._errors = {"f1": "boom"}

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ):
            snapshot = await initialized_registry.get_all()

        assert snapshot.initialization_errors == {"f1": "boom"}


# ---------------------------------------------------------------------------
# TestAddSession
# ---------------------------------------------------------------------------


class TestAddSession:
    @pytest.mark.asyncio
    async def test_add_session_not_initialized_raises(self, registry):
        m = MagicMock(spec=BaseItemManager)
        m.full_name = "enterprise:f1:s1"
        with pytest.raises(InternalError):
            await registry.add_session(m)

    @pytest.mark.asyncio
    async def test_add_session_success(self, initialized_registry):
        m = MagicMock(spec=BaseItemManager)
        m.full_name = "enterprise:f1:s1"
        await initialized_registry.add_session(m)
        assert "enterprise:f1:s1" in initialized_registry._items
        assert "enterprise:f1:s1" in initialized_registry._added_session_ids

    @pytest.mark.asyncio
    async def test_add_session_duplicate_raises(self, initialized_registry):
        m = MagicMock(spec=BaseItemManager)
        m.full_name = "enterprise:f1:s1"
        await initialized_registry.add_session(m)
        with pytest.raises(ValueError, match="already exists"):
            await initialized_registry.add_session(m)


# ---------------------------------------------------------------------------
# TestRemoveSession
# ---------------------------------------------------------------------------


class TestRemoveSession:
    @pytest.mark.asyncio
    async def test_remove_session_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.remove_session("enterprise:f1:s1")

    @pytest.mark.asyncio
    async def test_remove_session_exists(self, initialized_registry):
        m = MagicMock(spec=BaseItemManager)
        m.full_name = "enterprise:f1:s1"
        initialized_registry._items["enterprise:f1:s1"] = m
        initialized_registry._added_session_ids.add("enterprise:f1:s1")

        result = await initialized_registry.remove_session("enterprise:f1:s1")

        assert result is m
        assert "enterprise:f1:s1" not in initialized_registry._items
        assert "enterprise:f1:s1" not in initialized_registry._added_session_ids

    @pytest.mark.asyncio
    async def test_remove_session_not_exists_returns_none(self, initialized_registry):
        result = await initialized_registry.remove_session("enterprise:f1:nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_remove_session_idempotent(self, initialized_registry):
        result1 = await initialized_registry.remove_session("enterprise:f1:s1")
        result2 = await initialized_registry.remove_session("enterprise:f1:s1")
        assert result1 is None
        assert result2 is None


# ---------------------------------------------------------------------------
# TestCountAddedSessions
# ---------------------------------------------------------------------------


class TestCountAddedSessions:
    @pytest.mark.asyncio
    async def test_count_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.count_added_sessions(SystemType.ENTERPRISE, "f1")

    @pytest.mark.asyncio
    async def test_count_with_enum_type(self, initialized_registry):
        initialized_registry._items["enterprise:f1:s1"] = MagicMock()
        initialized_registry._added_session_ids.add("enterprise:f1:s1")

        count = await initialized_registry.count_added_sessions(
            SystemType.ENTERPRISE, "f1"
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_count_excludes_sessions_not_in_items(self, initialized_registry):
        """Sessions in _added_session_ids but not in _items are not counted."""
        initialized_registry._added_session_ids.add("enterprise:f1:gone")
        initialized_registry._items["enterprise:f1:valid"] = MagicMock()
        initialized_registry._added_session_ids.add("enterprise:f1:valid")

        count = await initialized_registry.count_added_sessions(
            SystemType.ENTERPRISE, "f1"
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_count_malformed_id_raises_internal_error(self, initialized_registry):
        """Malformed IDs in _added_session_ids indicate a programming bug."""
        initialized_registry._added_session_ids.add("bad-format")
        with pytest.raises(InternalError):
            await initialized_registry.count_added_sessions(SystemType.ENTERPRISE, "f1")

    @pytest.mark.asyncio
    async def test_count_zero_when_empty(self, initialized_registry):
        count = await initialized_registry.count_added_sessions(
            SystemType.ENTERPRISE, "f1"
        )
        assert count == 0


# ---------------------------------------------------------------------------
# TestIsAddedSession
# ---------------------------------------------------------------------------


class TestIsAddedSession:
    @pytest.mark.asyncio
    async def test_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.is_added_session("enterprise:f1:s1")

    @pytest.mark.asyncio
    async def test_true_for_added(self, initialized_registry):
        initialized_registry._added_session_ids.add("enterprise:f1:s1")
        assert await initialized_registry.is_added_session("enterprise:f1:s1") is True

    @pytest.mark.asyncio
    async def test_false_for_not_added(self, initialized_registry):
        assert await initialized_registry.is_added_session("enterprise:f1:s1") is False


# ---------------------------------------------------------------------------
# TestCommunityRegistry
# ---------------------------------------------------------------------------


class TestCommunityRegistry:
    @pytest.mark.asyncio
    async def test_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.community_registry()

    @pytest.mark.asyncio
    async def test_returns_community_registry(self, initialized_registry):
        result = await initialized_registry.community_registry()
        assert result is initialized_registry._community_registry

    @pytest.mark.asyncio
    async def test_raises_when_community_registry_none(self, initialized_registry):
        initialized_registry._community_registry = None
        with pytest.raises(InternalError, match="community registry is not available"):
            await initialized_registry.community_registry()


# ---------------------------------------------------------------------------
# TestEnterpriseRegistry
# ---------------------------------------------------------------------------


class TestEnterpriseRegistry:
    @pytest.mark.asyncio
    async def test_not_initialized_raises(self, registry):
        with pytest.raises(InternalError):
            await registry.enterprise_registry()

    @pytest.mark.asyncio
    async def test_returns_enterprise_registry(self, initialized_registry):
        result = await initialized_registry.enterprise_registry()
        assert result is initialized_registry._enterprise_registry

    @pytest.mark.asyncio
    async def test_raises_when_enterprise_registry_none(self, initialized_registry):
        initialized_registry._enterprise_registry = None
        with pytest.raises(InternalError, match="enterprise registry is not available"):
            await initialized_registry.enterprise_registry()


# ---------------------------------------------------------------------------
# TestSyncEnterpriseSessions
# ---------------------------------------------------------------------------


class TestSyncEnterpriseSessions:
    @pytest.mark.asyncio
    async def test_sync_calls_snapshot_query_apply(self, initialized_registry):
        """_sync_enterprise_sessions orchestrates the four phases."""
        mock_factory = _make_mock_factory_manager()
        mock_client = _make_mock_controller_client()
        # Pre-seed the client cache so _fetch_factory_pqs takes the cached-client path
        initialized_registry._controller_clients["f1"] = mock_client

        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )

        await initialized_registry._sync_enterprise_sessions(["f1"])

        assert initialized_registry._controller_clients.get("f1") is mock_client

    @pytest.mark.asyncio
    async def test_sync_cleans_up_sessions_for_disappeared_factory(
        self, initialized_registry
    ):
        """Sessions for a factory that disappeared from the registry are removed."""
        stale_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "nonexistent", "s1"
        )
        stale_mgr = MagicMock(spec=BaseItemManager)
        stale_mgr.full_name = stale_key
        stale_mgr.close = AsyncMock()
        initialized_registry._items[stale_key] = stale_mgr

        initialized_registry._enterprise_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("nonexistent")
        )
        await initialized_registry._sync_enterprise_sessions(["nonexistent"])

        assert stale_key not in initialized_registry._items
        stale_mgr.close.assert_awaited_once()
        assert "nonexistent" in initialized_registry._errors

    @pytest.mark.asyncio
    async def test_sync_adds_new_sessions(self, initialized_registry):
        """New sessions from controller are added to _items."""
        mock_factory = _make_mock_factory_manager()
        mock_client = _make_mock_controller_client()

        session_info = MagicMock()
        session_info.config.pb.name = "pq1"
        mock_client.map = AsyncMock(return_value={"k": session_info})

        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )
        initialized_registry._controller_clients["f1"] = mock_client

        await initialized_registry._sync_enterprise_sessions(["f1"])

        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "pq1")
        assert key in initialized_registry._items

    @pytest.mark.asyncio
    async def test_sync_removes_stale_sessions(self, initialized_registry):
        """Sessions no longer in controller are removed and closed."""
        mock_factory = _make_mock_factory_manager()
        mock_client = _make_mock_controller_client()
        mock_client.map = AsyncMock(return_value={})

        stale_key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "old")
        stale_mgr = MagicMock(spec=BaseItemManager)
        stale_mgr.full_name = stale_key
        stale_mgr.close = AsyncMock()
        initialized_registry._items[stale_key] = stale_mgr

        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )
        initialized_registry._controller_clients["f1"] = mock_client

        await initialized_registry._sync_enterprise_sessions(["f1"])

        assert stale_key not in initialized_registry._items
        stale_mgr.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stale_manager_close_error_does_not_propagate(
        self, initialized_registry
    ):
        """Errors from closing stale managers are logged, not propagated."""
        stale_key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "old")
        stale_mgr = MagicMock(spec=BaseItemManager)
        stale_mgr.full_name = stale_key
        stale_mgr.close = AsyncMock(side_effect=RuntimeError("close boom"))
        initialized_registry._items[stale_key] = stale_mgr

        mock_factory = _make_mock_factory_manager()
        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )
        mock_client = _make_mock_controller_client()
        mock_factory.get.return_value.controller_client = mock_client

        # Should not raise despite stale_mgr.close() raising.
        await initialized_registry._sync_enterprise_sessions(["f1"])

        stale_mgr.close.assert_awaited_once()
        assert stale_key not in initialized_registry._items

    @pytest.mark.asyncio
    async def test_sync_records_error_on_failure(self, initialized_registry):
        """Query failure is recorded in _errors, sessions removed."""
        mock_factory = _make_mock_factory_manager()
        mock_factory.get = AsyncMock(side_effect=RuntimeError("connection refused"))

        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )

        await initialized_registry._sync_enterprise_sessions(["f1"])

        assert "f1" in initialized_registry._errors
        assert "RuntimeError" in initialized_registry._errors["f1"]

    @pytest.mark.asyncio
    async def test_sync_parallel_execution(self, initialized_registry):
        """Multiple factories are queried in parallel."""
        call_order: list[str] = []

        async def slow_query(snapshot: _FactorySnapshot):
            call_order.append(f"start_{snapshot.factory_name}")
            await asyncio.sleep(0.01)
            call_order.append(f"end_{snapshot.factory_name}")
            return _FactoryQueryResult(
                factory_name=snapshot.factory_name,
                new_client=_make_mock_controller_client(),
                query_names=set(),
            )

        factories = {f"f{i}": _make_mock_factory_manager() for i in range(3)}
        initialized_registry._enterprise_registry.get = AsyncMock(
            side_effect=lambda name: factories[name]
        )

        with patch(
            "deephaven_mcp.resource_manager._registry_combined._fetch_factory_pqs",
            side_effect=slow_query,
        ):
            await initialized_registry._sync_enterprise_sessions(list(factories.keys()))

        starts = [i for i, x in enumerate(call_order) if x.startswith("start_")]
        ends = [i for i, x in enumerate(call_order) if x.startswith("end_")]
        assert max(starts) < min(ends)


# ---------------------------------------------------------------------------
# TestSnapshotFactoryState
# ---------------------------------------------------------------------------


class TestSnapshotFactoryState:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_enterprise_registry(
        self, initialized_registry
    ):
        initialized_registry._enterprise_registry = None
        result = await initialized_registry._snapshot_factory_state(["f1"])
        assert result == []

    @pytest.mark.asyncio
    async def test_skips_unknown_factories(self, initialized_registry):
        initialized_registry._enterprise_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("nonexistent")
        )
        result = await initialized_registry._snapshot_factory_state(["nonexistent"])
        assert result == []

    @pytest.mark.asyncio
    async def test_captures_cached_client(self, initialized_registry):
        mock_factory = _make_mock_factory_manager()
        mock_client = _make_mock_controller_client()
        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )
        initialized_registry._controller_clients["f1"] = mock_client

        snapshots = await initialized_registry._snapshot_factory_state(["f1"])

        assert len(snapshots) == 1
        assert snapshots[0].client is mock_client


# ---------------------------------------------------------------------------
# TestGetFactoryKeys
# ---------------------------------------------------------------------------


class TestGetFactoryKeys:
    def test_returns_matching_enterprise_keys(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        initialized_registry._items[key] = MagicMock(spec=BaseItemManager)

        keys = initialized_registry._get_factory_keys("f1")

        assert keys == {key}

    def test_excludes_different_factory(self, initialized_registry):
        key_f1 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        key_f2 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f2", "s1")
        initialized_registry._items[key_f1] = MagicMock(spec=BaseItemManager)
        initialized_registry._items[key_f2] = MagicMock(spec=BaseItemManager)

        keys = initialized_registry._get_factory_keys("f1")

        assert keys == {key_f1}
        assert key_f2 not in keys

    def test_excludes_community_sessions_with_same_source_name(
        self, initialized_registry
    ):
        """Bug #4: community sessions whose source matches factory_name must not match."""
        community_key = BaseItemManager.make_full_name(SystemType.COMMUNITY, "f1", "s1")
        enterprise_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "f1", "s1"
        )
        initialized_registry._items[community_key] = MagicMock(spec=BaseItemManager)
        initialized_registry._items[enterprise_key] = MagicMock(spec=BaseItemManager)

        keys = initialized_registry._get_factory_keys("f1")

        assert keys == {enterprise_key}
        assert community_key not in keys

    def test_returns_empty_when_no_matching_keys(self, initialized_registry):
        assert initialized_registry._get_factory_keys("nonexistent") == set()

    def test_raises_on_malformed_key(self, initialized_registry):
        initialized_registry._items["bad-key"] = MagicMock(spec=BaseItemManager)
        with pytest.raises(InternalError, match="Malformed key"):
            initialized_registry._get_factory_keys("f1")


# ---------------------------------------------------------------------------
# TestRemoveFactorySessionsByKeys
# ---------------------------------------------------------------------------


class TestRemoveFactorySessionsByKeys:
    def test_removes_specified_keys(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        mgr = MagicMock(spec=BaseItemManager)
        initialized_registry._items[key] = mgr

        result = initialized_registry._remove_factory_sessions_by_keys({key})

        assert key not in initialized_registry._items
        assert mgr in result

    def test_discards_from_added_session_ids(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        mgr = MagicMock(spec=BaseItemManager)
        initialized_registry._items[key] = mgr
        initialized_registry._added_session_ids.add(key)

        initialized_registry._remove_factory_sessions_by_keys({key})

        assert key not in initialized_registry._added_session_ids

    def test_tolerates_missing_keys(self, initialized_registry):
        result = initialized_registry._remove_factory_sessions_by_keys(
            {"enterprise:f1:gone"}
        )
        assert result == []

    def test_returns_empty_for_empty_input(self, initialized_registry):
        assert initialized_registry._remove_factory_sessions_by_keys(set()) == []


# ---------------------------------------------------------------------------
# TestRemoveFactorySessions
# ---------------------------------------------------------------------------


class TestRemoveFactorySessions:
    def test_removes_all_sessions_for_factory(self, initialized_registry):
        k1 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        k2 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s2")
        m1, m2 = MagicMock(spec=BaseItemManager), MagicMock(spec=BaseItemManager)
        initialized_registry._items[k1] = m1
        initialized_registry._items[k2] = m2

        result = initialized_registry._remove_factory_sessions("f1")

        assert k1 not in initialized_registry._items
        assert k2 not in initialized_registry._items
        assert set(result) == {m1, m2}

    def test_does_not_remove_other_factory(self, initialized_registry):
        k1 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        k2 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f2", "s1")
        initialized_registry._items[k1] = MagicMock(spec=BaseItemManager)
        initialized_registry._items[k2] = MagicMock(spec=BaseItemManager)

        initialized_registry._remove_factory_sessions("f1")

        assert k2 in initialized_registry._items

    def test_does_not_remove_community_same_source(self, initialized_registry):
        """Community sessions with the same source name must not be removed."""
        community_key = BaseItemManager.make_full_name(SystemType.COMMUNITY, "f1", "s1")
        enterprise_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "f1", "s1"
        )
        initialized_registry._items[community_key] = MagicMock(spec=BaseItemManager)
        initialized_registry._items[enterprise_key] = MagicMock(spec=BaseItemManager)

        initialized_registry._remove_factory_sessions("f1")

        assert community_key in initialized_registry._items
        assert enterprise_key not in initialized_registry._items


# ---------------------------------------------------------------------------
# TestApplyFactorySuccess
# ---------------------------------------------------------------------------


class TestApplyFactorySuccess:
    def _make_snapshot(self, factory_name: str = "f1") -> _FactorySnapshot:
        return _FactorySnapshot(
            factory_name=factory_name,
            factory_manager=_make_mock_factory_manager(),
            client=None,
        )

    def test_adds_new_sessions(self, initialized_registry):
        mock_client = _make_mock_controller_client()
        result = _FactoryQueryResult(
            factory_name="f1", new_client=mock_client, query_names={"s1", "s2"}
        )
        snapshot = self._make_snapshot()

        managers_to_close = initialized_registry._apply_factory_success(
            result, snapshot.factory_manager
        )

        assert managers_to_close == []
        assert (
            BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
            in initialized_registry._items
        )
        assert (
            BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s2")
            in initialized_registry._items
        )
        assert initialized_registry._controller_clients["f1"] is mock_client

    def test_removes_stale_sessions(self, initialized_registry):
        stale_key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "old")
        stale_mgr = MagicMock(spec=BaseItemManager)
        initialized_registry._items[stale_key] = stale_mgr

        result = _FactoryQueryResult(
            factory_name="f1",
            new_client=_make_mock_controller_client(),
            query_names=set(),
        )
        managers_to_close = initialized_registry._apply_factory_success(
            result, self._make_snapshot().factory_manager
        )

        assert stale_mgr in managers_to_close
        assert stale_key not in initialized_registry._items

    def test_does_not_recreate_existing_sessions(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        existing_mgr = MagicMock(spec=BaseItemManager)
        initialized_registry._items[key] = existing_mgr

        result = _FactoryQueryResult(
            factory_name="f1",
            new_client=_make_mock_controller_client(),
            query_names={"s1"},
        )
        initialized_registry._apply_factory_success(
            result, self._make_snapshot().factory_manager
        )

        assert initialized_registry._items[key] is existing_mgr

    def test_clears_previous_error(self, initialized_registry):
        initialized_registry._errors["f1"] = "previous error"
        result = _FactoryQueryResult(
            factory_name="f1",
            new_client=_make_mock_controller_client(),
            query_names=set(),
        )
        initialized_registry._apply_factory_success(
            result, self._make_snapshot().factory_manager
        )

        assert "f1" not in initialized_registry._errors

    def test_discard_stale_from_added_session_ids(self, initialized_registry):
        stale_key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "old")
        initialized_registry._items[stale_key] = MagicMock(spec=BaseItemManager)
        initialized_registry._added_session_ids.add(stale_key)

        result = _FactoryQueryResult(
            factory_name="f1",
            new_client=_make_mock_controller_client(),
            query_names=set(),
        )
        initialized_registry._apply_factory_success(
            result, self._make_snapshot().factory_manager
        )

        assert stale_key not in initialized_registry._added_session_ids


# ---------------------------------------------------------------------------
# TestApplyFactoryError
# ---------------------------------------------------------------------------


class TestApplyFactoryError:
    def test_records_error(self, initialized_registry):
        result = _FactoryQueryError(factory_name="f1", new_client=None, error="boom")
        initialized_registry._apply_factory_error(result)
        assert initialized_registry._errors["f1"] == "boom"

    def test_removes_all_sessions(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        mgr = MagicMock(spec=BaseItemManager)
        initialized_registry._items[key] = mgr

        result = _FactoryQueryError(factory_name="f1", new_client=None, error="boom")
        managers_to_close = initialized_registry._apply_factory_error(result)

        assert mgr in managers_to_close
        assert key not in initialized_registry._items

    def test_caches_new_client_when_present(self, initialized_registry):
        new_client = _make_mock_controller_client()
        result = _FactoryQueryError(
            factory_name="f1", new_client=new_client, error="boom"
        )
        initialized_registry._apply_factory_error(result)
        assert initialized_registry._controller_clients["f1"] is new_client

    def test_evicts_dead_client_when_no_new_client(self, initialized_registry):
        """When new_client is None, the stale cached client is evicted."""
        initialized_registry._controller_clients["f1"] = _make_mock_controller_client()
        result = _FactoryQueryError(factory_name="f1", new_client=None, error="boom")
        initialized_registry._apply_factory_error(result)
        assert "f1" not in initialized_registry._controller_clients

    def test_discards_from_added_session_ids(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        initialized_registry._items[key] = MagicMock(spec=BaseItemManager)
        initialized_registry._added_session_ids.add(key)

        result = _FactoryQueryError(factory_name="f1", new_client=None, error="boom")
        initialized_registry._apply_factory_error(result)

        assert key not in initialized_registry._added_session_ids


# ---------------------------------------------------------------------------
# TestApplyResults
# ---------------------------------------------------------------------------


class TestApplyResults:
    def _make_snapshot(self, factory_name: str = "f1") -> _FactorySnapshot:
        return _FactorySnapshot(
            factory_name=factory_name,
            factory_manager=_make_mock_factory_manager(),
            client=None,
        )

    def test_dispatches_success_result(self, initialized_registry):
        mock_client = _make_mock_controller_client()
        result = _FactoryQueryResult(
            factory_name="f1", new_client=mock_client, query_names={"s1"}
        )
        snapshot = self._make_snapshot()

        managers_to_close = initialized_registry._apply_results([result], [snapshot])

        assert managers_to_close == []
        assert (
            BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
            in initialized_registry._items
        )

    def test_dispatches_error_result(self, initialized_registry):
        key = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "s1")
        initialized_registry._items[key] = MagicMock(spec=BaseItemManager)

        result = _FactoryQueryError(factory_name="f1", new_client=None, error="boom")
        managers_to_close = initialized_registry._apply_results([result], [])

        assert key not in initialized_registry._items
        assert len(managers_to_close) == 1

    def test_raises_on_unexpected_result_type(self, initialized_registry):
        """Unexpected result type raises InternalError."""
        bad_result = MagicMock()
        bad_result.factory_name = "f1"
        with pytest.raises(InternalError, match="Unexpected result type"):
            initialized_registry._apply_results([bad_result], [])

    def test_raises_when_success_result_has_no_snapshot(self, initialized_registry):
        """_FactoryQueryResult with no matching snapshot raises InternalError."""
        result = _FactoryQueryResult(
            factory_name="f1",
            new_client=_make_mock_controller_client(),
            query_names={"s1"},
        )
        # Pass empty snapshots list — no snapshot for f1.
        with pytest.raises(InternalError, match="No snapshot found"):
            initialized_registry._apply_results([result], [])

    def test_collects_managers_from_multiple_factories(self, initialized_registry):
        k1 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f1", "old")
        k2 = BaseItemManager.make_full_name(SystemType.ENTERPRISE, "f2", "old")
        m1, m2 = MagicMock(spec=BaseItemManager), MagicMock(spec=BaseItemManager)
        initialized_registry._items[k1] = m1
        initialized_registry._items[k2] = m2

        results = [
            _FactoryQueryResult(
                factory_name="f1",
                new_client=_make_mock_controller_client(),
                query_names=set(),
            ),
            _FactoryQueryResult(
                factory_name="f2",
                new_client=_make_mock_controller_client(),
                query_names=set(),
            ),
        ]
        snapshots = [
            self._make_snapshot("f1"),
            self._make_snapshot("f2"),
        ]
        managers_to_close = initialized_registry._apply_results(results, snapshots)

        assert set(managers_to_close) == {m1, m2}


# ---------------------------------------------------------------------------
# TestDiscoverEnterpriseSessions
# ---------------------------------------------------------------------------


class TestDiscoverEnterpriseSessions:
    @pytest.mark.asyncio
    async def test_sets_loading_then_completed(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.PARTIAL
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._phase == InitializationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_cancelled_sets_failed_phase(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.PARTIAL

        async def slow():
            await asyncio.sleep(9999)

        initialized_registry._enterprise_registry.get_all = AsyncMock(side_effect=slow)

        task = asyncio.create_task(initialized_registry._discover_enterprise_sessions())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert initialized_registry._phase == InitializationPhase.FAILED

    @pytest.mark.asyncio
    async def test_cancelled_before_lock_sets_failed_phase(self, initialized_registry):
        """CancelledError raised while waiting for self._lock still sets FAILED.

        Previously the first lock acquire was outside the try block, so a
        cancellation at that point would leave _phase in its pre-discovery state.
        """
        initialized_registry._phase = InitializationPhase.PARTIAL

        # Hold the lock so the task blocks on the very first acquire.
        async with initialized_registry._lock:
            task = asyncio.create_task(
                initialized_registry._discover_enterprise_sessions()
            )
            # Yield so the task starts and blocks on the lock.
            await asyncio.sleep(0)
            assert not task.done()
            task.cancel()
            # Yield again so the cancellation is delivered while the lock is held.
            await asyncio.sleep(0)
        # Lock released — task should now handle CancelledError.
        with pytest.raises(asyncio.CancelledError):
            await task

        assert initialized_registry._phase == InitializationPhase.FAILED

    @pytest.mark.asyncio
    async def test_exception_records_error_and_completes(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.PARTIAL
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            side_effect=RuntimeError("discovery boom")
        )

        await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._phase == InitializationPhase.COMPLETED
        assert "enterprise_discovery" in initialized_registry._errors
        assert "RuntimeError" in initialized_registry._errors["enterprise_discovery"]

    @pytest.mark.asyncio
    async def test_completed_with_prior_errors_stays_completed(
        self, initialized_registry
    ):
        """Discovery completing with pre-existing errors still sets phase to COMPLETED."""
        initialized_registry._phase = InitializationPhase.PARTIAL
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )
        # Pre-seed an error to exercise the warning branch (line 847).
        initialized_registry._errors["f1"] = "some prior error"

        await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._phase == InitializationPhase.COMPLETED
        assert initialized_registry._errors["f1"] == "some prior error"

    @pytest.mark.asyncio
    async def test_calls_sync_for_all_factories(self, initialized_registry):
        mock_f1 = _make_mock_factory_manager()
        mock_f2 = _make_mock_factory_manager()
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"f1": mock_f1, "f2": mock_f2})
        )

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            await initialized_registry._discover_enterprise_sessions()

        mock_sync.assert_awaited_once()
        called_names = set(mock_sync.call_args[0][0])
        assert called_names == {"f1", "f2"}

    @pytest.mark.asyncio
    async def test_reads_enterprise_registry_under_lock(self, initialized_registry):
        """_enterprise_registry is read under self._lock so close() cannot race it."""
        read_while_locked = []

        original_get_all = initialized_registry._enterprise_registry.get_all

        async def check_lock_on_get_all():
            read_while_locked.append(initialized_registry._lock.locked())
            return RegistrySnapshot.simple(items={})

        initialized_registry._enterprise_registry.get_all = AsyncMock(
            side_effect=check_lock_on_get_all
        )

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ):
            await initialized_registry._discover_enterprise_sessions()

        # The lock should NOT be held when get_all() is called (only the read
        # of _enterprise_registry itself is under the lock, not the I/O call).
        assert read_while_locked == [False]

    @pytest.mark.asyncio
    async def test_skips_sync_when_enterprise_registry_none(self, initialized_registry):
        """If _enterprise_registry is None (closed concurrently), no sync is attempted."""
        initialized_registry._enterprise_registry = None

        with patch.object(
            initialized_registry, "_sync_enterprise_sessions", AsyncMock()
        ) as mock_sync:
            await initialized_registry._discover_enterprise_sessions()

        mock_sync.assert_not_awaited()
        assert initialized_registry._phase == InitializationPhase.COMPLETED


# ---------------------------------------------------------------------------
# TestBuildNotFoundMessage
# ---------------------------------------------------------------------------


class TestBuildNotFoundMessage:
    def test_basic_message(self, initialized_registry):
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "enterprise:f1:s1" in msg
        assert "CombinedSessionRegistry" in msg

    def test_includes_loading_note(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.LOADING
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "still in progress" in msg

    def test_no_note_when_completed(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.COMPLETED
        initialized_registry._errors = {}
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "Note:" not in msg

    def test_includes_matching_factory_error(self, initialized_registry):
        initialized_registry._errors = {"f1": "Connection refused"}
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "factory 'f1'" in msg
        assert "Connection refused" in msg

    def test_includes_other_factory_errors(self, initialized_registry):
        initialized_registry._errors = {"other": "Timeout"}
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "1 factory" in msg
        assert "other" in msg

    def test_includes_both_loading_and_error(self, initialized_registry):
        initialized_registry._phase = InitializationPhase.LOADING
        initialized_registry._errors = {"f1": "boom"}
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "still in progress" in msg
        assert "f1" in msg

    @pytest.mark.parametrize(
        "phase",
        [
            InitializationPhase.FAILED,
            InitializationPhase.NOT_STARTED,
            InitializationPhase.PARTIAL,
        ],
    )
    def test_non_loading_incomplete_phase_note(self, initialized_registry, phase):
        """FAILED/NOT_STARTED/PARTIAL produce 'has not completed' note, not 'in progress'."""
        initialized_registry._phase = phase
        initialized_registry._errors = {}
        msg = initialized_registry._build_not_found_message("enterprise:f1:s1")
        assert "has not completed" in msg
        assert phase.value in msg
        assert "still in progress" not in msg

    def test_malformed_name_with_errors_raises_internal_error(
        self, initialized_registry
    ):
        """Malformed name raises InternalError — caller is responsible for validation."""
        initialized_registry._errors = {"f1": "boom"}
        with pytest.raises(InternalError, match="malformed name"):
            initialized_registry._build_not_found_message("bad-name")


# ---------------------------------------------------------------------------
# TestMakeEnterpriseSessionManager
# ---------------------------------------------------------------------------


class TestMakeEnterpriseSessionManager:
    def test_creates_enterprise_session_manager(self):
        mock_factory = _make_mock_factory_manager()
        mgr = CombinedSessionRegistry._make_enterprise_session_manager(
            mock_factory, "f1", "s1"
        )
        assert isinstance(mgr, EnterpriseSessionManager)
        assert mgr._source == "f1"
        assert mgr._name == "s1"

    @pytest.mark.asyncio
    async def test_creation_function_calls_connect(self):
        mock_factory_instance = MagicMock()
        mock_factory_instance.connect_to_persistent_query = AsyncMock(
            return_value=MagicMock()
        )
        mock_factory = _make_mock_factory_manager()
        mock_factory.get = AsyncMock(return_value=mock_factory_instance)

        mgr = CombinedSessionRegistry._make_enterprise_session_manager(
            mock_factory, "f1", "s1"
        )
        await mgr._creation_function("f1", "s1")

        mock_factory.get.assert_awaited_once()
        mock_factory_instance.connect_to_persistent_query.assert_awaited_once_with("s1")


# ---------------------------------------------------------------------------
# TestQueryFactory
# ---------------------------------------------------------------------------


class TestQueryFactory:
    def _make_snapshot(
        self,
        factory_name: str = "f1",
        client=None,
    ) -> _FactorySnapshot:
        mock_factory = _make_mock_factory_manager()
        factory_instance = MagicMock()
        factory_instance.controller_client = client or _make_mock_controller_client()
        mock_factory.get = AsyncMock(return_value=factory_instance)
        return _FactorySnapshot(
            factory_name=factory_name,
            factory_manager=mock_factory,
            client=client,
        )

    @pytest.mark.asyncio
    async def test_creates_client_when_none(self):
        mock_client = _make_mock_controller_client()
        snapshot = self._make_snapshot(client=None)
        snapshot.factory_manager.get.return_value.controller_client = mock_client

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryResult)
        assert result.new_client is mock_client

    @pytest.mark.asyncio
    async def test_uses_cached_client(self):
        mock_client = _make_mock_controller_client()
        snapshot = self._make_snapshot(client=mock_client)

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryResult)
        assert result.new_client is mock_client

    @pytest.mark.asyncio
    async def test_ping_returns_false_recreates_client(self):
        """ping() returning False triggers client recreation (line 145)."""
        dead_client = _make_mock_controller_client()
        dead_client.ping = AsyncMock(return_value=False)  # False, not exception

        new_client = _make_mock_controller_client()
        snapshot = self._make_snapshot(client=dead_client)
        snapshot.factory_manager.get.return_value.controller_client = new_client

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryResult)
        assert result.new_client is new_client

    @pytest.mark.asyncio
    async def test_recreates_dead_client(self):
        dead_client = _make_mock_controller_client()
        dead_client.ping = AsyncMock(side_effect=RuntimeError("dead"))

        new_client = _make_mock_controller_client()
        snapshot = self._make_snapshot(client=dead_client)
        snapshot.factory_manager.get.return_value.controller_client = new_client

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryResult)
        assert result.new_client is new_client

    @pytest.mark.asyncio
    async def test_extracts_pq_names_from_map(self):
        mock_client = _make_mock_controller_client()
        session_info = MagicMock()
        session_info.config.pb.name = "pq1"
        mock_client.map = AsyncMock(return_value={"k": session_info})
        snapshot = self._make_snapshot(client=mock_client)

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryResult)
        assert "pq1" in result.query_names

    @pytest.mark.asyncio
    async def test_returns_error_on_exception(self):
        snapshot = self._make_snapshot(client=None)
        snapshot.factory_manager.get = AsyncMock(
            side_effect=RuntimeError("connection refused")
        )

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryError)
        assert "RuntimeError" in result.error
        assert "connection refused" in result.error

    @pytest.mark.asyncio
    async def test_extracts_multiple_pq_names_from_map(self):
        mock_client = _make_mock_controller_client()
        info1 = MagicMock()
        info1.config.pb.name = "alpha"
        info2 = MagicMock()
        info2.config.pb.name = "beta"
        mock_client.map = AsyncMock(return_value={"k1": info1, "k2": info2})
        snapshot = self._make_snapshot(client=mock_client)

        result = await _fetch_factory_pqs(snapshot)

        assert isinstance(result, _FactoryQueryResult)
        assert result.query_names == {"alpha", "beta"}
