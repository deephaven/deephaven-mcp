"""
Unit tests for Session Manager classes.
"""

import asyncio
import time
from typing import ClassVar, override
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from deephaven_mcp import client
from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    InternalError,
    InvalidSessionNameError,
    SessionCreationError,
)
from deephaven_mcp.client import (
    CONTROLLER_SUBSCRIBING_ERROR_CODE,
    CommunityClientTimeouts,
    CorePlusSession,
    EnterpriseClientTimeouts,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CommunitySessionManager,
    CorePlusSessionFactoryManager,
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SessionId,
    SessionOrigin,
    SystemType,
)
from deephaven_mcp.sessions import CommunitySessionConfig, EnterpriseSystemConfig


def _stub_session_config(name: str = "test", **overrides) -> CommunitySessionConfig:
    """Return a valid anonymous-auth CommunitySessionConfig for tests.

    Construction with a fully-validated declaration is required by the
    typed ``CommunitySessionManager`` API. Tests that previously passed
    arbitrary dicts get this stub when the dict's contents are not
    inspected by the assertions.
    """
    payload: dict = {
        "name": name,
        "auth": {"credentials": {"type": "anonymous"}},
    }
    payload.update(overrides)
    return CommunitySessionConfig.model_validate(payload)


def _stub_enterprise_config(
    name: str = "enterprise-test", **overrides
) -> EnterpriseSystemConfig:
    """Return a valid EnterpriseSystemConfig stub for tests."""
    payload: dict = {
        "name": name,
        "system_name": name,
        "connection_json_url": "https://example.com/iris/connection.json",
        "auth": {
            "credentials": {
                "type": "password",
                "username": "u",
                "password": "p",
            }
        },
    }
    payload.update(overrides)
    return EnterpriseSystemConfig.model_validate(payload)


# Base Item Manager Tests


class MockItem:
    """A mock item with async methods for testing."""

    def __init__(self):
        self.is_alive = AsyncMock(return_value=True)
        self.close = AsyncMock()


class MockSyncItem:
    """A mock item with a synchronous close method."""

    def __init__(self):
        self.is_alive = AsyncMock(return_value=True)
        self.close = MagicMock()


class ConcreteItemManager(BaseItemManager[MockItem]):
    """A concrete implementation of BaseItemManager for testing."""

    def __init__(
        self,
        system_type: SystemType,
        system: str,
        name: str,
        *,
        session_id: int = 0,
    ):
        super().__init__(system_type, system, SessionId.from_int(session_id), name)
        self._create_item_mock = AsyncMock(return_value=MockItem())

    async def _create_item(self) -> MockItem:
        return await self._create_item_mock()

    async def _check_liveness(
        self, item: MockItem
    ) -> tuple[ResourceLivenessStatus, str | None]:
        try:
            alive = await item.is_alive()
            if alive:
                return (ResourceLivenessStatus.ONLINE, None)
            else:
                return (ResourceLivenessStatus.OFFLINE, "Item not alive")
        except Exception as e:
            return (ResourceLivenessStatus.UNKNOWN, str(e))


@pytest.mark.asyncio
async def test_properties():
    """Test the basic properties of the manager."""
    manager = ConcreteItemManager(
        system_type=SystemType.COMMUNITY,
        system="test_source",
        name="test_manager",
        session_id=42,
    )
    assert manager.name == "test_manager"
    assert manager.system_type == SystemType.COMMUNITY
    assert manager.system == "test_source"
    assert manager.session_id == "42"
    assert str(manager.qualified_session_id) == "community:test_source:42"
    # Base manager no longer carries origin/kind; those live on subclasses.
    assert not hasattr(manager, "origin")
    assert not hasattr(manager, "kind")


@pytest.mark.asyncio
async def test_get_lazy_creation():
    """Test that the item is created lazily on the first get call."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    assert manager._item_cache is None

    # First call should create the item
    item1 = await manager.get()
    assert item1 is not None
    manager._create_item_mock.assert_called_once()
    assert manager._item_cache == item1

    # Second call should return the cached item
    item2 = await manager.get()
    assert item2 == item1
    manager._create_item_mock.assert_called_once()  # Still called only once


@pytest.mark.asyncio
async def test_is_alive():
    """Test the is_alive method."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")

    # Not alive if no item is cached
    assert not await manager.is_alive()

    item = await manager.get()
    item.is_alive.return_value = True
    assert await manager.is_alive()
    item.is_alive.assert_called_once()

    item.is_alive.return_value = False
    assert not await manager.is_alive()


@pytest.mark.asyncio
async def test_is_alive_exception():
    """Test that is_alive handles exceptions gracefully."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    item = await manager.get()
    item.is_alive.side_effect = Exception("Liveness check failed")
    assert not await manager.is_alive()


@pytest.mark.asyncio
async def test_close():
    """Test the close method."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    item = await manager.get()

    await manager.close()
    item.close.assert_called_once()
    assert manager._item_cache is None

    # Test idempotency
    await manager.close()
    item.close.assert_called_once()  # Still called only once


@pytest.mark.asyncio
async def test_close_calls_item_close_unconditionally():
    """close() unconditionally closes the cached item (no liveness pre-check).

    The two-phase close path captures the item ref under the lock, clears
    the cache, then closes outside the lock via ``_close_captured_item``.
    Any exceptions from item.close() are swallowed with a WARNING log;
    a previously-dead item is closed redundantly and the failure (if any)
    is silently ignored.
    """
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    item = await manager.get()

    # Mark the item as not alive — close() no longer pre-checks this.
    item.is_alive.return_value = False

    await manager.close()

    # close() is called on the item regardless of liveness state.
    item.close.assert_called_once()
    # Cache is cleared.
    assert manager._item_cache is None
    assert manager._last_accessed is None


@pytest.mark.asyncio
async def test_concurrent_get():
    """Test that get is thread-safe and creates only one item."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")

    # Simulate concurrent calls to get()
    tasks = [manager.get() for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # Check that the create method was called only once
    manager._create_item_mock.assert_called_once()

    # Check that all results are the same instance
    first_item = results[0]
    for item in results[1:]:
        assert item is first_item


@pytest.mark.asyncio
async def test_close_handles_sync_method_gracefully():
    """Test that close handles synchronous close methods gracefully without raising errors."""

    class ConcreteSyncItemManager(BaseItemManager[MockSyncItem]):
        def __init__(self, system_type: SystemType, system: str, name: str):
            super().__init__(system_type, system, SessionId.from_int(0), name)
            self._create_item_mock = AsyncMock(return_value=MockSyncItem())

        async def _create_item(self) -> MockSyncItem:
            return await self._create_item_mock()

        async def _check_liveness(
            self, item: MockSyncItem
        ) -> tuple[ResourceLivenessStatus, str | None]:
            # Ensure liveness check passes so close() proceeds
            return (ResourceLivenessStatus.ONLINE, None)

    manager = ConcreteSyncItemManager(SystemType.COMMUNITY, "test_sync", "test_sync")
    item = await manager.get()

    # close() should complete gracefully even with sync close method
    await manager.close()

    # Verify that the sync close method was called during cleanup
    # Note: May be called twice due to retry logic when sync method fails
    assert item.close.call_count >= 1

    # Verify cache is cleared
    assert manager._item_cache is None


# Session Manager Tests


def test_resource_liveness_status_str():
    """Covers line 231: str(enum) returns the enum name."""
    for status in ResourceLivenessStatus:
        assert str(status) == status.name


def test_system_type_str():
    """``str(SystemType.X)`` returns the lowercase value."""
    for system_type in SystemType:
        assert str(system_type) == system_type.value


from deephaven_mcp._exceptions import AuthenticationError, ConfigurationError


@pytest.mark.asyncio
async def test_slow_creation_does_not_block_unrelated_calls():
    """A slow ``_create_item`` must not stall the rest of the manager's surface.

    Regression guard: the manager lock used to be held across ``_create_item``,
    so every method that takes it — ``is_alive``, ``liveness_status``,
    ``close``, ``maybe_close_if_idle``, and a cache-hit ``get`` — queued behind
    a connect that can run for minutes. Fail-fast callers then had to hand-roll
    a bypass at each site, which is how the same defect kept resurfacing.
    """

    gate = asyncio.Event()
    created = MockItem()

    class SlowManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            await gate.wait()
            return created

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, None)

    manager = SlowManager(SystemType.COMMUNITY, "src", SessionId.from_int(1), "nm")

    get_task = asyncio.create_task(manager.get())
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # Every one of these would have blocked until the create finished.
    assert await asyncio.wait_for(manager.is_alive(), timeout=1.0) is False
    assert await asyncio.wait_for(manager.liveness_status(), timeout=1.0) == (
        ResourceLivenessStatus.OFFLINE,
        "No item cached",
    )
    assert (
        await asyncio.wait_for(
            manager.maybe_close_if_idle(timeout_seconds=0.0, now=1.0), timeout=1.0
        )
        is False
    )

    gate.set()
    assert await get_task is created


@pytest.mark.asyncio
async def test_concurrent_get_creates_once():
    """Concurrent cache misses share one creation and one cached item.

    Single-flight now comes from the creation lock rather than from holding the
    manager lock across the create, so it has to be asserted directly.
    """
    gate = asyncio.Event()
    creates = 0
    created = MockItem()

    class SlowManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            nonlocal creates
            creates += 1
            await gate.wait()
            return created

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, None)

    manager = SlowManager(SystemType.COMMUNITY, "src", SessionId.from_int(1), "nm")

    tasks = [asyncio.create_task(manager.get()) for _ in range(5)]
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    gate.set()

    assert await asyncio.gather(*tasks) == [created] * 5
    assert creates == 1


@pytest.mark.asyncio
async def test_classify_liveness_exceptions(monkeypatch):
    """Covers lines 961-962, 969-977: Exception handling in _classify_liveness."""

    class DummyManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            pass

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, None)

        async def _ensure_item(self):
            return MockItem()

    manager = DummyManager(SystemType.COMMUNITY, "src", SessionId.from_int(1), "nm")
    # Patch _ensure_item to raise AuthenticationError
    monkeypatch.setattr(
        manager, "_ensure_item", AsyncMock(side_effect=AuthenticationError("authfail"))
    )
    result = await manager._classify_liveness(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.UNAUTHORIZED
    assert "authfail" in result[1]

    # Patch _ensure_item to raise ConfigurationError
    monkeypatch.setattr(
        manager, "_ensure_item", AsyncMock(side_effect=ConfigurationError("cfgfail"))
    )
    result = await manager._classify_liveness(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.MISCONFIGURED
    assert "cfgfail" in result[1]

    # Patch _ensure_item to raise SessionCreationError (configuration issue)
    monkeypatch.setattr(
        manager, "_ensure_item", AsyncMock(side_effect=SessionCreationError("scfail"))
    )
    result = await manager._classify_liveness(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.MISCONFIGURED
    assert "scfail" in result[1]

    # Patch _ensure_item to raise SessionCreationError (connection failure)
    monkeypatch.setattr(
        manager,
        "_ensure_item",
        AsyncMock(side_effect=SessionCreationError("connection refused")),
    )
    result = await manager._classify_liveness(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.OFFLINE
    assert "connection refused" in result[1]

    # Patch _ensure_item to raise generic Exception
    monkeypatch.setattr(
        manager, "_ensure_item", AsyncMock(side_effect=RuntimeError("boom!"))
    )
    result = await manager._classify_liveness(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.UNKNOWN
    assert "boom!" in result[1]


@pytest.mark.asyncio
async def test_liveness_status_logs_and_modes(caplog):
    """Covers lines 1081-1089: Logging and return in liveness_status for both modes."""

    class DummyManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            return MockItem()

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, "ok")

        async def _ensure_item(self):
            return MockItem()

    manager = DummyManager(SystemType.COMMUNITY, "src", SessionId.from_int(1), "nm")
    # Mode: ensure_item=True
    with caplog.at_level("INFO"):
        status, detail = await manager.liveness_status(ensure_item=True)
        assert status == ResourceLivenessStatus.ONLINE
        assert "provisioning" in caplog.text or "cached-only" in caplog.text
        assert "Liveness check" in caplog.text
    # Mode: ensure_item=False (simulate cached item)
    manager._item_cache = MockItem()
    with caplog.at_level("INFO"):
        status, detail = await manager.liveness_status(ensure_item=False)
        assert status == ResourceLivenessStatus.ONLINE
        assert "cached-only" in caplog.text or "provisioning" in caplog.text


@pytest.mark.asyncio
async def test_close_swallows_item_close_exception(caplog):
    """close() swallows exceptions from item.close() with a WARNING log.

    The new close() path delegates the actual close to
    ``_close_captured_item``, which catches any exception from
    ``item.close()`` and logs at WARNING.  ``close()`` itself never
    raises.
    """

    class DummyManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            return MockItem()

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, None)

    manager = DummyManager(SystemType.COMMUNITY, "src", SessionId.from_int(1), "nm")
    item = MockItem()
    manager._item_cache = item
    item.close = AsyncMock(side_effect=Exception("close failed"))

    with caplog.at_level("WARNING"):
        await manager.close()  # must not raise

    item.close.assert_called_once()
    assert manager._item_cache is None
    # The warning from _close_captured_item should be present.
    assert any(
        r.levelname == "WARNING"
        and "Error closing item" in r.getMessage()
        and "community:src:1" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_static_community_session_manager_has_correct_source():
    """Test that StaticCommunitySessionManager sets source to 'config'."""
    from deephaven_mcp.resource_manager import StaticCommunitySessionManager, SystemType

    manager = StaticCommunitySessionManager(
        name="test-session",
        session_id=SessionId.from_int(123),
        session_config=_stub_session_config(name="test-session"),
        timeouts=CommunityClientTimeouts(),
    )

    assert manager.system == "community"
    assert manager.origin.value == "static"
    assert manager.system_type == SystemType.COMMUNITY
    assert manager.session_id == "123"
    assert str(manager.qualified_session_id) == "community:community:123"
    assert manager.name == "test-session"


@pytest.mark.asyncio
async def test_community_session_manager_session_config_property_returns_construction_argument():
    """CommunitySessionManager.session_config returns the typed declaration passed at construction."""
    from deephaven_mcp.resource_manager import StaticCommunitySessionManager

    cfg = _stub_session_config(name="cfg-prop")
    manager = StaticCommunitySessionManager(
        name="cfg-prop",
        session_id=SessionId.from_int(456),
        session_config=cfg,
        timeouts=CommunityClientTimeouts(),
    )

    assert manager.session_config is cfg


@pytest.mark.asyncio
async def test_community_session_manager_check_liveness_offline(monkeypatch):
    """Covers line 1698: CommunitySessionManager._check_liveness returns OFFLINE if is_alive() is False."""
    from deephaven_mcp.resource_manager import StaticCommunitySessionManager

    mgr = StaticCommunitySessionManager(
        name="test",
        session_id=SessionId.from_int(7),
        session_config=_stub_session_config(name="test"),
        timeouts=CommunityClientTimeouts(),
    )
    mock_session = Mock()
    mock_session.is_alive = AsyncMock(return_value=False)
    result = await mgr._check_liveness(mock_session)
    assert result == (ResourceLivenessStatus.OFFLINE, "Session not alive")


@pytest.mark.asyncio
async def test_enterprise_session_manager_check_liveness_offline(monkeypatch):
    """Covers line 2137: EnterpriseSessionManager._check_liveness returns OFFLINE if is_alive() is False."""

    async def dummy_creation(source, name):
        return Mock()

    mgr = EnterpriseSessionManager(
        "src",
        SessionId.from_int(11),
        "nm",
        dummy_creation,
        SessionOrigin.DISCOVERED,
    )
    mock_session = Mock()
    mock_session.is_alive = AsyncMock(return_value=False)
    result = await mgr._check_liveness(mock_session)
    assert result == (ResourceLivenessStatus.OFFLINE, "Session not alive")


# Additional obvious tests: public API error handling for BaseItemManager
@pytest.mark.asyncio
async def test_get_raises_if_create_item_fails(monkeypatch):
    """Test that get() raises if _create_item fails with uncaught exception."""

    class DummyManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            raise RuntimeError("fail-create")

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, None)

    manager = DummyManager(SystemType.COMMUNITY, "src", SessionId.from_int(1), "nm")
    with pytest.raises(RuntimeError, match="fail-create"):
        await manager.get()


class TestCommunitySessionManager:
    """Tests for the CommunitySessionManager class."""

    @pytest.mark.asyncio
    @patch("deephaven_mcp.client.CoreSession.from_session_config")
    async def test_create_item(self, mock_from_session_config):
        """Test that _create_item correctly calls CoreSession.from_session_config."""
        from deephaven_mcp.resource_manager import StaticCommunitySessionManager

        mock_from_session_config.return_value = "mock_session"
        session_config = _stub_session_config(name="test_community")
        manager = StaticCommunitySessionManager(
            name="test_community",
            session_config=session_config,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(1),
        )
        session = await manager._create_item()
        mock_from_session_config.assert_awaited_once_with(
            session_config, manager._timeouts
        )
        assert session == "mock_session"

    @pytest.mark.asyncio
    @patch("deephaven_mcp.client.CoreSession.from_session_config")
    async def test_create_item_raises_exception(self, mock_from_session_config):
        """Test that _create_item raises SessionCreationError on failure."""
        from deephaven_mcp.resource_manager import StaticCommunitySessionManager

        mock_from_session_config.side_effect = Exception("Connection failed")
        manager = StaticCommunitySessionManager(
            name="test_community",
            session_config=_stub_session_config(name="test_community"),
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(2),
        )
        with pytest.raises(SessionCreationError, match="Connection failed"):
            await manager._create_item()

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the session's is_alive method."""
        from deephaven_mcp.resource_manager import StaticCommunitySessionManager

        manager = StaticCommunitySessionManager(
            name="test_community",
            session_config=_stub_session_config(name="test_community"),
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(3),
        )
        mock_session = AsyncMock()
        mock_session.is_alive.return_value = True
        result = await manager._check_liveness(mock_session)
        mock_session.is_alive.assert_awaited_once()
        assert result == (ResourceLivenessStatus.ONLINE, None)


class TestEnterpriseSessionManager:
    """Tests for the EnterpriseSessionManager class."""


def test_enterprise_session_manager_constructor():
    """Explicitly test the constructor for coverage (lines 519-520)."""
    from deephaven_mcp.resource_manager import EnterpriseSessionManager

    def dummy_creation(source, name):
        pass

    mgr = EnterpriseSessionManager(
        "src",
        SessionId.from_int(42),
        "nm",
        dummy_creation,
        SessionOrigin.DISCOVERED,
    )
    assert mgr._creation_function is dummy_creation
    assert mgr.system == "src"
    # Enterprise session managers carry an ``origin`` (DYNAMIC or DISCOVERED).
    assert mgr.origin is SessionOrigin.DISCOVERED
    assert mgr.name == "nm"
    assert mgr.session_id == "42"
    assert str(mgr.qualified_session_id) == "enterprise:src:42"
    assert mgr.system_type.value == "enterprise"


@pytest.mark.asyncio
async def test_create_item_success_covers_try():
    """Covers the try/return branch of _create_item (line 539-540)."""
    mock_session = AsyncMock()

    async def creation(source, name):
        return mock_session

    mgr = EnterpriseSessionManager(
        "src",
        SessionId.from_int(1),
        "nm",
        creation,
        SessionOrigin.DISCOVERED,
    )
    result = await mgr._create_item()
    assert result is mock_session


@pytest.mark.asyncio
async def test_create_item_exception_covers_except():
    """Covers the except/raise branch of _create_item (lines 541-542)."""

    async def creation(source, name):
        raise RuntimeError("fail")

    mgr = EnterpriseSessionManager(
        "src",
        SessionId.from_int(2),
        "nm",
        creation,
        SessionOrigin.DISCOVERED,
    )
    with pytest.raises(
        SessionCreationError, match="Failed to create enterprise session for nm: fail"
    ):
        await mgr._create_item()


@pytest.mark.asyncio
async def test_check_liveness_covers_return():
    """Covers line 559: return await item.is_alive()."""
    mgr = EnterpriseSessionManager(
        "src",
        SessionId.from_int(3),
        "nm",
        AsyncMock(),
        SessionOrigin.DISCOVERED,
    )
    mock_session = AsyncMock()
    mock_session.is_alive = AsyncMock(return_value=True)
    result = await mgr._check_liveness(mock_session)
    assert result == (ResourceLivenessStatus.ONLINE, None)


@pytest.mark.asyncio
async def test_check_liveness_exception():
    """Covers that _check_liveness lets exceptions propagate (handled by liveness_status)."""
    mgr = EnterpriseSessionManager(
        "src",
        SessionId.from_int(4),
        "nm",
        AsyncMock(),
        SessionOrigin.DISCOVERED,
    )
    mock_session = AsyncMock()
    mock_session.is_alive = AsyncMock(side_effect=Exception("fail"))

    # _check_liveness no longer handles exceptions; they propagate up
    with pytest.raises(Exception, match="fail"):
        await mgr._check_liveness(mock_session)

    @pytest.mark.asyncio
    async def test_create_item_success(self):
        """Test that _create_item successfully calls the creation function."""
        mock_session = AsyncMock()
        mock_creation_function = AsyncMock(return_value=mock_session)

        manager = EnterpriseSessionManager(
            "test_source",
            SessionId.from_int(10),
            "test_session",
            mock_creation_function,
            SessionOrigin.DISCOVERED,
        )

        result = await manager._create_item()

        assert result is mock_session
        mock_creation_function.assert_awaited_once_with("test_source", "test_session")

    @pytest.mark.asyncio
    async def test_create_item_raises_session_creation_error(self):
        """Test that _create_item raises SessionCreationError when creation function fails."""
        mock_creation_function = AsyncMock(side_effect=Exception("Creation failed"))

        manager = EnterpriseSessionManager(
            "test_source",
            SessionId.from_int(11),
            "test_session",
            mock_creation_function,
            SessionOrigin.DISCOVERED,
        )

        with pytest.raises(
            SessionCreationError,
            match="Failed to create enterprise session for test_session: Creation failed",
        ):
            await manager._create_item()

        mock_creation_function.assert_awaited_once_with("test_source", "test_session")

    @pytest.mark.asyncio
    async def test_get_success(self):
        """Test that get() successfully returns a session from the creation function."""
        mock_session = AsyncMock()
        mock_session.is_alive = AsyncMock(return_value=True)
        mock_creation_function = AsyncMock(return_value=mock_session)

        manager = EnterpriseSessionManager(
            "test_source",
            SessionId.from_int(12),
            "test_session",
            mock_creation_function,
            SessionOrigin.DISCOVERED,
        )

        result = await manager.get()

        assert result is mock_session
        mock_creation_function.assert_awaited_once_with("test_source", "test_session")

    @pytest.mark.asyncio
    async def test_close(self):
        """Test that close correctly closes the cached session."""
        # Create a manager with a mock creation function
        mock_creation_function = AsyncMock()
        manager = EnterpriseSessionManager(
            "test_source",
            SessionId.from_int(13),
            "test_session",
            mock_creation_function,
            SessionOrigin.DISCOVERED,
        )
        mock_session = AsyncMock()

        # Set up the mock session to pass the liveness check
        mock_session.is_alive = AsyncMock(return_value=True)

        # Manually set the cached item
        manager._item_cache = mock_session

        # Call close and verify the session is closed
        await manager.close()
        mock_session.close.assert_awaited_once()
        assert manager._item_cache is None

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the session's is_alive method."""
        # Create a manager with a mock creation function
        mock_creation_function = AsyncMock()
        manager = EnterpriseSessionManager(
            "test_source",
            SessionId.from_int(14),
            "test_session",
            mock_creation_function,
            SessionOrigin.DISCOVERED,
        )

        # Test with a mock session where is_alive returns True
        mock_session = AsyncMock()
        mock_session.is_alive = AsyncMock(return_value=True)
        assert await manager._check_liveness(mock_session) is True
        mock_session.is_alive.assert_awaited_once()

        # Test with a mock session where is_alive returns False
        mock_session = AsyncMock()
        mock_session.is_alive = AsyncMock(return_value=False)
        assert await manager._check_liveness(mock_session) is False
        mock_session.is_alive.assert_awaited_once()

        # Test with a mock session where is_alive raises an exception
        mock_session = AsyncMock()
        mock_session.is_alive = AsyncMock(side_effect=Exception("Connection error"))
        # The _check_liveness method in EnterpriseSessionManager does not catch exceptions,
        # so we expect the exception to be raised
        with pytest.raises(Exception, match="Connection error"):
            await manager._check_liveness(mock_session)


class TestCorePlusSessionFactoryManager:
    """Tests for the CorePlusSessionFactoryManager."""

    def _make_creds(self):
        from deephaven_mcp.auth.credentials import PasswordCredentials

        return PasswordCredentials(username="u", password="p")

    def test_initialization(self):
        """Test that the manager initializes with the correct properties."""
        system_config = _stub_enterprise_config(name="test_factory")
        creds = self._make_creds()
        manager = CorePlusSessionFactoryManager(
            name="test_factory",
            system_config=system_config,
            creds=creds,
            timeouts=EnterpriseClientTimeouts(),
        )

        assert manager.system_type == SystemType.ENTERPRISE
        assert manager.system == "test_factory"
        assert manager.name == "factory"
        # Factory managers are discriminated by class, not by an enum.
        assert isinstance(manager, CorePlusSessionFactoryManager)
        assert manager._system_config is system_config
        assert manager._creds is creds

    """Tests for the CorePlusSessionFactoryManager class."""

    @pytest.mark.asyncio
    @patch(
        "deephaven_mcp.client.CorePlusSessionFactory.from_credentials",
        new_callable=AsyncMock,
    )
    async def test_create_item(self, mock_from_credentials):
        """Test that _create_item correctly calls the factory's from_credentials method."""
        mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
        mock_from_credentials.return_value = mock_factory

        system_config = _stub_enterprise_config(name="test_factory")
        creds = self._make_creds()
        manager = CorePlusSessionFactoryManager(
            name="test_factory",
            system_config=system_config,
            creds=creds,
            timeouts=EnterpriseClientTimeouts(),
        )

        created_factory = await manager._create_item()

        assert created_factory is mock_factory
        mock_from_credentials.assert_awaited_once_with(
            system_config, creds, manager._timeouts
        )

    @pytest.mark.asyncio
    @patch(
        "deephaven_mcp.client.CorePlusSessionFactory.from_credentials",
        new_callable=AsyncMock,
    )
    async def test_create_item_timeout(self, mock_from_credentials):
        """Test that _create_item raises DeephavenConnectionError on timeout."""
        from deephaven_mcp._exceptions import DeephavenConnectionError

        # Simulate a timeout by making from_credentials hang
        async def slow_operation(system_config, creds, timeouts):
            await asyncio.sleep(20)  # Longer than default timeout

        mock_from_credentials.side_effect = slow_operation

        system_config = _stub_enterprise_config(name="test_factory")
        creds = self._make_creds()
        # The factory connect timeout is now sourced from
        # ``EnterpriseClientTimeouts.session_connect_timeout_seconds``
        # (per-system override was retired). Drive the test by
        # constructing a tight-timeout EnterpriseClientTimeouts.
        manager = CorePlusSessionFactoryManager(
            name="test_factory",
            system_config=system_config,
            creds=creds,
            timeouts=EnterpriseClientTimeouts(session_connect_timeout_seconds=0.1),
        )

        with pytest.raises(
            DeephavenConnectionError, match="timed out after 0.1 seconds"
        ):
            await manager._create_item()

        mock_from_credentials.assert_awaited_once_with(
            system_config, creds, manager._timeouts
        )

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the item's ping method."""
        mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
        mock_factory.controller_client.is_poisoned = False
        manager = CorePlusSessionFactoryManager(
            name="test_factory",
            system_config=_stub_enterprise_config(name="test_factory"),
            creds=self._make_creds(),
            timeouts=EnterpriseClientTimeouts(),
        )

        # Test when ping returns True
        mock_factory.ping.return_value = True
        assert await manager._check_liveness(mock_factory) == (
            ResourceLivenessStatus.ONLINE,
            None,
        )
        mock_factory.ping.assert_awaited_once()

        # Test when ping returns False
        mock_factory.ping.reset_mock()
        mock_factory.ping.return_value = False
        assert await manager._check_liveness(mock_factory) == (
            ResourceLivenessStatus.OFFLINE,
            "Ping returned False",
        )
        mock_factory.ping.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_check_liveness_poisoned_controller(self):
        """A wedged controller subscription reports OFFLINE without pinging."""
        mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
        mock_factory.controller_client.is_poisoned = True
        manager = CorePlusSessionFactoryManager(
            name="test_factory",
            system_config=_stub_enterprise_config(name="test_factory"),
            creds=self._make_creds(),
            timeouts=EnterpriseClientTimeouts(),
        )

        status, detail = await manager._check_liveness(mock_factory)

        assert status == ResourceLivenessStatus.OFFLINE
        assert detail is not None and "connection" in detail
        mock_factory.ping.assert_not_awaited()

    @staticmethod
    def _make_factory_with_controller(poisoned: bool):
        """Return a mock factory whose controller_client.is_poisoned == poisoned."""
        controller = MagicMock()
        controller.is_poisoned = poisoned
        factory = MagicMock()
        factory.controller_client = controller
        # The healer closes the detached factory itself, so close must await.
        factory.close = AsyncMock()
        return factory, controller

    def _make_factory_manager(self):
        return CorePlusSessionFactoryManager(
            name="test_factory",
            system_config=_stub_enterprise_config(name="test_factory"),
            creds=self._make_creds(),
            timeouts=EnterpriseClientTimeouts(),
        )

    @pytest.mark.asyncio
    async def test_get_controller_client_healthy(self):
        """A healthy controller is returned without recreating the factory."""
        manager = self._make_factory_manager()
        factory, controller = self._make_factory_with_controller(poisoned=False)
        manager._item_cache = factory
        manager.get = AsyncMock(return_value=factory)
        manager.close = AsyncMock()

        result = await manager.get_controller_client()

        assert result is controller
        manager.get.assert_awaited_once()
        manager.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_controller_client_healthy_resets_episode(self):
        """A healthy call clears any recorded outage state."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._item_cache = factory
        manager.get = AsyncMock(return_value=factory)
        # Simulate a stale outage left over from a prior wedge.
        manager._healer.note_wedged(time.monotonic())
        manager._healer._outage.attempts = 4

        await manager.get_controller_client()

        assert manager._healer._outage is None

    @pytest.mark.asyncio
    async def test_get_controller_client_fails_fast_mid_episode_empty_cache(self):
        """An in-flight recreate does not drag callers into an inline creation.

        Between the healer's detach and its rebuild the cache is empty. Falling
        through to ``get()`` there would start a competing creation and block
        the caller for ``session_connect_timeout_seconds`` -- the very wait this
        whole path exists to avoid.
        """
        manager = self._make_factory_manager()
        manager._item_cache = None
        manager._healer.note_wedged(time.monotonic())
        manager._healer._outage.attempts = 2
        manager._get_unlocked = AsyncMock()
        manager.get = AsyncMock()

        with pytest.raises(DeephavenConnectionError) as exc_info:
            await manager.get_controller_client()

        assert CONTROLLER_SUBSCRIBING_ERROR_CODE in str(exc_info.value)
        # The blocking creation must never have been attempted, and the gate
        # must not have touched the lock a rebuild may be holding.
        manager.get.assert_not_called()
        manager._get_unlocked.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_controller_client_creates_when_idle_and_empty(self):
        """With no episode in progress an empty cache still creates normally."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._item_cache = None
        manager.get = AsyncMock(return_value=factory)

        result = await manager.get_controller_client()

        assert result is factory.controller_client
        manager.get.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_controller_client_fails_fast_while_a_rebuild_holds_the_lock(
        self,
    ):
        """The fail-fast gate does not queue behind an in-flight recreate.

        Regression guard: ``get()`` holds the manager lock for the whole of a
        factory creation, including the healer's rebuild. A gate that took that
        lock would block the caller for ``session_connect_timeout_seconds`` --
        the exact wait this path exists to avoid.
        """
        manager = self._make_factory_manager()
        manager._item_cache = None
        manager._healer.note_wedged(time.monotonic())

        async with manager._lock:
            with pytest.raises(DeephavenConnectionError) as exc_info:
                await manager.get_controller_client()

        assert CONTROLLER_SUBSCRIBING_ERROR_CODE in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_controller_client_poisoned_raises_with_code(self):
        """A wedged subscription fails fast with the CONTROLLER_SUBSCRIBING code."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = factory
        manager.get = AsyncMock(return_value=factory)
        manager.close = AsyncMock()

        with pytest.raises(DeephavenConnectionError) as exc_info:
            await manager.get_controller_client()

        msg = str(exc_info.value)
        assert CONTROLLER_SUBSCRIBING_ERROR_CODE in msg
        assert "recreate attempt" in msg
        # The message names the escape hatch and the system to pass to it.
        assert "enterprise_controller_reconnect" in msg
        assert "system='test_factory'" in msg
        # get_controller_client no longer recreates inline; the healer owns that.
        manager.close.assert_not_called()
        # The outage start was recorded for the status message.
        assert manager._healer._outage is not None

    @pytest.mark.asyncio
    async def test_get_controller_client_poisoned_preserves_subscribing_since(self):
        """A second poisoned call keeps the original outage-start timestamp."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = factory
        manager.get = AsyncMock(return_value=factory)

        with pytest.raises(DeephavenConnectionError):
            await manager.get_controller_client()
        first_outage = manager._healer._outage

        with pytest.raises(DeephavenConnectionError):
            await manager.get_controller_client()

        assert manager._healer._outage is first_outage

    @pytest.mark.asyncio
    async def test_get_controller_client_poisoned_reports_countdown(self):
        """When a next-recreate time is scheduled, the message reports its countdown."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = factory
        manager.get = AsyncMock(return_value=factory)
        manager._healer.note_wedged(time.monotonic())
        manager._healer._outage.attempts = 2
        manager._healer._outage.next_recreate = time.monotonic() + 25.0

        with pytest.raises(DeephavenConnectionError) as exc_info:
            await manager.get_controller_client()

        msg = str(exc_info.value)
        assert "next automatic recreate is in ~" in msg
        assert "2 recreate attempt" in msg

    def test_peek_controller_poisoned_none_when_no_cache(self):
        """With no cached factory there is nothing to heal."""
        manager = self._make_factory_manager()
        assert manager._item_cache is None
        assert manager.peek_controller_poisoned() is None

    @pytest.mark.asyncio
    async def test_liveness_status_fails_fast_mid_outage_with_empty_cache(self):
        """An active probe does not start a creation the healer already owns.

        Regression guard: ``system status --connect`` passes
        ``ensure_item=True``, so without this gate it would block for
        ``session_connect_timeout_seconds`` during exactly the outage it is
        being run to diagnose.
        """
        manager = self._make_factory_manager()
        manager._item_cache = None
        manager._healer.note_wedged(time.monotonic())
        manager._get_unlocked = AsyncMock()

        status, detail = await manager.liveness_status(ensure_item=True)

        assert status is ResourceLivenessStatus.OFFLINE
        assert detail == "controller connection unavailable"
        manager._get_unlocked.assert_not_called()

    @pytest.mark.asyncio
    async def test_liveness_status_probes_normally_when_no_outage(self):
        """With no outage the probe runs the ordinary base-class path."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        factory.ping = AsyncMock(return_value=True)
        manager._item_cache = factory

        assert await manager.liveness_status() == (ResourceLivenessStatus.ONLINE, None)

    def test_peek_controller_poisoned_reflects_cached_controller(self):
        """The peek reports the cached controller's poison state without creating."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = factory

        assert manager.peek_controller_poisoned() is True

        factory.controller_client.is_poisoned = False
        assert manager.peek_controller_poisoned() is False

    @pytest.mark.asyncio
    async def test_peek_controller_poisoned_does_not_take_the_lock(self):
        """The peek must not queue behind an in-flight factory creation.

        Regression guard: ``get()`` holds the manager lock across creation, so
        a locked peek would block the fail-fast and reconnect paths behind the
        very rebuild they report on.
        """
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = factory

        async with manager._lock:
            assert manager.peek_controller_poisoned() is True

    @pytest.mark.asyncio
    async def test_rebuild_factory_replaces_a_wedged_factory(self):
        """The wedged factory is discarded and closed, then a replacement created."""
        manager = self._make_factory_manager()
        poisoned_factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        manager.get = AsyncMock()

        await manager.rebuild_factory()

        poisoned_factory.close.assert_awaited_once()
        manager.get.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rebuild_factory_creates_when_cache_is_empty(self):
        """A rebuild following a failed attempt still repopulates the cache."""
        manager = self._make_factory_manager()
        assert manager._item_cache is None
        manager.get = AsyncMock()

        await manager.rebuild_factory()

        manager.get.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rebuild_factory_finishes_teardown_when_canceled(self):
        """Canceling mid-teardown still closes the factory the cache released.

        Regression guard: the detach drops the cache's only reference, so a
        healer stop landing inside the close would otherwise strand the
        factory's channels and threads with nothing left to release them.
        """
        manager = self._make_factory_manager()
        poisoned_factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        closing = asyncio.Event()
        closed = asyncio.Event()

        async def _slow_close(item):
            closing.set()
            await asyncio.sleep(0.05)
            closed.set()

        manager._close_captured_item = _slow_close
        manager.get = AsyncMock()

        task = asyncio.create_task(manager.rebuild_factory())
        await asyncio.wait_for(closing.wait(), timeout=1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert closed.is_set()
        # Cancellation still won: no replacement was created.
        manager.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_rebuild_factory_swallows_creation_failure(self):
        """A failed recreate is logged rather than raised, leaving the cache empty."""
        manager = self._make_factory_manager()
        poisoned_factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        manager.get = AsyncMock(side_effect=RuntimeError("still down"))

        await manager.rebuild_factory()

        poisoned_factory.close.assert_awaited_once()
        assert manager._item_cache is None

    @pytest.mark.asyncio
    async def test_rebuild_factory_does_not_close_a_factory_that_healed(self):
        """A controller that recovers before teardown is never torn down.

        Regression guard for the check-then-close race: the poison check and
        the close used to take ``_lock`` separately, so a factory that healed
        (or was replaced) between the healer's peek and the rebuild could be
        closed anyway.
        """
        manager = self._make_factory_manager()
        poisoned_factory, controller = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        manager.get = AsyncMock()
        # The controller recovers between the healer's peek and the rebuild.
        controller.is_poisoned = False

        await manager.rebuild_factory()

        poisoned_factory.close.assert_not_called()
        assert manager._item_cache is poisoned_factory

    @pytest.mark.asyncio
    async def test_detach_poisoned_item_returns_none_when_healthy(self):
        """The atomic detach leaves a healthy cached factory alone."""
        manager = self._make_factory_manager()
        healthy_factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._item_cache = healthy_factory

        assert await manager._detach_poisoned_item() is None
        assert manager._item_cache is healthy_factory

    @pytest.mark.asyncio
    async def test_detach_poisoned_item_detaches_wedged(self):
        """The atomic detach removes and returns a wedged factory."""
        manager = self._make_factory_manager()
        poisoned_factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        manager._last_accessed = time.monotonic()

        assert await manager._detach_poisoned_item() is poisoned_factory
        assert manager._item_cache is None
        assert manager._last_accessed is None

    @pytest.mark.asyncio
    async def test_detach_poisoned_item_returns_none_when_empty(self):
        """The atomic detach is a no-op when nothing is cached."""
        manager = self._make_factory_manager()
        assert await manager._detach_poisoned_item() is None

    @pytest.mark.asyncio
    async def test_creating_a_factory_starts_the_healer(self):
        """The healer runs as soon as there is a factory to watch."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._healer._heal_once = AsyncMock()

        assert manager._healer._task is None
        with patch(
            "deephaven_mcp.client.CorePlusSessionFactory.from_credentials",
            new_callable=AsyncMock,
            return_value=factory,
        ):
            await manager.get()
        try:
            task = manager._healer._task
            assert task is not None and not task.done()
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_repeated_creation_does_not_start_a_second_healer(self):
        """A rebuild re-enters _create_item; the healer start stays idempotent."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._healer._heal_once = AsyncMock()

        with patch(
            "deephaven_mcp.client.CorePlusSessionFactory.from_credentials",
            new_callable=AsyncMock,
            return_value=factory,
        ):
            await manager.get()
            task = manager._healer._task
            manager._item_cache = None
            await manager.get()
        try:
            assert manager._healer._task is task
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_close_stops_the_healer_and_closes_the_factory(self):
        """Close leaves neither a running healer nor a cached factory."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._item_cache = factory
        manager._healer._heal_once = AsyncMock()
        manager._healer.start()
        task = manager._healer._task

        await manager.close()

        assert manager._item_cache is None
        assert manager._healer._task is None
        assert task.done()
        factory.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_serializes_against_factory_creation(self):
        """Close holds the creation lock that ``_create_item`` starts healers under.

        Regression guard: stopping the healer outside that lock let a
        concurrent creation start one for a factory this close then dropped,
        leaving an orphan loop polling forever.
        """
        manager = self._make_factory_manager()
        manager._healer._heal_once = AsyncMock()
        manager._healer.start()

        async with manager._creation_lock:
            close_task = asyncio.create_task(manager.close())
            await asyncio.sleep(0)
            assert not close_task.done()
            assert manager._healer._task is not None

        await close_task
        assert manager._healer._task is None

    @pytest.mark.asyncio
    async def test_close_then_get_restarts_the_healer(self):
        """A manager reused after close gets a healer again with its new factory."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=False)
        manager._healer._heal_once = AsyncMock()

        with patch(
            "deephaven_mcp.client.CorePlusSessionFactory.from_credentials",
            new_callable=AsyncMock,
            return_value=factory,
        ):
            await manager.get()
            await manager.close()
            assert manager._healer._task is None

            await manager.get()
        try:
            task = manager._healer._task
            assert task is not None and not task.done()
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_close_while_wedged_leaves_the_manager_reusable(self):
        """Closing mid-outage clears it, so a reused manager creates normally.

        Regression guard: a surviving outage plus the empty cache close leaves
        behind would make ``get_controller_client`` fail fast forever, since
        the stopped healer is no longer there to rebuild.
        """
        manager = self._make_factory_manager()
        poisoned_factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        manager._healer._heal_once = AsyncMock()
        manager._healer.start()
        manager._healer.note_wedged(time.monotonic())

        await manager.close()
        assert manager._healer._outage is None

        healthy_factory, controller = self._make_factory_with_controller(poisoned=False)
        manager.get = AsyncMock(return_value=healthy_factory)

        assert await manager.get_controller_client() is controller

    @pytest.mark.asyncio
    async def test_request_reconnect_is_delegated(self):
        """request_reconnect reports the healer's actionability decision."""
        manager = self._make_factory_manager()
        factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = factory
        manager._healer._heal_once = AsyncMock()

        # No healer running yet: nothing to nudge.
        assert await manager.request_reconnect() is False

        manager._healer.start()
        try:
            assert await manager.request_reconnect() is True
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_healer_replaces_a_wedged_factory_end_to_end(self):
        """Manager and healer together discard a wedged factory and rebuild it."""
        manager = self._make_factory_manager()
        poisoned_factory, _ = self._make_factory_with_controller(poisoned=True)
        manager._item_cache = poisoned_factory
        replacement, _ = self._make_factory_with_controller(poisoned=False)
        manager.get = AsyncMock(return_value=replacement)

        # The deadline is already due, so the pass rebuilds.
        manager._healer.note_wedged(time.monotonic())
        manager._healer._outage.next_recreate = time.monotonic()
        await manager._healer._heal_once()

        poisoned_factory.close.assert_awaited_once()
        manager.get.assert_awaited_once()


class TestDynamicCommunitySessionManager:
    """Tests for DynamicCommunitySessionManager class."""

    def test_init_stores_launched_session(self):
        """Test that __init__ stores the launched session."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="psk",
            auth_token="test-token",
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        assert manager.launched_session == launched_session
        assert manager.launched_session.auth_token == "test-token"

    def test_to_dict_verbose_docker(self):
        """Test that to_dict(verbose=True) returns the launch-specific fields for Docker."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="psk",
            auth_token="test-token",
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        result = manager.to_dict(verbose=True)

        # Common identity is always present.
        assert result["id"] == str(manager.qualified_session_id)
        assert result["session_name"] == "test-session"
        # Launch-specific connection details are added when verbose.
        assert result["connection_url"] == "http://localhost:10000"
        # connection_url_with_auth is excluded for security
        assert result["port"] == 10000
        assert result["container_id"] == "test_container"
        assert "process_id" not in result
        assert result["auth_type"] == "PSK"
        assert result["launch_method"] == "docker"

    def test_to_dict_verbose_pip_process(self):
        """Test that to_dict(verbose=True) correctly identifies the pip launch method."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        launched_session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        result = manager.to_dict(verbose=True)

        assert result["launch_method"] == "python"
        assert result["process_id"] == 12345
        assert "container_id" not in result

    def test_to_dict_verbose_without_auth_token(self):
        """Test that to_dict(verbose=True) handles a missing auth token."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        result = manager.to_dict(verbose=True)

        assert result["connection_url"] == "http://localhost:10000"
        # connection_url_with_auth is excluded for security

    def test_to_dict_verbose_anonymous_auth(self):
        """Test that to_dict(verbose=True) handles the Anonymous auth type."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        result = manager.to_dict(verbose=True)

        assert result["auth_type"] == "ANONYMOUS"

    def test_to_dict_common_identity(self):
        """to_dict returns the common session identity, not connection details."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="psk",
            auth_token="test-token",
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        result = manager.to_dict()

        assert result == {
            "id": str(manager.qualified_session_id),
            "type": manager.system_type.value,
            "system": manager.system,
            "session_name": manager.name,
            "origin": manager.origin.value,
        }
        # Compact (default) output excludes connection details.
        assert "connection_url" not in result
        assert "port" not in result
        assert "auth_type" not in result
        # verbose=True is a strict superset: identity plus connection details.
        verbose = manager.to_dict(verbose=True)
        assert verbose.items() >= result.items()
        assert "connection_url" in verbose

    @pytest.mark.asyncio
    async def test_close_success(self):
        """close() captures and closes the cached session, stops the launcher,
        and flips ``_is_stopped`` under a single critical section."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        cached_session = MagicMock()
        cached_session.close = AsyncMock()
        manager._item_cache = cached_session
        manager._last_accessed = 0.0

        with patch.object(
            launched_session, "stop", new_callable=AsyncMock
        ) as mock_stop:
            await manager.close()

        mock_stop.assert_awaited_once()
        cached_session.close.assert_awaited_once()
        assert manager._is_stopped is True
        assert manager._item_cache is None
        assert manager._last_accessed is None

    @pytest.mark.asyncio
    async def test_close_handles_cached_session_close_error(self):
        """An error closing the cached session is swallowed so the launcher
        still gets stopped."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        cached_session = MagicMock()
        cached_session.close = AsyncMock(side_effect=Exception("session close failed"))
        manager._item_cache = cached_session
        manager._last_accessed = 0.0

        with patch.object(
            launched_session, "stop", new_callable=AsyncMock
        ) as mock_stop:
            # Should not raise, just log warning
            await manager.close()

            # Launcher stop should still be called
            mock_stop.assert_awaited_once()
            cached_session.close.assert_awaited_once()
            assert manager._is_stopped is True
            assert manager._item_cache is None

    @pytest.mark.asyncio
    async def test_close_handles_session_stop_error(self):
        """An error stopping the launcher is logged but does not raise.

        The cached session close still runs first.
        """
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        cached_session = MagicMock()
        cached_session.close = AsyncMock()
        manager._item_cache = cached_session
        manager._last_accessed = 0.0

        with patch.object(
            launched_session, "stop", new_callable=AsyncMock
        ) as mock_stop:
            mock_stop.side_effect = Exception("Stop failed")
            # Should not raise, just log error
            await manager.close()

            mock_stop.assert_awaited_once()
            # Cached session close still happened
            cached_session.close.assert_awaited_once()
            assert manager._is_stopped is True

    def test_properties(self):
        """Test all property accessors."""
        mock_process = MagicMock()
        mock_process.pid = 12345

        launched_session = PythonLaunchedSession(
            host="testhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )
        config = {"host": "testhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        assert manager.connection_url == "http://testhost:10000"
        assert (
            manager.connection_url_with_auth == "http://testhost:10000"
        )  # anonymous, no token
        assert manager.port == 10000
        assert manager.launch_method == "python"
        assert manager.container_id is None
        assert manager.process_id == 12345

    @pytest.mark.asyncio
    async def test_close_then_get_with_cached_item_raises(self):
        """Cache-hit ``get()`` racing ``close()`` must raise once the close has
        completed its single critical section.

        Models the H3 race: a caller that wins ``self._lock`` *after* ``close()``
        flipped ``_is_stopped`` and cleared the cache must see a clear error
        rather than a stale cached session.
        """
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="race-cached",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        cached_session = MagicMock()
        cached_session.close = AsyncMock()
        manager._item_cache = cached_session
        manager._last_accessed = 0.0

        with patch.object(launched_session, "stop", new_callable=AsyncMock):
            await manager.close()

        assert manager._is_stopped is True
        assert manager._item_cache is None
        # Even though cache is now empty, _get_unlocked must check
        # _is_stopped before any cache check to make the contract clear.
        with pytest.raises(SessionCreationError, match="has been stopped"):
            await manager.get()

    @pytest.mark.asyncio
    async def test_get_unlocked_refuses_when_stopped_with_populated_cache(self):
        """If the cache slot is populated but ``_is_stopped`` is set, ``_get_unlocked``
        raises before returning the cached item.

        Tightens the H3 contract: the gate is on ``_is_stopped``, not on whether
        ``_item_cache`` was cleared.
        """
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="stopped-but-cached",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        cached_session = MagicMock()
        manager._item_cache = cached_session
        manager._last_accessed = 0.0
        manager._is_stopped = True

        with pytest.raises(SessionCreationError, match="has been stopped"):
            await manager.get()
        # Cache slot is left untouched by _get_unlocked — only close() clears it.
        assert manager._item_cache is cached_session

    @pytest.mark.asyncio
    async def test_get_in_flight_during_close_race(self):
        """``close()`` does not wait on an in-flight ``_create_item``.

        Creation runs outside the manager lock, so the close completes
        immediately. The session that creation then produces belongs to nobody:
        it is discarded and closed rather than published into the cache the
        close just emptied, and the retry hits the stopped-manager gate.
        """
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="race-inflight",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        gate = asyncio.Event()
        created_session = MagicMock()
        created_session.close = AsyncMock()

        async def _slow_create():
            await gate.wait()
            return created_session

        with patch.object(
            CommunitySessionManager,
            "_create_item",
            new=AsyncMock(side_effect=_slow_create),
        ):
            with patch.object(launched_session, "stop", new_callable=AsyncMock):
                get_task = asyncio.create_task(manager.get())
                # Let get_task reach the await inside _create_item.
                await asyncio.sleep(0)
                await asyncio.sleep(0)

                # close() must not queue behind the creation.
                await asyncio.wait_for(manager.close(), timeout=1.0)
                assert manager._is_stopped is True
                assert manager._item_cache is None

                gate.set()
                with pytest.raises(SessionCreationError, match="has been stopped"):
                    await get_task

        # The orphaned session was released rather than leaked or cached.
        created_session.close.assert_awaited_once()
        assert manager._item_cache is None

        # A subsequent get() must raise too.
        with pytest.raises(SessionCreationError, match="has been stopped"):
            await manager.get()

    @pytest.mark.asyncio
    async def test_create_item_refuses_after_close(self):
        """After ``close()``, a cache-miss ``get()`` must raise instead of recreating."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="stopped",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        # Patch the parent _create_item to a fast no-op so the first
        # get() succeeds without a real network call.
        with patch.object(
            CommunitySessionManager,
            "_create_item",
            new_callable=AsyncMock,
            return_value=MagicMock(),
        ):
            await manager.get()
            assert manager._item_cache is not None

            with patch.object(launched_session, "stop", new_callable=AsyncMock):
                await manager.close()

            assert manager._is_stopped is True
            assert manager._item_cache is None

            # Cache miss after close must NOT call parent _create_item;
            # the subclass override raises before delegating.
            with pytest.raises(SessionCreationError, match="has been stopped"):
                await manager.get()

    @pytest.mark.asyncio
    async def test_create_item_directly_refuses_when_stopped(self):
        """Calling ``_create_item`` directly after ``_is_stopped`` is set must raise.

        The override in ``DynamicCommunitySessionManager._create_item``
        is a defense-in-depth check that complements the ``_get_unlocked``
        gate. This test exercises it directly so the safety net is
        verified independently of the cache-miss caller path.
        """
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        manager = DynamicCommunitySessionManager(
            name="direct-stopped",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )
        manager._is_stopped = True

        with pytest.raises(SessionCreationError, match="has been stopped"):
            await manager._create_item()

    def test_process_id_none_when_no_process(self):
        """Test process_id returns None when there's no process."""
        launched_session = DockerLaunchedSession(
            host="testhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "testhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            session_config=_stub_session_config(),
            launched_session=launched_session,
            timeouts=CommunityClientTimeouts(),
            session_id=SessionId.from_int(0),
        )

        assert manager.process_id is None


# ---------------------------------------------------------------------------
# Per-manager idle-close path
#
# The manager owns the per-item idle check + close
# (:meth:`BaseItemManager.maybe_close_if_idle`); the registry-level sweep
# loop lives on :class:`~deephaven_mcp.resource_manager.Evictor` and is
# tested separately in ``test__evictor.py``.
# ---------------------------------------------------------------------------


class _StubItem:
    """Trivial AsyncClosable used as the cached item type."""

    def __init__(self):
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _StubStaticManager(BaseItemManager[_StubItem]):
    """In-memory manager that creates a fresh _StubItem on each cache miss.

    ``evicts_on_idle = False`` (the default).  Used to verify that
    non-evicting managers reconnect lazily after an idle close.
    """

    def __init__(self, name: str, *, session_id: int = 0):
        super().__init__(
            SystemType.COMMUNITY, "test", SessionId.from_int(session_id), name
        )
        self.create_count = 0
        self.extra_close_count = 0

    @override
    async def _create_item(self) -> _StubItem:
        self.create_count += 1
        return _StubItem()

    @override
    async def _check_liveness(self, item: _StubItem):
        return (ResourceLivenessStatus.ONLINE, None)


class _StubDynamicManager(_StubStaticManager):
    """Like ``_StubStaticManager`` but with ``evicts_on_idle = True``.

    Overrides :meth:`close` to count subclass-specific teardown calls so
    tests can assert that the polymorphic close path (not just the base
    cache clear) runs during idle eviction.
    """

    evicts_on_idle: ClassVar[bool] = True

    @override
    async def close(self) -> None:
        await super().close()
        self.extra_close_count += 1


# --- last_accessed tracking ---


@pytest.mark.asyncio
async def test_last_accessed_set_on_first_get():
    mgr = _StubStaticManager("a")
    assert mgr._last_accessed is None
    await mgr.get()
    assert mgr._last_accessed is not None
    assert mgr.create_count == 1


@pytest.mark.asyncio
async def test_last_accessed_refreshed_on_subsequent_get():
    mgr = _StubStaticManager("a")
    await mgr.get()
    first = mgr._last_accessed
    await asyncio.sleep(0.01)
    await mgr.get()
    assert mgr._last_accessed is not None
    assert mgr._last_accessed > first


# --- maybe_close_if_idle ---


@pytest.mark.asyncio
async def test_maybe_close_if_idle_never_accessed_returns_false():
    mgr = _StubStaticManager("a")
    closed = await mgr.maybe_close_if_idle(0.0, time.monotonic())
    assert closed is False


@pytest.mark.asyncio
async def test_maybe_close_if_idle_within_timeout_returns_false():
    mgr = _StubStaticManager("a")
    await mgr.get()
    closed = await mgr.maybe_close_if_idle(3600.0, time.monotonic())
    assert closed is False
    assert mgr._item_cache is not None


@pytest.mark.asyncio
async def test_maybe_close_if_idle_closes_after_timeout():
    mgr = _StubStaticManager("a")
    await mgr.get()
    cached = mgr._item_cache
    closed = await mgr.maybe_close_if_idle(0.1, mgr._last_accessed + 1000.0)
    assert closed is True
    assert mgr._item_cache is None
    assert mgr._last_accessed is None
    assert cached is not None
    assert cached.closed is True


@pytest.mark.asyncio
async def test_maybe_close_if_idle_with_no_cache_resets_timer():
    mgr = _StubStaticManager("a")
    mgr._last_accessed = time.monotonic() - 1000.0
    mgr._item_cache = None
    closed = await mgr.maybe_close_if_idle(0.1, time.monotonic())
    assert closed is False
    assert mgr._last_accessed is None


@pytest.mark.asyncio
async def test_maybe_close_if_idle_invokes_polymorphic_close_on_dynamic():
    """``evicts_on_idle=True`` managers' polymorphic close path runs."""
    mgr = _StubDynamicManager("dyn")
    await mgr.get()
    closed = await mgr.maybe_close_if_idle(0.1, mgr._last_accessed + 1000.0)
    assert closed is True
    assert mgr.extra_close_count == 1
    assert mgr._item_cache is None


@pytest.mark.asyncio
async def test_get_after_idle_close_recreates_item():
    """After idle eviction, the next ``get()`` lazily reconnects."""
    mgr = _StubStaticManager("a")
    await mgr.get()
    await mgr.maybe_close_if_idle(0.1, mgr._last_accessed + 1000.0)
    await mgr.get()
    assert mgr.create_count == 2
    assert mgr._item_cache is not None
