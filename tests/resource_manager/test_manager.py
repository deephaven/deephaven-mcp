"""
Unit tests for Session Manager classes.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from deephaven_mcp import client
from deephaven_mcp._exceptions import InternalError, SessionCreationError
from deephaven_mcp.client import CorePlusSession
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CommunitySessionManager,
    CorePlusSessionFactoryManager,
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PipLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)

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

    def __init__(self, system_type: SystemType, source: str, name: str):
        super().__init__(system_type, source, name)
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
        source="test_source",
        name="test_manager",
    )
    assert manager.name == "test_manager"
    assert manager.system_type == SystemType.COMMUNITY
    assert manager.source == "test_source"
    assert manager.full_name == "community:test_source:test_manager"


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
async def test_close_not_alive():
    """Test that close handles an item that is not alive."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    item = await manager.get()

    # Mark the item as not alive
    item.is_alive.return_value = False

    await manager.close()

    # close() should not be called on the item
    item.close.assert_not_called()
    # Cache should be cleared
    assert manager._item_cache is None


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
        def __init__(self, system_type: SystemType, source: str, name: str):
            super().__init__(system_type, source, name)
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
    """Covers line 286: str(enum) returns the enum name."""
    for system_type in SystemType:
        assert str(system_type) == system_type.name


from deephaven_mcp._exceptions import AuthenticationError, ConfigurationError


@pytest.mark.asyncio
async def test_liveness_status_unlocked_exceptions(monkeypatch):
    """Covers lines 961-962, 969-977: Exception handling in _liveness_status_unlocked."""

    class DummyManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            pass

        async def _check_liveness(self, item):
            return (ResourceLivenessStatus.ONLINE, None)

        async def _get_unlocked(self):
            return MockItem()

    manager = DummyManager(SystemType.COMMUNITY, "src", "nm")
    # Patch _get_unlocked to raise AuthenticationError
    monkeypatch.setattr(
        manager, "_get_unlocked", AsyncMock(side_effect=AuthenticationError("authfail"))
    )
    result = await manager._liveness_status_unlocked(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.UNAUTHORIZED
    assert "authfail" in result[1]

    # Patch _get_unlocked to raise ConfigurationError
    monkeypatch.setattr(
        manager, "_get_unlocked", AsyncMock(side_effect=ConfigurationError("cfgfail"))
    )
    result = await manager._liveness_status_unlocked(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.MISCONFIGURED
    assert "cfgfail" in result[1]

    # Patch _get_unlocked to raise SessionCreationError (configuration issue)
    monkeypatch.setattr(
        manager, "_get_unlocked", AsyncMock(side_effect=SessionCreationError("scfail"))
    )
    result = await manager._liveness_status_unlocked(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.MISCONFIGURED
    assert "scfail" in result[1]

    # Patch _get_unlocked to raise SessionCreationError (connection failure)
    monkeypatch.setattr(
        manager,
        "_get_unlocked",
        AsyncMock(side_effect=SessionCreationError("connection refused")),
    )
    result = await manager._liveness_status_unlocked(ensure_item=True)
    assert result[0] == ResourceLivenessStatus.OFFLINE
    assert "connection refused" in result[1]

    # Patch _get_unlocked to raise generic Exception
    monkeypatch.setattr(
        manager, "_get_unlocked", AsyncMock(side_effect=RuntimeError("boom!"))
    )
    result = await manager._liveness_status_unlocked(ensure_item=True)
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

        async def _get_unlocked(self):
            return MockItem()

    manager = DummyManager(SystemType.COMMUNITY, "src", "nm")
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
async def test_close_logs_on_liveness_failure(monkeypatch, caplog):
    """Covers line 1319: Info log after successful close following liveness check failure."""

    class DummyManager(BaseItemManager[MockItem]):
        async def _create_item(self):
            return MockItem()

        async def _check_liveness(self, item):
            raise Exception("liveness fail!")

    manager = DummyManager(SystemType.COMMUNITY, "src", "nm")
    item = MockItem()
    manager._item_cache = item
    item.close = AsyncMock()
    # Patch _is_alive_unlocked to raise so that close takes the liveness failure path
    monkeypatch.setattr(
        manager,
        "_is_alive_unlocked",
        AsyncMock(side_effect=Exception("liveness fail!")),
    )
    with caplog.at_level("INFO"):
        await manager.close()
    expected = "[DummyManager] Successfully closed item for 'community:src:nm' despite earlier liveness failure"
    assert any(
        r.levelname == "INFO" and r.getMessage() == expected for r in caplog.records
    )


@pytest.mark.asyncio
async def test_static_community_session_manager_has_correct_source():
    """Test that StaticCommunitySessionManager sets source to 'config'."""
    from deephaven_mcp.resource_manager import StaticCommunitySessionManager, SystemType
    
    manager = StaticCommunitySessionManager("test-session", {"server": "localhost"})
    
    assert manager.source == "config"
    assert manager.system_type == SystemType.COMMUNITY
    assert manager.full_name == "community:config:test-session"
    assert manager.name == "test-session"


@pytest.mark.asyncio
async def test_community_session_manager_check_liveness_offline(monkeypatch):
    """Covers line 1698: CommunitySessionManager._check_liveness returns OFFLINE if is_alive() is False."""
    from deephaven_mcp.resource_manager import StaticCommunitySessionManager
    mgr = StaticCommunitySessionManager("test", {"server": "foo"})
    mock_session = Mock()
    mock_session.is_alive = AsyncMock(return_value=False)
    result = await mgr._check_liveness(mock_session)
    assert result == (ResourceLivenessStatus.OFFLINE, "Session not alive")


@pytest.mark.asyncio
async def test_enterprise_session_manager_check_liveness_offline(monkeypatch):
    """Covers line 2137: EnterpriseSessionManager._check_liveness returns OFFLINE if is_alive() is False."""

    async def dummy_creation(source, name):
        return Mock()

    mgr = EnterpriseSessionManager("src", "nm", dummy_creation)
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

    manager = DummyManager(SystemType.COMMUNITY, "src", "nm")
    with pytest.raises(RuntimeError, match="fail-create"):
        await manager.get()


class TestCommunitySessionManager:
    """Tests for the CommunitySessionManager class."""

    @pytest.mark.asyncio
    @patch("deephaven_mcp.client.CoreSession.from_config")
    async def test_create_item(self, mock_from_config):
        """Test that _create_item correctly calls CoreSession.from_config."""
        from deephaven_mcp.resource_manager import StaticCommunitySessionManager
        mock_from_config.return_value = "mock_session"
        manager = StaticCommunitySessionManager(
            name="test_community",
            config={"host": "localhost"},
        )
        session = await manager._create_item()
        mock_from_config.assert_awaited_once_with({"host": "localhost"})
        assert session == "mock_session"

    @pytest.mark.asyncio
    @patch("deephaven_mcp.client.CoreSession.from_config")
    async def test_create_item_raises_exception(self, mock_from_config):
        """Test that _create_item raises SessionCreationError on failure."""
        from deephaven_mcp.resource_manager import StaticCommunitySessionManager
        mock_from_config.side_effect = Exception("Connection failed")
        manager = StaticCommunitySessionManager(
            name="test_community",
            config={},
        )
        with pytest.raises(SessionCreationError, match="Connection failed"):
            await manager._create_item()

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the session's is_alive method."""
        from deephaven_mcp.resource_manager import StaticCommunitySessionManager
        manager = StaticCommunitySessionManager(
            name="test_community",
            config={},
        )
        mock_session = AsyncMock()
        mock_session.is_alive.return_value = True
        result = await manager._check_liveness(mock_session)
        mock_session.is_alive.assert_awaited_once()
        assert result == (ResourceLivenessStatus.ONLINE, None)


class TestEnterpriseSessionManager:
    """Tests for the EnterpriseSessionManager class."""


def test_make_full_name_static():
    """Directly test BaseItemManager.make_full_name static method."""
    from deephaven_mcp.resource_manager import BaseItemManager, SystemType

    assert (
        BaseItemManager.make_full_name(SystemType.ENTERPRISE, "factoryA", "sess42")
        == "enterprise:factoryA:sess42"
    )
    assert (
        BaseItemManager.make_full_name(SystemType.COMMUNITY, "sourceX", "foo")
        == "community:sourceX:foo"
    )


def test_parse_full_name_static():
    """Directly test BaseItemManager.parse_full_name static method."""
    from deephaven_mcp.resource_manager import BaseItemManager

    # Test valid enterprise session ID
    system_type, source, name = BaseItemManager.parse_full_name(
        "enterprise:factoryA:sess42"
    )
    assert system_type == "enterprise"
    assert source == "factoryA"
    assert name == "sess42"

    # Test valid community session ID
    system_type, source, name = BaseItemManager.parse_full_name("community:sourceX:foo")
    assert system_type == "community"
    assert source == "sourceX"
    assert name == "foo"

    # Test session ID with colons in the name (should be handled correctly)
    system_type, source, name = BaseItemManager.parse_full_name(
        "enterprise:prod:session:with:colons"
    )
    assert system_type == "enterprise"
    assert source == "prod"
    assert name == "session:with:colons"

    # Test session ID with special characters
    system_type, source, name = BaseItemManager.parse_full_name(
        "community:config-file.yaml:worker-1"
    )
    assert system_type == "community"
    assert source == "config-file.yaml"
    assert name == "worker-1"


def test_parse_full_name_invalid_formats():
    """Test BaseItemManager.parse_full_name with invalid formats."""
    from deephaven_mcp.resource_manager import BaseItemManager

    # Test empty string
    with pytest.raises(ValueError, match="Invalid full_name format"):
        BaseItemManager.parse_full_name("")

    # Test single component
    with pytest.raises(ValueError, match="Invalid full_name format"):
        BaseItemManager.parse_full_name("enterprise")

    # Test two components (missing name)
    with pytest.raises(ValueError, match="Invalid full_name format"):
        BaseItemManager.parse_full_name("enterprise:system")

    # Test no colons
    with pytest.raises(ValueError, match="Invalid full_name format"):
        BaseItemManager.parse_full_name("invalid-format")

    # Test starts with colon
    with pytest.raises(ValueError, match="Invalid full_name format"):
        BaseItemManager.parse_full_name(":enterprise:system")

    # Test ends with colon but missing components
    with pytest.raises(ValueError, match="Invalid full_name format"):
        BaseItemManager.parse_full_name("enterprise:")


def test_make_and_parse_full_name_roundtrip():
    """Test that make_full_name and parse_full_name are perfect inverses."""
    from deephaven_mcp.resource_manager import BaseItemManager, SystemType

    # Test enterprise roundtrip
    original_enterprise = (SystemType.ENTERPRISE, "prod-system", "analytics-session")
    full_name = BaseItemManager.make_full_name(*original_enterprise)
    parsed = BaseItemManager.parse_full_name(full_name)
    assert parsed == ("enterprise", "prod-system", "analytics-session")

    # Test community roundtrip
    original_community = (SystemType.COMMUNITY, "local-config.yaml", "worker-1")
    full_name = BaseItemManager.make_full_name(*original_community)
    parsed = BaseItemManager.parse_full_name(full_name)
    assert parsed == ("community", "local-config.yaml", "worker-1")

    # Test with special characters
    original_special = (SystemType.ENTERPRISE, "test-env_v2", "session-name_123")
    full_name = BaseItemManager.make_full_name(*original_special)
    parsed = BaseItemManager.parse_full_name(full_name)
    assert parsed == ("enterprise", "test-env_v2", "session-name_123")


def test_split_name_property():
    """Test BaseItemManager.split_name property."""
    from deephaven_mcp.resource_manager import SystemType

    # Test enterprise manager split_name
    manager = ConcreteItemManager(SystemType.ENTERPRISE, "prod-env", "session-1")
    system_type, source, name = manager.split_name
    assert system_type == "enterprise"
    assert source == "prod-env"
    assert name == "session-1"

    # Test community manager split_name
    manager = ConcreteItemManager(SystemType.COMMUNITY, "config.yaml", "worker-1")
    system_type, source, name = manager.split_name
    assert system_type == "community"
    assert source == "config.yaml"
    assert name == "worker-1"

    # Test that split_name matches full_name parsing
    manager = ConcreteItemManager(SystemType.ENTERPRISE, "test-system", "analytics")
    split_components = manager.split_name
    parsed_components = manager.parse_full_name(manager.full_name)
    assert split_components == parsed_components


def test_enterprise_session_manager_constructor():
    """Explicitly test the constructor for coverage (lines 519-520)."""
    from deephaven_mcp.resource_manager import EnterpriseSessionManager

    def dummy_creation(source, name):
        pass

    mgr = EnterpriseSessionManager("src", "nm", dummy_creation)
    assert mgr._creation_function is dummy_creation
    assert mgr.source == "src"
    assert mgr.name == "nm"
    assert mgr.system_type.value == "enterprise"


@pytest.mark.asyncio
async def test_create_item_success_covers_try():
    """Covers the try/return branch of _create_item (line 539-540)."""
    mock_session = AsyncMock()

    async def creation(source, name):
        return mock_session

    mgr = EnterpriseSessionManager("src", "nm", creation)
    result = await mgr._create_item()
    assert result is mock_session


@pytest.mark.asyncio
async def test_create_item_exception_covers_except():
    """Covers the except/raise branch of _create_item (lines 541-542)."""

    async def creation(source, name):
        raise RuntimeError("fail")

    mgr = EnterpriseSessionManager("src", "nm", creation)
    with pytest.raises(
        SessionCreationError, match="Failed to create enterprise session for nm: fail"
    ):
        await mgr._create_item()


@pytest.mark.asyncio
async def test_check_liveness_covers_return():
    """Covers line 559: return await item.is_alive()."""
    mgr = EnterpriseSessionManager("src", "nm", AsyncMock())
    mock_session = AsyncMock()
    mock_session.is_alive = AsyncMock(return_value=True)
    result = await mgr._check_liveness(mock_session)
    assert result == (ResourceLivenessStatus.ONLINE, None)


@pytest.mark.asyncio
async def test_check_liveness_exception():
    """Covers that _check_liveness lets exceptions propagate (handled by liveness_status)."""
    mgr = EnterpriseSessionManager("src", "nm", AsyncMock())
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
            "test_source", "test_session", mock_creation_function
        )

        result = await manager._create_item()

        assert result is mock_session
        mock_creation_function.assert_awaited_once_with("test_source", "test_session")

    @pytest.mark.asyncio
    async def test_create_item_raises_session_creation_error(self):
        """Test that _create_item raises SessionCreationError when creation function fails."""
        mock_creation_function = AsyncMock(side_effect=Exception("Creation failed"))

        manager = EnterpriseSessionManager(
            "test_source", "test_session", mock_creation_function
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
            "test_source", "test_session", mock_creation_function
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
            "test_source", "test_session", mock_creation_function
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
            "test_source", "test_session", mock_creation_function
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

    def test_initialization(self):
        """Test that the manager initializes with the correct properties."""
        config = {"host": "localhost"}
        manager = CorePlusSessionFactoryManager(name="test_factory", config=config)

        assert manager.system_type == SystemType.ENTERPRISE
        assert manager.source == "factory"
        assert manager.name == "test_factory"
        assert manager._config == config

    """Tests for the CorePlusSessionFactoryManager class."""

    @pytest.mark.asyncio
    @patch(
        "deephaven_mcp.client.CorePlusSessionFactory.from_config",
        new_callable=AsyncMock,
    )
    async def test_create_item(self, mock_from_config):
        """Test that _create_item correctly calls the factory's from_config method."""
        mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
        mock_from_config.return_value = mock_factory

        config = {"host": "localhost"}
        manager = CorePlusSessionFactoryManager(name="test_factory", config=config)

        created_factory = await manager._create_item()

        assert created_factory is mock_factory
        mock_from_config.assert_awaited_once_with(config)

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the item's ping method."""
        mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
        manager = CorePlusSessionFactoryManager(name="test_factory", config={})

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
            config=config,
            launched_session=launched_session,
        )

        assert manager.launched_session == launched_session
        assert manager.launched_session.auth_token == "test-token"

    def test_to_dict_returns_session_info_docker(self):
        """Test that to_dict returns comprehensive session information for Docker."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="psk",
            auth_token="test-token",
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000, "auth_type": "PSK"}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        result = manager.to_dict()

        assert result["connection_url"] == "http://localhost:10000"
        # Note: connection_url_with_auth removed from to_dict() for security
        assert result["port"] == 10000
        assert result["container_id"] == "test_container"
        assert "process_id" not in result
        assert result["auth_type"] == "PSK"
        assert result["launch_method"] == "docker"

    def test_to_dict_with_pip_process(self):
        """Test that to_dict correctly identifies pip launch method."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        launched_session = PipLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        result = manager.to_dict()

        assert result["launch_method"] == "pip"
        assert result["process_id"] == 12345
        assert "container_id" not in result

    def test_to_dict_without_auth_token(self):
        """Test that to_dict handles missing auth token."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        result = manager.to_dict()

        assert result["connection_url"] == "http://localhost:10000"
        # Note: connection_url_with_auth removed from to_dict() for security

    def test_to_dict_with_anonymous_auth(self):
        """Test that to_dict handles Anonymous auth type."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000, "auth_type": "Anonymous"}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        result = manager.to_dict()

        assert result["auth_type"] == "ANONYMOUS"
        # Note: connection_url_with_auth removed from to_dict() for security

    @pytest.mark.asyncio
    async def test_close_success(self):
        """Test successful close."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        # Mock parent close and session stop
        with patch.object(manager.__class__.__bases__[0], "close", new_callable=AsyncMock) as mock_parent_close:
            with patch.object(launched_session, "stop", new_callable=AsyncMock) as mock_stop:
                await manager.close()

                mock_stop.assert_called_once()
                mock_parent_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_parent_close_error(self):
        """Test close handles parent close errors."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        # Mock parent close to raise error
        with patch.object(manager.__class__.__bases__[0], "close", new_callable=AsyncMock) as mock_parent_close:
            mock_parent_close.side_effect = Exception("Parent close failed")
            with patch.object(launched_session, "stop", new_callable=AsyncMock) as mock_stop:
                # Should not raise, just log warning
                await manager.close()

                # Session stop should still be called
                mock_stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_session_stop_error(self):
        """Test close handles session stop errors."""
        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        # Mock parent close and session stop
        with patch.object(manager.__class__.__bases__[0], "close", new_callable=AsyncMock) as mock_parent_close:
            with patch.object(launched_session, "stop", new_callable=AsyncMock) as mock_stop:
                mock_stop.side_effect = Exception("Stop failed")
                # Should not raise, just log error
                await manager.close()

                mock_stop.assert_called_once()
                # Parent close should still be called
                mock_parent_close.assert_called_once()

    def test_properties(self):
        """Test all property accessors."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        
        launched_session = PipLaunchedSession(
            host="testhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )
        config = {"host": "testhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        assert manager.connection_url == "http://testhost:10000"
        assert manager.connection_url_with_auth == "http://testhost:10000"  # anonymous, no token
        assert manager.port == 10000
        assert manager.launch_method == "pip"
        assert manager.container_id is None
        assert manager.process_id == 12345

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
            config=config,
            launched_session=launched_session,
        )

        assert manager.process_id is None
