"""
Tests for deephaven_mcp.mcp_systems_server._lifespan.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from deephaven_mcp.mcp_systems_server._lifespan import (
    make_community_lifespan,
    make_enterprise_lifespan,
)

# ---------------------------------------------------------------------------
# make_enterprise_lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_yields_context_and_cleans_up():
    """Enterprise lifespan yields required context keys and closes on exit."""

    class DummyServer:
        name = "dummy-enterprise-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"

    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=refresh_lock,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_enterprise_lifespan(config_path="/mock/enterprise.json")
        server = DummyServer()
        async with lifespan(server) as context:
            assert "config_manager" in context
            assert "session_registry" in context
            assert "refresh_lock" in context
            assert "instance_tracker" in context
        session_registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_cleanup_on_error():
    """Enterprise lifespan cleans up session_registry and instance_tracker even on error."""

    class DummyServer:
        name = "dummy-enterprise-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    instance_tracker = AsyncMock()

    config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("config load failed")
    )
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=asyncio.Lock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_enterprise_lifespan(config_path="/mock/enterprise.json")
        server = DummyServer()
        with pytest.raises(RuntimeError, match="config load failed"):
            async with lifespan(server):
                pass  # pragma: no cover
        # session_registry was not yet initialized when error occurred, so close should not be called
        session_registry.close.assert_not_awaited()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_cleanup_continues_after_registry_close_error():
    """Enterprise lifespan unregisters instance_tracker even if session_registry.close raises."""

    class DummyServer:
        name = "dummy-enterprise-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"

    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock(side_effect=RuntimeError("close failed"))
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=asyncio.Lock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_enterprise_lifespan(config_path="/mock/enterprise.json")
        server = DummyServer()
        async with lifespan(server):
            pass
        session_registry.close.assert_awaited_once()
        # instance_tracker.unregister must still be called despite the registry close error
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_cleanup_on_initialize_error():
    """Enterprise lifespan closes registry and unregisters tracker when initialize() raises."""

    class DummyServer:
        name = "dummy-enterprise-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"

    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock(side_effect=RuntimeError("init failed"))
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=asyncio.Lock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_enterprise_lifespan(config_path="/mock/enterprise.json")
        server = DummyServer()
        with pytest.raises(RuntimeError, match="init failed"):
            async with lifespan(server):
                pass  # pragma: no cover
        # session_registry was assigned before initialize() raised, so close() must be called
        session_registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_cleanup_continues_after_tracker_unregister_error():
    """Enterprise lifespan completes shutdown even if instance_tracker.unregister raises."""

    class DummyServer:
        name = "dummy-enterprise-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"

    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock(
        side_effect=RuntimeError("unregister failed")
    )

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=asyncio.Lock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_enterprise_lifespan(config_path="/mock/enterprise.json")
        server = DummyServer()
        # Should not raise even though unregister fails
        async with lifespan(server):
            pass
        session_registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


# ---------------------------------------------------------------------------
# make_community_lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_community_lifespan_yields_context_and_cleans_up():
    """Community lifespan yields required context keys and closes on exit."""

    class DummyServer:
        name = "dummy-community-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"

    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunitySessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=refresh_lock,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_community_lifespan(config_path="/mock/community.json")
        server = DummyServer()
        async with lifespan(server) as context:
            assert "config_manager" in context
            assert "session_registry" in context
            assert "refresh_lock" in context
            assert "instance_tracker" in context
        session_registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_cleanup_on_error():
    """Community lifespan cleans up even when config fails to load."""

    class DummyServer:
        name = "dummy-community-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    instance_tracker = AsyncMock()

    config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("community config failed")
    )
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunitySessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=asyncio.Lock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_community_lifespan(config_path="/mock/community.json")
        server = DummyServer()
        with pytest.raises(RuntimeError, match="community config failed"):
            async with lifespan(server):
                pass  # pragma: no cover
        session_registry.close.assert_not_awaited()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_cleanup_on_initialize_error():
    """Community lifespan closes registry and unregisters tracker when initialize() raises."""

    class DummyServer:
        name = "dummy-community-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"

    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock(
        side_effect=RuntimeError("community init failed")
    )
    session_registry.close = AsyncMock()
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunitySessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.asyncio.Lock",
            return_value=asyncio.Lock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.InstanceTracker.create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.cleanup_orphaned_resources",
            AsyncMock(),
        ),
    ):
        lifespan = make_community_lifespan(config_path="/mock/community.json")
        server = DummyServer()
        with pytest.raises(RuntimeError, match="community init failed"):
            async with lifespan(server):
                pass  # pragma: no cover
        # session_registry was assigned before initialize() raised, so close() must be called
        session_registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()
