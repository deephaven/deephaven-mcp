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


class DummyServer:
    name = "dummy-server"


# ---------------------------------------------------------------------------
# make_enterprise_lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_yields_context_keys():
    """Enterprise lifespan yields required context keys."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_enterprise_lifespan(
            session_registry_manager, config_path="/mock/enterprise.json"
        )
        async with lifespan(DummyServer()) as context:
            assert "config_manager" in context
            assert "session_registry_manager" in context
            assert "refresh_lock" in context
            assert "instance_tracker" in context
            assert "session_registry" not in context


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_start_called():
    """Enterprise lifespan calls session_registry_manager.start() during startup."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_enterprise_lifespan(
            session_registry_manager, config_path="/mock/enterprise.json"
        )
        async with lifespan(DummyServer()):
            session_registry_manager.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_stop_called_on_shutdown():
    """Enterprise lifespan calls session_registry_manager.stop() and instance_tracker.unregister() on normal shutdown."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_enterprise_lifespan(
            session_registry_manager, config_path="/mock/enterprise.json"
        )
        async with lifespan(DummyServer()):
            pass
        session_registry_manager.stop.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_stop_called_on_config_error():
    """Enterprise lifespan calls session_registry_manager.stop() even when config fails to load."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("config load failed")
    )
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_enterprise_lifespan(
            session_registry_manager, config_path="/mock/enterprise.json"
        )
        with pytest.raises(RuntimeError, match="config load failed"):
            async with lifespan(DummyServer()):
                pass  # pragma: no cover
        session_registry_manager.stop.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_tracker_unregister_called_even_if_stop_raises():
    """Enterprise lifespan calls instance_tracker.unregister() even if session_registry_manager.stop() raises."""
    session_registry_manager = AsyncMock()
    session_registry_manager.stop = AsyncMock(side_effect=RuntimeError("stop failed"))
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_enterprise_lifespan(
            session_registry_manager, config_path="/mock/enterprise.json"
        )
        async with lifespan(DummyServer()):
            pass
        session_registry_manager.stop.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


# ---------------------------------------------------------------------------
# make_community_lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_community_lifespan_yields_context_keys():
    """Community lifespan yields required context keys."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_community_lifespan(
            session_registry_manager, config_path="/mock/community.json"
        )
        async with lifespan(DummyServer()) as context:
            assert "config_manager" in context
            assert "session_registry_manager" in context
            assert "refresh_lock" in context
            assert "instance_tracker" in context
            assert "session_registry" not in context


@pytest.mark.asyncio
async def test_make_community_lifespan_start_called():
    """Community lifespan calls session_registry_manager.start() during startup."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_community_lifespan(
            session_registry_manager, config_path="/mock/community.json"
        )
        async with lifespan(DummyServer()):
            session_registry_manager.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_stop_called_on_shutdown():
    """Community lifespan calls session_registry_manager.stop() and instance_tracker.unregister() on normal shutdown."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_community_lifespan(
            session_registry_manager, config_path="/mock/community.json"
        )
        async with lifespan(DummyServer()):
            pass
        session_registry_manager.stop.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_stop_called_on_config_error():
    """Community lifespan calls session_registry_manager.stop() even when config fails to load."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("community config failed")
    )
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_community_lifespan(
            session_registry_manager, config_path="/mock/community.json"
        )
        with pytest.raises(RuntimeError, match="community config failed"):
            async with lifespan(DummyServer()):
                pass  # pragma: no cover
        session_registry_manager.stop.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_tracker_unregister_called_even_if_stop_raises():
    """Community lifespan calls instance_tracker.unregister() even if session_registry_manager.stop() raises."""
    session_registry_manager = AsyncMock()
    session_registry_manager.stop = AsyncMock(side_effect=RuntimeError("stop failed"))
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_community_lifespan(
            session_registry_manager, config_path="/mock/community.json"
        )
        async with lifespan(DummyServer()):
            pass
        session_registry_manager.stop.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_unregister_exception_is_swallowed():
    """Enterprise lifespan swallows exceptions from instance_tracker.unregister()."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock(
        side_effect=RuntimeError("unregister failed")
    )

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_enterprise_lifespan(
            session_registry_manager, config_path="/mock/enterprise.json"
        )
        async with lifespan(DummyServer()):
            pass
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_unregister_exception_is_swallowed():
    """Community lifespan swallows exceptions from instance_tracker.unregister()."""
    session_registry_manager = AsyncMock()
    config_manager = AsyncMock()
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker.unregister = AsyncMock(
        side_effect=RuntimeError("unregister failed")
    )

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
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
        lifespan = make_community_lifespan(
            session_registry_manager, config_path="/mock/community.json"
        )
        async with lifespan(DummyServer()):
            pass
        instance_tracker.unregister.assert_awaited_once()
