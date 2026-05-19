"""
Tests for deephaven_mcp.mcp_systems_server._lifespan.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp.mcp_systems_server._lifespan import (
    make_community_lifespan,
    make_enterprise_lifespan,
)


class DummyServer:
    name = "dummy-server"


def _make_registry():
    """Build a mock registry with the lifespan-relevant async methods."""
    registry = AsyncMock()
    registry.initialize = AsyncMock()
    registry.close = AsyncMock()
    return registry


def _patch_enterprise_registry(registry):
    """Patch ``EnterpriseSessionRegistry`` on the _lifespan module.

    Returns a context manager whose target's ``return_value`` is set to
    ``registry`` and that ``assert_called_once_with`` works the same as a
    plain MagicMock class. Tests use it in place of injecting a custom
    registry class through the public factory's signature.
    """
    return patch(
        "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseSessionRegistry",
        return_value=registry,
    )


def _patch_community_registry(registry):
    """Patch ``CommunitySessionRegistry`` on the _lifespan module."""
    return patch(
        "deephaven_mcp.mcp_systems_server._lifespan.CommunitySessionRegistry",
        return_value=registry,
    )


# ---------------------------------------------------------------------------
# make_enterprise_lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_yields_context_keys():
    """Enterprise lifespan yields the four required context keys."""
    registry = _make_registry()
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
        _patch_enterprise_registry(registry),
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
            idle_timeout_seconds=600.0,
            config_path="/mock/enterprise.json",
        )
        async with lifespan(DummyServer()) as context:
            assert "config_manager" in context
            assert "registry" in context
            assert "refresh_lock" in context
            assert "instance_tracker" in context
            assert context["registry"] is registry


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_initialize_called():
    """Enterprise lifespan calls registry.initialize(config_manager) during startup."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        _patch_enterprise_registry(registry) as mock_registry_class,
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
            idle_timeout_seconds=600.0,
            config_path="/mock/enterprise.json",
        )
        async with lifespan(DummyServer()):
            registry.initialize.assert_awaited_once_with(config_manager)
        mock_registry_class.assert_called_once_with()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_close_called_on_shutdown():
    """Enterprise lifespan calls registry.close() and instance_tracker.unregister() on shutdown."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        _patch_enterprise_registry(registry),
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
        async with lifespan(DummyServer()):
            pass
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_close_called_on_config_error():
    """Enterprise lifespan unregisters the instance tracker even when config fails to load."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("config load failed")
    )
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        _patch_enterprise_registry(registry) as mock_registry_class,
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
        with pytest.raises(RuntimeError, match="config load failed"):
            async with lifespan(DummyServer()):
                pass  # pragma: no cover
        # Registry was never constructed because config failed first.
        mock_registry_class.assert_not_called()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_evictor_stop_exception_is_swallowed(caplog):
    """Enterprise lifespan logs and swallows exceptions from ``evictor.stop()``.

    Covers the defensive ``except Exception`` branch around ``evictor.stop()``
    in :func:`_make_lifespan` so a buggy sweep loop on shutdown does not
    prevent the registry / instance_tracker cleanup that follows.
    """
    import logging

    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    instance_tracker.unregister = AsyncMock()

    bad_evictor = AsyncMock()
    bad_evictor.start = AsyncMock()
    bad_evictor.stop = AsyncMock(side_effect=RuntimeError("evictor stop failed"))

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        _patch_enterprise_registry(registry),
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.Evictor",
            return_value=bad_evictor,
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
        with caplog.at_level(
            logging.ERROR, logger="deephaven_mcp.mcp_systems_server._lifespan"
        ):
            async with lifespan(DummyServer()):
                pass

        bad_evictor.stop.assert_awaited_once()
        # The next cleanup steps still run.
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()
        assert any(
            "Error stopping evictor" in r.message and r.levelno == logging.ERROR
            for r in caplog.records
        )


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_tracker_unregister_called_even_if_close_raises():
    """Enterprise lifespan calls instance_tracker.unregister() even if registry.close() raises."""
    registry = _make_registry()
    registry.close = AsyncMock(side_effect=RuntimeError("close failed"))
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        _patch_enterprise_registry(registry),
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
        async with lifespan(DummyServer()):
            pass
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_enterprise_lifespan_unregister_exception_is_swallowed():
    """Enterprise lifespan swallows exceptions from instance_tracker.unregister()."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-enterprise-id"
    instance_tracker.unregister = AsyncMock(
        side_effect=RuntimeError("unregister failed")
    )

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.EnterpriseServerConfigManager",
            return_value=config_manager,
        ),
        _patch_enterprise_registry(registry),
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
        async with lifespan(DummyServer()):
            pass
        instance_tracker.unregister.assert_awaited_once()


# ---------------------------------------------------------------------------
# make_community_lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_community_lifespan_yields_context_keys():
    """Community lifespan yields the four required context keys."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        _patch_community_registry(registry),
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
        async with lifespan(DummyServer()) as context:
            assert "config_manager" in context
            assert "registry" in context
            assert "refresh_lock" in context
            assert "instance_tracker" in context
            assert context["registry"] is registry


@pytest.mark.asyncio
async def test_make_community_lifespan_initialize_called():
    """Community lifespan calls registry.initialize(config_manager) during startup."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        _patch_community_registry(registry),
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
        async with lifespan(DummyServer()):
            registry.initialize.assert_awaited_once_with(config_manager)


@pytest.mark.asyncio
async def test_make_community_lifespan_close_called_on_shutdown():
    """Community lifespan calls registry.close() and instance_tracker.unregister() on shutdown."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        _patch_community_registry(registry),
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
        async with lifespan(DummyServer()):
            pass
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_close_called_on_config_error():
    """Community lifespan unregisters the instance tracker even when config fails to load."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("community config failed")
    )
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        _patch_community_registry(registry) as mock_registry_class,
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
        with pytest.raises(RuntimeError, match="community config failed"):
            async with lifespan(DummyServer()):
                pass  # pragma: no cover
        mock_registry_class.assert_not_called()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_tracker_unregister_called_even_if_close_raises():
    """Community lifespan calls instance_tracker.unregister() even if registry.close() raises."""
    registry = _make_registry()
    registry.close = AsyncMock(side_effect=RuntimeError("close failed"))
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    instance_tracker.unregister = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        _patch_community_registry(registry),
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
        async with lifespan(DummyServer()):
            pass
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_community_lifespan_unregister_exception_is_swallowed():
    """Community lifespan swallows exceptions from instance_tracker.unregister()."""
    registry = _make_registry()
    config_manager = AsyncMock()
    config_manager.get_config = AsyncMock(return_value={})
    instance_tracker = AsyncMock()
    instance_tracker.instance_id = "test-community-id"
    instance_tracker.unregister = AsyncMock(
        side_effect=RuntimeError("unregister failed")
    )

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._lifespan.CommunityServerConfigManager",
            return_value=config_manager,
        ),
        _patch_community_registry(registry),
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
        async with lifespan(DummyServer()):
            pass
        instance_tracker.unregister.assert_awaited_once()
