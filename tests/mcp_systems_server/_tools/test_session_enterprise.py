"""
Tests for deephaven_mcp.mcp_systems_server._tools.session_enterprise.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    RegistryItemNotFoundError,
    SessionCreationError,
)
from deephaven_mcp.client import CorePlusQuerySerial
from deephaven_mcp.mcp_systems_server._tools.session_enterprise import (
    _SHORT_REASON_MAX_LEN,
    _check_session_id_available,
    _check_session_limit,
    _collect_one_enterprise_system_status,
    _generate_session_name_if_none,
    _resolve_session_parameters,
    _short_reason,
    enterprise_systems_status,
    register_tools,
    session_enterprise_create,
    session_enterprise_delete,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    EnterpriseSessionRegistry,
    InitializationPhase,
    PythonLaunchedSession,
    QualifiedSessionId,
    RegistrySnapshot,
    ResourceLivenessStatus,
    SessionOrigin,
    SystemType,
)

from ._helpers import (
    MockContext,
    create_mock_instance_tracker,
)

_TEST_SYSTEM_NAME = "system"

_SE_MODULE = "deephaven_mcp.mcp_systems_server._tools.session_enterprise"


@pytest.mark.asyncio
async def test_collect_one_enterprise_system_status_returns_compact_health_record():
    """``_collect_one_enterprise_system_status`` returns runtime health only.

    The returned record is the ``.status`` view (name, type, liveness_status,
    is_alive, optional liveness_detail). It deliberately does not include the
    system's declared ``config`` — that lives in 'list_systems' / config tools.
    """
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = "prod"
    mock_registry.get_all = AsyncMock(return_value=RegistrySnapshot.simple(items={}))
    mock_registry.factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, "all good")
    )
    mock_registry.factory_manager.is_alive = AsyncMock(return_value=True)

    with patch(f"{_SE_MODULE}.get_enterprise_registry", return_value=mock_registry):
        info, _, _ = await _collect_one_enterprise_system_status(
            MagicMock(), "prod", False
        )

    assert info == {
        "name": "prod",
        "type": "enterprise",
        "liveness_status": "ONLINE",
        "is_alive": True,
        "liveness_detail": "all good",
    }


def test_short_reason_extracts_exception_type_prefix():
    """Init errors are recorded as 'Type: message'; ``_short_reason`` returns
    the type prefix — the kubectl-style code shown in ``liveness_detail``."""
    assert (
        _short_reason("DeephavenConnectionError: Failed to connect to ...")
        == "DeephavenConnectionError"
    )


def test_short_reason_returns_input_when_no_separator():
    """Strings that don't follow the 'Type: message' convention are returned
    verbatim so the column is still populated."""
    assert _short_reason("something broke") == "something broke"


def test_short_reason_truncates_overlong_input():
    """Unconventional inputs are capped to a table-friendly width with an
    ellipsis marker so a single row can't blow up the table."""
    overlong = "x" * (_SHORT_REASON_MAX_LEN + 50)
    short = _short_reason(overlong)
    assert len(short) == _SHORT_REASON_MAX_LEN
    assert short.endswith("\u2026")


@pytest.mark.asyncio
async def test_collect_one_promotes_init_error_type_when_probe_uninformative():
    """When the cached probe falls back to 'No item cached' and discovery
    recorded an init error, ``liveness_detail`` carries the exception type
    instead of the uninformative sentinel — the actionable signal wins."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = "prod"
    mock_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={"factory": "DeephavenConnectionError: Network is unreachable"},
        )
    )
    mock_registry.factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, "No item cached")
    )
    mock_registry.factory_manager.is_alive = AsyncMock(return_value=False)

    with patch(f"{_SE_MODULE}.get_enterprise_registry", return_value=mock_registry):
        info, init_errors, _ = await _collect_one_enterprise_system_status(
            MagicMock(), "prod", False
        )

    assert info["liveness_detail"] == "DeephavenConnectionError"
    # Full message is preserved in init_errors for partial_result.errors.
    assert init_errors == {
        "factory": "DeephavenConnectionError: Network is unreachable"
    }


@pytest.mark.asyncio
async def test_collect_one_keeps_probe_message_over_init_error():
    """When the probe supplied a non-sentinel message (e.g. an actual probe
    failure), it wins over discovery-time init errors — the live signal is
    more recent than the recorded one."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = "prod"
    mock_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={"factory": "DeephavenConnectionError: Network is unreachable"},
        )
    )
    mock_registry.factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, "Ping returned False")
    )
    mock_registry.factory_manager.is_alive = AsyncMock(return_value=False)

    with patch(f"{_SE_MODULE}.get_enterprise_registry", return_value=mock_registry):
        info, _, _ = await _collect_one_enterprise_system_status(
            MagicMock(), "prod", False
        )

    assert info["liveness_detail"] == "Ping returned False"


@pytest.mark.asyncio
async def test_collect_one_joins_multiple_init_error_types_with_comma():
    """Multiple recorded sources for one system join their exception-type
    prefixes with ', ' so the table cell stays one line."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = "prod"
    mock_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={
                "factory": "DeephavenConnectionError: Network unreachable",
                "client": "AuthenticationError: Token expired",
            },
        )
    )
    mock_registry.factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_registry.factory_manager.is_alive = AsyncMock(return_value=False)

    with patch(f"{_SE_MODULE}.get_enterprise_registry", return_value=mock_registry):
        info, _, _ = await _collect_one_enterprise_system_status(
            MagicMock(), "prod", False
        )

    reasons = info["liveness_detail"].split(", ")
    assert set(reasons) == {"DeephavenConnectionError", "AuthenticationError"}


@pytest.mark.asyncio
async def test_collect_one_does_not_promote_init_error_when_online():
    """An ONLINE system has recovered; stale init errors must not surface
    as ``liveness_detail`` and falsely imply the system is broken."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = "prod"
    mock_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={"factory": "DeephavenConnectionError: was unreachable earlier"},
        )
    )
    mock_registry.factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_registry.factory_manager.is_alive = AsyncMock(return_value=True)

    with patch(f"{_SE_MODULE}.get_enterprise_registry", return_value=mock_registry):
        info, _, _ = await _collect_one_enterprise_system_status(
            MagicMock(), "prod", False
        )

    assert "liveness_detail" not in info
    assert info["liveness_status"] == "ONLINE"


@pytest.mark.asyncio
async def test_session_enterprise_create_system_missing_from_config_returns_error():
    """``session_enterprise_create`` returns an error response when the
    registry resolves a system absent from the loaded multi-config — a
    registry/config inconsistency that bypasses ``get_enterprise_registry``."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    multi_config = SimpleNamespace(enterprise=None)

    with (
        patch(f"{_SE_MODULE}.get_enterprise_registry", return_value=mock_registry),
        patch(f"{_SE_MODULE}.get_multi_config", return_value=multi_config),
    ):
        result = await session_enterprise_create(MagicMock(), "prod", None)

    assert result["success"] is False
    assert result["isError"] is True
    assert "not configured" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_name_no_username_and_language_transformer():
    """Covers auto-generated name without username (mcp-worker-...), language transformer execution, and creation_function."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Use private-key credentials (no username field) to exercise the
    # "no-username branch" of _generate_session_name_if_none.
    flat_config = {
        "connection_json_url": "https://example.com/iris/connection.json",
        "auth": {
            "credentials": {
                "type": "private_key",
                "key_text": "-----BEGIN KEY-----\nfake\n-----END KEY-----",
            }
        },
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "heap_size_gb": 2.0,
                "auto_delete_timeout": 600,
                "server": "server-east-1",
                "engine": "DeephavenCommunity",
            },
        },
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20241126-1500"

        # Use factory_manager directly (plain property on session_registry)
        mock_factory_manager = AsyncMock()
        mock_factory = MagicMock()
        mock_session = MagicMock()
        mock_session.pqinfo = AsyncMock(
            return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=1)))
        )
        mock_registry.factory_manager = mock_factory_manager
        mock_factory_manager.get = AsyncMock(return_value=mock_factory)
        # Set up the factory mock to capture configuration_transformer calls
        captured_config_transformer = None

        def capture_transformer(*args, **kwargs):
            nonlocal captured_config_transformer
            captured_config_transformer = kwargs.get("configuration_transformer")
            return mock_session

        mock_factory.connect_to_new_worker = AsyncMock(side_effect=capture_transformer)

        # Mock the session registry operations
        mock_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("Session not found")
        )
        mock_registry.add_session = AsyncMock()
        mock_registry.count_added_sessions = AsyncMock(return_value=0)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "registry": mock_registry,
            }
        )

        # Use a non-Python programming language to exercise configuration_transformer
        result = await session_enterprise_create(
            context,
            _TEST_SYSTEM_NAME,
            None,
            programming_language="Groovy",
        )

        assert result["success"] is True
        # Name should be generated without username prefix
        assert result["session_name"] == "mcp-session-20241126-1500"

        # Verify the factory was called with a configuration_transformer for non-Python language
        mock_factory.connect_to_new_worker.assert_called_once()
        assert captured_config_transformer is not None

        # Test the language transformer - now accesses config.pb.scriptLanguage
        mock_config = MagicMock()
        result_config = captured_config_transformer(mock_config)
        assert result_config is mock_config
        assert mock_config.pb.scriptLanguage == "Groovy"

        # Verify session was added using add_session method - check the call was made
        id = "enterprise:system:1"
        mock_registry.add_session.assert_called_once()
        call_args = mock_registry.add_session.call_args
        session_manager = call_args[0][0]  # First (and only) argument is the manager
        assert str(session_manager.qualified_session_id) == id
        returned_session = await session_manager._creation_function(
            "system", "mcp-session-20241126-1500"
        )
        assert returned_session is mock_session


@pytest.mark.asyncio
async def test_session_enterprise_delete_removal_missing_in_registry():
    """Covers branch where pop returns None (lines 1959-1960)."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    # Mock remove to return None (simulating session not found in registry)
    mock_registry.remove = AsyncMock(return_value=None)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "enterprise:system:11")

    assert result["success"] is True
    mock_controller.delete_query.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_enterprise_delete_cleanup_created_sessions_empty():
    """Test session removal - session tracking now handled by registry automatically."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}

    # Mock remove to return the manager (simulating successful removal)
    full_id = "enterprise:system:12"
    mock_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "enterprise:system:12")

    assert result["success"] is True
    # The persistent query is deleted on the controller (serial 12)
    mock_controller.delete_query.assert_awaited_once()
    # Session tracking is now handled internally by the registry


@pytest.mark.asyncio
async def test_session_enterprise_delete_pq_deletion_failure():
    """Controller delete_query failure returns an error and leaves the registry intact."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock(side_effect=Exception("controller down"))
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "enterprise:system:15")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Failed to delete persistent query" in result["error"]
    # On PQ-deletion failure we must NOT remove the session from the registry,
    # so the caller can retry.
    mock_registry.remove.assert_not_awaited()
    mock_session_manager.close.assert_not_awaited()


@pytest.mark.parametrize(
    "origin", [SessionOrigin.DISCOVERED, SessionOrigin.STATIC], ids=lambda o: o.value
)
@pytest.mark.asyncio
async def test_session_enterprise_delete_refuses_non_dynamic_session(origin):
    """A session MCP did not create is refused without touching the controller.

    A ``DISCOVERED`` session is a persistent query that pre-existed MCP and
    outlives it. Deleting the PQ is irreversible, so the guard must fire
    *before* any controller call -- asserting the refusal alone would still
    pass if the PQ had already been destroyed.
    """
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = origin
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "enterprise:system:16")

    assert result["success"] is False
    assert result["isError"] is True
    assert "not a dynamically created session" in result["error"]
    assert f"origin: '{origin.value}'" in result["error"]
    assert "pq_delete" in result["error"]
    # The point of the guard: the PQ must survive, and the session must stay
    # usable rather than being half-torn-down.
    mock_controller.delete_query.assert_not_awaited()
    mock_registry.remove.assert_not_awaited()
    mock_session_manager.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_enterprise_delete_registry_pop_raises_error():
    """Covers error path on removal (lines 1973-1977)."""

    class BadItems:
        def pop(self, *args, **kwargs):
            raise RuntimeError("pop failed")

    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove = AsyncMock(side_effect=Exception("Simulated registry error"))
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "enterprise:system:13")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Failed to remove session" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_outer_exception_logger_info_raises():
    """Force outer exception handler (lines 1991-1998) by making _LOGGER.info raise."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}
    full_id = "enterprise:system:14"
    mock_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    # Only raise on the second info() call (the first is before the try block)
    call_counter = {"n": 0}

    def info_side_effect(*args, **kwargs):
        call_counter["n"] += 1
        if call_counter["n"] == 2:
            raise Exception("log fail")
        return None

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise._LOGGER.info",
        side_effect=info_side_effect,
    ):
        result = await session_enterprise_delete(context, "enterprise:system:14")

    assert result["success"] is False
    assert result["isError"] is True
    assert "log fail" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_success():
    """Test successful retrieval of enterprise systems status."""
    # Mock factory_manager with liveness_status and is_alive methods
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, "System is healthy")
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=True)

    # Mock session registry - factory_manager is a plain property (not async)
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager - returns flat config directly
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"url": "http://example.com", "api_key": "secret_key"}
    )

    # Create context
    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function to match the actual implementation
    """Test enterprise systems status with attempt_to_connect=True."""
    # Mock factory_manager with liveness_status and is_alive methods
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=True)

    # Mock session registry - factory_manager is a plain property (not async)
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager - returns flat config
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    # Create context
    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    # Call the function with attempt_to_connect=True
    result = await enterprise_systems_status(
        context, _TEST_SYSTEM_NAME, attempt_to_connect=True
    )

    # Verify the result - always one system with name="system"
    assert result["success"] is True
    assert len(result["systems"]) == 1

    # Check system (always named "system")
    system = result["systems"][0]
    assert system["name"] == "system"
    assert system["type"] == "enterprise"
    assert system["liveness_status"] == "ONLINE"
    assert "liveness_detail" not in system  # No detail was provided
    assert system["is_alive"] is True
    # Health-only contract: no declared configuration in the response.
    assert "config" not in system

    # COMPLETED with no errors should not include initialization info
    assert "partial_result" not in result

    # Verify liveness_status was called with attempt_to_connect=True
    mock_factory_manager.liveness_status.assert_called_once_with(ensure_item=True)


@pytest.mark.asyncio
async def test_enterprise_systems_status_no_systems():
    """Test enterprise systems status - new code always returns exactly one system."""
    # Mock factory_manager reporting OFFLINE status
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    # Mock session registry - factory_manager is a plain property (not async)
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager - returns flat config
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    # Create context
    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    # Verify the result - new code always returns exactly 1 system
    assert result["success"] is True
    assert len(result["systems"]) == 1
    assert result["systems"][0]["name"] == "system"
    assert result["systems"][0]["liveness_status"] == "OFFLINE"
    assert result["systems"][0]["is_alive"] is False
    # COMPLETED with no errors should not include initialization info
    assert "partial_result" not in result


@pytest.mark.asyncio
async def test_enterprise_systems_status_all_status_types():
    """Test enterprise systems status with all possible status types (single system)."""
    # New code has a single system; test each status type individually
    status_details = [
        (ResourceLivenessStatus.ONLINE, "System is healthy"),
        (ResourceLivenessStatus.OFFLINE, "System is not responding"),
        (ResourceLivenessStatus.UNAUTHORIZED, "Authentication failed"),
        (ResourceLivenessStatus.MISCONFIGURED, "Invalid configuration"),
        (ResourceLivenessStatus.UNKNOWN, "Unknown error occurred"),
    ]

    for status, detail in status_details:
        mock_factory_manager = AsyncMock()
        mock_factory_manager.liveness_status = AsyncMock(return_value=(status, detail))
        mock_factory_manager.is_alive = AsyncMock(
            return_value=(status == ResourceLivenessStatus.ONLINE)
        )

        mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
        mock_session_registry.system_name = _TEST_SYSTEM_NAME
        mock_session_registry.factory_manager = mock_factory_manager
        mock_session_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        mock_config_manager = AsyncMock()
        mock_config_manager.get_config = AsyncMock(return_value={})

        context = MockContext(
            {
                "registry": mock_session_registry,
                "config_manager": mock_config_manager,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

        assert result["success"] is True
        assert len(result["systems"]) == 1
        system = result["systems"][0]
        assert system["name"] == "system"
        assert system["liveness_status"] == status.name
        assert system["liveness_detail"] == detail
        assert system["is_alive"] == (status == ResourceLivenessStatus.ONLINE)
        assert "partial_result" not in result


@pytest.mark.asyncio
async def test_enterprise_systems_status_registry_error():
    """Test enterprise systems status when session_registry.get_all() fails."""
    # Mock session registry where get_all raises an exception
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.get_all = AsyncMock(side_effect=Exception("Registry error"))

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    # Create context
    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    # Verify the result
    assert result["success"] is False
    assert result["isError"] is True
    assert "Registry error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_liveness_error():
    """Test enterprise systems status when factory_manager.liveness_status raises."""
    # Mock factory_manager with liveness_status that raises an exception
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        side_effect=Exception("Liveness error")
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    # Mock session registry
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    # Create context
    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    # Verify the result
    assert result["success"] is False
    assert result["isError"] is True
    assert "Liveness error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_no_enterprise_registry():
    """Test enterprise systems status - new code always returns one system (OFFLINE when not connected)."""
    # Mock factory_manager returning OFFLINE
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    # Create context
    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    # Verify the result - new code always returns exactly 1 system
    assert result["success"] is True
    assert len(result["systems"]) == 1
    assert result["systems"][0]["name"] == "system"
    assert result["systems"][0]["liveness_status"] == "OFFLINE"
    assert result["systems"][0]["is_alive"] is False
    # COMPLETED with no errors should not include initialization info
    assert "partial_result" not in result


@pytest.mark.asyncio
async def test_enterprise_systems_status_factory_snapshot_unexpected_phase():
    """Test enterprise_systems_status surfaces initialization status for LOADING phase."""
    from deephaven_mcp.resource_manager import InitializationPhase

    # Mock factory_manager
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    # session_registry.get_all() returns a LOADING phase snapshot
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={}, phase=InitializationPhase.LOADING, errors={}
        )
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    # LOADING phase is surfaced in initialization info, not as an error
    assert result["success"] is True
    assert len(result["systems"]) == 1
    assert result["systems"][0]["name"] == "system"
    assert "partial_result" in result
    assert "actively running" in result["partial_result"]["detail"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_factory_snapshot_with_errors():
    """Test enterprise_systems_status surfaces init errors from session registry snapshot."""
    from deephaven_mcp.resource_manager import InitializationPhase

    # Mock factory_manager
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=True)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    # session_registry.get_all() returns a COMPLETED snapshot with errors
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={"factory_reg": "something broke"},
        )
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    # Errors from snapshot are surfaced in initialization info,
    # keyed by system name so the diagnostic attributes per system.
    assert result["success"] is True
    assert len(result["systems"]) == 1
    assert result["systems"][0]["name"] == "system"
    assert "partial_result" in result
    assert result["partial_result"]["errors"] == {"system": "something broke"}


@pytest.mark.asyncio
async def test_enterprise_systems_status_joins_multiple_sources_with_semicolon():
    """Multiple error sources for one system join with '; ' in partial_result.errors.

    Pins the merged-errors contract documented in the Returns section: a single
    system reporting several discovery sources collapses to one '; '-joined
    string keyed by system name, so the diagnostic stays one line per system.
    Sources join in sorted-by-key order so the output is deterministic.
    """
    from deephaven_mcp.resource_manager import InitializationPhase

    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={
                "factory_reg": "DeephavenConnectionError: network down",
                "client": "AuthenticationError: bad token",
            },
        )
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    assert result["partial_result"]["errors"] == {
        "system": "AuthenticationError: bad token; "
        "DeephavenConnectionError: network down"
    }


# =============================================================================
# enterprise_systems_status initialization status tests
# =============================================================================


@pytest.mark.asyncio
async def test_enterprise_systems_status_discovery_in_progress():
    """Test enterprise_systems_status shows status when discovery is in progress."""
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.LOADING,
            errors={},
        )
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    assert result["success"] is True
    assert "partial_result" in result
    assert "actively running" in result["partial_result"]["detail"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_discovery_in_progress_with_errors():
    """Test enterprise_systems_status shows both status and errors during discovery."""
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=False)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.LOADING,
            errors={"factory1": "Connection refused"},
        )
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    assert result["success"] is True
    assert "partial_result" in result
    assert "actively running" in result["partial_result"]["detail"]
    assert result["partial_result"]["errors"] == {"system": "Connection refused"}


@pytest.mark.asyncio
async def test_enterprise_systems_status_completed_with_errors():
    """Test enterprise_systems_status shows init_errors."""
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=True)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={"factory1": "Connection failed: Connection refused"},
        )
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    assert result["success"] is True
    assert "partial_result" in result
    assert result["partial_result"]["errors"] == {
        "system": "Connection failed: Connection refused"
    }
    assert "connection issues" in result["partial_result"]["detail"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_completed_no_errors():
    """Test enterprise_systems_status omits init fields when no errors."""
    mock_factory_manager = AsyncMock()
    mock_factory_manager.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_factory_manager.is_alive = AsyncMock(return_value=True)

    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(return_value={})

    context = MockContext(
        {
            "registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context, _TEST_SYSTEM_NAME)

    assert result["success"] is True
    assert "partial_result" not in result


@pytest.mark.asyncio
async def test_session_enterprise_create_no_session_creation_config():
    """session_enterprise_create returns error when session_creation section is absent."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={
            "connection_json_url": "https://prod.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "admin",
            "password": "secret",
        }
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(context, _TEST_SYSTEM_NAME, "test-worker")

    assert result["isError"] is True
    assert result["success"] is False
    assert "session_creation" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_create_success_with_defaults():
    """Test session_enterprise_create with config defaults."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Flat config format returned directly by get_config()
    flat_config = {
        "connection_json_url": "https://prod.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "admin",
        "password": "secret",
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "heap_size_gb": 8.0,
                "auto_delete_timeout": 3600,
                "server": "server-east-1",
                "engine": "DeephavenCommunity",
            },
        },
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # Mock session registry and factories - use factory_manager directly
    mock_factory_manager = AsyncMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_session.pqinfo = AsyncMock(
        return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=1)))
    )

    mock_registry.factory_manager = mock_factory_manager
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    # Mock no existing workers (under limit)
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )  # No conflict
    mock_registry.add_session = AsyncMock()
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(context, _TEST_SYSTEM_NAME, "test-worker")

    assert result["success"] is True
    assert result["id"] == "enterprise:system:1"
    assert result["system_name"] == "system"
    assert result["session_name"] == "test-worker"
    assert result["configuration"]["heap_size_gb"] == 8.0
    assert result["configuration"]["auto_delete_timeout"] == 3600
    assert result["configuration"]["server"] == "server-east-1"
    assert result["configuration"]["engine"] == "DeephavenCommunity"

    # Verify worker was created with correct parameters
    mock_factory.connect_to_new_worker.assert_called_once_with(
        name="test-worker",
        heap_size_gb=8.0,
        auto_delete_timeout=3600,
        server="server-east-1",
        engine="DeephavenCommunity",
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        configuration_transformer=None,
        session_arguments=None,
    )

    # Verify session was added to registry
    # Verify add_session was called with manager only
    mock_registry.add_session.assert_called_once()
    call_args = mock_registry.add_session.call_args
    session_manager = call_args[0][0]  # Manager is the only argument
    assert str(session_manager.qualified_session_id) == "enterprise:system:1"
    assert session_manager.origin is SessionOrigin.DYNAMIC


@pytest.mark.asyncio
async def test_session_enterprise_create_success_with_overrides():
    """Test session_enterprise_create with parameter overrides."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Flat config format returned directly by get_config()
    flat_config = {
        "connection_json_url": "https://prod.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "admin",
        "password": "secret",
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4.0, "auto_delete_timeout": 1800},
        },
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # Mock session registry and factories - use factory_manager directly
    mock_factory_manager = AsyncMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_session.pqinfo = AsyncMock(
        return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=1)))
    )

    mock_registry.factory_manager = mock_factory_manager
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )  # No conflict
    mock_registry.add_session = AsyncMock()
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(
        context,
        _TEST_SYSTEM_NAME,
        "custom-worker",
        heap_size_gb=16.0,
        auto_delete_timeout=7200,
        server="server-west-1",
        engine="DeephavenEnterprise",
    )

    assert result["success"] is True
    assert result["configuration"]["heap_size_gb"] == 16.0  # Override
    assert result["configuration"]["auto_delete_timeout"] == 7200  # Override
    assert result["configuration"]["server"] == "server-west-1"  # Override
    assert result["configuration"]["engine"] == "DeephavenEnterprise"  # Override

    mock_factory.connect_to_new_worker.assert_called_once_with(
        name="custom-worker",
        heap_size_gb=16.0,
        auto_delete_timeout=7200,
        server="server-west-1",
        engine="DeephavenEnterprise",
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        configuration_transformer=None,
        session_arguments=None,
    )


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_generate_name():
    """Test session_enterprise_create auto-generates worker name when None."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Flat config format returned directly by get_config()
    flat_config = {
        "connection_json_url": "https://test.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "test",
        "password": "test",
        "session_creation": {
            "max_concurrent_sessions": 3,
            "defaults": {"heap_size_gb": 4},
        },
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20241126-1430"

        # Mock session registry and factories - use factory_manager directly
        mock_factory_manager = AsyncMock()
        mock_factory = MagicMock()
        mock_session = MagicMock()
        mock_session.pqinfo = AsyncMock(
            return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=1)))
        )

        mock_registry.factory_manager = mock_factory_manager
        mock_factory_manager.get = AsyncMock(return_value=mock_factory)
        mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

        mock_registry.get_all = AsyncMock(return_value={})
        mock_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("Session not found")
        )  # No conflict
        mock_registry.add_session = AsyncMock()
        mock_registry.count_added_sessions = AsyncMock(return_value=0)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "registry": mock_registry,
            }
        )

        result = await session_enterprise_create(context, _TEST_SYSTEM_NAME)

        assert result["success"] is True
        assert result["session_name"] == "mcp-test-20241126-1430"
        assert result["id"] == "enterprise:system:1"


@pytest.mark.asyncio
async def test_session_enterprise_create_system_not_found():
    """Test session_enterprise_create when factory connection fails."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Flat config with sessions enabled
    flat_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4},
        }
    }
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # factory_manager.get() raises a connection error
    mock_factory_manager = AsyncMock()
    mock_factory_manager.get = AsyncMock(
        side_effect=RuntimeError("connection failed: system not available")
    )
    mock_registry.factory_manager = mock_factory_manager
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(context, _TEST_SYSTEM_NAME, "worker")

    assert result["success"] is False
    assert "connection failed" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_max_workers_exceeded():
    """Test session_enterprise_create when max concurrent workers limit exceeded."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Flat config format returned directly by get_config()
    flat_config = {
        "connection_json_url": "https://limited.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
        "session_creation": {"max_concurrent_sessions": 2},  # Low limit for testing
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # Mock registry to return 2 existing sessions (at limit)
    mock_registry.count_added_sessions = AsyncMock(return_value=2)

    # Mock session registry get to simulate existing sessions for counting
    async def mock_session_get(id):
        if id in [
            "enterprise:system:101",
            "enterprise:system:102",
        ]:
            return MagicMock(spec=EnterpriseSessionManager)
        elif id == "enterprise:system:103":
            raise RegistryItemNotFoundError(
                "Session not found"
            )  # New session doesn't exist yet
        else:
            raise RegistryItemNotFoundError("Session not found")

    mock_registry.get = AsyncMock(side_effect=mock_session_get)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(context, _TEST_SYSTEM_NAME, "worker3")

    assert result["success"] is False
    assert "Max concurrent sessions (2) reached" in result["error"]
    assert result["isError"] is True

    # No cleanup needed - session tracking handled by registry


@pytest.mark.asyncio
async def test_session_enterprise_create_no_pre_create_name_conflict_check():
    """Display-name conflict is not checked before create: the controller
    assigns the integer serial (the SessionId), so two sessions sharing
    a display name still get distinct ids by construction.

    This pins the new behavior — the old name-based pre-create conflict check
    is gone.
    """
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    flat_config = {
        "connection_json_url": "https://conflict.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
        "session_creation": {"max_concurrent_sessions": 5},
    }
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    mock_factory_manager = AsyncMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_session.pqinfo = AsyncMock(
        return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=200)))
    )
    mock_registry.factory_manager = mock_factory_manager
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    mock_registry.get = AsyncMock(return_value=MagicMock())
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_registry.add_session = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(
        context, _TEST_SYSTEM_NAME, "existing-worker"
    )

    assert result["success"] is True
    assert result["id"] == "enterprise:system:200"


@pytest.mark.asyncio
async def test_session_enterprise_create_factory_creation_failure():
    """Test session_enterprise_create when worker creation fails."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # Flat config format returned directly by get_config()
    flat_config = {
        "connection_json_url": "https://failing.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4},
        },
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # Mock session registry - no conflict
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("No session found")
    )
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    # Mock factory_manager directly - factory fails during worker creation
    mock_factory_manager = AsyncMock()
    mock_factory = MagicMock()

    mock_registry.factory_manager = mock_factory_manager
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(
        side_effect=Exception("Resource exhausted")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(
        context, _TEST_SYSTEM_NAME, "failing-worker"
    )

    assert result["success"] is False
    assert "Resource exhausted" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_at_session_limit():
    """Reaching ``max_concurrent_sessions`` rejects new creates.

    The previous ``0``-as-disabled sentinel is gone; the cap must be
    a positive integer, and ``None`` disables it. We exercise the
    per-system cap by saturating the registry to its configured
    limit and asserting the typed error path."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_registry.count_added_sessions = AsyncMock(return_value=1)
    mock_config_manager = MagicMock()

    flat_config = {
        "connection_json_url": "https://limit.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
        "session_creation": {
            "max_concurrent_sessions": 1,
            "defaults": {"heap_size_gb": 4},
        },
    }

    # get_config() returns flat config directly
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_create(context, _TEST_SYSTEM_NAME, "test-worker")

    assert result["success"] is False
    assert "Max concurrent sessions" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_success():
    """Test session_enterprise_delete successful deletion."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    enterprise_config = {
        "system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock existing enterprise session manager
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock()
    mock_session_manager.name = "test-worker"

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, "enterprise:system:1")

    assert result["success"] is True
    assert result["id"] == "enterprise:system:1"
    assert result["system_name"] == "system"
    assert result["session_name"] == "test-worker"

    # Verify the persistent query was deleted on the controller (serial 1)
    mock_controller.delete_query.assert_awaited_once_with(CorePlusQuerySerial(1))
    # Verify session was closed and removed
    mock_session_manager.close.assert_called_once()
    # Verify remove was called
    mock_registry.remove.assert_called_once_with(
        QualifiedSessionId.from_str("enterprise:system:1")
    )


@pytest.mark.asyncio
async def test_session_enterprise_delete_system_not_found():
    """Test session_enterprise_delete when session registry raises unexpected error."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    # session_registry.get() raises unexpected RuntimeError (not RegistryItemNotFoundError)
    mock_registry.get = AsyncMock(
        side_effect=RuntimeError("connection to registry failed")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, "enterprise:system:104")

    assert result["success"] is False
    assert "connection to registry failed" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_session_not_found():
    """Test session_enterprise_delete when session not found."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    enterprise_config = {
        "system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, "enterprise:system:999")

    assert result["success"] is False
    assert "Session 'enterprise:system:999' not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_not_enterprise_session():
    """Test session_enterprise_delete when session is not an EnterpriseSessionManager."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    enterprise_config = {
        "system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock non-enterprise session manager
    mock_session_manager = MagicMock()  # Not an EnterpriseSessionManager

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, "enterprise:system:300")

    assert result["success"] is False
    assert "is not an enterprise session" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_close_failure_continues():
    """Test session_enterprise_delete continues removal even if close fails."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME
    mock_config_manager = MagicMock()

    enterprise_config = {
        "system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock session manager that fails to close
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.close = AsyncMock(side_effect=Exception("Close failed"))

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, "enterprise:system:400")

    # Should succeed despite close failure
    assert result["success"] is True
    assert result["id"] == "enterprise:system:400"

    # Verify session was still removed from registry
    # Verify remove was called even after close failure
    mock_registry.remove.assert_called_once_with(
        QualifiedSessionId.from_str("enterprise:system:400")
    )


@pytest.mark.asyncio
async def test_session_enterprise_delete_invalid_session_id_format():
    """session_enterprise_delete returns error for malformed id."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, id="not-a-valid-id")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid id format" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_wrong_system_type():
    """session_enterprise_delete returns error when id is not enterprise type."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = _TEST_SYSTEM_NAME

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await session_enterprise_delete(context, id="community:system:1")

    assert result["success"] is False
    assert result["isError"] is True
    assert "is not an enterprise session" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_unknown_system():
    """session_enterprise_delete returns a clear error when the session id
    targets an enterprise system this server does not manage.

    The multiplexed server can host any number of enterprise systems, so
    routing by the ``<system_name>`` segment of the session id now
    surfaces an :class:`InvalidSessionNameError` listing the configured
    systems rather than the single-system mismatch message used by the
    legacy DHE-only server.
    """
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.system_name = "prod"  # The only configured system.

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # id targets "dev", which is not in enterprise_systems.
    result = await session_enterprise_delete(context, id="enterprise:dev:11")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Enterprise system 'dev' is not configured" in result["error"]
    assert "'prod'" in result["error"]


def test_env_vars_to_list_helper_round_trip():
    """``_env_vars_to_list`` adapts the ``dict[str, str]`` schema form
    to the controller's ``["NAME=value", ...]`` wire format and
    propagates ``None`` through unchanged."""
    from deephaven_mcp.mcp_systems_server._tools.session_enterprise import (
        _env_vars_to_list,
    )

    assert _env_vars_to_list(None) is None
    assert _env_vars_to_list({}) == []
    out = _env_vars_to_list({"FOO": "1", "BAR": "two"})
    assert sorted(out) == sorted(["FOO=1", "BAR=two"])


@pytest.mark.parametrize("bad_language", ["python", "GROOVY", "Rust"])
def test_resolve_session_parameters_rejects_invalid_programming_language(
    bad_language,
):
    """Untyped callers get a friendly error, never silent wrong-case forwarding."""
    from deephaven_mcp.sessions import EnterpriseSessionCreationDefaults

    defaults = EnterpriseSessionCreationDefaults.model_validate({})

    with pytest.raises(
        SessionCreationError,
        match=f"Invalid programming_language '{bad_language}'. "
        "Valid options: 'Groovy', 'Python'.",
    ):
        _resolve_session_parameters(
            heap_size_gb=None,
            auto_delete_timeout=None,
            server=None,
            engine=None,
            extra_jvm_args=None,
            environment_vars=None,
            admin_groups=None,
            viewer_groups=None,
            session_arguments=None,
            programming_language=bad_language,  # type: ignore[arg-type]  # exercising the untyped-caller guard
            defaults=defaults,
        )


def test_resolve_session_parameters():
    """Test _resolve_session_parameters helper function."""
    from deephaven_mcp.sessions import EnterpriseSessionCreationDefaults

    defaults = EnterpriseSessionCreationDefaults.model_validate(
        {
            "heap_size_gb": 4.0,
            "auto_delete_timeout": 1800,
            "server": "default-server",
            "engine": "DeephavenCommunity",
            "extra_jvm_args": ["-Xmx1g"],
            "environment_vars": {"ENV": "test"},
            "admin_groups": ["admins"],
            "viewer_groups": ["viewers"],
            "session_arguments": {"key": "value"},
            "programming_language": "Python",
        }
    )

    # Test with all parameters provided (should override defaults).
    # ``engine`` is a Literal at the schema layer, but the resolver
    # itself just forwards whatever the caller supplies; pin to a
    # legal value to keep the helper-level test self-consistent.
    result = _resolve_session_parameters(
        heap_size_gb=8.0,
        auto_delete_timeout=3600,
        server="custom-server",
        engine="DeephavenEnterprise",
        extra_jvm_args=["-Xmx2g"],
        environment_vars={"ENV": "prod"},
        admin_groups=["custom-admins"],
        viewer_groups=["custom-viewers"],
        session_arguments={"custom": "args"},
        programming_language="Groovy",
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 8.0
    assert result["auto_delete_timeout"] == 3600
    assert result["server"] == "custom-server"
    assert result["engine"] == "DeephavenEnterprise"
    assert result["extra_jvm_args"] == ["-Xmx2g"]
    assert result["extra_environment_vars"] == ["ENV=prod"]
    assert result["admin_groups"] == ["custom-admins"]
    assert result["viewer_groups"] == ["custom-viewers"]
    assert result["session_arguments"] == {"custom": "args"}
    assert result["programming_language"] == "Groovy"

    # Test with no parameters provided (should use defaults)
    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 4.0
    assert result["auto_delete_timeout"] == 1800
    assert result["server"] == "default-server"
    assert result["engine"] == "DeephavenCommunity"
    assert result["extra_jvm_args"] == ["-Xmx1g"]
    assert result["extra_environment_vars"] == ["ENV=test"]
    assert result["admin_groups"] == ["admins"]
    assert result["viewer_groups"] == ["viewers"]
    assert result["session_arguments"] == {"key": "value"}
    assert result["programming_language"] == "Python"

    # Test with mixed parameters (some provided, some defaults)
    result = _resolve_session_parameters(
        heap_size_gb=16.0,  # Override
        auto_delete_timeout=None,  # Use default
        server="override-server",  # Override
        engine=None,  # Use default
        extra_jvm_args=None,
        environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 16.0
    assert result["auto_delete_timeout"] == 1800
    assert result["server"] == "override-server"
    assert result["engine"] == "DeephavenCommunity"

    # Test with minimal defaults; every field on
    # ``EnterpriseSessionCreationDefaults`` carries a schema-level
    # default now, including ``heap_size_gb`` (4.0).
    minimal_defaults = EnterpriseSessionCreationDefaults.model_validate(
        {"heap_size_gb": 1.0}
    )
    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        session_arguments=None,
        programming_language=None,
        defaults=minimal_defaults,
    )

    assert result["heap_size_gb"] == 1.0
    assert result["auto_delete_timeout"] is None
    assert result["server"] is None
    assert result["engine"] == "DeephavenCommunity"
    assert result["extra_jvm_args"] is None
    assert result["extra_environment_vars"] is None
    assert result["admin_groups"] is None
    assert result["viewer_groups"] is None
    assert result["session_arguments"] is None
    assert result["programming_language"] == "Python"  # Schema default


@pytest.mark.asyncio
async def test_session_enterprise_create_success():
    """Test successful enterprise session creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_factory_manager = AsyncMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_session.pqinfo = AsyncMock(
        return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=1)))
    )

    # Configure factory_manager directly on session_registry (plain property)
    mock_session_registry.factory_manager = mock_factory_manager
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Mock session registry get to raise RegistryItemNotFoundError for non-existent sessions
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    # Flat config format returned directly by get_config()
    flat_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4.0, "programming_language": "Python"},
        },
        "username": "testuser",
    }

    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_enterprise_create(
        context,
        _TEST_SYSTEM_NAME,
        session_name="test-session",
        heap_size_gb=8.0,
        programming_language="Groovy",
    )

    # Verify success
    assert result["success"] is True
    assert result["id"] == "enterprise:system:1"
    assert result["system_name"] == "system"
    assert result["session_name"] == "test-session"

    # Verify session was added to registry
    mock_session_registry.add_session.assert_called_once()
    call_args = mock_session_registry.add_session.call_args
    session_manager = call_args[0][0]  # Manager is the only argument
    assert str(session_manager.qualified_session_id) == "enterprise:system:1"

    # Session tracking is now verified through registry methods
    # Verify session was added (tracked automatically by add_session)


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_generated_name():
    """Test enterprise session creation with auto-generated session name."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_factory_manager = AsyncMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_session.pqinfo = AsyncMock(
        return_value=MagicMock(config=MagicMock(pb=MagicMock(serial=1)))
    )

    # Configure factory_manager directly on session_registry (plain property)
    mock_session_registry.factory_manager = mock_factory_manager
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Mock session registry get to raise RegistryItemNotFoundError for non-existent sessions
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    # Wire-format config with password credentials carrying ``alice``
    # so the auto-name generator embeds the username in the session name.
    flat_config = {
        "connection_json_url": "https://auto.example.com/iris/connection.json",
        "auth": {
            "credentials": {
                "type": "password",
                "username": "alice",
                "password": "shh",
            }
        },
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4.0},
        },
    }

    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_enterprise_create(
        context,
        _TEST_SYSTEM_NAME,
        session_name=None,  # This should trigger auto-generation
    )

    # Verify success
    assert result["success"] is True
    # The SessionId is the controller-assigned serial (mocked to 1).
    assert result["id"] == "enterprise:system:1"
    assert result["system_name"] == "system"
    assert result["session_name"].startswith("mcp-alice-")


@pytest.mark.asyncio
async def test_session_enterprise_create_max_sessions_reached():
    """Test enterprise session creation when max concurrent sessions reached."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME

    # Flat config format with low max limit
    flat_config = {"session_creation": {"max_concurrent_sessions": 2, "defaults": {}}}

    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    # Mock registry to return 2 existing sessions (at limit)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    # Mock the session registry to return sessions for count validation
    async def mock_get(id):
        if id in [
            "enterprise:system:501",
            "enterprise:system:502",
        ]:
            return MagicMock()
        raise RegistryItemNotFoundError(f"Session {id} not found")

    mock_session_registry.get = AsyncMock(side_effect=mock_get)

    result = await session_enterprise_create(
        context, _TEST_SYSTEM_NAME, session_name="test-session"
    )

    # Verify failure due to max sessions reached
    assert result["success"] is False
    assert result["isError"] is True
    assert "Max concurrent sessions (2) reached" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_create_unbounded_when_cap_null():
    """``max_concurrent_sessions: null`` disables the cap (unbounded).

    A high in-flight count must not block new creates when the cap
    has been disabled at the per-system level."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.count_added_sessions = AsyncMock(return_value=10_000)

    flat_config = {
        "session_creation": {
            "max_concurrent_sessions": None,
            "defaults": {"heap_size_gb": 4},
        }
    }
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # ``factory_manager.get`` is reached only when the limit check
    # passes; raise here to short-circuit further work and prove that
    # the limit gate did not fire.
    mock_factory_manager = AsyncMock()
    mock_factory_manager.get = AsyncMock(
        side_effect=RuntimeError("limit-gate bypassed; reached factory")
    )
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_enterprise_create(
        context, _TEST_SYSTEM_NAME, session_name="test-session"
    )

    assert result["success"] is False
    assert "limit-gate bypassed" in result["error"]
    assert result["isError"] is True
    # ``count_added_sessions`` must not be consulted when the cap is
    # disabled; reaching it would be wasted work and would also drag
    # the registry into the gate path needlessly.
    mock_session_registry.count_added_sessions.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_enterprise_create_system_not_found_v2():
    """Test enterprise session creation when factory connection fails."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME

    # Flat config with sessions enabled
    flat_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4},
        }
    }
    mock_config_manager.get_config = AsyncMock(return_value=flat_config)

    # factory_manager.get() raises a connection error
    mock_factory_manager = AsyncMock()
    mock_factory_manager.get = AsyncMock(
        side_effect=RuntimeError("factory connection failed")
    )
    mock_session_registry.factory_manager = mock_factory_manager
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_enterprise_create(
        context, _TEST_SYSTEM_NAME, session_name="test-session"
    )

    # Verify failure due to factory connection failure
    assert result["success"] is False
    assert result["isError"] is True
    assert "factory connection failed" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_success_v2():
    """Test successful enterprise session deletion."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.origin = SessionOrigin.DYNAMIC
    mock_session_manager.name = "test-session"

    # Mock config
    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    # Mock session registry
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.close = AsyncMock()
    mock_session_registry.remove = AsyncMock(return_value=mock_session_manager)
    mock_controller = MagicMock()
    mock_controller.delete_query = AsyncMock()
    mock_factory = MagicMock(controller_client=mock_controller)
    mock_session_registry.factory_manager.get = AsyncMock(return_value=mock_factory)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    # Session tracking is now handled by registry - no manual setup needed

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, id="enterprise:system:1")

    # Verify success
    assert result["success"] is True
    assert result["id"] == "enterprise:system:1"
    assert result["system_name"] == "system"
    assert result["session_name"] == "test-session"

    # Verify session was removed from registry
    mock_session_registry.remove.assert_called_once_with(
        QualifiedSessionId.from_str("enterprise:system:1")
    )

    # Session tracking cleanup is now handled automatically by remove()


@pytest.mark.asyncio
async def test_session_enterprise_delete_not_found():
    """Test enterprise session deletion when session doesn't exist."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME

    # Mock config
    enterprise_config = {"system": {"session_creation": {"max_concurrent_sessions": 5}}}
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry to return RegistryItemNotFoundError for non-existent session
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_enterprise_delete(context, id="enterprise:system:9999")

    # Verify failure due to session not found
    assert result["success"] is False
    assert result["isError"] is True
    assert "Session 'enterprise:system:9999' not found" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_system_not_found_v2():
    """Test enterprise session deletion when session registry raises unexpected error."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME

    # session_registry.get() raises unexpected RuntimeError (not RegistryItemNotFoundError)
    mock_session_registry.get = AsyncMock(
        side_effect=RuntimeError("registry backend unavailable")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_enterprise_delete(context, id="enterprise:system:1")

    # Verify failure due to registry error
    assert result["success"] is False
    assert result["isError"] is True
    assert "registry backend unavailable" in result["error"]


@pytest.mark.asyncio
async def test_check_session_limit_disabled_when_cap_none():
    """``max_sessions=None`` disables the cap; the helper returns
    ``None`` and never queries the registry."""
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.count_added_sessions = AsyncMock(return_value=999)

    result = await _check_session_limit(mock_session_registry, None)

    assert result is None
    mock_session_registry.count_added_sessions.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_session_limit_under_limit():
    """Test _check_session_limit when under the session limit."""
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    result = await _check_session_limit(mock_session_registry, 5)

    assert result is None  # No error when under limit
    mock_session_registry.count_added_sessions.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_session_limit_at_limit():
    """Test _check_session_limit when at the session limit."""
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.system_name = _TEST_SYSTEM_NAME
    mock_session_registry.count_added_sessions = AsyncMock(return_value=5)

    result = await _check_session_limit(mock_session_registry, 5)

    assert result is not None
    assert result["success"] is False
    assert result["error"] == "Max concurrent sessions (5) reached for system 'system'"
    assert result["isError"] is True
    mock_session_registry.count_added_sessions.assert_awaited_once()


def _make_password_system(username: str = "testuser"):
    """Build a real :class:`EnterpriseSystemConfig` with password creds."""
    from deephaven_mcp.sessions import EnterpriseSystemConfig

    return EnterpriseSystemConfig.model_validate(
        {
            "name": "system",
            "connection_json_url": "https://example.com/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": username,
                    "password": "shh",
                }
            },
        }
    )


def _make_private_key_system():
    """Build a real :class:`EnterpriseSystemConfig` with private-key creds."""
    from deephaven_mcp.sessions import EnterpriseSystemConfig

    return EnterpriseSystemConfig.model_validate(
        {
            "name": "system",
            "connection_json_url": "https://example.com/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "private_key",
                    "key_text": "-----BEGIN KEY-----\nfake\n-----END KEY-----",
                }
            },
        }
    )


def test_generate_session_name_if_none_with_name():
    """Test _generate_session_name_if_none when session_name is provided."""
    system_config = _make_password_system()

    result = _generate_session_name_if_none(system_config, "provided-name")

    assert result == "provided-name"


def test_generate_session_name_if_none_with_username():
    """Test _generate_session_name_if_none when no name provided but username exists."""
    system_config = _make_password_system(username="testuser")

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20240101-1200"
        result = _generate_session_name_if_none(system_config, None)

    assert result == "mcp-testuser-20240101-1200"


def test_generate_session_name_if_none_without_username():
    """Without a username-bearing credential type, the name omits the user."""
    system_config = _make_private_key_system()  # No username field

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20240101-1200"
        result = _generate_session_name_if_none(system_config, None)

    assert result == "mcp-session-20240101-1200"


@pytest.mark.asyncio
async def test_check_session_id_available_success():
    """Test _check_session_id_available when session ID is available."""
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    result = await _check_session_id_available(
        mock_session_registry, "enterprise:prod:42"
    )

    assert result is None  # No error when session doesn't exist


@pytest.mark.asyncio
async def test_check_session_id_available_conflict():
    """Test _check_session_id_available when session ID already exists."""
    mock_session_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_session_registry.get = AsyncMock(return_value=MagicMock())  # Session exists

    result = await _check_session_id_available(
        mock_session_registry, "enterprise:prod:42"
    )

    assert result is not None
    assert result["success"] is False
    assert result["error"] == "Session 'enterprise:prod:42' already exists"
    assert result["isError"] is True


def test_resolve_session_parameters_with_defaults():
    """Test _resolve_session_parameters using configuration defaults."""
    from deephaven_mcp.sessions import EnterpriseSessionCreationDefaults

    defaults = EnterpriseSessionCreationDefaults.model_validate(
        {
            "heap_size_gb": 8.0,
            "auto_delete_timeout": 3600,
            "server": "default-server",
            "programming_language": "Python",
        }
    )

    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 8.0
    assert result["auto_delete_timeout"] == 3600
    assert result["server"] == "default-server"
    assert result["engine"] == "DeephavenCommunity"  # Default when not specified
    assert result["programming_language"] == "Python"


def test_resolve_session_parameters_with_overrides():
    """Test _resolve_session_parameters with parameter overrides."""
    from deephaven_mcp.sessions import EnterpriseSessionCreationDefaults

    defaults = EnterpriseSessionCreationDefaults.model_validate(
        {
            "heap_size_gb": 8.0,
            "auto_delete_timeout": 3600,
            "programming_language": "Python",
        }
    )

    result = _resolve_session_parameters(
        heap_size_gb=16.0,  # Override
        auto_delete_timeout=7200,  # Override
        server="custom-server",  # Override
        engine="DeephavenEnterprise",  # Override
        extra_jvm_args=["-Xms4g"],
        environment_vars={"VAR": "value"},
        admin_groups=["admins"],
        viewer_groups=["viewers"],
        session_arguments={"arg": "value"},
        programming_language="Groovy",  # Override
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 16.0
    assert result["auto_delete_timeout"] == 7200
    assert result["server"] == "custom-server"
    assert result["engine"] == "DeephavenEnterprise"
    assert result["extra_jvm_args"] == ["-Xms4g"]
    assert result["extra_environment_vars"] == ["VAR=value"]
    assert result["admin_groups"] == ["admins"]
    assert result["viewer_groups"] == ["viewers"]
    assert result["session_arguments"] == {"arg": "value"}
    assert result["programming_language"] == "Groovy"


def test_resolve_session_parameters_zero_values():
    """Test _resolve_session_parameters handles ``auto_delete_timeout=0`` correctly.

    Explicit ``auto_delete_timeout=0`` from the tool call is preserved by
    the resolver (``is not None`` check) rather than being treated as
    falsy and replaced by the default.
    """
    from deephaven_mcp.sessions import EnterpriseSessionCreationDefaults

    defaults = EnterpriseSessionCreationDefaults.model_validate(
        {
            "heap_size_gb": 1.0,
            "auto_delete_timeout": 3600,
        }
    )

    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=0,  # Explicitly set to 0
        server=None,
        engine=None,
        extra_jvm_args=None,
        environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["auto_delete_timeout"] == 0  # Should use explicit 0, not default


def test_register_tools_registers_all_enterprise_tools():
    """register_tools() registers every Enterprise session tool, including the
    always-available enterprise_systems_status."""
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("test-enterprise-server")
    register_tools(server)
    tools = server._tool_manager._tools
    assert set(tools) == {
        "enterprise_systems_status",
        "session_enterprise_create",
        "session_enterprise_delete",
    }


@pytest.mark.asyncio
async def test_session_enterprise_create_input_schema_advertises_programming_language():
    """The MCP inputSchema advertises the exact enum for programming_language.

    Regression guard: if the parameter reverts to a bare ``str``, AI
    agents lose the vocabulary from the tool schema and uncanonical
    values flow verbatim to the controller's ``scriptLanguage`` field.
    """
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("test-enterprise-server")
    register_tools(server)
    (tool,) = [
        t for t in await server.list_tools() if t.name == "session_enterprise_create"
    ]
    props = tool.inputSchema["properties"]

    language_variants = props["programming_language"]["anyOf"]
    assert {"enum": ["Python", "Groovy"], "type": "string"} in language_variants


# ---------------------------------------------------------------------------
# enterprise_systems_status with system=None (aggregation)
# ---------------------------------------------------------------------------


def _build_aggregating_context(systems: dict[str, dict]) -> MockContext:
    """Build a MockContext exposing multiple enterprise systems.

    ``systems`` maps system name to a dict with keys: ``status``,
    ``detail``, ``is_alive``, ``raw_config``, ``init_phase``,
    ``init_errors``.
    """
    enterprise_systems: dict[str, MagicMock] = {}
    enterprise_cfg_systems: dict[str, MagicMock] = {}
    for sys_name, spec in systems.items():
        factory_manager = AsyncMock()
        factory_manager.liveness_status = AsyncMock(
            return_value=(spec["status"], spec["detail"])
        )
        factory_manager.is_alive = AsyncMock(return_value=spec["is_alive"])

        reg = MagicMock(spec=EnterpriseSessionRegistry)
        reg.system_name = sys_name
        reg.factory_manager = factory_manager
        reg.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=spec.get("init_phase", InitializationPhase.COMPLETED),
                errors=spec.get("init_errors", {}),
            )
        )
        enterprise_systems[sys_name] = reg
        sys_cfg = MagicMock()
        sys_cfg.raw = spec["raw_config"]
        enterprise_cfg_systems[sys_name] = sys_cfg

    multi_registry = MagicMock()
    multi_registry.community = None
    multi_registry.enterprise_systems = enterprise_systems

    multi_config = MagicMock()
    multi_config.community = None
    multi_config.enterprise = MagicMock()
    multi_config.enterprise.systems = enterprise_cfg_systems
    multi_config.server = MagicMock()

    return MockContext(
        {
            "registry": multi_registry,
            "multi_config": multi_config,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )


@pytest.mark.asyncio
async def test_enterprise_systems_status_aggregates_when_system_none():
    """``system=None`` returns one entry per configured enterprise system."""
    context = _build_aggregating_context(
        {
            "prod": {
                "status": ResourceLivenessStatus.ONLINE,
                "detail": "ok",
                "is_alive": True,
                "raw_config": {"url": "http://prod"},
                "init_phase": InitializationPhase.COMPLETED,
                "init_errors": {},
            },
            "staging": {
                "status": ResourceLivenessStatus.OFFLINE,
                "detail": "down",
                "is_alive": False,
                "raw_config": {"url": "http://staging"},
                "init_phase": InitializationPhase.LOADING,
                "init_errors": {"factory": "boom"},
            },
        }
    )

    result = await enterprise_systems_status(context)

    assert result["success"] is True
    assert {s["name"] for s in result["systems"]} == {"prod", "staging"}
    # Every error is keyed by its originating system name.
    assert "partial_result" in result
    assert result["partial_result"]["errors"] == {"staging": "boom"}


@pytest.mark.asyncio
async def test_enterprise_systems_status_aggregation_no_enterprise_group():
    """``system=None`` with no enterprise group returns an empty list."""
    multi_config = MagicMock()
    multi_config.enterprise = None
    multi_registry = MagicMock()
    multi_registry.community = None
    multi_registry.enterprise_systems = {}
    context = MockContext(
        {
            "registry": multi_registry,
            "multi_config": multi_config,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )
    result = await enterprise_systems_status(context)
    assert result == {"success": True, "systems": []}


@pytest.mark.asyncio
async def test_enterprise_systems_status_aggregation_failed_outranks_loading():
    """A single FAILED sibling must surface in the merged phase, not be hidden by LOADING.

    Regression: the aggregation previously used a local ``phase_order`` table
    that ranked FAILED between LOADING and COMPLETED, which caused
    ``{FAILED, LOADING}`` to fold to LOADING and silently masked failures.
    The merge now reuses ``_least_advanced_phase`` from
    ``MultiSystemRegistry`` so FAILED always wins.
    """
    context = _build_aggregating_context(
        {
            "prod": {
                "status": ResourceLivenessStatus.OFFLINE,
                "detail": "down",
                "is_alive": False,
                "raw_config": {"url": "http://prod"},
                "init_phase": InitializationPhase.FAILED,
                "init_errors": {"factory": "fatal"},
            },
            "staging": {
                "status": ResourceLivenessStatus.OFFLINE,
                "detail": "starting",
                "is_alive": False,
                "raw_config": {"url": "http://staging"},
                "init_phase": InitializationPhase.LOADING,
                "init_errors": {},
            },
        }
    )

    result = await enterprise_systems_status(context)

    assert result["success"] is True
    assert "partial_result" in result
    # FAILED outranks LOADING so the merged status surfaces the failure
    # message rather than the in-progress "actively running" message.
    assert "failed critically" in result["partial_result"]["detail"]
    assert result["partial_result"]["errors"] == {"prod": "fatal"}


@pytest.mark.asyncio
async def test_enterprise_systems_status_aggregate_no_enterprise_returns_empty():
    """Aggregating with no enterprise section returns an empty system list.

    This is the Community-only path exercised by ``dhcli system status``:
    ``system=None`` with no enterprise config returns early, before any
    enterprise registry is consulted.
    """
    with patch(
        f"{_SE_MODULE}.get_multi_config",
        return_value=SimpleNamespace(enterprise=None),
    ):
        result = await enterprise_systems_status(MagicMock())

    assert result == {"success": True, "systems": []}
