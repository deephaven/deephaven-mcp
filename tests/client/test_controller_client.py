import asyncio
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session", autouse=True)
def patch_enterprise_modules():
    # Patch sys.modules for enterprise imports before _controller_client is imported
    mock_enterprise = types.ModuleType("deephaven_enterprise")
    mock_proto = types.ModuleType("deephaven_enterprise.proto")
    mock_auth_pb2 = types.ModuleType("deephaven_enterprise.proto.auth_pb2")
    mock_controller = types.ModuleType("deephaven_enterprise.client.controller")
    mock_controller.ControllerClient = MagicMock()
    sys.modules["deephaven_enterprise"] = mock_enterprise
    sys.modules["deephaven_enterprise.proto"] = mock_proto
    sys.modules["deephaven_enterprise.proto.auth_pb2"] = mock_auth_pb2
    sys.modules["deephaven_enterprise.client"] = types.ModuleType(
        "deephaven_enterprise.client"
    )
    sys.modules["deephaven_enterprise.client.controller"] = mock_controller
    sys.modules["deephaven_enterprise.client.util"] = types.ModuleType(
        "deephaven_enterprise.client.util"
    )
    yield


@pytest.fixture(scope="session")
def controller_client_mod(patch_enterprise_modules):
    from deephaven_mcp.client import _controller_client

    return _controller_client


from deephaven_mcp._exceptions import (
    AuthenticationError,
    DeephavenConnectionError,
    QueryError,
)


@pytest.fixture
def dummy_controller_client():
    client = MagicMock()
    client.map = MagicMock(return_value={})
    client.get = MagicMock(return_value="info")
    client.delete_query = MagicMock()
    client.start_and_wait = MagicMock()
    client.stop_query = MagicMock()
    client.wait_for_change = MagicMock(return_value=None)
    client.restart_query = MagicMock()
    client.start_and_wait = MagicMock()
    client.stop_query = MagicMock()
    client.stop_and_wait = MagicMock()
    client.ping = MagicMock(return_value=True)
    client.wait_for_change = MagicMock()
    client.get_serial_for_name = MagicMock(return_value="serial")
    client.add_query = MagicMock(return_value="serial")
    client.make_pq_config = MagicMock(return_value="config")
    client.subscribe = MagicMock(return_value=None)
    return client


@pytest.fixture
def coreplus_controller_client(dummy_controller_client, controller_client_mod):
    return controller_client_mod.CorePlusControllerClient(dummy_controller_client)


@pytest.mark.asyncio
async def test_map_success(coreplus_controller_client, dummy_controller_client):
    # Simulate successful subscription
    coreplus_controller_client._subscribed = True
    dummy_controller_client.map.return_value = {"serial": "info"}
    with patch(
        "deephaven_mcp.client._controller_client.CorePlusQuerySerial",
        side_effect=lambda x: f"serial-{x}",
    ):
        with patch(
            "deephaven_mcp.client._controller_client.CorePlusQueryInfo",
            side_effect=lambda x: f"info-{x}",
        ):
            result = await coreplus_controller_client.map()
            assert result == {"serial": "info-info"}


@pytest.mark.asyncio
async def test_map_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.map.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.map()


@pytest.mark.asyncio
async def test_map_other_error(coreplus_controller_client, dummy_controller_client):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.map.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.map()


@pytest.mark.asyncio
async def test_get_success(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.get.return_value = "info"
    with patch(
        "deephaven_mcp.client._controller_client.CorePlusQueryInfo",
        side_effect=lambda x: f"info-{x}",
    ):
        result = await coreplus_controller_client.get("serial")
        assert result == "info-info"
        dummy_controller_client.get.assert_called_once_with("serial", 0)


@pytest.mark.asyncio
async def test_get_query_error(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.get.side_effect = KeyError("not found")
    with pytest.raises(QueryError):
        await coreplus_controller_client.get("serial")


@pytest.mark.asyncio
async def test_get_timeout_error(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.get.side_effect = TimeoutError("timeout")
    with pytest.raises(TimeoutError):
        await coreplus_controller_client.get("serial")


@pytest.mark.asyncio
async def test_get_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.get.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.get("serial")


@pytest.mark.asyncio
async def test_get_other_error(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.get.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.get("serial")


@pytest.mark.asyncio
async def test_delete_query_success(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.delete_query.return_value = None
    await coreplus_controller_client.delete_query("serial")
    dummy_controller_client.delete_query.assert_called_once_with("serial")


@pytest.mark.asyncio
async def test_delete_query_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.delete_query.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.delete_query("serial")


@pytest.mark.asyncio
async def test_delete_query_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.delete_query.side_effect = ValueError("bad")
    with pytest.raises(ValueError):
        await coreplus_controller_client.delete_query("serial")


@pytest.mark.asyncio
async def test_delete_query_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.delete_query.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.delete_query("serial")


# --- Additional Coverage Tests ---
import builtins


@pytest.mark.asyncio
async def test_ping_success(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.ping.return_value = True
    result = await coreplus_controller_client.ping()
    assert result is True


@pytest.mark.asyncio
async def test_ping_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.ping.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.ping()


@pytest.mark.asyncio
async def test_ping_other_error(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.ping.side_effect = Exception("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.ping()


@pytest.mark.asyncio
async def test_wait_for_change_success(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change.return_value = None
    await coreplus_controller_client.wait_for_change(1.0)
    dummy_controller_client.wait_for_change.assert_called_once()


@pytest.mark.asyncio
async def test_wait_for_change_timeout(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change.side_effect = TimeoutError("timeout")
    with pytest.raises(TimeoutError):
        await coreplus_controller_client.wait_for_change(1.0)


@pytest.mark.asyncio
async def test_wait_for_change_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.wait_for_change(1.0)


@pytest.mark.asyncio
async def test_wait_for_change_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.wait_for_change(1.0)


@pytest.mark.asyncio
async def test_get_serial_for_name_success(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.get_serial_for_name.return_value = "serial"
    result = await coreplus_controller_client.get_serial_for_name("name")
    assert result == "serial"


@pytest.mark.asyncio
async def test_get_serial_for_name_timeout(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.get_serial_for_name.side_effect = TimeoutError("timeout")
    with pytest.raises(TimeoutError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_get_serial_for_name_value_error(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.get_serial_for_name.side_effect = ValueError("bad")
    with pytest.raises(ValueError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_get_serial_for_name_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.get_serial_for_name.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_get_serial_for_name_other_error(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.get_serial_for_name.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_add_query_success(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.add_query.return_value = "serial"
    # Set up query_config.pb with all fields accessed by logging
    query_config = MagicMock()
    query_config.pb.name = "test-query"
    query_config.pb.heapSizeGb = 8.0
    query_config.pb.scriptLanguage = "Python"
    query_config.pb.configurationType = "Script"
    query_config.pb.enabled = True
    query_config.pb.scriptCode = "print('hello')"
    query_config.pb.scriptPath = ""
    query_config.pb.serverName = ""
    query_config.pb.workerKind = "DeephavenCommunity"
    result = await coreplus_controller_client.add_query(query_config)
    assert result == "serial"


@pytest.mark.asyncio
async def test_add_query_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    query_config = MagicMock()
    query_config.config = "config"
    dummy_controller_client.add_query.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.add_query(query_config)


@pytest.mark.asyncio
async def test_add_query_value_error(
    coreplus_controller_client, dummy_controller_client
):
    query_config = MagicMock()
    query_config.config = "config"
    dummy_controller_client.add_query.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_controller_client.add_query(query_config)


@pytest.mark.asyncio
async def test_add_query_resource_error(
    coreplus_controller_client, dummy_controller_client
):
    query_config = MagicMock()
    query_config.config = "config"
    import deephaven_mcp._exceptions as exc

    dummy_controller_client.add_query.side_effect = exc.ResourceError("fail")
    with pytest.raises(exc.ResourceError):
        await coreplus_controller_client.add_query(query_config)


@pytest.mark.asyncio
async def test_add_query_other_error(
    coreplus_controller_client, dummy_controller_client
):
    query_config = MagicMock()
    query_config.config = "config"
    dummy_controller_client.add_query.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.add_query(query_config)


@pytest.mark.asyncio
async def test_make_pq_config_success(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    dummy_controller_client.make_pq_config.return_value = "config"
    # Patch CorePlusQueryConfig to a dummy class for test
    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        mock_cfg.return_value.config = "config"
        result = await coreplus_controller_client.make_pq_config("name", 1.0)
        assert hasattr(result, "config")


@pytest.mark.asyncio
async def test_make_pq_config_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.make_temporary_config.side_effect = RuntimeError(
        "config creation failed"
    )
    with pytest.raises(RuntimeError):
        await coreplus_controller_client.make_pq_config("name", 1.0)


@pytest.mark.asyncio
async def test_make_pq_config_mutually_exclusive_scripts(coreplus_controller_client):
    """Test that script_body and script_path are mutually exclusive."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        await coreplus_controller_client.make_pq_config(
            "name", 1.0, script_body="code", script_path="path/to/script.py"
        )


@pytest.mark.asyncio
async def test_make_pq_config_with_all_parameters(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that all config parameters are applied correctly with script_body."""
    mock_config = MagicMock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            script_body="print('hello')",
            programming_language="Python",
            configuration_type="RunAndDone",
            enabled=False,
            schedule=["SchedulerType=Daily", "StartTime=08:00:00"],
            restart_users="RU_ADMIN",
            extra_class_path=["/opt/libs/custom.jar"],
            init_timeout_nanos=5000000000,
            jvm_profile="large-memory",
            python_virtual_environment="my-venv",
        )

        # Verify all parameters were applied to config
        assert mock_config.scriptLanguage == "Python"
        assert mock_config.scriptCode == "print('hello')"
        assert mock_config.configurationType == "RunAndDone"
        assert mock_config.enabled == False
        assert mock_config.restartUsers == "RU_ADMIN"
        mock_config.extraClassPath.extend.assert_called_once_with(
            ["/opt/libs/custom.jar"]
        )
        mock_config.scheduling.extend.assert_called_once_with(
            ["SchedulerType=Daily", "StartTime=08:00:00"]
        )
        assert mock_config.initTimeoutNanos == 5000000000
        assert mock_config.jvmProfile == "large-memory"
        assert mock_config.pythonVirtualEnvironment == "my-venv"


@pytest.mark.asyncio
async def test_make_pq_config_with_script_path(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that script_path parameter is applied correctly."""
    mock_config = MagicMock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            script_path="IrisQueries/groovy/analytics.groovy",
            programming_language="Groovy",
        )

        # Verify script_path was applied
        assert mock_config.scriptPath == "IrisQueries/groovy/analytics.groovy"
        assert mock_config.scriptLanguage == "Groovy"


@pytest.mark.asyncio
async def test_make_pq_config_none_defaults_preserve_config(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that None parameters don't override make_temporary_config defaults."""
    mock_config = MagicMock()
    # Set up some default values that make_temporary_config would have set
    mock_config.scriptLanguage = "Groovy"
    mock_config.configurationType = "InteractiveConsole"
    mock_config.enabled = False
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        # Call with minimal parameters - None defaults should NOT override
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            # Not passing programming_language, configuration_type, enabled
            # so they should remain as set by make_temporary_config
        )

        # Verify the original values were preserved (not overwritten)
        assert mock_config.scriptLanguage == "Groovy"
        assert mock_config.configurationType == "InteractiveConsole"
        assert mock_config.enabled == False


@pytest.mark.asyncio
async def test_make_pq_config_auto_delete_timeout_passed_to_make_temporary_config(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that auto_delete_timeout is passed to make_temporary_config."""
    mock_config = MagicMock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=300,  # 5 minutes
        )

        # Verify auto_delete_timeout was passed to make_temporary_config
        dummy_controller_client.make_temporary_config.assert_called_once()
        call_args = dummy_controller_client.make_temporary_config.call_args
        # auto_delete_timeout is the 7th positional arg (after name, heap, server, extra_jvm_args, extra_env_vars, engine)
        assert call_args[0][6] == 300


@pytest.mark.asyncio
async def test_make_pq_config_enabled_true_is_applied(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that enabled=True is explicitly applied to config."""
    mock_config = MagicMock()
    mock_config.enabled = False  # Default from make_temporary_config
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            enabled=True,  # Explicitly set to True
        )

        # Verify enabled=True was applied (overriding the mock's False default)
        assert mock_config.enabled == True


@pytest.mark.asyncio
async def test_make_pq_config_permanent_query_clears_scheduling(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that permanent queries (auto_delete_timeout=None) clear temporary scheduling."""
    mock_config = MagicMock()
    mock_scheduling = MagicMock()
    mock_config.scheduling = mock_scheduling
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=None,  # Permanent query
        )

        # Verify make_temporary_config was called with a default timeout (600)
        dummy_controller_client.make_temporary_config.assert_called_once()
        call_args = dummy_controller_client.make_temporary_config.call_args
        # auto_delete_timeout is the 7th positional arg
        assert call_args[0][6] == 600  # Default timeout used for creation

        # Verify scheduling was cleared and set to continuous for permanent query
        mock_scheduling.__delitem__.assert_called_once_with(slice(None))
        # Verify all continuous scheduling parameters were appended
        append_calls = [call[0][0] for call in mock_scheduling.append.call_args_list]
        assert (
            "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerContinuous"
            in append_calls
        )
        assert "StartTime=00:00:00" in append_calls
        assert "DailyRestart=false" in append_calls
        assert "SchedulingDisabled=false" in append_calls


@pytest.mark.asyncio
async def test_restart_query_success(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.restart_query("serial")
    dummy_controller_client.restart_query.assert_called_once()


@pytest.mark.asyncio
async def test_restart_query_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.restart_query.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.restart_query("serial")


@pytest.mark.asyncio
async def test_restart_query_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.restart_query.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_controller_client.restart_query("serial")


@pytest.mark.asyncio
async def test_restart_query_key_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.restart_query.side_effect = KeyError("fail")
    with pytest.raises(KeyError):
        await coreplus_controller_client.restart_query("serial")


@pytest.mark.asyncio
async def test_restart_query_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.restart_query.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.restart_query("serial")


@pytest.mark.asyncio
async def test_start_and_wait_success(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.start_and_wait("serial")
    dummy_controller_client.start_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_start_and_wait_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.start_and_wait.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.start_and_wait("serial")


@pytest.mark.asyncio
async def test_start_and_wait_timeout(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.start_and_wait.side_effect = TimeoutError("fail")
    with pytest.raises(TimeoutError):
        await coreplus_controller_client.start_and_wait("serial")


@pytest.mark.asyncio
async def test_start_and_wait_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.start_and_wait.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_controller_client.start_and_wait("serial")


@pytest.mark.asyncio
async def test_start_and_wait_key_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.start_and_wait.side_effect = KeyError("fail")
    with pytest.raises(KeyError):
        await coreplus_controller_client.start_and_wait("serial")


@pytest.mark.asyncio
async def test_start_and_wait_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.start_and_wait.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.start_and_wait("serial")


@pytest.mark.asyncio
async def test_stop_query_success(coreplus_controller_client, dummy_controller_client):
    await coreplus_controller_client.stop_query("serial")
    dummy_controller_client.stop_query.assert_called_once()


@pytest.mark.asyncio
async def test_stop_query_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_query.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.stop_query("serial")


@pytest.mark.asyncio
async def test_stop_query_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_query.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_controller_client.stop_query("serial")


@pytest.mark.asyncio
async def test_stop_query_key_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_query.side_effect = KeyError("fail")
    with pytest.raises(KeyError):
        await coreplus_controller_client.stop_query("serial")


@pytest.mark.asyncio
async def test_stop_query_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_query.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.stop_query("serial")


@pytest.mark.asyncio
async def test_stop_and_wait_success(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.stop_and_wait("serial")
    dummy_controller_client.stop_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_stop_and_wait_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_and_wait.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.stop_and_wait("serial")


@pytest.mark.asyncio
async def test_stop_and_wait_timeout(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_and_wait.side_effect = TimeoutError("fail")
    with pytest.raises(TimeoutError):
        await coreplus_controller_client.stop_and_wait("serial")


@pytest.mark.asyncio
async def test_stop_and_wait_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_and_wait.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_controller_client.stop_and_wait("serial")


@pytest.mark.asyncio
async def test_stop_and_wait_key_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_and_wait.side_effect = KeyError("fail")
    with pytest.raises(KeyError):
        await coreplus_controller_client.stop_and_wait("serial")


@pytest.mark.asyncio
async def test_stop_and_wait_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.stop_and_wait.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.stop_and_wait("serial")


@pytest.mark.asyncio
async def test_subscribe_success(coreplus_controller_client, dummy_controller_client):
    await coreplus_controller_client.subscribe()
    dummy_controller_client.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.subscribe.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.subscribe()


@pytest.mark.asyncio
async def test_subscribe_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.subscribe.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.subscribe()


@pytest.mark.asyncio
async def test_subscribe_idempotent(
    coreplus_controller_client, dummy_controller_client
):
    """Test that calling subscribe() multiple times is safe and only subscribes once."""
    # First call should actually subscribe
    await coreplus_controller_client.subscribe()
    dummy_controller_client.subscribe.assert_called_once()

    # Second call should be a no-op
    await coreplus_controller_client.subscribe()
    # Still only called once
    dummy_controller_client.subscribe.assert_called_once()

    # Third call should also be a no-op
    await coreplus_controller_client.subscribe()
    dummy_controller_client.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_map_without_subscribe_raises_internal_error(
    coreplus_controller_client, dummy_controller_client
):
    """Test that calling map() without subscribe() raises InternalError."""
    from deephaven_mcp._exceptions import InternalError

    with pytest.raises(InternalError) as exc_info:
        await coreplus_controller_client.map()
    assert "subscribe() must be called before map()" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_serial_for_name_without_subscribe_raises_internal_error(
    coreplus_controller_client, dummy_controller_client
):
    """Test that calling get_serial_for_name() without subscribe() raises InternalError."""
    from deephaven_mcp._exceptions import InternalError

    with pytest.raises(InternalError) as exc_info:
        await coreplus_controller_client.get_serial_for_name("test")
    assert "subscribe() must be called before get_serial_for_name()" in str(
        exc_info.value
    )
