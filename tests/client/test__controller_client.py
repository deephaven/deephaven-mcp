import asyncio
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session")
def controller_client_mod():
    from deephaven_mcp.client import _controller_client

    return _controller_client


from deephaven_mcp._exceptions import (
    AuthenticationError,
    DeephavenConnectionError,
    QueryError,
)


@pytest.fixture(scope="session")
def pq_config_mod():
    from deephaven_mcp.client import _pq_config

    return _pq_config


def _make_config_mock():
    """Build a MagicMock PQ config protobuf usable by the field appliers.

    ``typeSpecificFieldsJson`` is a real empty string so ``_set_termination_delay``
    can ``json.loads`` it; ``scheduling`` is a MagicMock that records ``del x[:]``
    (``__delitem__``) and ``extend`` calls.
    """
    config = MagicMock()
    config.typeSpecificFieldsJson = ""
    config.scheduling = MagicMock()
    return config


@pytest.fixture
def dummy_controller_client():
    from deephaven_enterprise.client.controller import SubState

    client = MagicMock()
    # Model a vendor client that has not been subscribed yet (e.g. by the
    # vendor SessionManager during authentication).
    client.sub_state = SubState.NOT_SUBSCRIBED
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
    from deephaven_mcp.client._timeouts import EnterpriseClientTimeouts

    return controller_client_mod.CorePlusControllerClient(
        dummy_controller_client, timeouts=EnterpriseClientTimeouts()
    )


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


@pytest.mark.asyncio
async def test_delete_query_timeout(
    coreplus_controller_client, dummy_controller_client
):
    """Test that delete_query() raises DeephavenConnectionError on timeout."""
    import time

    def slow_delete(serial):
        time.sleep(0.1)

    dummy_controller_client.delete_query.side_effect = slow_delete

    # Use a tiny config-driven default to force a timeout.
    coreplus_controller_client._timeouts = (
        coreplus_controller_client._timeouts.model_copy(
            update={"pq_management_timeout_seconds": 0.01}
        )
    )
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.delete_query("serial")
    assert "timed out" in str(exc_info.value)


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
async def test_ping_timeout(coreplus_controller_client, dummy_controller_client):
    """Test that ping() raises DeephavenConnectionError on timeout."""
    import time

    def slow_ping():
        time.sleep(0.05)

    dummy_controller_client.ping.side_effect = slow_ping

    coreplus_controller_client._timeouts = (
        coreplus_controller_client._timeouts.model_copy(
            update={"quick_operation_timeout_seconds": 0.01}
        )
    )
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.ping()
    assert "timed out" in str(exc_info.value)


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
async def test_add_query_timeout(coreplus_controller_client, dummy_controller_client):
    """Test that add_query() raises DeephavenConnectionError on timeout."""
    import time

    def slow_add(config):
        time.sleep(0.1)

    query_config = MagicMock()
    query_config.pb = MagicMock()
    dummy_controller_client.add_query.side_effect = slow_add

    coreplus_controller_client._timeouts = (
        coreplus_controller_client._timeouts.model_copy(
            update={"pq_management_timeout_seconds": 0.01}
        )
    )
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.add_query(query_config)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_make_pq_config_success(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    dummy_controller_client.make_temporary_config.return_value = _make_config_mock()
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
    coreplus_controller_client,
    dummy_controller_client,
    controller_client_mod,
    pq_config_mod,
):
    """Test that all config parameters are applied correctly with script_body.

    An explicit ``schedule`` is mutually exclusive with ``auto_delete_timeout``, so it
    replaces the scheduling block wholesale and the auto-delete path is a no-op. The
    simple fields are written under their protobuf names (``classPathAdditions``,
    ``timeoutNanos``, ``pythonControl``), and ``restart_users`` routes through the enum
    converter (``RestartUsersEnum`` is patched since the Core+ wheel is absent in CI).
    """
    mock_config = _make_config_mock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with (
        patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True),
        patch.object(pq_config_mod, "RestartUsersEnum") as mock_restart_enum,
    ):
        mock_restart_enum.Value.return_value = 1
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

        # Verify all parameters were applied to config under their protobuf names.
        assert mock_config.scriptLanguage == "Python"
        assert mock_config.scriptCode == "print('hello')"
        assert mock_config.configurationType == "RunAndDone"
        assert mock_config.enabled == False
        # restart_users is converted via the enum (RU_ADMIN -> 1), not stored as a string.
        mock_restart_enum.Value.assert_called_once_with("RU_ADMIN")
        assert mock_config.restartUsers == 1
        mock_config.classPathAdditions.extend.assert_called_once_with(
            ["/opt/libs/custom.jar"]
        )
        mock_config.scheduling.extend.assert_called_once_with(
            ["SchedulerType=Daily", "StartTime=08:00:00"]
        )
        assert mock_config.timeoutNanos == 5000000000
        assert mock_config.jvmProfile == "large-memory"
        assert mock_config.pythonControl == "my-venv"


@pytest.mark.asyncio
async def test_make_pq_config_with_script_path(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that script_path parameter is applied correctly."""
    mock_config = _make_config_mock()
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
    mock_config = _make_config_mock()
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
    coreplus_controller_client,
    dummy_controller_client,
    controller_client_mod,
    pq_config_mod,
):
    """make_temporary_config always receives None for the auto-delete arg.

    Construction of the temporary scheduler now lives in ``_apply_pq_config_fields``,
    so make_pq_config passes ``None`` as the 7th positional arg and installs the
    temporary scheduler itself (via the patched ``GenerateScheduling``).
    """
    mock_config = _make_config_mock()
    dummy_controller_client.make_temporary_config.return_value = mock_config
    temp_scheduler = ["SchedulerType=Temporary", "AutoDelete=true"]

    with (
        patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True),
        patch.object(pq_config_mod, "GenerateScheduling") as mock_gen_scheduling,
    ):
        mock_gen_scheduling.generate_temporary_scheduler.return_value = temp_scheduler
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=300,  # 5 minutes
        )

        # make_temporary_config always gets None as the 7th positional arg now.
        dummy_controller_client.make_temporary_config.assert_called_once()
        call_args = dummy_controller_client.make_temporary_config.call_args
        assert call_args[0][6] is None

        # The temporary scheduler from GenerateScheduling replaces the scheduling block,
        # and TerminationDelay is set to 300s (in ms).
        mock_config.scheduling.__delitem__.assert_called_once_with(slice(None))
        mock_config.scheduling.extend.assert_called_once_with(temp_scheduler)
        import json as _json

        assert _json.loads(mock_config.typeSpecificFieldsJson)["TerminationDelay"] == {
            "type": "long",
            "value": "300000",
        }


@pytest.mark.asyncio
async def test_make_pq_config_auto_delete_timeout_zero_normalized_to_none(
    coreplus_controller_client,
    dummy_controller_client,
    controller_client_mod,
    pq_config_mod,
):
    """Test that auto_delete_timeout=0 keeps make_temporary_config's auto-delete arg None.

    make_temporary_config always gets None now; the permanent (continuous) scheduler is
    installed by ``_apply_pq_config_fields`` for ``auto_delete_timeout=0``.
    """
    mock_config = _make_config_mock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=0,  # 0 means permanent, same as None
        )

        # make_temporary_config always gets None as the 7th positional arg.
        dummy_controller_client.make_temporary_config.assert_called_once()
        call_args = dummy_controller_client.make_temporary_config.call_args
        assert call_args[0][6] is None

        # Permanent: continuous scheduling installed, no TerminationDelay.
        mock_config.scheduling.__delitem__.assert_called_once_with(slice(None))
        mock_config.scheduling.extend.assert_called_once_with(
            pq_config_mod._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING
        )
        mock_config.ClearField.assert_called_once_with("typeSpecificFieldsJson")


@pytest.mark.asyncio
async def test_make_pq_config_env_vars_converted_to_wire_format(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Env vars reach the vendor call in the controller's alternating wire format.

    Regression test: the controller reads extraEnvironmentVariables as a flat
    alternating key/value list; passing "KEY=VALUE" strings through unconverted
    is rejected server-side with "Has an invalid key with no value".
    """
    mock_config = _make_config_mock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True):
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            extra_environment_vars=["A=1", "OPTS=-Da=b"],
        )

    call_args = dummy_controller_client.make_temporary_config.call_args
    # extra_environment_vars is the 5th positional arg.
    assert call_args[0][4] == ["A", "1", "OPTS", "-Da=b"]


@pytest.mark.asyncio
async def test_make_pq_config_malformed_env_var_raises_value_error(
    coreplus_controller_client, dummy_controller_client
):
    with pytest.raises(ValueError, match="expected 'KEY=VALUE'"):
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            extra_environment_vars=["NOEQUALS"],
        )
    dummy_controller_client.make_temporary_config.assert_not_called()


@pytest.mark.asyncio
async def test_make_pq_config_owner_applied_when_supplied(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that owner is applied to config when supplied to make_pq_config."""
    mock_config = _make_config_mock()
    mock_config.owner = "authenticated-user"  # Default from make_temporary_config
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            owner="service-account",
        )

        assert mock_config.owner == "service-account"


@pytest.mark.asyncio
async def test_make_pq_config_owner_untouched_when_omitted(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that owner is left as the make_temporary_config default when not supplied."""
    mock_config = _make_config_mock()
    mock_config.owner = "authenticated-user"  # Default from make_temporary_config
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
        )

        assert mock_config.owner == "authenticated-user"


@pytest.mark.asyncio
async def test_make_pq_config_enabled_true_is_applied(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Test that enabled=True is explicitly applied to config."""
    mock_config = _make_config_mock()
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
    coreplus_controller_client,
    dummy_controller_client,
    controller_client_mod,
    pq_config_mod,
):
    """Test that permanent queries (auto_delete_timeout=None) install continuous scheduling."""
    mock_config = _make_config_mock()
    mock_scheduling = mock_config.scheduling
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=None,  # Permanent query
        )

        # Regression test for #165: permanent queries must pass auto_delete_timeout=None
        # to make_temporary_config so no TerminationDelay / TemporaryAutoDelete is set;
        # a non-None placeholder would survive the scheduling replacement and cause the
        # controller to auto-delete the "permanent" query.
        dummy_controller_client.make_temporary_config.assert_called_once()
        call_args = dummy_controller_client.make_temporary_config.call_args
        # auto_delete_timeout is the 7th positional arg, always None now.
        assert call_args[0][6] is None

        # Scheduling cleared and replaced wholesale with the continuous (permanent) block.
        mock_scheduling.__delitem__.assert_called_once_with(slice(None))
        mock_scheduling.extend.assert_called_once_with(
            pq_config_mod._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING
        )
        extended = mock_scheduling.extend.call_args[0][0]
        assert (
            "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerContinuous"
            in extended
        )
        assert "StartTime=00:00:00" in extended
        assert "DailyRestart=false" in extended
        assert "SchedulingDisabled=false" in extended
        # Permanent → no TerminationDelay: the presence-tracked field is cleared.
        mock_config.ClearField.assert_called_once_with("typeSpecificFieldsJson")


@pytest.mark.asyncio
async def test_make_pq_config_temporary_schedule_none_no_default_installed(
    coreplus_controller_client,
    dummy_controller_client,
    controller_client_mod,
    pq_config_mod,
):
    """Temporary PQ (auto_delete_timeout set) with schedule=None: install temp scheduler.

    With no explicit schedule, ``auto_delete_timeout`` drives the scheduler: the temporary
    scheduler from ``GenerateScheduling`` replaces the scheduling block and TerminationDelay
    is set.
    """
    mock_config = _make_config_mock()
    mock_scheduling = mock_config.scheduling
    dummy_controller_client.make_temporary_config.return_value = mock_config
    temp_scheduler = ["SchedulerType=Temporary"]

    with (
        patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True),
        patch.object(pq_config_mod, "GenerateScheduling") as mock_gen_scheduling,
    ):
        mock_gen_scheduling.generate_temporary_scheduler.return_value = temp_scheduler
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=300,  # Temporary query
            schedule=None,
        )

        # Temporary scheduler installed wholesale; TerminationDelay set.
        mock_scheduling.__delitem__.assert_called_once_with(slice(None))
        mock_scheduling.extend.assert_called_once_with(temp_scheduler)
        mock_gen_scheduling.generate_temporary_scheduler.assert_called_once()
        import json as _json

        assert "TerminationDelay" in _json.loads(mock_config.typeSpecificFieldsJson)


@pytest.mark.asyncio
async def test_make_pq_config_permanent_schedule_empty_clears_without_default(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Permanent PQ with schedule=[]: scheduling cleared, no continuous default installed.

    An explicit ``schedule`` (even empty) is mutually exclusive with auto_delete_timeout,
    so the auto-delete path is a no-op and the list applier clears the block to empty.
    """
    mock_config = _make_config_mock()
    mock_scheduling = mock_config.scheduling
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True):
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=None,  # Permanent
            schedule=[],  # Explicit "no scheduling"
        )

        # The list applier cleared the block and extended it with the empty list;
        # the continuous-default path did not run (no continuous entries).
        mock_scheduling.__delitem__.assert_called_once_with(slice(None))
        mock_scheduling.extend.assert_called_once_with([])
        # No TerminationDelay (auto-delete path was a no-op).
        assert mock_config.typeSpecificFieldsJson == ""


@pytest.mark.asyncio
async def test_make_pq_config_temporary_schedule_empty_clears(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Temporary PQ with schedule=[]: auto_delete_timeout and schedule are mutually exclusive.

    Supplying both a positive auto_delete_timeout and any explicit schedule (including the
    empty list) is rejected up front, because auto_delete_timeout installs its own scheduler.
    """
    with pytest.raises(ValueError, match="mutually exclusive"):
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=300,  # Temporary
            schedule=[],  # Explicit clear — mutually exclusive with auto_delete_timeout
        )


@pytest.mark.asyncio
async def test_make_pq_config_permanent_schedule_explicit_replaces_default(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Permanent PQ with a non-empty schedule: caller's list replaces default wholesale.

    Crucially, no entries from the default continuous scheduler must leak into the
    final scheduling list — the caller's list is authoritative.
    """
    mock_config = MagicMock()
    mock_scheduling = MagicMock()
    mock_config.scheduling = mock_scheduling
    dummy_controller_client.make_temporary_config.return_value = mock_config

    caller_schedule = [
        "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerDaily",
        "StartTime=09:00:00",
        "SchedulingDisabled=true",
    ]

    with patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True):
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=None,  # Permanent
            schedule=caller_schedule,
        )

        # extend called exactly once, with exactly the caller's list.
        mock_scheduling.extend.assert_called_once_with(caller_schedule)
        # The default continuous scheduler's append path must NOT have run,
        # so no default entries like "SchedulingDisabled=false" were appended.
        mock_scheduling.append.assert_not_called()


@pytest.mark.asyncio
async def test_make_pq_config_temporary_schedule_explicit_replaces_temp(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    """Temporary PQ with a non-empty schedule: auto_delete_timeout and schedule conflict.

    Supplying both is rejected up front; auto_delete_timeout installs its own scheduler.
    """
    caller_schedule = ["SchedulerType=Daily", "StartTime=08:00:00"]

    with pytest.raises(ValueError, match="mutually exclusive"):
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            auto_delete_timeout=300,  # Temporary
            schedule=caller_schedule,
        )


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
async def test_subscribe_timeout(coreplus_controller_client, dummy_controller_client):
    """Test that subscribe() raises DeephavenConnectionError on timeout."""
    import time

    def slow_subscribe():
        time.sleep(0.05)  # Simulate slow blocking response

    dummy_controller_client.subscribe.side_effect = slow_subscribe

    # Use a very short configured timeout to trigger timeout quickly
    coreplus_controller_client._timeouts = (
        coreplus_controller_client._timeouts.model_copy(
            update={"subscribe_timeout_seconds": 0.01}
        )
    )
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.subscribe()

    assert "timed out" in str(exc_info.value)


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
async def test_subscribe_adopts_existing_vendor_subscription(
    coreplus_controller_client, dummy_controller_client
):
    """A SUBSCRIBED vendor-side subscription is adopted, not duplicated.

    Regression test: the vendor SessionManager subscribes the controller client
    during authentication (_init_controller). Opening a second stream makes the
    controller server terminate the first, whose response thread auto-resubscribes
    and terminates the second — an infinite kill/re-subscribe loop that starves
    map()/get() ("Deadline exceeded waiting for subscription to finish").
    """
    from deephaven_enterprise.client.controller import SubState

    dummy_controller_client.sub_state = SubState.SUBSCRIBED
    await coreplus_controller_client.subscribe()
    dummy_controller_client.subscribe.assert_not_called()
    assert coreplus_controller_client._subscribed is True

    # State-read methods are usable after adoption.
    dummy_controller_client.map.return_value = {}
    assert await coreplus_controller_client.map() == {}


@pytest.mark.asyncio
async def test_subscribe_poisoned_vendor_subscription_raises(
    coreplus_controller_client, dummy_controller_client
):
    """A vendor client wedged at SUBSCRIBING is treated as poisoned, not adopted.

    Adopting it would leave every state read blocking on the vendor's
    subscription timeout; racing a second stream would deadlock. The wrapper
    raises so the owning factory is recreated instead.
    """
    from deephaven_enterprise.client.controller import SubState

    from deephaven_mcp._exceptions import DeephavenConnectionError

    dummy_controller_client.sub_state = SubState.SUBSCRIBING
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.subscribe()
    dummy_controller_client.subscribe.assert_not_called()
    assert coreplus_controller_client._subscribed is False


@pytest.mark.asyncio
async def test_subscribe_concurrent_callers_subscribe_once(
    coreplus_controller_client, dummy_controller_client
):
    """Concurrent ``subscribe()`` callers are serialized by the lock.

    Without the ``_subscribe_lock`` two tasks could both pass the
    ``self._subscribed`` check before either sets it to ``True`` and cause a
    duplicate subscription on the wrapped client.
    """
    import time

    def slow_subscribe():
        time.sleep(0.05)

    dummy_controller_client.subscribe.side_effect = slow_subscribe

    tasks = [
        asyncio.create_task(coreplus_controller_client.subscribe()) for _ in range(5)
    ]
    await asyncio.gather(*tasks)

    assert dummy_controller_client.subscribe.call_count == 1
    assert coreplus_controller_client._subscribed is True


@pytest.mark.asyncio
async def test_map_without_subscribe_raises_internal_error(
    coreplus_controller_client, dummy_controller_client
):
    """Test that calling map() without subscribe() raises InternalError."""
    from deephaven_mcp._exceptions import InternalError

    with pytest.raises(InternalError) as exc_info:
        await coreplus_controller_client.map()
    assert "subscribe() must be called before map()" in str(exc_info.value)


def test_is_poisoned_reflects_vendor_sub_state(
    coreplus_controller_client, dummy_controller_client
):
    """``is_poisoned`` is True only when the vendor is wedged at SUBSCRIBING."""
    from deephaven_enterprise.client.controller import SubState

    dummy_controller_client.sub_state = SubState.SUBSCRIBING
    assert coreplus_controller_client.is_poisoned is True

    dummy_controller_client.sub_state = SubState.SUBSCRIBED
    assert coreplus_controller_client.is_poisoned is False

    dummy_controller_client.sub_state = SubState.NOT_SUBSCRIBED
    assert coreplus_controller_client.is_poisoned is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method, args",
    [
        ("map", ()),
        ("map_and_version", ()),
        ("get", (12345,)),
        ("get_serial_for_name", ("some-pq",)),
        ("wait_for_change", (5.0,)),
        ("wait_for_change_from_version", (7, 5.0)),
    ],
)
async def test_reads_fast_fail_when_poisoned(
    coreplus_controller_client, dummy_controller_client, method, args
):
    """Poisoned reads raise DeephavenConnectionError immediately without touching the vendor.

    The vendor call would otherwise block for the full subscription timeout
    before failing; the fast-fail removes that wait and names the recovery step.
    Covers every subscription-backed public read, the long-poll waits included.
    """
    from deephaven_enterprise.client.controller import SubState

    from deephaven_mcp._exceptions import DeephavenConnectionError
    from deephaven_mcp.client import CONTROLLER_SUBSCRIBING_ERROR_CODE

    coreplus_controller_client._subscribed = True
    dummy_controller_client.sub_state = SubState.SUBSCRIBING

    with pytest.raises(DeephavenConnectionError) as exc_info:
        await getattr(coreplus_controller_client, method)(*args)

    message = str(exc_info.value)
    assert CONTROLLER_SUBSCRIBING_ERROR_CODE in message
    lower = message.lower()
    assert "connect" in lower
    assert "retry" in lower
    getattr(dummy_controller_client, method).assert_not_called()


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


@pytest.mark.asyncio
async def test_modify_query_success(
    coreplus_controller_client, dummy_controller_client
):
    """Test that modify_query successfully calls wrapped client."""
    from deephaven_mcp.client._protobuf import CorePlusQueryConfig

    mock_config = MagicMock(spec=CorePlusQueryConfig)
    mock_config.pb = MagicMock()
    mock_config.pb.serial = 12345
    mock_config.pb.name = "test_query"

    dummy_controller_client.modify_query = MagicMock()

    await coreplus_controller_client.modify_query(mock_config, restart=False)

    dummy_controller_client.modify_query.assert_called_once_with(mock_config.pb, False)


@pytest.mark.asyncio
async def test_modify_query_with_restart(
    coreplus_controller_client, dummy_controller_client
):
    """Test that modify_query passes restart parameter correctly."""
    from deephaven_mcp.client._protobuf import CorePlusQueryConfig

    mock_config = MagicMock(spec=CorePlusQueryConfig)
    mock_config.pb = MagicMock()
    mock_config.pb.serial = 12345
    mock_config.pb.name = "test_query"

    dummy_controller_client.modify_query = MagicMock()

    await coreplus_controller_client.modify_query(mock_config, restart=True)

    dummy_controller_client.modify_query.assert_called_once_with(mock_config.pb, True)


@pytest.mark.asyncio
async def test_modify_query_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    """Test that modify_query raises DeephavenConnectionError on connection failure."""
    from deephaven_mcp.client._protobuf import CorePlusQueryConfig

    mock_config = MagicMock(spec=CorePlusQueryConfig)
    mock_config.pb = MagicMock()
    mock_config.pb.serial = 12345
    mock_config.pb.name = "test_query"

    dummy_controller_client.modify_query = MagicMock(
        side_effect=ConnectionError("Connection failed")
    )

    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.modify_query(mock_config, restart=False)
    assert "Unable to connect to controller service" in str(exc_info.value)


@pytest.mark.asyncio
async def test_modify_query_value_error(
    coreplus_controller_client, dummy_controller_client
):
    """Test that modify_query re-raises ValueError unchanged."""
    from deephaven_mcp.client._protobuf import CorePlusQueryConfig

    mock_config = MagicMock(spec=CorePlusQueryConfig)
    mock_config.pb = MagicMock()
    mock_config.pb.serial = 12345
    mock_config.pb.name = "test_query"

    dummy_controller_client.modify_query = MagicMock(
        side_effect=ValueError("Invalid config")
    )

    with pytest.raises(ValueError) as exc_info:
        await coreplus_controller_client.modify_query(mock_config, restart=False)
    assert "Invalid config" in str(exc_info.value)


@pytest.mark.asyncio
async def test_modify_query_other_error(
    coreplus_controller_client, dummy_controller_client
):
    """Test that modify_query wraps other errors in QueryError."""
    from deephaven_mcp.client._protobuf import CorePlusQueryConfig

    mock_config = MagicMock(spec=CorePlusQueryConfig)
    mock_config.pb = MagicMock()
    mock_config.pb.serial = 12345
    mock_config.pb.name = "test_query"

    dummy_controller_client.modify_query = MagicMock(
        side_effect=RuntimeError("Internal error")
    )

    with pytest.raises(QueryError) as exc_info:
        await coreplus_controller_client.modify_query(mock_config, restart=False)
    assert "Failed to modify query" in str(exc_info.value)


@pytest.mark.asyncio
async def test_modify_query_timeout(
    coreplus_controller_client, dummy_controller_client
):
    """Test that modify_query() raises DeephavenConnectionError on timeout."""
    import time

    from deephaven_mcp.client._protobuf import CorePlusQueryConfig

    def slow_modify(config, restart):
        time.sleep(0.1)

    mock_config = MagicMock(spec=CorePlusQueryConfig)
    mock_config.pb = MagicMock()
    mock_config.pb.serial = 12345
    mock_config.pb.name = "test_query"

    dummy_controller_client.modify_query = slow_modify

    coreplus_controller_client._timeouts = (
        coreplus_controller_client._timeouts.model_copy(
            update={"pq_management_timeout_seconds": 0.01}
        )
    )
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.modify_query(mock_config, restart=False)
    assert "timed out" in str(exc_info.value)


# --- map_and_version() tests ---


@pytest.mark.asyncio
async def test_map_and_version_success(
    coreplus_controller_client, dummy_controller_client
):
    from deephaven_mcp._exceptions import InternalError

    coreplus_controller_client._subscribed = True
    mock_query_info = MagicMock()
    dummy_controller_client.map_and_version.return_value = (
        {1: mock_query_info, 2: mock_query_info},
        42,
    )

    result_map, version = await coreplus_controller_client.map_and_version()

    assert len(result_map) == 2
    assert version == 42
    dummy_controller_client.map_and_version.assert_called_once()


@pytest.mark.asyncio
async def test_map_and_version_not_subscribed(coreplus_controller_client):
    from deephaven_mcp._exceptions import InternalError

    coreplus_controller_client._subscribed = False

    with pytest.raises(InternalError) as exc_info:
        await coreplus_controller_client.map_and_version()
    assert "subscribe() must be called before map_and_version()" in str(exc_info.value)


@pytest.mark.asyncio
async def test_map_and_version_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.map_and_version.side_effect = ConnectionError(
        "connection failed"
    )

    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.map_and_version()
    assert "Unable to connect to controller service" in str(exc_info.value)


@pytest.mark.asyncio
async def test_map_and_version_other_error(
    coreplus_controller_client, dummy_controller_client
):
    coreplus_controller_client._subscribed = True
    dummy_controller_client.map_and_version.side_effect = Exception("unexpected error")

    with pytest.raises(QueryError) as exc_info:
        await coreplus_controller_client.map_and_version()
    assert "Failed to retrieve query state with version" in str(exc_info.value)


# --- wait_for_change_from_version() tests ---


@pytest.mark.asyncio
async def test_wait_for_change_from_version_changed(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change_from_version.return_value = True

    result = await coreplus_controller_client.wait_for_change_from_version(42, 10.0)

    assert result is True
    dummy_controller_client.wait_for_change_from_version.assert_called_once_with(
        42, 10.0
    )


@pytest.mark.asyncio
async def test_wait_for_change_from_version_timeout(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change_from_version.return_value = False

    result = await coreplus_controller_client.wait_for_change_from_version(42, 5.0)

    assert result is False
    dummy_controller_client.wait_for_change_from_version.assert_called_once_with(
        42, 5.0
    )


@pytest.mark.asyncio
async def test_wait_for_change_from_version_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change_from_version.side_effect = ConnectionError(
        "connection lost"
    )

    with pytest.raises(DeephavenConnectionError) as exc_info:
        await coreplus_controller_client.wait_for_change_from_version(42, 10.0)
    assert "Unable to connect to controller service" in str(exc_info.value)


@pytest.mark.asyncio
async def test_wait_for_change_from_version_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.wait_for_change_from_version.side_effect = RuntimeError(
        "unexpected error"
    )

    with pytest.raises(QueryError) as exc_info:
        await coreplus_controller_client.wait_for_change_from_version(42, 10.0)
    assert "Failed to wait for version change from 42" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_timeout", [0, -1, -0.001])
async def test_wait_for_change_from_version_invalid_timeout(
    coreplus_controller_client, dummy_controller_client, bad_timeout
):
    with pytest.raises(ValueError, match="timeout_seconds must be a positive value"):
        await coreplus_controller_client.wait_for_change_from_version(42, bad_timeout)
    dummy_controller_client.wait_for_change_from_version.assert_not_called()


# ---------------------------------------------------------------------------
# wait=True/False forwarding to upstream ControllerClient
# ---------------------------------------------------------------------------
#
# The PQ-state-change wrappers expose a ``wait: bool = True`` parameter.
# When ``wait=True`` the wrapper passes its configured
# ``pq_state_change_timeout_seconds`` to upstream; when ``wait=False`` it
# passes ``0`` (upstream's fire-and-forget convention).


@pytest.mark.asyncio
async def test_start_and_wait_passes_configured_timeout_when_waiting(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.start_and_wait(42)
    expected = coreplus_controller_client._timeouts.pq_state_change_timeout_seconds
    dummy_controller_client.start_and_wait.assert_called_once_with(42, expected)


@pytest.mark.asyncio
async def test_start_and_wait_passes_zero_when_not_waiting(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.start_and_wait(42, wait=False)
    dummy_controller_client.start_and_wait.assert_called_once_with(42, 0)


@pytest.mark.asyncio
async def test_stop_query_passes_configured_timeout_when_waiting(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.stop_query([42])
    expected = coreplus_controller_client._timeouts.pq_state_change_timeout_seconds
    dummy_controller_client.stop_query.assert_called_once_with([42], expected)


@pytest.mark.asyncio
async def test_stop_query_passes_zero_when_not_waiting(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.stop_query([42], wait=False)
    dummy_controller_client.stop_query.assert_called_once_with([42], 0)


@pytest.mark.asyncio
async def test_stop_and_wait_passes_configured_timeout_when_waiting(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.stop_and_wait(42)
    expected = coreplus_controller_client._timeouts.pq_state_change_timeout_seconds
    dummy_controller_client.stop_and_wait.assert_called_once_with(42, expected)


@pytest.mark.asyncio
async def test_stop_and_wait_passes_zero_when_not_waiting(
    coreplus_controller_client, dummy_controller_client
):
    await coreplus_controller_client.stop_and_wait(42, wait=False)
    dummy_controller_client.stop_and_wait.assert_called_once_with(42, 0)


# ===========================================================================
# update_pq_config tests
# ===========================================================================


def _update_config_wrapper():
    """A CorePlusQueryConfig-like wrapper whose .pb is a field-applier-ready mock."""
    wrapper = MagicMock()
    wrapper.pb = _make_config_mock()
    return wrapper


def test_update_pq_config_all_none_returns_false(coreplus_controller_client):
    wrapper = _update_config_wrapper()
    assert coreplus_controller_client.update_pq_config(wrapper) is False


def test_update_pq_config_sets_owner(coreplus_controller_client):
    wrapper = _update_config_wrapper()
    assert coreplus_controller_client.update_pq_config(wrapper, owner="svc") is True
    assert wrapper.pb.owner == "svc"


def test_update_pq_config_auto_delete_zero_continuous(
    coreplus_controller_client, pq_config_mod
):
    wrapper = _update_config_wrapper()
    wrapper.pb.typeSpecificFieldsJson = (
        '{"TerminationDelay": {"type": "long", "value": "1"}}'
    )
    assert (
        coreplus_controller_client.update_pq_config(wrapper, auto_delete_timeout=0)
        is True
    )
    wrapper.pb.scheduling.__delitem__.assert_called_once_with(slice(None))
    wrapper.pb.scheduling.extend.assert_called_once_with(
        pq_config_mod._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING
    )
    wrapper.pb.ClearField.assert_called_once_with("typeSpecificFieldsJson")


def test_update_pq_config_auto_delete_positive_temporary(
    coreplus_controller_client, pq_config_mod
):
    import json

    wrapper = _update_config_wrapper()
    temp_scheduler = ["SchedulerType=Temporary"]
    with patch.object(pq_config_mod, "GenerateScheduling") as mock_gen:
        mock_gen.generate_temporary_scheduler.return_value = temp_scheduler
        assert (
            coreplus_controller_client.update_pq_config(
                wrapper, auto_delete_timeout=120
            )
            is True
        )
    wrapper.pb.scheduling.extend.assert_called_once_with(temp_scheduler)
    assert json.loads(wrapper.pb.typeSpecificFieldsJson)["TerminationDelay"] == {
        "type": "long",
        "value": "120000",
    }


def test_update_pq_config_reject_auto_delete_and_schedule(coreplus_controller_client):
    wrapper = _update_config_wrapper()
    with pytest.raises(ValueError, match="mutually exclusive"):
        coreplus_controller_client.update_pq_config(
            wrapper, auto_delete_timeout=60, schedule=["s"]
        )


def test_update_pq_config_reject_script_body_and_path(coreplus_controller_client):
    wrapper = _update_config_wrapper()
    with pytest.raises(ValueError, match="mutually exclusive"):
        coreplus_controller_client.update_pq_config(
            wrapper, script_body="code", script_path="path"
        )


def test_update_pq_config_restart_users_enum_conversion(
    coreplus_controller_client, pq_config_mod
):
    wrapper = _update_config_wrapper()
    with patch.object(pq_config_mod, "RestartUsersEnum") as mock_enum:
        mock_enum.Value.return_value = 3
        assert (
            coreplus_controller_client.update_pq_config(
                wrapper, restart_users="RU_ADMIN"
            )
            is True
        )
    assert wrapper.pb.restartUsers == 3


@pytest.mark.asyncio
async def test_make_pq_config_restart_users_enum_conversion(
    coreplus_controller_client,
    dummy_controller_client,
    controller_client_mod,
    pq_config_mod,
):
    """Regression: restart_users routes through the enum converter, not raw assignment."""
    mock_config = _make_config_mock()
    dummy_controller_client.make_temporary_config.return_value = mock_config

    with (
        patch.object(controller_client_mod, "CorePlusQueryConfig", autospec=True),
        patch.object(pq_config_mod, "RestartUsersEnum") as mock_enum,
    ):
        mock_enum.Value.return_value = 1
        await coreplus_controller_client.make_pq_config(
            name="test-pq",
            heap_size_gb=8.0,
            restart_users="RU_ADMIN",
        )
        mock_enum.Value.assert_called_once_with("RU_ADMIN")
        assert mock_config.restartUsers == 1
