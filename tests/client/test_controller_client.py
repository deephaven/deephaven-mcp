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
    client.authenticate = MagicMock()
    client.subscribe = MagicMock()
    client.map = MagicMock(return_value={})
    client.get = MagicMock(return_value="info")
    client.delete_query = MagicMock()
    client.close = MagicMock()
    client.start_and_wait = MagicMock()
    client.stop_query = MagicMock()
    client.wait_for_change = MagicMock(return_value=None)
    client.restart_query = MagicMock()
    client.start_and_wait = MagicMock()
    client.stop_query = MagicMock()
    client.stop_and_wait = MagicMock()
    client.ping = MagicMock(return_value=True)
    client.set_auth_client = MagicMock()
    client.wait_for_change = MagicMock()
    client.get_serial_for_name = MagicMock(return_value="serial")
    client.add_query = MagicMock(return_value="serial")
    client.make_temporary_config = MagicMock(return_value="config")
    return client


@pytest.fixture
def coreplus_controller_client(dummy_controller_client, controller_client_mod):
    return controller_client_mod.CorePlusControllerClient(dummy_controller_client)


@pytest.mark.asyncio
async def test_authenticate_success(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.authenticate.return_value = None
    token = MagicMock()
    await coreplus_controller_client.authenticate(token)
    dummy_controller_client.authenticate.assert_called_once_with(token, None)


@pytest.mark.asyncio
async def test_authenticate_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.authenticate.side_effect = ConnectionError("fail")
    token = MagicMock()
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.authenticate(token)


@pytest.mark.asyncio
async def test_authenticate_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.authenticate.side_effect = ValueError("bad token")
    token = MagicMock()
    with pytest.raises(AuthenticationError):
        await coreplus_controller_client.authenticate(token)


@pytest.mark.asyncio
async def test_authenticate_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.authenticate.side_effect = Exception("fail")
    token = MagicMock()
    with pytest.raises(AuthenticationError):
        await coreplus_controller_client.authenticate(token)


@pytest.mark.asyncio
async def test_subscribe_success(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.subscribe.return_value = None
    await coreplus_controller_client.subscribe()
    dummy_controller_client.subscribe.assert_called_once_with()


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
async def test_map_success(coreplus_controller_client, dummy_controller_client):
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
    dummy_controller_client.map.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.map()


@pytest.mark.asyncio
async def test_map_other_error(coreplus_controller_client, dummy_controller_client):
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
async def test_close_success(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.close.return_value = None
    await coreplus_controller_client.close()
    dummy_controller_client.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_close_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.close.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.close()


@pytest.mark.asyncio
async def test_close_other_error(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.close.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.close()


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
async def test_set_auth_client_success(
    coreplus_controller_client, dummy_controller_client
):
    auth_client = MagicMock()
    auth_client.wrapped = MagicMock()
    await coreplus_controller_client.set_auth_client(auth_client)
    dummy_controller_client.set_auth_client.assert_called_once()


@pytest.mark.asyncio
async def test_set_auth_client_error(
    coreplus_controller_client, dummy_controller_client
):
    auth_client = MagicMock()
    auth_client.wrapped = MagicMock()
    dummy_controller_client.set_auth_client.side_effect = Exception("fail")
    with pytest.raises(AuthenticationError):
        await coreplus_controller_client.set_auth_client(auth_client)


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
    dummy_controller_client.get_serial_for_name.return_value = "serial"
    result = await coreplus_controller_client.get_serial_for_name("name")
    assert result == "serial"


@pytest.mark.asyncio
async def test_get_serial_for_name_timeout(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.get_serial_for_name.side_effect = TimeoutError("timeout")
    with pytest.raises(TimeoutError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_get_serial_for_name_value_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.get_serial_for_name.side_effect = ValueError("bad")
    with pytest.raises(ValueError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_get_serial_for_name_connection_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.get_serial_for_name.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_get_serial_for_name_other_error(
    coreplus_controller_client, dummy_controller_client
):
    dummy_controller_client.get_serial_for_name.side_effect = Exception("fail")
    with pytest.raises(QueryError):
        await coreplus_controller_client.get_serial_for_name("name")


@pytest.mark.asyncio
async def test_add_query_success(coreplus_controller_client, dummy_controller_client):
    dummy_controller_client.add_query.return_value = "serial"
    query_config = MagicMock()
    query_config.config = "config"
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
async def test_make_temporary_config_success(
    coreplus_controller_client, dummy_controller_client, controller_client_mod
):
    dummy_controller_client.make_temporary_config.return_value = "config"
    # Patch CorePlusQueryConfig to a dummy class for test
    with patch.object(
        controller_client_mod, "CorePlusQueryConfig", autospec=True
    ) as mock_cfg:
        mock_cfg.return_value.config = "config"
        result = await coreplus_controller_client.make_temporary_config("name", 1.0)
        assert hasattr(result, "config")


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
