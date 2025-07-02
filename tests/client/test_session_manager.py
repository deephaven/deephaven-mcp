import asyncio
import io
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

import deephaven_mcp._exceptions as exc

# Patch sys.modules for enterprise imports BEFORE any tested imports
mock_enterprise = types.ModuleType("deephaven_enterprise")
mock_sm = types.ModuleType("deephaven_enterprise.client.session_manager")
mock_sm.SessionManager = MagicMock()
sys.modules["deephaven_enterprise"] = mock_enterprise
sys.modules["deephaven_enterprise.client"] = types.ModuleType(
    "deephaven_enterprise.client"
)
sys.modules["deephaven_enterprise.client.session_manager"] = mock_sm
# Patch controller client as well for _protobuf.py import
mock_controller = types.ModuleType("deephaven_enterprise.client.controller")
mock_controller.ControllerClient = MagicMock()
sys.modules["deephaven_enterprise.client.controller"] = mock_controller

from deephaven_mcp.client._session_manager import CorePlusSessionManager


@pytest.fixture
def dummy_session_manager():
    sm = MagicMock()
    sm.close = MagicMock()
    sm.ping = MagicMock(return_value=True)
    sm.password = MagicMock()
    sm.private_key = MagicMock()
    sm.saml = MagicMock()
    sm.upload_key = MagicMock()
    sm.delete_key = MagicMock()
    sm.connect_to_new_worker = MagicMock()
    sm.connect_to_persistent_query = MagicMock()
    sm.create_auth_client = MagicMock()
    sm.create_controller_client = MagicMock()
    return sm


@pytest.fixture
def coreplus_session_manager(dummy_session_manager):
    return CorePlusSessionManager(dummy_session_manager)


@pytest.mark.asyncio
async def test_close_success(coreplus_session_manager, dummy_session_manager):
    await coreplus_session_manager.close()
    dummy_session_manager.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_failure(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.close.side_effect = Exception("fail")
    with pytest.raises(exc.SessionError):
        await coreplus_session_manager.close()


@pytest.mark.asyncio
async def test_ping_success(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.ping.return_value = True
    result = await coreplus_session_manager.ping()
    assert result is True


@pytest.mark.asyncio
async def test_ping_failure(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.ping.side_effect = Exception("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.ping()


@pytest.mark.asyncio
async def test_password_success(coreplus_session_manager, dummy_session_manager):
    await coreplus_session_manager.password("user", "pw")
    dummy_session_manager.password.assert_called_once_with("user", "pw", None)


@pytest.mark.asyncio
async def test_password_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.password.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.password("user", "pw")


@pytest.mark.asyncio
async def test_password_auth_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.password.side_effect = Exception("fail")
    with pytest.raises(exc.AuthenticationError):
        await coreplus_session_manager.password("user", "pw")


@pytest.mark.asyncio
async def test_private_key_success(coreplus_session_manager, dummy_session_manager):
    await coreplus_session_manager.private_key("/fake/path")
    dummy_session_manager.private_key.assert_called_once_with("/fake/path")


@pytest.mark.asyncio
async def test_private_key_file_not_found(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.private_key.side_effect = FileNotFoundError("no file")
    with pytest.raises(exc.AuthenticationError) as excinfo:
        await coreplus_session_manager.private_key("/fake/path")
    assert "file not found" in str(excinfo.value).lower()


@pytest.mark.asyncio
async def test_private_key_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.private_key.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.private_key("/fake/path")


@pytest.mark.asyncio
async def test_private_key_auth_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.private_key.side_effect = Exception("fail")
    with pytest.raises(exc.AuthenticationError):
        await coreplus_session_manager.private_key("/fake/path")


@pytest.mark.asyncio
async def test_private_key_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.private_key.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.private_key("/fake/path")


@pytest.mark.asyncio
async def test_saml_success(coreplus_session_manager, dummy_session_manager):
    await coreplus_session_manager.saml()
    dummy_session_manager.saml.assert_called_once()


@pytest.mark.asyncio
async def test_saml_connection_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.saml.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.saml()


@pytest.mark.asyncio
async def test_saml_value_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.saml.side_effect = ValueError("fail")
    with pytest.raises(exc.AuthenticationError):
        await coreplus_session_manager.saml()


@pytest.mark.asyncio
async def test_saml_other_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.saml.side_effect = Exception("fail")
    with pytest.raises(exc.AuthenticationError):
        await coreplus_session_manager.saml()


@pytest.mark.asyncio
async def test_upload_key_success(coreplus_session_manager, dummy_session_manager):
    await coreplus_session_manager.upload_key("pubkey")
    dummy_session_manager.upload_key.assert_called_once_with("pubkey")


@pytest.mark.asyncio
async def test_upload_key_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.upload_key.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.upload_key("pubkey")


@pytest.mark.asyncio
async def test_upload_key_other_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.upload_key.side_effect = Exception("fail")
    with pytest.raises(exc.ResourceError):
        await coreplus_session_manager.upload_key("pubkey")


@pytest.mark.asyncio
async def test_delete_key_success(coreplus_session_manager, dummy_session_manager):
    await coreplus_session_manager.delete_key("pubkey")
    dummy_session_manager.delete_key.assert_called_once_with("pubkey")


@pytest.mark.asyncio
async def test_delete_key_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.delete_key.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.delete_key("pubkey")


@pytest.mark.asyncio
async def test_delete_key_other_error(coreplus_session_manager, dummy_session_manager):
    dummy_session_manager.delete_key.side_effect = Exception("fail")
    with pytest.raises(exc.ResourceError):
        await coreplus_session_manager.delete_key("pubkey")


@pytest.mark.asyncio
async def test_connect_to_new_worker_success(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session = MagicMock()
    dummy_session_manager.connect_to_new_worker.return_value = dummy_session
    with patch(
        "deephaven_mcp.client._session_manager.CorePlusSession", autospec=True
    ) as mock_session:
        mock_session.return_value = "wrapped_session"
        result = await coreplus_session_manager.connect_to_new_worker(name="worker")
        dummy_session_manager.connect_to_new_worker.assert_called_once_with(
            name="worker",
            heap_size_gb=None,
            server=None,
            extra_jvm_args=None,
            extra_environment_vars=None,
            engine="DeephavenCommunity",
            auto_delete_timeout=600,
            admin_groups=None,
            viewer_groups=None,
            timeout_seconds=60,
            configuration_transformer=None,
            session_arguments=None,
        )
        assert result == "wrapped_session"


@pytest.mark.asyncio
async def test_connect_to_new_worker_resource_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = exc.ResourceError("fail")
    with pytest.raises(exc.ResourceError):
        await coreplus_session_manager.connect_to_new_worker(name="worker")


@pytest.mark.asyncio
async def test_connect_to_new_worker_creation_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = exc.SessionCreationError(
        "fail"
    )
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_new_worker(name="worker")


@pytest.mark.asyncio
async def test_connect_to_new_worker_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.connect_to_new_worker(name="worker")


@pytest.mark.asyncio
async def test_connect_to_new_worker_other_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = Exception("fail")
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_new_worker(name="worker")


@pytest.mark.asyncio
async def test_connect_to_persistent_query_success(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session = MagicMock()
    dummy_session_manager.connect_to_persistent_query.return_value = dummy_session
    with patch(
        "deephaven_mcp.client._session_manager.CorePlusSession", autospec=True
    ) as mock_session:
        mock_session.return_value = "wrapped_session"
        result = await coreplus_session_manager.connect_to_persistent_query(name="pq")
        dummy_session_manager.connect_to_persistent_query.assert_called_once_with(
            name="pq", serial=None, session_arguments=None
        )
        assert result == "wrapped_session"


@pytest.mark.asyncio
async def test_connect_to_persistent_query_value_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_session_manager.connect_to_persistent_query(name="pq")


@pytest.mark.asyncio
async def test_connect_to_persistent_query_query_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = exc.QueryError(
        "fail"
    )
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_persistent_query(name="pq")


@pytest.mark.asyncio
async def test_connect_to_persistent_query_creation_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = (
        exc.SessionCreationError("fail")
    )
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_persistent_query(name="pq")


@pytest.mark.asyncio
async def test_connect_to_persistent_query_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = ConnectionError(
        "fail"
    )
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.connect_to_persistent_query(name="pq")


@pytest.mark.asyncio
async def test_connect_to_persistent_query_key_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = KeyError("fail")
    with pytest.raises(exc.QueryError):
        await coreplus_session_manager.connect_to_persistent_query(name="pq")


@pytest.mark.asyncio
async def test_create_auth_client_success(
    coreplus_session_manager, dummy_session_manager
):
    dummy_auth_client = MagicMock()
    dummy_session_manager.create_auth_client.return_value = dummy_auth_client
    with patch(
        "deephaven_mcp.client._session_manager.CorePlusAuthClient", autospec=True
    ) as mock_auth:
        mock_auth.return_value = "wrapped_auth"
        result = await coreplus_session_manager.create_auth_client()
        dummy_session_manager.create_auth_client.assert_called_once_with(None)
        assert result == "wrapped_auth"


@pytest.mark.asyncio
async def test_create_auth_client_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.create_auth_client.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.create_auth_client()


@pytest.mark.asyncio
async def test_create_auth_client_auth_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.create_auth_client.side_effect = Exception("fail")
    with pytest.raises(exc.AuthenticationError):
        await coreplus_session_manager.create_auth_client()


@pytest.mark.asyncio
async def test_create_controller_client_success(
    coreplus_session_manager, dummy_session_manager
):
    dummy_ctrl_client = MagicMock()
    dummy_session_manager.create_controller_client.return_value = dummy_ctrl_client
    with patch(
        "deephaven_mcp.client._session_manager.CorePlusControllerClient", autospec=True
    ) as mock_ctrl:
        mock_ctrl.return_value = "wrapped_ctrl"
        result = await coreplus_session_manager.create_controller_client()
        dummy_session_manager.create_controller_client.assert_called_once_with()
        assert result == "wrapped_ctrl"


@pytest.mark.asyncio
async def test_create_controller_client_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.create_controller_client.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.create_controller_client()


@pytest.mark.asyncio
async def test_create_controller_client_session_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.create_controller_client.side_effect = Exception("fail")
    with pytest.raises(exc.SessionError):
        await coreplus_session_manager.create_controller_client()


# from_url is a classmethod, test it separately
@pytest.mark.asyncio
async def test_from_url_success():
    with (
        patch("deephaven_mcp.client._session_manager.is_enterprise_available", True),
        patch("deephaven_enterprise.client.session_manager.SessionManager") as mock_sm,
    ):
        instance = MagicMock()
        mock_sm.return_value = instance
        result = CorePlusSessionManager.from_url("http://fake")
        assert isinstance(result, CorePlusSessionManager)
        assert result.wrapped == instance


@pytest.mark.asyncio
async def test_from_url_not_enterprise():
    with patch("deephaven_mcp.client._session_manager.is_enterprise_available", False):
        with pytest.raises(exc.InternalError):
            CorePlusSessionManager.from_url("http://fake")


@pytest.mark.asyncio
async def test_from_url_connection_error():
    with (
        patch("deephaven_mcp.client._session_manager.is_enterprise_available", True),
        patch(
            "deephaven_enterprise.client.session_manager.SessionManager",
            side_effect=Exception("fail"),
        ),
    ):
        with pytest.raises(exc.DeephavenConnectionError):
            CorePlusSessionManager.from_url("http://fake")
