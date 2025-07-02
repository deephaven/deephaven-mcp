import asyncio
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Patch sys.modules so _auth_client can be imported even if enterprise modules are missing
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

from deephaven_mcp._exceptions import AuthenticationError, DeephavenConnectionError
from deephaven_mcp.client import _auth_client


class DummyToken:
    pass


@pytest.fixture
def dummy_auth_client():
    client = MagicMock()
    client.authenticate = MagicMock(return_value=DummyToken())
    client.authenticate_with_token = MagicMock(return_value=DummyToken())
    client.create_token = MagicMock(return_value=DummyToken())
    client.close = MagicMock()
    return client


@pytest.fixture
def coreplus_auth_client(dummy_auth_client):
    return _auth_client.CorePlusAuthClient(dummy_auth_client)


@pytest.mark.asyncio
async def test_authenticate_success(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.authenticate.return_value = "tok"
    with patch(
        "deephaven_mcp.client._auth_client.CorePlusToken",
        side_effect=lambda t: f"wrapped-{t}",
    ):
        result = await coreplus_auth_client.authenticate("user", "pass")
        assert result == "wrapped-tok"
        dummy_auth_client.authenticate.assert_called_once_with("user", "pass", None)


@pytest.mark.asyncio
async def test_authenticate_connection_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.authenticate.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_auth_client.authenticate("user", "pass")


@pytest.mark.asyncio
async def test_authenticate_other_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.authenticate.side_effect = Exception("fail")
    with pytest.raises(AuthenticationError):
        await coreplus_auth_client.authenticate("user", "pass")


@pytest.mark.asyncio
async def test_authenticate_with_token_success(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.authenticate_with_token.return_value = "tok2"
    with patch(
        "deephaven_mcp.client._auth_client.CorePlusToken",
        side_effect=lambda t: f"wrapped-{t}",
    ):
        result = await coreplus_auth_client.authenticate_with_token("tok")
        assert result == "wrapped-tok2"
        dummy_auth_client.authenticate_with_token.assert_called_once_with("tok", None)


@pytest.mark.asyncio
async def test_authenticate_with_token_connection_error(
    coreplus_auth_client, dummy_auth_client
):
    dummy_auth_client.authenticate_with_token.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_auth_client.authenticate_with_token("tok")


@pytest.mark.asyncio
async def test_authenticate_with_token_other_error(
    coreplus_auth_client, dummy_auth_client
):
    dummy_auth_client.authenticate_with_token.side_effect = Exception("fail")
    with pytest.raises(AuthenticationError):
        await coreplus_auth_client.authenticate_with_token("tok")


@pytest.mark.asyncio
async def test_create_token_success(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.create_token.return_value = "tok3"
    with patch(
        "deephaven_mcp.client._auth_client.CorePlusToken",
        side_effect=lambda t: f"wrapped-{t}",
    ):
        result = await coreplus_auth_client.create_token("svc", "user", 123)
        assert result == "wrapped-tok3"
        dummy_auth_client.create_token.assert_called_once_with("svc", "user", 123, None)


@pytest.mark.asyncio
async def test_create_token_connection_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.create_token.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_auth_client.create_token("svc")


@pytest.mark.asyncio
async def test_create_token_other_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.create_token.side_effect = Exception("fail")
    with pytest.raises(AuthenticationError):
        await coreplus_auth_client.create_token("svc")


@pytest.mark.asyncio
async def test_close_success(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.close.return_value = None
    await coreplus_auth_client.close()
    dummy_auth_client.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_close_connection_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.close.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_auth_client.close()


@pytest.mark.asyncio
async def test_close_other_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.close.side_effect = Exception("fail")
    with pytest.raises(AuthenticationError):
        await coreplus_auth_client.close()
