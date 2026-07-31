import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import AuthenticationError, DeephavenConnectionError
from deephaven_mcp.client import _auth_client


class DummyToken:
    pass


@pytest.fixture
def dummy_auth_client():
    client = MagicMock()
    client.authenticate = MagicMock(return_value=DummyToken())
    client.authenticate_with_token = MagicMock(return_value=DummyToken())
    client.get_token = MagicMock(return_value=DummyToken())
    client.close = MagicMock()
    return client


@pytest.fixture
def coreplus_auth_client(dummy_auth_client):
    from deephaven_mcp.client import EnterpriseClientTimeouts

    return _auth_client.CorePlusAuthClient(
        dummy_auth_client, EnterpriseClientTimeouts()
    )


@pytest.mark.asyncio
async def test_get_token_success(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.get_token.return_value = "tok3"
    with patch(
        "deephaven_mcp.client._auth_client.CorePlusToken",
        side_effect=lambda t: f"wrapped-{t}",
    ):
        result = await coreplus_auth_client.get_token("svc")
        assert result == "wrapped-tok3"
        dummy_auth_client.get_token.assert_called_once_with("svc")


@pytest.mark.asyncio
async def test_get_token_connection_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.get_token.side_effect = ConnectionError("fail")
    with pytest.raises(DeephavenConnectionError):
        await coreplus_auth_client.get_token("svc")


@pytest.mark.asyncio
async def test_get_token_other_error(coreplus_auth_client, dummy_auth_client):
    dummy_auth_client.get_token.side_effect = Exception("fail")
    with pytest.raises(AuthenticationError):
        await coreplus_auth_client.get_token("svc")


@pytest.mark.asyncio
async def test_get_token_timeout(dummy_auth_client):
    """Slow upstream get_token must surface as DeephavenConnectionError ("timed out").

    Regression test: an earlier refactor dropped the timeout entirely,
    leaving the synchronous SDK call running in ``asyncio.to_thread`` with
    no bound. The wrapper must enforce ``auth_timeout_seconds`` via
    ``asyncio.wait_for``.
    """
    import time

    from deephaven_mcp.client import EnterpriseClientTimeouts

    def slow_get_token(service):
        time.sleep(0.05)
        return DummyToken()

    dummy_auth_client.get_token.side_effect = slow_get_token

    client = _auth_client.CorePlusAuthClient(
        dummy_auth_client,
        EnterpriseClientTimeouts(auth_timeout_seconds=0.01),
    )
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await client.get_token("svc")
    assert "timed out" in str(exc_info.value)
    assert "auth_timeout_seconds" in str(exc_info.value)
