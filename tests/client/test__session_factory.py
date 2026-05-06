import asyncio
import io
import logging
import sys
import types
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

import deephaven_mcp._exceptions as exc
from deephaven_mcp.client._auth_client import CorePlusAuthClient
from deephaven_mcp.client._controller_client import CorePlusControllerClient

# This MUST happen at import time before any other imports that depend on enterprise modules
try:
    import deephaven_enterprise.client.controller  # noqa: F401
    import deephaven_enterprise.client.session_manager  # noqa: F401

    ENTERPRISE_AVAILABLE = True
except ImportError:
    ENTERPRISE_AVAILABLE = False

# Import the session factory AFTER setting up mocks
from deephaven_mcp.client._session_factory import CorePlusSessionFactory


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
    return sm


@pytest.fixture
def coreplus_session_manager(dummy_session_manager, monkeypatch):
    monkeypatch.setattr(
        "deephaven_mcp.client._base.is_enterprise_available", lambda: True
    )
    # The factory is now created directly with the mocked SessionManager
    return CorePlusSessionFactory(session_manager=dummy_session_manager)


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
async def test_ping_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that ping() raises DeephavenConnectionError on timeout."""
    import time

    def slow_ping():
        time.sleep(0.05)

    dummy_session_manager.ping.side_effect = slow_ping

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.ping(timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


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
    import deephaven_mcp.client._session_factory as sm_mod

    dummy_session_manager.private_key.side_effect = FileNotFoundError("no file")
    with pytest.raises(sm_mod.AuthenticationError) as excinfo:
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
async def test_upload_key_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that upload_key() raises DeephavenConnectionError on timeout."""
    import time

    def slow_upload_key(key):
        time.sleep(0.05)

    dummy_session_manager.upload_key.side_effect = slow_upload_key

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.upload_key("pubkey", timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


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
async def test_delete_key_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that delete_key() raises DeephavenConnectionError on timeout."""
    import time

    def slow_delete_key(key):
        time.sleep(0.05)

    dummy_session_manager.delete_key.side_effect = slow_delete_key

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.delete_key("pubkey", timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_connect_to_new_worker_success(
    coreplus_session_manager, dummy_session_manager
):
    mock_session_instance = MagicMock()
    mock_session_instance._session_type = "python"  # Mock the _session_type attribute
    dummy_session_manager.connect_to_new_worker.return_value = mock_session_instance

    with patch(
        "deephaven_mcp.client._session_factory.CorePlusSession",
        return_value="wrapped_session",
    ) as mock_core_plus_session:
        result = await coreplus_session_manager.connect_to_new_worker(
            name="worker",
            heap_size_gb=4,
            session_arguments={"programming_language": "python"},
        )

        dummy_session_manager.connect_to_new_worker.assert_called_once_with(
            name="worker",
            heap_size_gb=4,
            server=None,
            extra_jvm_args=None,
            extra_environment_vars=None,
            engine="DeephavenCommunity",
            auto_delete_timeout=600,
            admin_groups=None,
            viewer_groups=None,
            timeout_seconds=60,
            configuration_transformer=None,
            session_arguments={"programming_language": "python"},
        )
        mock_core_plus_session.assert_called_once_with(mock_session_instance, "python")
        assert result == "wrapped_session"


@pytest.mark.asyncio
async def test_connect_to_new_worker_resource_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = exc.ResourceError("fail")
    with pytest.raises(exc.ResourceError):
        await coreplus_session_manager.connect_to_new_worker(
            heap_size_gb=4,
            name="worker",
            session_arguments={"programming_language": "python"},
        )


@pytest.mark.asyncio
async def test_connect_to_new_worker_creation_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = exc.SessionCreationError(
        "fail"
    )
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_new_worker(
            heap_size_gb=4,
            name="worker",
            session_arguments={"programming_language": "python"},
        )


@pytest.mark.asyncio
async def test_connect_to_new_worker_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.connect_to_new_worker(
            heap_size_gb=4,
            name="worker",
            session_arguments={"programming_language": "python"},
        )


@pytest.mark.asyncio
async def test_connect_to_new_worker_other_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = Exception("fail")
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_new_worker(
            heap_size_gb=4,
            name="worker",
            session_arguments={"programming_language": "python"},
        )


@pytest.mark.asyncio
async def test_connect_to_persistent_query_success(
    coreplus_session_manager, dummy_session_manager
):
    mock_session_instance = MagicMock()
    mock_session_instance._session_type = "python"  # Mock the _session_type attribute
    dummy_session_manager.connect_to_persistent_query.return_value = (
        mock_session_instance
    )

    with patch(
        "deephaven_mcp.client._session_factory.CorePlusSession",
        return_value="wrapped_session",
    ) as mock_core_plus_session:
        result = await coreplus_session_manager.connect_to_persistent_query(
            name="pq", session_arguments={"programming_language": "python"}
        )

        dummy_session_manager.connect_to_persistent_query.assert_called_once_with(
            name="pq", serial=None, session_arguments={"programming_language": "python"}
        )
        mock_core_plus_session.assert_called_once_with(mock_session_instance, "python")
        assert result == "wrapped_session"


@pytest.mark.asyncio
async def test_connect_to_persistent_query_value_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = ValueError("fail")
    with pytest.raises(ValueError):
        await coreplus_session_manager.connect_to_persistent_query(
            name="pq", session_arguments={"programming_language": "python"}
        )


@pytest.mark.asyncio
async def test_connect_to_persistent_query_query_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = exc.QueryError(
        "fail"
    )
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_persistent_query(
            name="pq", session_arguments={"programming_language": "python"}
        )


@pytest.mark.asyncio
async def test_connect_to_persistent_query_creation_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = (
        exc.SessionCreationError("fail")
    )
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_persistent_query(
            name="pq", session_arguments={"programming_language": "python"}
        )


@pytest.mark.asyncio
async def test_connect_to_persistent_query_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = ConnectionError(
        "fail"
    )
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.connect_to_persistent_query(
            name="pq", session_arguments={"programming_language": "python"}
        )


@pytest.mark.asyncio
async def test_connect_to_persistent_query_key_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_persistent_query.side_effect = KeyError("fail")
    with pytest.raises(exc.QueryError):
        await coreplus_session_manager.connect_to_persistent_query(
            name="pq", session_arguments={"programming_language": "python"}
        )


def test_controller_client_property_success(
    coreplus_session_manager, dummy_session_manager
):
    # Access the property
    result = coreplus_session_manager.controller_client

    # Should return the initialized controller client
    assert result is not None
    assert isinstance(result, CorePlusControllerClient)


def test_auth_client_property_success(coreplus_session_manager, dummy_session_manager):
    # Access the property
    result = coreplus_session_manager.auth_client

    # Should return the initialized auth client
    assert result is not None
    assert isinstance(result, CorePlusAuthClient)


def test_controller_client_property_connection_error():
    mock_session_manager = MagicMock()
    mock_property = PropertyMock()
    mock_property.__get__ = MagicMock(side_effect=ConnectionError("network failure"))
    type(mock_session_manager).controller_client = mock_property

    with (
        patch("deephaven_mcp.client._base.is_enterprise_available", True),
        pytest.raises(exc.SessionError),
    ):
        CorePlusSessionFactory(mock_session_manager)


def test_controller_client_property_session_error():
    mock_session_manager = MagicMock()
    mock_property = PropertyMock()
    mock_property.__get__ = MagicMock(side_effect=Exception("generic failure"))
    type(mock_session_manager).controller_client = mock_property

    with (
        patch("deephaven_mcp.client._base.is_enterprise_available", True),
        pytest.raises(exc.SessionError),
    ):
        CorePlusSessionFactory(mock_session_manager)


def test_auth_client_property_connection_error():
    mock_session_manager = MagicMock()
    mock_controller = MagicMock()
    mock_auth = PropertyMock()
    mock_auth.__get__ = MagicMock(side_effect=ConnectionError("network failure"))

    type(mock_session_manager).controller_client = PropertyMock(
        return_value=mock_controller
    )
    type(mock_session_manager).auth_client = mock_auth

    with (
        patch("deephaven_mcp.client._base.is_enterprise_available", True),
        pytest.raises(exc.AuthenticationError),
    ):
        CorePlusSessionFactory(mock_session_manager)


def test_auth_client_property_auth_error():
    mock_session_manager = MagicMock()
    mock_controller = MagicMock()
    mock_auth = PropertyMock()
    mock_auth.__get__ = MagicMock(side_effect=Exception("generic failure"))

    type(mock_session_manager).controller_client = PropertyMock(
        return_value=mock_controller
    )
    type(mock_session_manager).auth_client = mock_auth

    with (
        patch("deephaven_mcp.client._base.is_enterprise_available", True),
        pytest.raises(exc.AuthenticationError),
    ):
        CorePlusSessionFactory(mock_session_manager)


# =============================================================================
# Enterprise Not Available Tests
# =============================================================================


@pytest.mark.asyncio
async def test_from_url_when_enterprise_not_available(monkeypatch):
    """Test that from_url handles enterprise not available appropriately."""
    monkeypatch.setattr(
        "deephaven_mcp.client._session_factory.is_enterprise_available", False
    )

    import deephaven_mcp.client._session_factory as sm_mod

    # Should raise MissingEnterprisePackageError when enterprise not available
    with pytest.raises(exc.MissingEnterprisePackageError) as excinfo:
        await sm_mod.CorePlusSessionFactory.from_url(
            "https://example.com/iris/connection.json"
        )

    assert "deephaven-coreplus-client" in str(excinfo.value)


# =============================================================================
# Timeout Tests
# =============================================================================


@pytest.mark.asyncio
async def test_password_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that password() raises DeephavenConnectionError on timeout."""
    import time

    def slow_password(*args):
        time.sleep(0.05)

    dummy_session_manager.password.side_effect = slow_password

    # Use the timeout_seconds parameter directly
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.password("user", "pw", timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_private_key_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that private_key() raises DeephavenConnectionError on timeout."""
    import time

    def slow_auth(*args):
        time.sleep(0.05)

    dummy_session_manager.private_key.side_effect = slow_auth

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.private_key("/fake/path", timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_saml_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that saml() raises DeephavenConnectionError on timeout."""
    import time

    def slow_auth():
        time.sleep(0.05)

    dummy_session_manager.saml.side_effect = slow_auth

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.saml(timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_connect_to_new_worker_timeout(
    coreplus_session_manager, dummy_session_manager
):
    """Test that connect_to_new_worker() raises DeephavenConnectionError on SDK timeout."""
    # SDK raises TimeoutError when its internal timeout fires
    dummy_session_manager.connect_to_new_worker.side_effect = TimeoutError(
        "SDK timeout"
    )
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.connect_to_new_worker(
            heap_size_gb=4, timeout_seconds=60.0
        )


@pytest.mark.asyncio
async def test_connect_to_persistent_query_timeout(
    coreplus_session_manager, dummy_session_manager
):
    """Test that connect_to_persistent_query() raises DeephavenConnectionError on timeout."""
    import time

    def slow_connect(*args, **kwargs):
        time.sleep(0.05)

    dummy_session_manager.connect_to_persistent_query.side_effect = slow_connect

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.connect_to_persistent_query(
            name="test", timeout_seconds=0.01
        )
    assert "timed out" in str(exc_info.value)


def _patches_for_from_url(side_effect=None):
    """Patch the deephaven_enterprise.client.session_manager import chain.

    Mirrors ``_patches_for_from_credentials`` but without the config
    validator patch, since ``from_url`` takes only a URL.
    """
    mock_manager_class = MagicMock(side_effect=side_effect)
    mock_sm_module = MagicMock()
    mock_sm_module.SessionManager = mock_manager_class
    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_sm_module
    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module
    return [
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_sm_module,
            },
        ),
    ], mock_manager_class


@pytest.mark.asyncio
async def test_from_url_success():
    """from_url constructs a SessionManager and subscribes the controller client."""
    patches, _mgr_cls = _patches_for_from_url()
    p1, p2 = patches
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusControllerClient,
            "subscribe",
            new_callable=AsyncMock,
        ) as mock_subscribe:
            instance = await sm_mod.CorePlusSessionFactory.from_url(
                "https://example.com/iris/connection.json"
            )
        assert isinstance(instance, sm_mod.CorePlusSessionFactory)
        mock_subscribe.assert_awaited_once()


@pytest.mark.asyncio
async def test_from_url_timeout():
    """from_url raises DeephavenConnectionError if the constructor times out."""
    import time as _time

    def _slow_ctor(*_a, **_kw):
        _time.sleep(5.0)

    patches, _mgr_cls = _patches_for_from_url(side_effect=_slow_ctor)
    p1, p2 = patches
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError, match="timed out"):
            await sm_mod.CorePlusSessionFactory.from_url(
                "https://example.com/iris/connection.json",
                timeout_seconds=0.01,
            )


@pytest.mark.asyncio
async def test_from_url_connection_error():
    """from_url wraps unexpected SessionManager errors in DeephavenConnectionError."""
    patches, _mgr_cls = _patches_for_from_url(side_effect=RuntimeError("boom"))
    p1, p2 = patches
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError, match="boom"):
            await sm_mod.CorePlusSessionFactory.from_url(
                "https://example.com/iris/connection.json"
            )


# ---------------------------------------------------------------------------
# from_credentials
# ---------------------------------------------------------------------------


_FROM_CREDENTIALS_CONFIG = {
    "system_name": "test-system",
    "connection_json_url": "https://server/iris/connection.json",
    "auth": {"backends": ["password", "private_key"]},
}


def _make_session_manager_modules(side_effect=None):
    """Build the deephaven_enterprise.client.session_manager mock chain.

    Returns ``(mock_session_manager_module, mock_client_module, mock_enterprise_module)``.
    ``side_effect`` is forwarded to the ``SessionManager`` MagicMock (e.g. to
    simulate a slow constructor for timeout tests).
    """
    mock_manager_class = MagicMock(side_effect=side_effect)
    mock_sm_module = MagicMock()
    mock_sm_module.SessionManager = mock_manager_class
    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_sm_module
    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module
    return mock_sm_module, mock_client_module, mock_enterprise_module


def _patches_for_from_credentials(side_effect=None):
    """Standard set of patches used by every from_credentials test."""
    sm_mod, client_mod, ent_mod = _make_session_manager_modules(side_effect)
    return [
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.client._session_factory.validate_enterprise_config"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": ent_mod,
                "deephaven_enterprise.client": client_mod,
                "deephaven_enterprise.client.session_manager": sm_mod,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_from_credentials_password_success():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="alice", password="pw")

    p1, p2, p3 = _patches_for_from_credentials()
    with p1, p2, p3:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ) as mock_password:
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds
            )
        mock_password.assert_awaited_once_with("alice", "pw", None)


@pytest.mark.asyncio
async def test_from_credentials_password_with_effective_user():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="alice", password="pw", effective_user="bob")

    p1, p2, p3 = _patches_for_from_credentials()
    with p1, p2, p3:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ) as mock_password:
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds
            )
        mock_password.assert_awaited_once_with("alice", "pw", "bob")


@pytest.mark.asyncio
async def test_from_credentials_private_key_success():
    from deephaven_mcp.auth.credentials import PrivateKeyCredentials

    creds = PrivateKeyCredentials(key_text="DH key payload\n")

    p1, p2, p3 = _patches_for_from_credentials()
    with p1, p2, p3:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "private_key", new_callable=AsyncMock
        ) as mock_pk:
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds
            )
        # The key_text should arrive wrapped in an io.StringIO so the
        # upstream SessionManager.private_key() sees an in-memory text
        # stream rather than a filesystem path.
        assert mock_pk.await_count == 1
        (arg,), _kwargs = mock_pk.await_args
        assert isinstance(arg, io.StringIO)
        assert arg.getvalue() == "DH key payload\n"


# Note: there is no longer a ``test_from_credentials_private_key_invalid_utf8``
# at this layer. :class:`PrivateKeyCredentials` now carries ``key_text: str``,
# so invalid-UTF-8 material cannot reach ``from_credentials`` in the first
# place -- the UTF-8 validation happens at the producing edge in
# :class:`PrivateKeyBackend.derive_credentials`. See
# ``tests/auth/backends/test__private_key.py::test_derive_credentials_rejects_non_utf8_key_bytes``.


@pytest.mark.asyncio
async def test_from_credentials_unsupported_creds_type():
    p1, p2, p3 = _patches_for_from_credentials()
    with p1, p2, p3:
        import deephaven_mcp.client._session_factory as sm_mod

        # PSKCredentials is a valid Credentials union member but not a
        # legal input for the enterprise factory.
        from deephaven_mcp.auth.credentials import PSKCredentials

        with pytest.raises(exc.AuthenticationError, match="Unsupported credentials"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, PSKCredentials(psk="x")  # type: ignore[arg-type]
            )


@pytest.mark.asyncio
async def test_from_credentials_no_enterprise_package():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")
    with patch("deephaven_mcp.client._session_factory.is_enterprise_available", False):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.MissingEnterprisePackageError):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds
            )


@pytest.mark.asyncio
async def test_from_credentials_invalid_config_propagates():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")
    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch(
            "deephaven_mcp.client._session_factory.validate_enterprise_config",
            side_effect=exc.ConfigurationError("boom"),
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.ConfigurationError, match="boom"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds
            )


@pytest.mark.asyncio
async def test_from_credentials_session_manager_timeout():
    import time as time_mod

    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")

    def slow_init(_url):
        time_mod.sleep(0.1)

    p1, p2, p3 = _patches_for_from_credentials(side_effect=slow_init)
    with p1, p2, p3:
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError, match="timed out"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds, timeout_seconds=0.01
            )


@pytest.mark.asyncio
async def test_from_credentials_session_manager_failure():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")

    def boom(_url):
        raise RuntimeError("connect failed")

    p1, p2, p3 = _patches_for_from_credentials(side_effect=boom)
    with p1, p2, p3:
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError, match="connect failed"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds
            )
