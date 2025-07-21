import asyncio
import io
import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

import deephaven_mcp._exceptions as exc

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
    sm.create_controller_client = MagicMock()
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
    mock_session_instance = MagicMock()
    mock_session_instance._session_type = "python"  # Mock the _session_type attribute
    dummy_session_manager.connect_to_new_worker.return_value = mock_session_instance

    with patch(
        "deephaven_mcp.client._session_factory.CorePlusSession",
        return_value="wrapped_session",
    ) as mock_core_plus_session:
        result = await coreplus_session_manager.connect_to_new_worker(
            name="worker", session_arguments={"programming_language": "python"}
        )

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
            name="worker", session_arguments={"programming_language": "python"}
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
            name="worker", session_arguments={"programming_language": "python"}
        )


@pytest.mark.asyncio
async def test_connect_to_new_worker_connection_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = ConnectionError("fail")
    with pytest.raises(exc.DeephavenConnectionError):
        await coreplus_session_manager.connect_to_new_worker(
            name="worker", session_arguments={"programming_language": "python"}
        )


@pytest.mark.asyncio
async def test_connect_to_new_worker_other_error(
    coreplus_session_manager, dummy_session_manager
):
    dummy_session_manager.connect_to_new_worker.side_effect = Exception("fail")
    with pytest.raises(exc.SessionCreationError):
        await coreplus_session_manager.connect_to_new_worker(
            name="worker", session_arguments={"programming_language": "python"}
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


@pytest.mark.asyncio
async def test_create_auth_client_success(
    coreplus_session_manager, dummy_session_manager
):
    dummy_auth_client = MagicMock()
    dummy_session_manager.create_auth_client.return_value = dummy_auth_client
    with patch(
        "deephaven_mcp.client._session_factory.CorePlusAuthClient", autospec=True
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
        "deephaven_mcp.client._session_factory.CorePlusControllerClient", autospec=True
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

from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_from_config_password_success(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "bob",
        "password": "pw",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        # Patch password method on the instance after creation
        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ) as mock_password:
            result = await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
            mock_password.assert_awaited_once_with("bob", "pw", None)


@pytest.mark.asyncio
async def test_from_config_password_env(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "alice",
        "password_env_var": "PW_ENV",
    }
    monkeypatch.setenv("PW_ENV", "env_pw")
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ) as mock_password:
            result = await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
            mock_password.assert_awaited_once_with("alice", "env_pw", None)


@pytest.mark.asyncio
async def test_from_config_private_key_success(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "private_key",
        "private_key": "---KEY---",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "private_key", new_callable=AsyncMock
        ) as mock_pk:
            result = await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
            assert mock_pk.await_count == 1
            arg = mock_pk.await_args.args[0]
            assert hasattr(arg, "read")  # Should be a StringIO
            assert arg.getvalue() == "---KEY---"


@pytest.mark.asyncio
async def test_from_config_invalid_config(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {"connection_json_url": "url", "auth_type": "password"}
    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch(
            "deephaven_mcp.config.validate_single_enterprise_system",
            side_effect=Exception("bad config"),
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(Exception) as excinfo:
            await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
        # Accept any error message for this generic invalid config test
        assert excinfo.value is not None


@pytest.mark.asyncio
async def test_from_config_not_enterprise(monkeypatch):
    # Test when enterprise functionality is not available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", False)
    worker_cfg = {"connection_json_url": "url", "auth_type": "password"}
    with patch("deephaven_mcp.client._session_factory.is_enterprise_available", False):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.InternalError):
            await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)


@pytest.mark.asyncio
async def test_from_config_connection_error(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "url",
        "auth_type": "password",
        "username": "bob",
        "password": "pw",
    }
    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(
        side_effect=ConnectionError("Connection failed")
    )
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError):
            await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)


@pytest.mark.asyncio
async def test_from_config_password_env_missing(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "alice",
        "password_env_var": "PW_ENV",
    }
    # Do NOT setenv
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ):
            with pytest.raises(sm_mod.AuthenticationError) as excinfo:
                await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
            assert "not set for password authentication" in str(excinfo.value)


@pytest.mark.asyncio
async def test_from_config_password_missing(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "alice",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ):
            with pytest.raises(sm_mod.EnterpriseSystemConfigurationError) as excinfo:
                await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
            assert "must define 'password'" in str(
                excinfo.value
            ) or "must define 'username'" in str(excinfo.value)


@pytest.mark.asyncio
async def test_from_config_private_key_missing(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "private_key",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "private_key", new_callable=AsyncMock
        ):
            with pytest.raises(sm_mod.EnterpriseSystemConfigurationError) as excinfo:
                await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
            assert "must define 'private_key'" in str(excinfo.value)


@pytest.mark.asyncio
async def test_from_config_unsupported_auth(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "saml",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch("deephaven_mcp.config.validate_single_enterprise_system"),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(sm_mod.EnterpriseSystemConfigurationError) as excinfo:
            await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
        assert "must be one of" in str(excinfo.value) and "saml" in str(excinfo.value)


@pytest.mark.asyncio
async def test_from_config_not_enterprise_available(monkeypatch):
    # Test when enterprise functionality is not available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", False)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "bob",
        "password": "pw",
    }
    with patch("deephaven_mcp.client._session_factory.is_enterprise_available", False):
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(sm_mod.InternalError) as excinfo:
            await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
        assert "Core+ features are not available" in str(excinfo.value)


@pytest.mark.asyncio
async def test_from_url_success(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)

    # Create mock modules for deephaven_enterprise hierarchy
    instance = MagicMock()
    mock_session_manager_class = MagicMock(return_value=instance)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        result = CorePlusSessionFactory.from_url("http://fake")
        assert isinstance(result, CorePlusSessionFactory)
        assert result.wrapped == instance


@pytest.mark.asyncio
async def test_from_url_not_enterprise(monkeypatch):
    # Test when enterprise functionality is not available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", False)
    with patch("deephaven_mcp.client._session_factory.is_enterprise_available", False):
        with pytest.raises(exc.InternalError):
            CorePlusSessionFactory.from_url("http://fake")


@pytest.mark.asyncio
async def test_from_url_connection_error(monkeypatch):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(side_effect=Exception("fail"))
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        with pytest.raises(exc.DeephavenConnectionError):
            CorePlusSessionFactory.from_url("http://fake")


# --- Coverage for unreachable error/warning branches in from_config ---


@pytest.mark.asyncio
async def test_from_config_missing_password_branch(monkeypatch, caplog):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "alice",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch(
            "deephaven_mcp.client._session_factory.validate_single_enterprise_system",
            return_value=None,
        ),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with caplog.at_level(logging.ERROR):
            with patch.object(
                sm_mod.CorePlusSessionFactory, "password", new_callable=MagicMock
            ):
                with pytest.raises(sm_mod.AuthenticationError) as excinfo:
                    await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
                assert "No password provided" in str(excinfo.value)
                assert "No password provided for password authentication" in caplog.text


@pytest.mark.asyncio
async def test_from_config_missing_private_key_branch(monkeypatch, caplog):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "private_key",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch(
            "deephaven_mcp.client._session_factory.validate_single_enterprise_system",
            return_value=None,
        ),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with caplog.at_level(logging.ERROR):
            with patch.object(
                sm_mod.CorePlusSessionFactory, "private_key", new_callable=MagicMock
            ):
                with pytest.raises(sm_mod.AuthenticationError) as excinfo:
                    await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
                assert "No private_key provided" in str(excinfo.value)
                assert (
                    "No private_key provided for private_key authentication"
                    in caplog.text
                )


@pytest.mark.asyncio
async def test_from_config_unsupported_auth_type_branch(monkeypatch, caplog):
    # Test enterprise functionality when available
    monkeypatch.setattr("deephaven_mcp.client._base.is_enterprise_available", True)
    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "saml",
    }
    mock_manager = MagicMock()

    # Create mock modules for deephaven_enterprise hierarchy
    mock_session_manager_class = MagicMock(return_value=mock_manager)
    mock_session_manager_module = MagicMock()
    mock_session_manager_module.SessionManager = mock_session_manager_class

    mock_client_module = MagicMock()
    mock_client_module.session_manager = mock_session_manager_module

    mock_enterprise_module = MagicMock()
    mock_enterprise_module.client = mock_client_module

    with (
        patch("deephaven_mcp.client._session_factory.is_enterprise_available", True),
        patch(
            "deephaven_mcp.client._session_factory.validate_single_enterprise_system",
            return_value=None,
        ),
        patch.dict(
            "sys.modules",
            {
                "deephaven_enterprise": mock_enterprise_module,
                "deephaven_enterprise.client": mock_client_module,
                "deephaven_enterprise.client.session_manager": mock_session_manager_module,
            },
        ),
    ):
        import deephaven_mcp.client._session_factory as sm_mod

        with caplog.at_level(logging.WARNING):
            with (
                patch.object(
                    sm_mod.CorePlusSessionFactory, "password", new_callable=MagicMock
                ),
                patch.object(
                    sm_mod.CorePlusSessionFactory, "private_key", new_callable=MagicMock
                ),
            ):
                # Should not raise
                result = await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)
                assert isinstance(result, sm_mod.CorePlusSessionFactory)
                assert (
                    "Auth type 'saml' is not supported for automatic authentication"
                    in caplog.text
                )


# =============================================================================
# Enterprise Not Available Tests
# =============================================================================


@pytest.mark.asyncio
async def test_from_config_when_enterprise_not_available(monkeypatch):
    """Test that from_config handles enterprise not available appropriately."""
    monkeypatch.setattr(
        "deephaven_mcp.client._session_factory.is_enterprise_available", False
    )

    worker_cfg = {
        "connection_json_url": "https://server/iris/connection.json",
        "auth_type": "password",
        "username": "username",
        "password": "password",
    }

    import deephaven_mcp.client._session_factory as sm_mod

    # Should raise InternalError when enterprise not available
    with pytest.raises(exc.InternalError) as excinfo:
        await sm_mod.CorePlusSessionFactory.from_config(worker_cfg)

    assert "Core+ features are not available" in str(excinfo.value)


@pytest.mark.asyncio
async def test_from_url_when_enterprise_not_available(monkeypatch):
    """Test that from_url handles enterprise not available appropriately."""
    monkeypatch.setattr(
        "deephaven_mcp.client._session_factory.is_enterprise_available", False
    )

    import deephaven_mcp.client._session_factory as sm_mod

    # Should raise InternalError when enterprise not available
    with pytest.raises(exc.InternalError) as excinfo:
        sm_mod.CorePlusSessionFactory.from_url(
            "https://example.com/iris/connection.json"
        )

    assert "Core+ features are not available" in str(excinfo.value)
