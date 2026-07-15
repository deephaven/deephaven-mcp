import asyncio
import io
import logging
from contextlib import nullcontext
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import deephaven_mcp._exceptions as exc
from deephaven_mcp.client._auth_client import CorePlusAuthClient
from deephaven_mcp.client._controller_client import CorePlusControllerClient
from deephaven_mcp.client._session_factory import CorePlusSessionFactory
from deephaven_mcp.client._timeouts import EnterpriseClientTimeouts


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
def coreplus_session_manager(dummy_session_manager):
    # The factory is created directly with the mocked SessionManager
    return CorePlusSessionFactory(
        session_manager=dummy_session_manager, timeouts=EnterpriseClientTimeouts()
    )


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

    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"quick_operation_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.ping()
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

    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"quick_operation_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.upload_key("pubkey")
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

    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"quick_operation_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.delete_key("pubkey")
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
            timeout_seconds=EnterpriseClientTimeouts().worker_creation_timeout_seconds,
            configuration_transformer=None,
            session_arguments={"programming_language": "python"},
        )
        mock_core_plus_session.assert_called_once_with(mock_session_instance, "python")
        assert result == "wrapped_session"


@pytest.mark.asyncio
async def test_connect_to_new_worker_env_vars_converted_to_wire_format(
    coreplus_session_manager, dummy_session_manager
):
    """Env vars reach the vendor call in the controller's alternating wire format.

    Regression test: the controller reads extraEnvironmentVariables as a flat
    alternating key/value list; passing "KEY=VALUE" strings through unconverted
    is rejected server-side with "Has an invalid key with no value".
    """
    mock_session_instance = MagicMock()
    mock_session_instance._session_type = "python"
    dummy_session_manager.connect_to_new_worker.return_value = mock_session_instance

    with patch(
        "deephaven_mcp.client._session_factory.CorePlusSession",
        return_value="wrapped_session",
    ):
        await coreplus_session_manager.connect_to_new_worker(
            name="worker",
            heap_size_gb=4,
            extra_environment_vars=["A=1", "OPTS=-Da=b"],
        )

    kwargs = dummy_session_manager.connect_to_new_worker.call_args.kwargs
    assert kwargs["extra_environment_vars"] == ["A", "1", "OPTS", "-Da=b"]


@pytest.mark.asyncio
async def test_connect_to_new_worker_malformed_env_var_raises_value_error(
    coreplus_session_manager, dummy_session_manager
):
    with pytest.raises(ValueError, match="expected 'KEY=VALUE'"):
        await coreplus_session_manager.connect_to_new_worker(
            name="worker",
            heap_size_gb=4,
            extra_environment_vars=["NOEQUALS"],
        )
    dummy_session_manager.connect_to_new_worker.assert_not_called()


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
async def test_connect_to_new_worker_python_side_timeout(
    coreplus_session_manager, dummy_session_manager
):
    """Python-side ``asyncio.wait_for`` raises ``DeephavenConnectionError`` when the
    SDK does not honor its own ``timeout_seconds`` argument.

    The wrapped call is replaced with a slow synchronous function so the
    underlying ``asyncio.to_thread`` would block past ``timeout_seconds``; the
    new ``asyncio.wait_for`` wrap is what raises.
    """
    import time

    def slow_connect_to_new_worker(**kwargs):
        time.sleep(0.05)

    dummy_session_manager.connect_to_new_worker.side_effect = slow_connect_to_new_worker
    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"worker_creation_timeout_seconds": 0.01}
    )

    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.connect_to_new_worker(
            heap_size_gb=4,
            name="worker",
            session_arguments={"programming_language": "python"},
        )
    assert "Worker creation timed out" in str(exc_info.value)
    assert "worker_creation_timeout_seconds" in str(exc_info.value)


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


def test_get_programming_language_returns_session_type():
    """When session._session_type is set, it is returned verbatim."""
    session = MagicMock(spec=["_session_type"])
    session._session_type = "groovy"
    assert CorePlusSessionFactory._get_programming_language(session) == "groovy"


def test_get_programming_language_defaults_when_session_type_empty():
    """When session._session_type is falsy, the default 'python' is returned."""
    session = MagicMock(spec=["_session_type"])
    session._session_type = ""
    assert CorePlusSessionFactory._get_programming_language(session) == "python"


def test_get_programming_language_defaults_on_attribute_error(caplog):
    """When pydeephaven does not expose _session_type, return 'python' and WARN."""

    class _NoSessionType:
        """Stand-in pydeephaven.Session that lacks _session_type."""

    session = _NoSessionType()
    with caplog.at_level(logging.WARNING):
        result = CorePlusSessionFactory._get_programming_language(session)
    assert result == "python"
    assert any(
        "[CorePlusSessionFactory:_get_programming_language]" in record.getMessage()
        and "DH-19984" in record.getMessage()
        and record.levelno == logging.WARNING
        for record in caplog.records
    )


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

    # Use a short configured auth timeout to trigger fast.
    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"auth_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.password("user", "pw")
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_private_key_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that private_key() raises DeephavenConnectionError on timeout."""
    import time

    def slow_auth(*args):
        time.sleep(0.05)

    dummy_session_manager.private_key.side_effect = slow_auth

    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"auth_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.private_key("/fake/path")
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_saml_timeout(coreplus_session_manager, dummy_session_manager):
    """Test that saml() raises DeephavenConnectionError on timeout."""
    import time

    def slow_auth():
        time.sleep(0.05)

    dummy_session_manager.saml.side_effect = slow_auth

    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"saml_auth_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.saml()
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
        await coreplus_session_manager.connect_to_new_worker(heap_size_gb=4)


@pytest.mark.asyncio
async def test_connect_to_persistent_query_timeout(
    coreplus_session_manager, dummy_session_manager
):
    """Test that connect_to_persistent_query() raises DeephavenConnectionError on timeout."""
    import time

    def slow_connect(*args, **kwargs):
        time.sleep(0.05)

    dummy_session_manager.connect_to_persistent_query.side_effect = slow_connect

    coreplus_session_manager._timeouts = coreplus_session_manager._timeouts.model_copy(
        update={"pq_connection_timeout_seconds": 0.01}
    )
    with pytest.raises(exc.DeephavenConnectionError) as exc_info:
        await coreplus_session_manager.connect_to_persistent_query(name="test")
    assert "timed out" in str(exc_info.value)


def _patches_for_from_url(side_effect=None):
    """Patch the module-level ``SessionManager`` used by ``from_url``.

    Mirrors ``_patches_for_from_credentials`` but without the config
    validator patch, since ``from_url`` takes only a URL.
    """
    mock_manager_class = MagicMock(side_effect=side_effect)
    return [
        patch(
            "deephaven_mcp.client._session_factory.SessionManager",
            mock_manager_class,
        ),
        nullcontext(),
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
                "https://example.com/iris/connection.json",
                timeouts=EnterpriseClientTimeouts(),
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
                EnterpriseClientTimeouts(session_connect_timeout_seconds=0.01),
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
                "https://example.com/iris/connection.json",
                timeouts=EnterpriseClientTimeouts(),
            )


# ---------------------------------------------------------------------------
# from_credentials
# ---------------------------------------------------------------------------


def _make_from_credentials_config():
    """Build a valid ``EnterpriseSystemConfig`` for ``from_credentials`` tests.

    The factory's typed entry point now accepts a pre-validated
    declaration; tests construct one here rather than passing a
    raw dict (the dict-based path was removed when
    :class:`EnterpriseSystemConfig` moved to
    :mod:`deephaven_mcp.sessions`).
    """
    from deephaven_mcp.sessions import EnterpriseSystemConfig

    return EnterpriseSystemConfig.model_validate(
        {
            "name": "test-system",
            "system_name": "test-system",
            "connection_json_url": "https://server/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "placeholder",
                    "password": "placeholder",
                }
            },
        }
    )


_FROM_CREDENTIALS_CONFIG = _make_from_credentials_config()


def _patches_for_from_credentials(side_effect=None):
    """Standard set of patches used by every from_credentials test.

    The new code path validates the config inline via
    :meth:`EnterpriseSystemConfig.model_validate`; tests therefore
    pass a fully-valid config dict (``_FROM_CREDENTIALS_CONFIG``)
    rather than patching out the validator.
    """
    mock_manager_class = MagicMock(side_effect=side_effect)
    return [
        patch(
            "deephaven_mcp.client._session_factory.SessionManager",
            mock_manager_class,
        ),
        nullcontext(),
    ]


@pytest.mark.asyncio
async def test_from_credentials_password_success():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="alice", password="pw")

    p1, p2 = _patches_for_from_credentials()
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ) as mock_password:
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds, timeouts=EnterpriseClientTimeouts()
            )
        mock_password.assert_awaited_once_with("alice", "pw", None)


@pytest.mark.asyncio
async def test_from_credentials_password_with_effective_user():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="alice", password="pw", effective_user="bob")

    p1, p2 = _patches_for_from_credentials()
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "password", new_callable=AsyncMock
        ) as mock_password:
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds, timeouts=EnterpriseClientTimeouts()
            )
        mock_password.assert_awaited_once_with("alice", "pw", "bob")


@pytest.mark.asyncio
async def test_from_credentials_private_key_success():
    from deephaven_mcp.auth.credentials import PrivateKeyCredentials

    creds = PrivateKeyCredentials(key_text="DH key payload\n")

    p1, p2 = _patches_for_from_credentials()
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with patch.object(
            sm_mod.CorePlusSessionFactory, "private_key", new_callable=AsyncMock
        ) as mock_pk:
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds, timeouts=EnterpriseClientTimeouts()
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
    """An object that is not a Credentials member is rejected with a typed error."""
    p1, p2 = _patches_for_from_credentials()
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        # The Credentials union dropped PSKCredentials when the auth
        # backends were torn down. Pass a plain object to exercise the
        # "unsupported credentials" branch.
        class _NotACredential:
            pass

        with pytest.raises(exc.AuthenticationError, match="Unsupported credentials"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG,
                _NotACredential(),  # type: ignore[arg-type]
                timeouts=EnterpriseClientTimeouts(),
            )


# ``test_from_credentials_invalid_config_propagates`` was retired when
# the dict-based entry point was removed: invalid configurations now
# fail at :meth:`EnterpriseSystemConfig.model_validate` time, well
# before the ``from_credentials`` call, and that schema-level rejection
# is exercised by ``tests/sessions/test__enterprise.py``.


@pytest.mark.asyncio
async def test_from_credentials_session_manager_timeout():
    import time as time_mod

    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")

    def slow_init(_url):
        time_mod.sleep(0.1)

    p1, p2 = _patches_for_from_credentials(side_effect=slow_init)
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError, match="timed out"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG,
                creds,
                EnterpriseClientTimeouts(session_connect_timeout_seconds=0.01),
            )


@pytest.mark.asyncio
async def test_from_credentials_session_manager_failure():
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")

    def boom(_url):
        raise RuntimeError("connect failed")

    p1, p2 = _patches_for_from_credentials(side_effect=boom)
    with p1, p2:
        import deephaven_mcp.client._session_factory as sm_mod

        with pytest.raises(exc.DeephavenConnectionError, match="connect failed"):
            await sm_mod.CorePlusSessionFactory.from_credentials(
                _FROM_CREDENTIALS_CONFIG, creds, timeouts=EnterpriseClientTimeouts()
            )
