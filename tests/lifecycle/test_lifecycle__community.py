from unittest.mock import AsyncMock, MagicMock

import pytest

from deephaven_mcp.sessions._errors import SessionCreationError
from deephaven_mcp.sessions._lifecycle import community as _lifecycle_community


@pytest.mark.asyncio
async def test_get_session_parameters_missing_fields():
    # Should not raise if only host provided
    params = await _lifecycle_community._get_session_parameters({"host": "localhost"})
    assert params["host"] == "localhost"
    # Should fill defaults
    assert params["auth_type"] == "Anonymous"
    assert params["auth_token"] == ""
    assert params["never_timeout"] is False
    assert params["session_type"] == "python"
    assert params["use_tls"] is False


@pytest.mark.asyncio
async def test_get_session_parameters_with_and_without_files():
    from unittest.mock import AsyncMock, patch

    with patch(
        "deephaven_mcp.sessions._lifecycle.community.load_bytes",
        new=AsyncMock(return_value=b"binary"),
    ):
        # All fields present (as file paths)
        cfg = {
            "host": "localhost",
            "port": 10000,
            "auth_type": "token",
            "auth_token": "tok",
            "never_timeout": True,
            "session_type": "python",
            "use_tls": True,
            "tls_root_certs": "/tmp/root.pem",
            "client_cert_chain": "/tmp/chain.pem",
            "client_private_key": "/tmp/key.pem",
        }
        params = await _lifecycle_community._get_session_parameters(cfg)
        assert params["tls_root_certs"] == b"binary"
        assert params["client_cert_chain"] == b"binary"
        assert params["client_private_key"] == b"binary"
        # No files present
        cfg = {"host": "localhost"}
        params = await _lifecycle_community._get_session_parameters(cfg)
        assert params["host"] == "localhost"


@pytest.mark.asyncio
async def test_get_session_parameters_file_error():
    # Patch load_bytes to raise
    async def raise_io(path):
        raise IOError("fail")

    from unittest.mock import patch

    with patch("deephaven_mcp.sessions._lifecycle.community.load_bytes", new=raise_io):
        cfg = {"tls_root_certs": "/bad/path"}
        with pytest.raises(IOError):
            await _lifecycle_community._get_session_parameters(cfg)


@pytest.mark.asyncio
async def test_get_session_parameters_auth_token_from_env_var(monkeypatch):
    """Test auth_token is sourced from environment variable when auth_token_env_var is set."""
    env_var_name = "MY_TEST_TOKEN_VAR"
    expected_token = "token_from_environment"
    monkeypatch.setenv(env_var_name, expected_token)
    worker_cfg = {
        "auth_token_env_var": env_var_name,
        # As per config validation, auth_token should not be present if auth_token_env_var is.
    }
    params = await _lifecycle_community._get_session_parameters(worker_cfg)
    assert params["auth_token"] == expected_token
    monkeypatch.delenv(env_var_name)  # Clean up


@pytest.mark.asyncio
async def test_get_session_parameters_auth_token_env_var_not_set(monkeypatch, caplog):
    """Test auth_token is empty and warning logged if auth_token_env_var is set but env var is not."""
    env_var_name = "MY_MISSING_TOKEN_VAR"
    monkeypatch.delenv(env_var_name, raising=False)  # Ensure it's not set
    worker_cfg = {
        "auth_token_env_var": env_var_name,
    }
    params = await _lifecycle_community._get_session_parameters(worker_cfg)
    assert params["auth_token"] == ""
    assert (
        f"Environment variable {env_var_name} specified for auth_token but not found. Using empty token."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_get_session_parameters_auth_token_from_config():
    """Test auth_token is sourced from config when auth_token_env_var is not set."""
    expected_token = "token_from_config_direct"
    worker_cfg = {
        "auth_token": expected_token,
    }
    params = await _lifecycle_community._get_session_parameters(worker_cfg)
    assert params["auth_token"] == expected_token


@pytest.mark.asyncio
async def test_get_session_parameters_no_auth_token_provided():
    """Test auth_token is empty if neither auth_token nor auth_token_env_var is provided."""
    worker_cfg = {"host": "localhost"}  # Some other config, but no auth token fields
    params = await _lifecycle_community._get_session_parameters(worker_cfg)
    assert params["auth_token"] == ""


@pytest.mark.asyncio
async def test_create_session_for_worker(monkeypatch):
    def fake_get_config_section(config, path):
        # Expect new signature: get_config_section(config, ["community", "sessions", "workerZ"])
        assert path == ["community", "sessions", "workerZ"]
        return {"host": "localhost"}

    async def fake_get_config():
        return {"community": {"sessions": {"workerZ": {"host": "localhost"}}}}

    async def fake__get_session_parameters(cfg):
        assert cfg["host"] == "localhost"
        return {"host": "localhost"}

    async def fake_create_session(**kwargs):
        assert kwargs["host"] == "localhost"
        return "SESSION"

    cfg_mgr = MagicMock()
    cfg_mgr.get_config = AsyncMock(
        return_value={"community": {"sessions": {"workerZ": {"host": "localhost"}}}}
    )

    monkeypatch.setattr(
        _lifecycle_community.config, "get_config_section", fake_get_config_section
    )
    monkeypatch.setattr(
        _lifecycle_community, "_get_session_parameters", fake__get_session_parameters
    )
    monkeypatch.setattr(_lifecycle_community, "create_session", fake_create_session)

    session = await _lifecycle_community.create_session_for_worker(cfg_mgr, "workerZ")
    assert session == "SESSION"


@pytest.mark.asyncio
async def test_create_session_for_worker_config_fail(monkeypatch):
    def fake_get_config_section(config, path):
        raise RuntimeError("fail-config")

    async def fake_get_config():
        return {"community": {"sessions": {"workerZ": {"host": "localhost"}}}}

    cfg_mgr = MagicMock()
    cfg_mgr.get_config = AsyncMock(
        return_value={"community": {"sessions": {"workerZ": {"host": "localhost"}}}}
    )
    monkeypatch.setattr(
        _lifecycle_community.config, "get_config_section", fake_get_config_section
    )
    with pytest.raises(RuntimeError):
        await _lifecycle_community.create_session_for_worker(cfg_mgr, "workerZ")


@pytest.mark.asyncio
async def test_create_session_for_worker_session_fail(monkeypatch):
    def fake_get_config_section(config, path):
        # Expect new signature: get_config_section(config, ["community", "sessions", "workerZ"])
        assert path == ["community", "sessions", "workerZ"]
        return {"host": "localhost"}

    async def fake_get_config():
        return {"community": {"sessions": {"workerZ": {"host": "localhost"}}}}

    async def fake__get_session_parameters(cfg):
        return {"host": "localhost"}

    async def fake_create_session(**kwargs):
        raise RuntimeError("fail-create")

    cfg_mgr = MagicMock()
    cfg_mgr.get_config = AsyncMock(
        return_value={"community": {"sessions": {"workerZ": {"host": "localhost"}}}}
    )

    monkeypatch.setattr(
        _lifecycle_community.config, "get_config_section", fake_get_config_section
    )
    monkeypatch.setattr(
        _lifecycle_community, "_get_session_parameters", fake__get_session_parameters
    )
    monkeypatch.setattr(_lifecycle_community, "create_session", fake_create_session)
    with pytest.raises(RuntimeError):
        await _lifecycle_community.create_session_for_worker(cfg_mgr, "workerZ")
