"""Tests for deephaven_mcp.config.enterprise."""

import os

import pytest

from deephaven_mcp._exceptions import (
    ConfigurationError,
)
from deephaven_mcp.config import (
    CONFIG_ENV_VAR,
    DEFAULT_CONNECTION_TIMEOUT_SECONDS,
    EnterpriseServerConfigManager,
)
from deephaven_mcp.config.enterprise import (
    _validate_auth_type_logic,
    _validate_session_creation,
    _validate_top_level_fields,
    redact_enterprise_config,
    validate_enterprise_config,
)


@pytest.fixture(autouse=True)
def _clear_env():
    old = os.environ.pop(CONFIG_ENV_VAR, None)
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


def _minimal_password_config(**overrides):
    base = {
        "system_name": "prod",
        "connection_json_url": "https://x/iris/connection.json",
        "auth_type": "password",
        "username": "u",
        "password": "p",
    }
    base.update(overrides)
    return base


def _minimal_private_key_config(**overrides):
    base = {
        "system_name": "prod",
        "connection_json_url": "https://x/iris/connection.json",
        "auth_type": "private_key",
        "private_key_path": "/key",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_default_timeout_constant():
    assert DEFAULT_CONNECTION_TIMEOUT_SECONDS == 10.0


# ---------------------------------------------------------------------------
# redact_enterprise_config
# ---------------------------------------------------------------------------


def test_redact_password_present():
    cfg = {"username": "u", "password": "s"}
    out = redact_enterprise_config(cfg)
    assert out == {"username": "u", "password": "[REDACTED]"}
    assert cfg["password"] == "s"


def test_redact_password_absent():
    assert redact_enterprise_config({"username": "u"}) == {"username": "u"}


# ---------------------------------------------------------------------------
# _validate_top_level_fields
# ---------------------------------------------------------------------------


def test_top_level_missing_required():
    with pytest.raises(ConfigurationError, match="connection_json_url"):
        _validate_top_level_fields("x", {"system_name": "x", "auth_type": "password"})


def test_top_level_bad_base_type():
    with pytest.raises(ConfigurationError, match="connection_json_url"):
        _validate_top_level_fields(
            "x",
            {"system_name": "x", "connection_json_url": 1, "auth_type": "password"},
        )


def test_top_level_bad_auth_type():
    cfg = {
        "system_name": "x",
        "connection_json_url": "u",
        "auth_type": "bogus",
    }
    with pytest.raises(ConfigurationError, match="auth_type"):
        _validate_top_level_fields("x", cfg)


def test_top_level_unknown_field_rejected():
    cfg = _minimal_password_config(surprise=1)
    with pytest.raises(ConfigurationError, match="Unknown field 'surprise'"):
        _validate_top_level_fields("prod", cfg)


def test_top_level_ok_password():
    auth_type, allowed = _validate_top_level_fields("prod", _minimal_password_config())
    assert auth_type == "password"
    assert "username" in allowed


def test_top_level_ok_private_key():
    auth_type, allowed = _validate_top_level_fields(
        "prod", _minimal_private_key_config()
    )
    assert auth_type == "private_key"
    assert "private_key_path" in allowed


# ---------------------------------------------------------------------------
# _validate_auth_type_logic
# ---------------------------------------------------------------------------


def test_auth_password_missing_username():
    cfg = {"password": "p"}
    with pytest.raises(ConfigurationError, match="username"):
        _validate_auth_type_logic("x", cfg, "password")


def test_auth_password_both_password_fields():
    cfg = {"username": "u", "password": "p", "password_env_var": "E"}
    with pytest.raises(ConfigurationError, match="mutually exclusive"):
        _validate_auth_type_logic("x", cfg, "password")


def test_auth_password_neither_password_field():
    cfg = {"username": "u"}
    with pytest.raises(ConfigurationError, match="'password' or 'password_env_var'"):
        _validate_auth_type_logic("x", cfg, "password")


def test_auth_password_env_var_ok():
    _validate_auth_type_logic(
        "x", {"username": "u", "password_env_var": "E"}, "password"
    )


def test_auth_private_key_missing_path():
    with pytest.raises(ConfigurationError, match="private_key_path"):
        _validate_auth_type_logic("x", {}, "private_key")


def test_auth_private_key_ok():
    _validate_auth_type_logic("x", {"private_key_path": "/k"}, "private_key")


# ---------------------------------------------------------------------------
# _validate_session_creation
# ---------------------------------------------------------------------------


def test_session_creation_absent_ok():
    _validate_session_creation("x", {})


def test_session_creation_unknown_top_level():
    cfg = {"session_creation": {"bogus": 1}}
    with pytest.raises(ConfigurationError, match="Unknown field 'bogus'"):
        _validate_session_creation("x", cfg)


def test_session_creation_bad_max_concurrent():
    cfg = {
        "session_creation": {
            "max_concurrent_sessions": -1,
            "defaults": {"heap_size_gb": 1},
        }
    }
    with pytest.raises(ConfigurationError, match="max_concurrent_sessions"):
        _validate_session_creation("x", cfg)


def test_session_creation_missing_defaults():
    cfg = {"session_creation": {"max_concurrent_sessions": 1}}
    with pytest.raises(ConfigurationError, match="defaults.*required"):
        _validate_session_creation("x", cfg)


def test_session_creation_defaults_unknown_field():
    cfg = {"session_creation": {"defaults": {"heap_size_gb": 1, "bogus": 2}}}
    with pytest.raises(ConfigurationError, match="Unknown field 'bogus'"):
        _validate_session_creation("x", cfg)


def test_session_creation_defaults_missing_heap_size():
    cfg = {"session_creation": {"defaults": {"server": "s"}}}
    with pytest.raises(ConfigurationError, match="heap_size_gb.*required"):
        _validate_session_creation("x", cfg)


def test_session_creation_ok_full():
    cfg = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "heap_size_gb": 4,
                "auto_delete_timeout": 60,
                "server": "s",
                "engine": "e",
                "extra_jvm_args": ["-Xmx1g"],
                "extra_environment_vars": ["X=Y"],
                "admin_groups": ["a"],
                "viewer_groups": ["v"],
                "timeout_seconds": 30,
                "session_arguments": {"k": "v"},
                "programming_language": "Python",
            },
        }
    }
    _validate_session_creation("x", cfg)


# ---------------------------------------------------------------------------
# validate_enterprise_config (public entry point)
# ---------------------------------------------------------------------------


def test_enterprise_config_not_dict():
    with pytest.raises(ConfigurationError, match="must be a dictionary"):
        validate_enterprise_config("nope")


def test_enterprise_config_system_name_not_str():
    # system_name wrong type → caught by _validate_top_level_fields; "<invalid>"
    # is used as context placeholder.
    cfg = {"system_name": 1, "connection_json_url": "u", "auth_type": "password"}
    with pytest.raises(ConfigurationError, match="system_name"):
        validate_enterprise_config(cfg)


def test_enterprise_config_system_name_missing():
    cfg = {"connection_json_url": "u", "auth_type": "password"}
    with pytest.raises(ConfigurationError, match="system_name"):
        validate_enterprise_config(cfg)


def test_enterprise_config_password_ok():
    assert validate_enterprise_config(_minimal_password_config()) is not None


def test_enterprise_config_private_key_ok():
    assert validate_enterprise_config(_minimal_private_key_config()) is not None


def test_enterprise_config_bad_connection_timeout():
    cfg = _minimal_password_config(connection_timeout=-1)
    with pytest.raises(ConfigurationError, match="connection_timeout"):
        validate_enterprise_config(cfg)


def test_enterprise_config_bad_idle_timeout():
    cfg = _minimal_password_config(mcp_session_idle_timeout_seconds=0)
    with pytest.raises(ConfigurationError, match="mcp_session_idle_timeout_seconds"):
        validate_enterprise_config(cfg)


def test_enterprise_config_with_session_creation_ok():
    cfg = _minimal_password_config(session_creation={"defaults": {"heap_size_gb": 1}})
    validate_enterprise_config(cfg)


# ---------------------------------------------------------------------------
# EnterpriseServerConfigManager
# ---------------------------------------------------------------------------


def _write_cfg(
    tmp_path,
    cfg_str='{"system_name": "prod", "connection_json_url": "u", "auth_type": "password", "username": "u", "password": "p"}',
):
    p = tmp_path / "cfg.json"
    p.write_text(cfg_str)
    return str(p)


@pytest.mark.asyncio
async def test_manager_loads_from_explicit_path(tmp_path):
    path = _write_cfg(tmp_path)
    mgr = EnterpriseServerConfigManager(config_path=path)
    result = await mgr.get_config()
    assert result["system_name"] == "prod"


@pytest.mark.asyncio
async def test_manager_caches(tmp_path):
    path = _write_cfg(tmp_path)
    mgr = EnterpriseServerConfigManager(config_path=path)
    r1 = await mgr.get_config()
    r2 = await mgr.get_config()
    assert r1 is r2


@pytest.mark.asyncio
async def test_manager_uses_env_var(tmp_path, monkeypatch):
    path = _write_cfg(tmp_path)
    monkeypatch.setenv(CONFIG_ENV_VAR, path)
    mgr = EnterpriseServerConfigManager()
    result = await mgr.get_config()
    assert result["system_name"] == "prod"


@pytest.mark.asyncio
async def test_manager_set_cache_validates():
    mgr = EnterpriseServerConfigManager(config_path="/nonexistent")
    await mgr._set_config_cache(_minimal_password_config())
    assert (await mgr.get_config())["system_name"] == "prod"


@pytest.mark.asyncio
async def test_manager_set_cache_invalid_raises():
    mgr = EnterpriseServerConfigManager(config_path="/nonexistent")
    with pytest.raises(ConfigurationError):
        await mgr._set_config_cache({"bogus": 1})


@pytest.mark.asyncio
async def test_manager_clear_cache(tmp_path):
    path = _write_cfg(tmp_path)
    mgr = EnterpriseServerConfigManager(config_path=path)
    await mgr.get_config()
    await mgr.clear_config_cache()
    assert mgr._cache is None


@pytest.mark.asyncio
async def test_manager_invalid_file_raises(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("not json")
    mgr = EnterpriseServerConfigManager(config_path=str(p))
    with pytest.raises(ConfigurationError):
        await mgr.get_config()
