"""Tests for deephaven_mcp.config.enterprise."""

import json
import os

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config import (
    CONFIG_ENV_VAR,
    DEFAULT_CONNECTION_TIMEOUT_SECONDS,
    EnterpriseServerConfigManager,
)
from deephaven_mcp.config.enterprise import (
    SUPPORTED_AUTH_BACKENDS,
    _validate_auth_block,
    _validate_session_creation,
    _validate_top_level_fields,
    get_enterprise_allow_effective_user,
    get_enterprise_auth_backends,
    redact_enterprise_config,
    validate_enterprise_config,
)


@pytest.fixture(autouse=True)
def _clear_env():
    old = os.environ.pop(CONFIG_ENV_VAR, None)
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


def _minimal_config(**overrides):
    base = {
        "system_name": "prod",
        "connection_json_url": "https://x/iris/connection.json",
        "auth": {"backends": ["password"]},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_default_timeout_constant():
    assert DEFAULT_CONNECTION_TIMEOUT_SECONDS == 10.0


def test_supported_backends_constant():
    assert SUPPORTED_AUTH_BACKENDS == frozenset({"password", "private_key"})


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_redact_returns_shallow_copy():
    cfg = _minimal_config()
    out = redact_enterprise_config(cfg)
    assert out == cfg
    assert out is not cfg
    # Mutating the output must not mutate the input.
    out["system_name"] = "other"
    assert cfg["system_name"] == "prod"


# ---------------------------------------------------------------------------
# _validate_top_level_fields
# ---------------------------------------------------------------------------


def test_top_level_missing_required_connection_url():
    with pytest.raises(ConfigurationError, match="connection_json_url"):
        _validate_top_level_fields(
            "x", {"system_name": "x", "auth": {"backends": ["password"]}}
        )


def test_top_level_missing_required_auth():
    with pytest.raises(ConfigurationError, match="'auth'"):
        _validate_top_level_fields(
            "x", {"system_name": "x", "connection_json_url": "u"}
        )


def test_top_level_bad_base_type():
    with pytest.raises(ConfigurationError, match="connection_json_url"):
        _validate_top_level_fields(
            "x",
            {
                "system_name": "x",
                "connection_json_url": 1,
                "auth": {"backends": ["password"]},
            },
        )


def test_top_level_auth_wrong_type():
    with pytest.raises(ConfigurationError, match="auth"):
        _validate_top_level_fields(
            "x",
            {
                "system_name": "x",
                "connection_json_url": "u",
                "auth": "not-a-dict",
            },
        )


def test_top_level_unknown_field_rejected():
    cfg = _minimal_config(surprise=1)
    with pytest.raises(ConfigurationError, match="Unknown field 'surprise'"):
        _validate_top_level_fields("prod", cfg)


def test_top_level_ok():
    # Should not raise.
    _validate_top_level_fields("prod", _minimal_config())


# ---------------------------------------------------------------------------
# _validate_auth_block
# ---------------------------------------------------------------------------


def test_auth_unknown_field_rejected():
    with pytest.raises(ConfigurationError, match="Unknown field 'extra'"):
        _validate_auth_block("x", {"backends": ["password"], "extra": 1})


def test_auth_missing_backends_rejected():
    with pytest.raises(ConfigurationError, match="'backends' missing"):
        _validate_auth_block("x", {})


def test_auth_backends_wrong_type_rejected():
    with pytest.raises(ConfigurationError, match="backends"):
        _validate_auth_block("x", {"backends": "password"})


def test_auth_backends_empty_rejected():
    with pytest.raises(ConfigurationError, match="non-empty"):
        _validate_auth_block("x", {"backends": []})


def test_auth_backends_duplicates_rejected():
    with pytest.raises(ConfigurationError, match="duplicate"):
        _validate_auth_block("x", {"backends": ["password", "password"]})


def test_auth_backends_non_string_rejected():
    with pytest.raises(ConfigurationError, match="only strings"):
        _validate_auth_block("x", {"backends": [1]})


def test_auth_backends_unsupported_value_rejected():
    with pytest.raises(ConfigurationError, match="unsupported entry 'kerberos'"):
        _validate_auth_block("x", {"backends": ["kerberos"]})


def test_auth_backends_password_ok():
    _validate_auth_block("x", {"backends": ["password"]})


def test_auth_backends_private_key_ok():
    _validate_auth_block("x", {"backends": ["private_key"]})


def test_auth_backends_both_ok():
    _validate_auth_block("x", {"backends": ["password", "private_key"]})


def test_auth_allow_effective_user_wrong_type_rejected():
    with pytest.raises(ConfigurationError, match="allow_effective_user"):
        _validate_auth_block(
            "x", {"backends": ["password"], "allow_effective_user": "yes"}
        )


def test_auth_allow_effective_user_true_without_password_rejected():
    with pytest.raises(ConfigurationError, match="'password' is included"):
        _validate_auth_block(
            "x", {"backends": ["private_key"], "allow_effective_user": True}
        )


def test_auth_allow_effective_user_false_without_password_ok():
    _validate_auth_block(
        "x", {"backends": ["private_key"], "allow_effective_user": False}
    )


def test_auth_allow_effective_user_true_with_password_ok():
    _validate_auth_block(
        "x",
        {"backends": ["password", "private_key"], "allow_effective_user": True},
    )


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
                "heap_size_gb": 2,
                "auto_delete_timeout": 60,
                "server": "s",
                "engine": "e",
                "extra_jvm_args": [],
                "extra_environment_vars": [],
                "admin_groups": [],
                "viewer_groups": [],
                "timeout_seconds": 30,
                "session_arguments": {},
                "programming_language": "Python",
            },
        }
    }
    _validate_session_creation("x", cfg)


# ---------------------------------------------------------------------------
# validate_enterprise_config
# ---------------------------------------------------------------------------


def test_enterprise_config_not_dict():
    with pytest.raises(ConfigurationError, match="must be a dictionary"):
        validate_enterprise_config("nope")


def test_enterprise_config_system_name_not_str():
    cfg = {
        "system_name": 1,
        "connection_json_url": "u",
        "auth": {"backends": ["password"]},
    }
    with pytest.raises(ConfigurationError, match="system_name"):
        validate_enterprise_config(cfg)


def test_enterprise_config_system_name_missing():
    cfg = {"connection_json_url": "u", "auth": {"backends": ["password"]}}
    with pytest.raises(ConfigurationError, match="system_name"):
        validate_enterprise_config(cfg)


def test_enterprise_config_minimal_ok():
    assert validate_enterprise_config(_minimal_config()) is not None


def test_enterprise_config_bad_connection_timeout():
    cfg = _minimal_config(connection_timeout=-1)
    with pytest.raises(ConfigurationError, match="connection_timeout"):
        validate_enterprise_config(cfg)


def test_enterprise_config_bad_idle_timeout():
    cfg = _minimal_config(session_idle_timeout_seconds=0)
    with pytest.raises(ConfigurationError, match="session_idle_timeout_seconds"):
        validate_enterprise_config(cfg)


def test_enterprise_config_bad_sweep_interval():
    cfg = _minimal_config(session_idle_sweep_interval_seconds=0)
    with pytest.raises(ConfigurationError, match="session_idle_sweep_interval_seconds"):
        validate_enterprise_config(cfg)


def test_enterprise_config_sweep_interval_accepted():
    cfg = _minimal_config(session_idle_sweep_interval_seconds=15)
    assert validate_enterprise_config(cfg) is cfg


def test_enterprise_config_with_session_creation_ok():
    cfg = _minimal_config(session_creation={"defaults": {"heap_size_gb": 1}})
    validate_enterprise_config(cfg)


def test_enterprise_config_with_full_auth_block_ok():
    cfg = _minimal_config(
        auth={
            "backends": ["password", "private_key"],
            "allow_effective_user": True,
        }
    )
    validate_enterprise_config(cfg)


def test_enterprise_config_rejects_legacy_auth_type_field():
    """The old top-level ``auth_type`` field is now unknown."""
    cfg = {
        "system_name": "prod",
        "connection_json_url": "u",
        "auth": {"backends": ["password"]},
        "auth_type": "password",
    }
    with pytest.raises(ConfigurationError, match="Unknown field 'auth_type'"):
        validate_enterprise_config(cfg)


def test_enterprise_config_rejects_legacy_password_field():
    cfg = _minimal_config(password="secret")
    with pytest.raises(ConfigurationError, match="Unknown field 'password'"):
        validate_enterprise_config(cfg)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def test_get_enterprise_auth_backends_returns_list_copy():
    cfg = _minimal_config(auth={"backends": ["password", "private_key"]})
    out = get_enterprise_auth_backends(cfg)
    assert out == ["password", "private_key"]
    out.append("kerberos")
    assert cfg["auth"]["backends"] == ["password", "private_key"]


def test_get_enterprise_allow_effective_user_default_false():
    cfg = _minimal_config()
    assert get_enterprise_allow_effective_user(cfg) is False


def test_get_enterprise_allow_effective_user_explicit_true():
    cfg = _minimal_config(auth={"backends": ["password"], "allow_effective_user": True})
    assert get_enterprise_allow_effective_user(cfg) is True


# ---------------------------------------------------------------------------
# EnterpriseServerConfigManager
# ---------------------------------------------------------------------------


def _write_cfg(tmp_path, cfg=None):
    cfg = cfg if cfg is not None else _minimal_config()
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))
    return str(p)


@pytest.mark.asyncio
async def test_manager_loads_from_explicit_path(tmp_path):
    path = _write_cfg(tmp_path)
    mgr = EnterpriseServerConfigManager(config_path=path)
    result = await mgr.get_config()
    assert result["system_name"] == "prod"
    assert result["auth"]["backends"] == ["password"]


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
    await mgr._set_config_cache(_minimal_config())
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
