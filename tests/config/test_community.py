"""Tests for deephaven_mcp.config.community."""

import logging
import os
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import (
    ConfigurationError,
)
from deephaven_mcp.config import (
    CONFIG_ENV_VAR,
    CommunityServerConfigManager,
)
from deephaven_mcp.config.community import (
    _redact_session_creation_config,
    _validate_security_config,
    _validate_session_creation_config,
    _validate_session_creation_defaults,
    _validate_sessions_config,
    redact_community_config,
    redact_community_session_config,
    validate_community_config,
    validate_community_session_config,
)


@pytest.fixture(autouse=True)
def _clear_env():
    old = os.environ.pop(CONFIG_ENV_VAR, None)
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


# ---------------------------------------------------------------------------
# redact_community_session_config
# ---------------------------------------------------------------------------


def test_redact_session_all_sensitive():
    cfg = {
        "host": "h",
        "auth_token": "secret",
        "tls_root_certs": b"bytes",
        "client_cert_chain": b"c",
        "client_private_key": b"k",
    }
    out = redact_community_session_config(cfg)
    assert out["auth_token"] == "[REDACTED]"
    assert out["tls_root_certs"] == "[REDACTED]"
    assert out["client_cert_chain"] == "[REDACTED]"
    assert out["client_private_key"] == "[REDACTED]"
    assert out["host"] == "h"
    # original untouched
    assert cfg["auth_token"] == "secret"


def test_redact_session_auth_token_empty_not_redacted():
    assert redact_community_session_config({"auth_token": ""})["auth_token"] == ""


def test_redact_session_string_tls_preserved():
    cfg = {"tls_root_certs": "/path/to/ca.pem"}
    assert redact_community_session_config(cfg)["tls_root_certs"] == "/path/to/ca.pem"


def test_redact_session_binary_skipped_when_flag_false():
    cfg = {"auth_token": "t", "tls_root_certs": b"b"}
    out = redact_community_session_config(cfg, redact_binary_values=False)
    assert out["auth_token"] == "[REDACTED]"
    assert out["tls_root_certs"] == b"b"


def test_redact_session_empty():
    assert redact_community_session_config({}) == {}


# ---------------------------------------------------------------------------
# _redact_session_creation_config
# ---------------------------------------------------------------------------


def test_redact_session_creation_with_auth_token():
    cfg = {"max_concurrent_sessions": 5, "defaults": {"auth_token": "s", "x": 1}}
    out = _redact_session_creation_config(cfg)
    assert out["defaults"]["auth_token"] == "[REDACTED]"
    assert out["defaults"]["x"] == 1
    assert cfg["defaults"]["auth_token"] == "s"  # deep copy


def test_redact_session_creation_no_defaults():
    assert _redact_session_creation_config({"max_concurrent_sessions": 5}) == {
        "max_concurrent_sessions": 5
    }


def test_redact_session_creation_defaults_not_dict():
    cfg = {"defaults": "not-a-dict"}
    assert _redact_session_creation_config(cfg) == cfg


def test_redact_session_creation_defaults_without_auth_token():
    cfg = {"defaults": {"x": 1}}
    assert _redact_session_creation_config(cfg) == cfg


# ---------------------------------------------------------------------------
# redact_community_config
# ---------------------------------------------------------------------------


def test_redact_community_config_full():
    cfg = {
        "sessions": {"a": {"auth_token": "s"}, "b": "not-a-dict"},
        "session_creation": {"defaults": {"auth_token": "t"}},
        "security": {"credential_retrieval_mode": "none"},
    }
    out = redact_community_config(cfg)
    assert out["sessions"]["a"]["auth_token"] == "[REDACTED]"
    assert out["sessions"]["b"] == "not-a-dict"
    assert out["session_creation"]["defaults"]["auth_token"] == "[REDACTED]"
    assert cfg["sessions"]["a"]["auth_token"] == "s"


def test_redact_community_config_sessions_not_dict():
    cfg = {"sessions": "nope"}
    assert redact_community_config(cfg) == cfg


def test_redact_community_config_session_creation_not_dict():
    cfg = {"session_creation": "nope"}
    assert redact_community_config(cfg) == cfg


def test_redact_community_config_empty():
    assert redact_community_config({}) == {}


# ---------------------------------------------------------------------------
# _validate_security_config
# ---------------------------------------------------------------------------


def test_validate_security_empty_ok():
    _validate_security_config({})


def test_validate_security_all_valid_modes():
    for mode in ["none", "dynamic_only", "static_only", "all"]:
        _validate_security_config({"credential_retrieval_mode": mode})


def test_validate_security_unknown_field_rejected():
    with pytest.raises(ConfigurationError, match="Unknown field 'extra'"):
        _validate_security_config({"extra": 1})


def test_validate_security_mode_wrong_type():
    with pytest.raises(ConfigurationError, match="must be of type str"):
        _validate_security_config({"credential_retrieval_mode": 5})


def test_validate_security_mode_invalid_value():
    with pytest.raises(
        ConfigurationError,
        match="credential_retrieval_mode",
    ):
        _validate_security_config({"credential_retrieval_mode": "bogus"})


# ---------------------------------------------------------------------------
# validate_community_session_config
# ---------------------------------------------------------------------------


def test_validate_session_not_dict():
    with pytest.raises(ConfigurationError, match="must be a dictionary"):
        validate_community_session_config("s", "bad")


def test_validate_session_empty_ok():
    validate_community_session_config("s", {})


def test_validate_session_full_ok():
    validate_community_session_config(
        "s",
        {
            "host": "h",
            "port": 10000,
            "auth_type": "PSK",
            "auth_token": "t",
            "never_timeout": True,
            "session_type": "python",
            "use_tls": False,
            "tls_root_certs": None,
            "client_cert_chain": "/x",
            "client_private_key": None,
        },
    )


def test_validate_session_unknown_field_rejected():
    with pytest.raises(ConfigurationError, match="Unknown field 'bogus'"):
        validate_community_session_config("s", {"bogus": 1})


def test_validate_session_wrong_type():
    with pytest.raises(ConfigurationError, match="port"):
        validate_community_session_config("s", {"port": "str"})


def test_validate_session_mutually_exclusive():
    with pytest.raises(ConfigurationError, match="mutually exclusive"):
        validate_community_session_config(
            "s", {"auth_token": "a", "auth_token_env_var": "B"}
        )


def test_validate_session_unknown_auth_type_warns(caplog):
    caplog.set_level(logging.WARNING)
    validate_community_session_config("s", {"auth_type": "CustomThing"})
    assert any("CustomThing" in rec.message for rec in caplog.records)


def test_validate_session_known_auth_type_no_warn(caplog):
    caplog.set_level(logging.WARNING)
    validate_community_session_config("s", {"auth_type": "PSK"})
    assert not any(rec.levelno == logging.WARNING for rec in caplog.records)


# ---------------------------------------------------------------------------
# _validate_sessions_config
# ---------------------------------------------------------------------------


def test_validate_sessions_not_dict():
    with pytest.raises(ConfigurationError, match="must be a dictionary"):
        _validate_sessions_config("bad")


def test_validate_sessions_empty_ok():
    _validate_sessions_config({})


def test_validate_sessions_bad_child():
    with pytest.raises(ConfigurationError, match="Unknown field 'x'"):
        _validate_sessions_config({"local": {"x": 1}})


# ---------------------------------------------------------------------------
# _validate_session_creation_defaults
# ---------------------------------------------------------------------------


def test_defaults_empty_ok():
    _validate_session_creation_defaults({})


def test_defaults_all_fields_ok():
    _validate_session_creation_defaults(
        {
            "launch_method": "docker",
            "auth_type": "PSK",
            "auth_token": "t",
            "programming_language": "python",
            "docker_image": "img",
            "docker_memory_limit_gb": 4.0,
            "docker_cpu_limit": 2,
            "docker_volumes": ["/a:/b"],
            "python_venv_path": "/venv",
            "heap_size_gb": 1,
            "extra_jvm_args": ["-Xmx1g"],
            "environment_vars": {"K": "V"},
            "startup_timeout_seconds": 30,
            "startup_check_interval_seconds": 1.0,
            "startup_retries": 3,
        }
    )


def test_defaults_unknown_field_rejected():
    with pytest.raises(ConfigurationError, match="Unknown field 'bogus'"):
        _validate_session_creation_defaults({"bogus": 1})


def test_defaults_mutually_exclusive():
    with pytest.raises(ConfigurationError, match="mutually exclusive"):
        _validate_session_creation_defaults(
            {"auth_token": "a", "auth_token_env_var": "X"}
        )


def test_defaults_bad_launch_method():
    with pytest.raises(ConfigurationError, match="launch_method"):
        _validate_session_creation_defaults({"launch_method": "vm"})


def test_defaults_unknown_auth_type_warns(caplog):
    caplog.set_level(logging.WARNING)
    _validate_session_creation_defaults({"auth_type": "Custom"})
    assert any("Custom" in rec.message for rec in caplog.records)


def test_defaults_bad_heap_size():
    with pytest.raises(ConfigurationError, match="heap_size_gb"):
        _validate_session_creation_defaults({"heap_size_gb": -1})


def test_defaults_bad_startup_retries():
    with pytest.raises(ConfigurationError, match="startup_retries"):
        _validate_session_creation_defaults({"startup_retries": -1})


def test_defaults_bad_docker_volumes():
    with pytest.raises(ConfigurationError, match="docker_volumes"):
        _validate_session_creation_defaults({"docker_volumes": [1]})


def test_defaults_bad_env_vars():
    with pytest.raises(ConfigurationError, match="environment_vars"):
        _validate_session_creation_defaults({"environment_vars": {"K": 1}})


# ---------------------------------------------------------------------------
# _validate_session_creation_config
# ---------------------------------------------------------------------------


def test_session_creation_not_dict():
    with pytest.raises(ConfigurationError, match="must be a dictionary"):
        _validate_session_creation_config("bad")


def test_session_creation_empty_ok():
    _validate_session_creation_config({})


def test_session_creation_unknown_field():
    with pytest.raises(ConfigurationError, match="Unknown field 'foo'"):
        _validate_session_creation_config({"foo": 1})


def test_session_creation_bad_max_concurrent():
    with pytest.raises(ConfigurationError, match="max_concurrent_sessions"):
        _validate_session_creation_config({"max_concurrent_sessions": -1})


def test_session_creation_with_valid_defaults():
    _validate_session_creation_config(
        {"max_concurrent_sessions": 3, "defaults": {"launch_method": "python"}}
    )


# ---------------------------------------------------------------------------
# validate_community_config
# ---------------------------------------------------------------------------


def test_community_config_not_dict():
    with pytest.raises(ConfigurationError, match="must be a dictionary"):
        validate_community_config([1, 2])


def test_community_config_unknown_top_level():
    with pytest.raises(ConfigurationError, match="Unknown field 'extra'"):
        validate_community_config({"extra": 1})


def test_community_config_empty_ok():
    assert validate_community_config({}) == {}


def test_community_config_all_sections():
    cfg = {
        "security": {"credential_retrieval_mode": "none"},
        "sessions": {"a": {"host": "h"}},
        "session_creation": {"defaults": {"launch_method": "python"}},
        "mcp_session_idle_timeout_seconds": 60,
    }
    assert validate_community_config(cfg) is cfg


def test_community_config_bad_idle_timeout():
    with pytest.raises(ConfigurationError, match="mcp_session_idle_timeout_seconds"):
        validate_community_config({"mcp_session_idle_timeout_seconds": 0})


def test_community_config_idle_timeout_wrong_type():
    with pytest.raises(ConfigurationError, match="mcp_session_idle_timeout_seconds"):
        validate_community_config({"mcp_session_idle_timeout_seconds": "x"})


# ---------------------------------------------------------------------------
# CommunityServerConfigManager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manager_loads_from_explicit_path(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))
    result = await mgr.get_config()
    assert result == {"sessions": {}}


@pytest.mark.asyncio
async def test_manager_caches(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))
    r1 = await mgr.get_config()
    r2 = await mgr.get_config()
    assert r1 is r2


@pytest.mark.asyncio
async def test_manager_uses_env_var(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')
    monkeypatch.setenv(CONFIG_ENV_VAR, str(cfg_file))
    mgr = CommunityServerConfigManager()
    result = await mgr.get_config()
    assert result == {"sessions": {}}


@pytest.mark.asyncio
async def test_manager_set_cache_validates():
    mgr = CommunityServerConfigManager(config_path="/nonexistent")
    await mgr._set_config_cache({"sessions": {}})
    assert await mgr.get_config() == {"sessions": {}}


@pytest.mark.asyncio
async def test_manager_set_cache_invalid_raises():
    mgr = CommunityServerConfigManager(config_path="/nonexistent")
    with pytest.raises(ConfigurationError):
        await mgr._set_config_cache({"bogus": 1})


@pytest.mark.asyncio
async def test_manager_clear_cache(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))
    await mgr.get_config()
    await mgr.clear_config_cache()
    assert mgr._cache is None


@pytest.mark.asyncio
async def test_manager_invalid_file_raises(tmp_path):
    cfg_file = tmp_path / "bad.json"
    cfg_file.write_text("not json")
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))
    with pytest.raises(ConfigurationError):
        await mgr.get_config()


@pytest.mark.asyncio
async def test_manager_log_summary_fallback_on_json_error(tmp_path, caplog):
    # Force _log_config_summary to handle a redacted config that fails to serialize.
    # This is exercised by a config that validates fine but the redactor returns
    # something un-serializable. Simplest approach: monkey-patch json5.dumps.
    import json5

    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))

    original_dumps = json5.dumps

    def bad_dumps(*a, **kw):
        raise TypeError("boom")

    with patch.object(json5, "dumps", bad_dumps):
        caplog.set_level(logging.INFO)
        await mgr.get_config()

    # Restore explicit reference (noop since patch.object unwinds on exit).
    assert original_dumps is json5.dumps
