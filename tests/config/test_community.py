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
    _validate_auth_config,
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


def test_community_config_missing_auth_raises():
    # The top-level 'auth' block is required. Validator must reject configs
    # that omit it with a clear "missing field" error guiding the operator
    # to psk_env_var / psk / enabled:false, rather than silently passing
    # and crashing later in the startup loader at config["auth"].
    with pytest.raises(
        ConfigurationError, match="'auth' missing in community configuration"
    ):
        validate_community_config({})


def test_community_config_minimal_anonymous_ok():
    cfg = {"auth": {"enabled": False}}
    assert validate_community_config(cfg) is cfg


def test_community_config_all_sections():
    cfg = {
        "auth": {"enabled": False},
        "security": {"credential_retrieval_mode": "none"},
        "sessions": {"a": {"host": "h"}},
        "session_creation": {"defaults": {"launch_method": "python"}},
        "mcp_session_idle_timeout_seconds": 60,
    }
    assert validate_community_config(cfg) is cfg


def test_community_config_bad_idle_timeout():
    with pytest.raises(ConfigurationError, match="mcp_session_idle_timeout_seconds"):
        validate_community_config(
            {"auth": {"enabled": False}, "mcp_session_idle_timeout_seconds": 0}
        )


def test_community_config_idle_timeout_wrong_type():
    with pytest.raises(ConfigurationError, match="mcp_session_idle_timeout_seconds"):
        validate_community_config(
            {"auth": {"enabled": False}, "mcp_session_idle_timeout_seconds": "x"}
        )


# ---------------------------------------------------------------------------
# CommunityServerConfigManager
# ---------------------------------------------------------------------------


_MINIMAL_CFG_JSON = '{"auth": {"enabled": false}, "sessions": {}}'
_MINIMAL_CFG_DICT = {"auth": {"enabled": False}, "sessions": {}}


@pytest.mark.asyncio
async def test_manager_loads_from_explicit_path(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(_MINIMAL_CFG_JSON)
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))
    result = await mgr.get_config()
    assert result == _MINIMAL_CFG_DICT


@pytest.mark.asyncio
async def test_manager_caches(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(_MINIMAL_CFG_JSON)
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))
    r1 = await mgr.get_config()
    r2 = await mgr.get_config()
    assert r1 is r2


@pytest.mark.asyncio
async def test_manager_uses_env_var(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(_MINIMAL_CFG_JSON)
    monkeypatch.setenv(CONFIG_ENV_VAR, str(cfg_file))
    mgr = CommunityServerConfigManager()
    result = await mgr.get_config()
    assert result == _MINIMAL_CFG_DICT


@pytest.mark.asyncio
async def test_manager_set_cache_validates():
    mgr = CommunityServerConfigManager(config_path="/nonexistent")
    await mgr._set_config_cache(dict(_MINIMAL_CFG_DICT))
    assert await mgr.get_config() == _MINIMAL_CFG_DICT


@pytest.mark.asyncio
async def test_manager_set_cache_invalid_raises():
    mgr = CommunityServerConfigManager(config_path="/nonexistent")
    with pytest.raises(ConfigurationError):
        await mgr._set_config_cache({"bogus": 1})


@pytest.mark.asyncio
async def test_manager_clear_cache(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(_MINIMAL_CFG_JSON)
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
    cfg_file.write_text(_MINIMAL_CFG_JSON)
    mgr = CommunityServerConfigManager(config_path=str(cfg_file))

    original_dumps = json5.dumps

    def bad_dumps(*a, **kw):
        raise TypeError("boom")

    with patch.object(json5, "dumps", bad_dumps):
        caplog.set_level(logging.INFO)
        await mgr.get_config()

    # Restore explicit reference (noop since patch.object unwinds on exit).
    assert original_dumps is json5.dumps


# ---------------------------------------------------------------------------
# _validate_auth_config
# ---------------------------------------------------------------------------


def test_auth_empty_block_rejected_when_defaults_to_enabled():
    with pytest.raises(ConfigurationError, match="enabled: true"):
        _validate_auth_config({})


def test_auth_inline_psk_ok():
    _validate_auth_config({"psk": "s3cret"})


def test_auth_env_var_ok():
    _validate_auth_config({"psk_env_var": "DH_MCP_PSK"})


def test_auth_both_inline_and_env_is_mutually_exclusive():
    with pytest.raises(ConfigurationError, match="mutually exclusive"):
        _validate_auth_config({"psk": "s", "psk_env_var": "X"})


def test_auth_enabled_false_without_secret_ok():
    _validate_auth_config({"enabled": False})


def test_auth_enabled_false_with_secret_rejected():
    with pytest.raises(ConfigurationError, match="enabled: false"):
        _validate_auth_config({"enabled": False, "psk": "s"})


def test_auth_unknown_field_rejected():
    with pytest.raises(ConfigurationError, match="Unknown field"):
        _validate_auth_config({"psk": "s", "bogus": True})


def test_auth_enabled_wrong_type_rejected():
    with pytest.raises(ConfigurationError, match="psk"):
        # psk must be str, not int; covers the type-check branch.
        _validate_auth_config({"psk": 1234})


def test_auth_empty_psk_rejected():
    # Regression: empty-string psk used to pass _validate_auth_config because
    # has_secret was a key-presence check ("psk" in auth_config). At startup,
    # resolve_secret_field returns None for empty strings (the truthy-string
    # check at _validators.py:resolve_secret_field), so the server would
    # silently fall into the auth-disabled middleware branch with only a
    # WARNING banner, despite the operator config explicitly enabling auth.
    # has_secret is now a truthy check, so this is rejected at validation.
    with pytest.raises(ConfigurationError, match="enabled: true"):
        _validate_auth_config({"psk": ""})


def test_auth_empty_psk_env_var_rejected():
    # Same defect class as test_auth_empty_psk_rejected, but for the env-var
    # indirection field. Templating tools (sed, envsubst, helm) commonly drop
    # unset placeholders to "", which previously slipped past the validator.
    with pytest.raises(ConfigurationError, match="enabled: true"):
        _validate_auth_config({"psk_env_var": ""})


def test_auth_empty_psk_with_enabled_false_rejected():
    # The "enabled: false but PSK provided" rule uses key-presence (not
    # truthiness), so even an empty string trips it. This is intentional:
    # a config with both `enabled: false` and a `psk` field — empty or
    # otherwise — is confused and should be flagged so the operator picks
    # one consistent intent. (The earlier truthy-only fix accidentally
    # weakened this; the validator now keeps two separate variables for
    # the two distinct checks.)
    with pytest.raises(ConfigurationError, match="enabled: false"):
        _validate_auth_config({"enabled": False, "psk": ""})


def test_auth_enabled_false_clean_ok():
    # Negative control: `enabled: false` with no PSK fields at all is
    # accepted (loopback-only deployments).
    _validate_auth_config({"enabled": False})


# Note: tests for community PSK resolution moved out of this file.
# - The underlying env-var resolution mechanics are tested in
#   tests/config/test__validators.py against resolve_secret_field /
#   resolve_required_env_var.
# - The 'enabled: false -> None' policy and end-to-end PSK wiring at
#   server startup are tested in tests/mcp_systems_server/test_server.py
#   against _load_community_startup_state.


# ---------------------------------------------------------------------------
# validate_community_config: auth block wiring
# ---------------------------------------------------------------------------


def test_validate_community_config_accepts_auth_block():
    cfg = {"auth": {"psk": "s"}}
    validate_community_config(cfg)


def test_validate_community_config_rejects_bad_auth_block():
    cfg = {"auth": {"psk": "s", "psk_env_var": "X"}}
    with pytest.raises(ConfigurationError):
        validate_community_config(cfg)


# ---------------------------------------------------------------------------
# redact_community_config: auth token
# ---------------------------------------------------------------------------


def test_redact_community_config_redacts_psk():
    cfg = {"auth": {"psk": "s3cret"}}
    out = redact_community_config(cfg)
    assert out["auth"]["psk"] == "[REDACTED]"
    assert cfg["auth"]["psk"] == "s3cret"  # original untouched


def test_redact_community_config_preserves_env_var_name():
    cfg = {"auth": {"psk_env_var": "DH_PSK"}}
    out = redact_community_config(cfg)
    assert out["auth"]["psk_env_var"] == "DH_PSK"
