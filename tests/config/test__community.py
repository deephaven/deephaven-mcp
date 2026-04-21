"""Tests for deephaven_mcp.config._community — community config logic and CommunityServerConfigManager."""

import json
import logging
import os
import re
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import aiofiles
import pytest

import deephaven_mcp.config._community as _community_module
from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    ConfigurationError,
)
from deephaven_mcp.config import CONFIG_ENV_VAR, ConfigManager, CommunityServerConfigManager
from deephaven_mcp.config._community import (
    _get_all_config_names,
    _get_config_section,
    _validate_community_config,
    redact_community_session_config,
    redact_community_session_creation_config,
    validate_community_session_creation_config,
    validate_community_sessions_config,
    validate_security_config,
    validate_single_community_session_config,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_env():
    old = os.environ.get(CONFIG_ENV_VAR)
    if CONFIG_ENV_VAR in os.environ:
        del os.environ[CONFIG_ENV_VAR]
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


# ---------------------------------------------------------------------------
# Redaction Tests
# ---------------------------------------------------------------------------


def test_redact_community_session_config():
    original_config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "token",
        "auth_token": "sensitive_token_value",
        "auth_token_env_var": "MY_ENV_VAR_NAME",
        "session_type": "python",
        "tls_root_certs": b"binaryca",
        "client_cert_chain": b"binarychain",
        "client_private_key": b"binarykey",
    }
    redacted = redact_community_session_config(original_config)

    assert redacted["host"] == "localhost"
    assert redacted["port"] == 10000
    assert redacted["auth_type"] == "token"
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["auth_token_env_var"] == "MY_ENV_VAR_NAME"
    assert redacted["session_type"] == "python"
    assert redacted["tls_root_certs"] == "[REDACTED]"
    assert redacted["client_cert_chain"] == "[REDACTED]"
    assert redacted["client_private_key"] == "[REDACTED]"
    assert "sensitive_token_value" not in redacted.values()
    assert b"binaryca" not in redacted.values()
    assert b"binarychain" not in redacted.values()
    assert b"binarykey" not in redacted.values()
    assert original_config["auth_token"] == "sensitive_token_value"
    assert original_config["tls_root_certs"] == b"binaryca"
    assert original_config["client_cert_chain"] == b"binarychain"
    assert original_config["client_private_key"] == b"binarykey"


def test_redact_community_session_config_comprehensive():
    config = {
        "auth_token": "secret",
        "tls_root_certs": b"bytes",
        "client_cert_chain": b"chain-bytes",
        "client_private_key": b"key-bytes",
        "foo": "bar",
    }
    redacted = redact_community_session_config(config)
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["tls_root_certs"] == "[REDACTED]"
    assert redacted["client_cert_chain"] == "[REDACTED]"
    assert redacted["client_private_key"] == "[REDACTED]"
    assert redacted["foo"] == "bar"
    config = {"auth_token": "tok", "tls_root_certs": b"binary"}
    redacted = redact_community_session_config(config, redact_binary_values=False)
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["tls_root_certs"] == b"binary"


def test_redact_community_session_config_empty():
    cfg = {"foo": "bar"}
    assert redact_community_session_config(cfg) == cfg


def test_redact_community_session_config_edge_cases():
    assert redact_community_session_config({}) == {}
    cfg = {"auth_token": 123, "tls_root_certs": None, "foo": b"bar"}
    result = redact_community_session_config(cfg)
    assert result["auth_token"] == "[REDACTED]"
    assert result["tls_root_certs"] is None
    assert result["foo"] == b"bar"

    original_config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "token",
        "auth_token": "sensitive_token_value",
        "auth_token_env_var": "MY_ENV_VAR_NAME",
        "session_type": "python",
        "tls_root_certs": b"binaryca",
        "client_cert_chain": b"binarychain",
        "client_private_key": b"binarykey",
    }
    redacted = redact_community_session_config(original_config)

    assert redacted["host"] == "localhost"
    assert redacted["port"] == 10000
    assert redacted["auth_type"] == "token"
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["auth_token_env_var"] == "MY_ENV_VAR_NAME"
    assert redacted["session_type"] == "python"
    assert redacted["tls_root_certs"] == "[REDACTED]"
    assert redacted["client_cert_chain"] == "[REDACTED]"
    assert redacted["client_private_key"] == "[REDACTED]"
    assert "sensitive_token_value" not in redacted.values()
    assert b"binaryca" not in redacted.values()
    assert b"binarychain" not in redacted.values()
    assert b"binarykey" not in redacted.values()
    assert original_config["auth_token"] == "sensitive_token_value"
    assert original_config["tls_root_certs"] == b"binaryca"
    assert original_config["client_cert_chain"] == b"binarychain"
    assert original_config["client_private_key"] == b"binarykey"


def test_redact_community_session_config_no_token():
    original_config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "none",
    }
    redacted = redact_community_session_config(original_config)
    assert redacted == original_config
    assert "auth_token" not in redacted


# ---------------------------------------------------------------------------
# validate_single_community_session_config Tests
# ---------------------------------------------------------------------------


def test_validate_single_cs_valid_minimal():
    config_item = {}
    try:
        validate_single_community_session_config("test_min_session", config_item)
    except ValueError as e:
        pytest.fail(f"Minimal valid config raised ValueError: {e}")


def test_validate_single_cs_valid_full():
    config_item = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "token",
        "auth_token": "tokenval",
        "never_timeout": True,
        "session_type": "python",
        "use_tls": False,
        "tls_root_certs": None,
        "client_cert_chain": None,
        "client_private_key": None,
    }
    try:
        validate_single_community_session_config("test_full_session", config_item)
    except ValueError as e:
        pytest.fail(f"Full valid config raised ValueError: {e}")


def test_validate_single_cs_config_not_dict():
    with pytest.raises(
        CommunitySessionConfigurationError,
        match=r"Community session config for session_not_dict must be a dictionary, got <class 'str'>",
    ):
        validate_single_community_session_config("session_not_dict", "not_a_dict")


def test_validate_single_cs_unknown_field_foo():
    bad_config = {"host": "localhost", "foo": 1}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Unknown field 'foo' in community session config for local_session",
    ):
        validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_field_wrong_type_host():
    bad_config = {"host": 123}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'host' in community session config for local_session must be of type str, got int",
    ):
        validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_field_wrong_type_port():
    bad_config = {"port": "10000"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'port' in community session config for local_session must be of type int, got str",
    ):
        validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_wrong_type_in_tuple_tls_root_certs():
    bad_config = {"host": "localhost", "tls_root_certs": 123}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match=r"Field 'tls_root_certs' in community session config for test_session must be one of types \(str, NoneType\), got int",
    ):
        validate_single_community_session_config("test_session", bad_config)


def test_validate_single_cs_missing_required_field():
    with patch("deephaven_mcp.config._community._REQUIRED_FIELDS", ["host"]):
        bad_config = {"port": 10000}
        with pytest.raises(
            CommunitySessionConfigurationError,
            match="Missing required field 'host' in community session config for local_session",
        ):
            validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_auth_token_and_env_var_exclusive():
    config_item = {
        "auth_token": "some_token",
        "auth_token_env_var": "SOME_ENV_VAR",
    }
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="In community session config for 'test_both_auth', both 'auth_token' and 'auth_token_env_var' are set. Please use only one.",
    ):
        validate_single_community_session_config("test_both_auth", config_item)


def test_validate_single_cs_valid_with_auth_token_env_var():
    config_item = {
        "auth_token_env_var": "MY_TOKEN_VAR",
    }
    try:
        validate_single_community_session_config("test_env_var_only", config_item)
    except CommunitySessionConfigurationError as e:
        pytest.fail(f"Valid config with auth_token_env_var raised error: {e}")


# ---------------------------------------------------------------------------
# validate_community_sessions_config Tests
# ---------------------------------------------------------------------------


def test_validate_community_sessions_valid():
    valid_sessions_map = {
        "session1": {"host": "localhost"},
        "session2": {"host": "remote", "port": 9999},
    }
    try:
        validate_community_sessions_config(valid_sessions_map)
    except ValueError as e:
        pytest.fail(f"Valid community_sessions_map raised ValueError: {e}")


def test_validate_community_sessions_not_dict():
    try:
        validate_community_sessions_config(None)
    except ValueError as e:
        pytest.fail(f"validate_community_sessions_config(None) raised ValueError: {e}")

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'sessions' must be a dictionary in Deephaven community session config",
    ):
        validate_community_sessions_config("not_a_dict")

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'sessions' must be a dictionary in Deephaven community session config",
    ):
        validate_community_sessions_config([{"session1": {}}])


def test_validate_community_sessions_empty_dict():
    validate_community_sessions_config({})


@patch("deephaven_mcp.config._community.validate_single_community_session_config")
def test_validate_community_sessions_invalid_item(mock_validate_single):
    sessions_map_with_invalid = {
        "valid_session": {"host": "localhost"},
        "invalid_session": {"port": "not_an_int"},
    }

    def side_effect_func(name, cfg):
        if name == "invalid_session":
            raise ValueError("Mocked single validation error for invalid_session")

    mock_validate_single.side_effect = side_effect_func

    with pytest.raises(
        ValueError, match="Mocked single validation error for invalid_session"
    ):
        validate_community_sessions_config(sessions_map_with_invalid)

    assert mock_validate_single.call_count == 2
    mock_validate_single.assert_any_call("valid_session", {"host": "localhost"})
    mock_validate_single.assert_any_call("invalid_session", {"port": "not_an_int"})


def test_validate_single_cs_auth_type_known_values_no_warning(caplog):
    with caplog.at_level(logging.WARNING):
        validate_single_community_session_config(
            "test_session", {"auth_type": "Anonymous"}
        )
        validate_single_community_session_config("test_session", {"auth_type": "Basic"})

    assert len(caplog.records) == 0


def test_validate_single_cs_auth_type_unknown_value_warning(caplog):
    with caplog.at_level(logging.WARNING):
        validate_single_community_session_config(
            "test_session", {"auth_type": "anonymous"}
        )

    assert len(caplog.records) == 1
    warning_message = caplog.records[0].message
    assert "auth_type='anonymous'" in warning_message
    assert "not a commonly known value" in warning_message
    assert "Anonymous, Basic" in warning_message
    assert "Custom authenticators are also valid" in warning_message


def test_validate_single_cs_auth_type_custom_authenticator_warning(caplog):
    with caplog.at_level(logging.WARNING):
        validate_single_community_session_config(
            "test_session", {"auth_type": "com.example.custom.AuthenticationHandler"}
        )

    assert len(caplog.records) == 1
    warning_message = caplog.records[0].message
    assert "com.example.custom.AuthenticationHandler" in warning_message
    assert "not a commonly known value" in warning_message
    assert "Custom authenticators are also valid" in warning_message


# ---------------------------------------------------------------------------
# Session Creation Configuration Tests
# ---------------------------------------------------------------------------


def test_session_creation_none_config_is_valid():
    validate_community_session_creation_config(None)


def test_session_creation_empty_dict_is_valid():
    validate_community_session_creation_config({})


def test_session_creation_valid_minimal_config():
    config = {"max_concurrent_sessions": 5}
    validate_community_session_creation_config(config)


def test_session_creation_valid_full_config_docker():
    config = {
        "max_concurrent_sessions": 10,
        "defaults": {
            "launch_method": "docker",
            "auth_type": "PSK",
            "auth_token": "test-token",
            "docker_image": "ghcr.io/deephaven/server:latest",
            "docker_memory_limit_gb": 8.0,
            "docker_cpu_limit": 2.0,
            "docker_volumes": ["/host:/container:ro"],
            "heap_size_gb": 4,
            "extra_jvm_args": ["-XX:+UseG1GC"],
            "environment_vars": {"KEY": "value"},
            "startup_timeout_seconds": 60,
            "startup_check_interval_seconds": 2,
            "startup_retries": 3,
        },
    }
    validate_community_session_creation_config(config)


def test_session_creation_valid_full_config_pip():
    config = {
        "max_concurrent_sessions": 3,
        "defaults": {
            "launch_method": "python",
            "auth_type": "Anonymous",
            "heap_size_gb": 2,
            "startup_timeout_seconds": 30,
        },
    }
    validate_community_session_creation_config(config)


def test_session_creation_invalid_not_dict():
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'session_creation' must be a dictionary in community config",
    ):
        validate_community_session_creation_config("not a dict")


def test_session_creation_invalid_max_concurrent_sessions_negative():
    config = {"max_concurrent_sessions": -1}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'max_concurrent_sessions' must be non-negative",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_max_concurrent_sessions_not_int():
    config = {"max_concurrent_sessions": "five"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'max_concurrent_sessions' in session_creation config must be of type int",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_defaults_not_dict():
    config = {"defaults": "not a dict"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'defaults' in session_creation config must be of type dict",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_launch_method():
    config = {"defaults": {"launch_method": "invalid"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'launch_method' must be one of",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_auth_type_generates_warning(caplog):
    config = {"defaults": {"auth_type": "InvalidAuth"}}
    validate_community_session_creation_config(config)
    assert "auth_type='InvalidAuth' which is not a commonly known value" in caplog.text


def test_session_creation_programming_language_accepted():
    config = {"defaults": {"programming_language": "Python"}}
    validate_community_session_creation_config(config)


def test_session_creation_auth_token_and_env_var_mutually_exclusive():
    config = {
        "defaults": {
            "auth_token": "token",
            "auth_token_env_var": "ENV_VAR",
        }
    }
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="both 'auth_token' and 'auth_token_env_var' are set",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_heap_size_negative():
    config = {"defaults": {"heap_size_gb": -1}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'heap_size_gb' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_docker_memory_limit_negative():
    config = {"defaults": {"docker_memory_limit_gb": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'docker_memory_limit_gb' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_docker_cpu_limit_negative():
    config = {"defaults": {"docker_cpu_limit": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'docker_cpu_limit' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_startup_timeout_negative():
    config = {"defaults": {"startup_timeout_seconds": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'startup_timeout_seconds' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_startup_check_interval_negative():
    config = {"defaults": {"startup_check_interval_seconds": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'startup_check_interval_seconds' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_startup_retries_negative():
    config = {"defaults": {"startup_retries": -1}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'startup_retries' must be non-negative",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_docker_volumes_not_list():
    config = {"defaults": {"docker_volumes": "not a list"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'docker_volumes' in session_creation.defaults must be of type list",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_extra_jvm_args_not_list():
    config = {"defaults": {"extra_jvm_args": "not a list"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'extra_jvm_args' in session_creation.defaults must be of type list",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_environment_vars_not_dict():
    config = {"defaults": {"environment_vars": "not a dict"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'environment_vars' in session_creation.defaults must be of type dict",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_unknown_field_raises_error():
    config = {"defaults": {"unknown_field": "value"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Unknown field 'unknown_field' in session_creation.defaults config",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_unknown_field_in_session_creation():
    config = {"unknown_field": "value"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Unknown field 'unknown_field' in session_creation config",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_field_wrong_type_tuple_allowed():
    config = {
        "defaults": {
            "docker_memory_limit_gb": "not_a_number",
        }
    }
    with pytest.raises(
        CommunitySessionConfigurationError, match="must be one of types"
    ):
        validate_community_session_creation_config(config)


def test_session_creation_docker_volumes_item_not_string():
    config = {
        "defaults": {
            "docker_volumes": [
                "/valid/path",
                123,
                "/another/path",
            ],
        }
    }
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'docker_volumes\\[1\\]' must be a string",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_extra_jvm_args_item_not_string():
    config = {
        "defaults": {
            "extra_jvm_args": ["-XX:+UseG1GC", 123, "-Xms1g"],
        }
    }
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'extra_jvm_args\\[1\\]' must be a string",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_environment_vars_key_not_string():
    config = {
        "defaults": {
            "environment_vars": {123: "value"},
        }
    }
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'environment_vars' key must be a string",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_environment_vars_value_not_string():
    config = {
        "defaults": {
            "environment_vars": {"KEY": 123},
        }
    }
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'environment_vars\\[KEY\\]' value must be a string",
    ):
        validate_community_session_creation_config(config)


# ---------------------------------------------------------------------------
# Session Creation Redaction Tests
# ---------------------------------------------------------------------------


def test_session_creation_redact_empty_config_returns_empty():
    assert redact_community_session_creation_config({}) == {}


def test_session_creation_redact_redacts_auth_token():
    config = {"defaults": {"auth_token": "secret-token"}}
    redacted = redact_community_session_creation_config(config)
    assert redacted["defaults"]["auth_token"] == "[REDACTED]"


def test_session_creation_redact_preserves_non_sensitive_fields():
    config = {
        "max_concurrent_sessions": 5,
        "defaults": {
            "launch_method": "docker",
            "auth_type": "PSK",
            "heap_size_gb": 4.0,
        },
    }
    redacted = redact_community_session_creation_config(config)
    assert redacted["max_concurrent_sessions"] == 5
    assert redacted["defaults"]["launch_method"] == "docker"
    assert redacted["defaults"]["auth_type"] == "PSK"
    assert redacted["defaults"]["heap_size_gb"] == 4.0


def test_session_creation_redact_does_not_redact_auth_token_env_var():
    config = {"defaults": {"auth_token_env_var": "MY_TOKEN"}}
    redacted = redact_community_session_creation_config(config)
    assert redacted["defaults"]["auth_token_env_var"] == "MY_TOKEN"


def test_session_creation_redact_does_not_modify_original():
    config = {"defaults": {"auth_token": "secret"}}
    original_token = config["defaults"]["auth_token"]
    redact_community_session_creation_config(config)
    assert config["defaults"]["auth_token"] == original_token


# ---------------------------------------------------------------------------
# validate_security_config Tests
# ---------------------------------------------------------------------------


def test_validate_security_config_none():
    validate_security_config(None)


def test_validate_security_config_empty_dict():
    validate_security_config({})


def test_validate_security_config_mode_none():
    config = {"credential_retrieval_mode": "none"}
    validate_security_config(config)


def test_validate_security_config_mode_dynamic_only():
    config = {"credential_retrieval_mode": "dynamic_only"}
    validate_security_config(config)


def test_validate_security_config_mode_static_only():
    config = {"credential_retrieval_mode": "static_only"}
    validate_security_config(config)


def test_validate_security_config_mode_all():
    config = {"credential_retrieval_mode": "all"}
    validate_security_config(config)


def test_validate_security_config_not_dict():
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'security' must be a dictionary",
    ):
        validate_security_config("not_a_dict")


def test_validate_security_config_mode_not_string():
    config = {"credential_retrieval_mode": True}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'security.credential_retrieval_mode' must be a string, got bool",
    ):
        validate_security_config(config)


def test_validate_security_config_invalid_mode():
    config = {"credential_retrieval_mode": "invalid_mode"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'security.credential_retrieval_mode' must be one of",
    ):
        validate_security_config(config)


def test_validate_security_config_with_other_fields():
    config = {
        "credential_retrieval_mode": "dynamic_only",
        "future_security_field": "value",
    }
    validate_security_config(config)


# ---------------------------------------------------------------------------
# _validate_community_config Tests
# ---------------------------------------------------------------------------


def test_validate_community_config_accepts_empty():
    assert _validate_community_config({}) == {}


def test_validate_community_config_accepts_sessions_only():
    config = {
        "sessions": {
            "local": {
                "host": "localhost",
                "port": 10000,
                "auth_type": "token",
                "auth_token": "tokenval",
                "never_timeout": True,
                "session_type": "single",
                "use_tls": False,
            }
        }
    }
    assert _validate_community_config(config) == config


def test_validate_community_config_rejects_enterprise_key():
    with pytest.raises(ConfigurationError):
        _validate_community_config({"enterprise": {"systems": {}}})


def test_validate_community_config_rejects_unknown_top_level():
    with pytest.raises(ConfigurationError):
        _validate_community_config({"foo": {}})


def test_validate_community_config_non_dict():
    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        _validate_community_config("not a dict")

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        _validate_community_config(123)

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        _validate_community_config(["list", "not", "dict"])


def test_validate_community_config_raises_on_wrong_type_for_schema_key():
    with pytest.raises(ConfigurationError, match="must be of type dict"):
        _validate_community_config({"sessions": "not_a_dict"})


def test_validate_community_config_wraps_session_error_as_config_error():
    with pytest.raises(ConfigurationError):
        _validate_community_config({"sessions": {"x": {"host": 1}}})


# ---------------------------------------------------------------------------
# _get_config_section / _get_all_config_names Tests
# ---------------------------------------------------------------------------


def test_get_config_section_invalid_section():
    cfg = {"sessions": {}}
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['not_a_section', 'foo'] does not exist in configuration"
        ),
    ):
        _get_config_section(cfg, ["not_a_section", "foo"])


def test_get_all_config_names_returns_keys():
    cfg = {
        "sessions": {"a": {"host": "localhost"}, "b": {"host": "localhost"}}
    }
    names = _get_all_config_names(cfg, ["sessions"])
    assert set(names) == {"a", "b"}

    cfg2 = {"sessions": {}}
    names2 = _get_all_config_names(cfg2, ["sessions"])
    assert names2 == []

    cfg3 = {"sessions": {}}
    names3 = _get_all_config_names(cfg3, ["enterprise", "systems"])
    assert names3 == []


def test_get_all_config_names_not_dict_raises():
    cfg = {"community_sessions": "not_a_dict"}
    result = _get_all_config_names(cfg, ["community_sessions"])
    assert result == []


def test_named_config_missing():
    cfg = {"sessions": {"foo": {"host": "localhost"}}}
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['sessions', 'bar'] does not exist in configuration"
        ),
    ):
        _get_config_section(cfg, ["sessions", "bar"])


def test_get_all_config_names_returns_empty_for_non_dict_section(caplog):
    cfg = {"not_a_section": "not_a_dict"}
    caplog.set_level("WARNING", logger="deephaven_mcp.config._community")
    result = _get_all_config_names(cfg, ["not_a_section"])
    assert result == []
    assert (
        "Section at path ['not_a_section'] is not a dictionary, returning empty list of names."
        in caplog.text
    )


# ---------------------------------------------------------------------------
# CommunityServerConfigManager Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_config_other_os_error_on_read(monkeypatch, caplog):
    config_file_path = "/fake/path/config_for_os_error_read.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)

    mock_file_read = mock.AsyncMock(side_effect=os.error("Simulated OS error on read"))
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read

    with patch("aiofiles.open", mock.MagicMock(return_value=mock_async_context_manager)):
        cm = CommunityServerConfigManager()
        with pytest.raises(
            ConfigurationError,
            match=rf"Unexpected error loading or parsing config file {re.escape(config_file_path)}: Simulated OS error on read",
        ):
            await cm.get_config()

    assert (
        f"Unexpected error loading or parsing config file {config_file_path}: Simulated OS error on read"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_validate_community_config_missing_required_key_bytes_mode(caplog, monkeypatch):
    with patch.object(
        _community_module,
        "_SCHEMA_PATHS",
        {
            **_community_module._SCHEMA_PATHS,
            ("must_have_this",): _community_module._ConfigPathSpec(
                required=True, expected_type=dict, validator=None
            ),
        },
    ):
        cm = CommunityServerConfigManager()
        invalid_config_data = {}  # Missing 'must_have_this'

        config_file_path = "/fake/path/config_missing_req.json"
        monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)

        mock_file_read_content = mock.AsyncMock(
            return_value=json.dumps(invalid_config_data).encode("utf-8")
        )
        mock_async_context_manager_req = mock.AsyncMock()
        mock_async_context_manager_req.__aenter__.return_value.read = (
            mock_file_read_content
        )

        with patch("aiofiles.open", mock.MagicMock(return_value=mock_async_context_manager_req)):
            with pytest.raises(
                ConfigurationError,
                match=re.escape(
                    "Error loading configuration file: Missing required keys at config path (): {'must_have_this'}"
                ),
            ):
                await cm.get_config()

        assert (
            "Missing required keys at config path (): {'must_have_this'}" in caplog.text
        )


@pytest.mark.asyncio
async def test_get_config_uses_cache_and_logs(monkeypatch, caplog):
    config_file_path = "/fake/path/config_for_cache_test.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)
    valid_config_data = {"sessions": {"test_session": {"host": "localhost"}}}

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(valid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    aiofiles_open_mock = mock.MagicMock(return_value=mock_async_context_manager)

    with patch("aiofiles.open", aiofiles_open_mock):
        cm = CommunityServerConfigManager()
        config1 = await cm.get_config()
        assert valid_config_data == config1
        assert (
            "Returning cached Deephaven MCP application configuration." not in caplog.text
        )
        aiofiles_open_mock.assert_called_once_with(config_file_path)

        caplog.clear()
        caplog.set_level(logging.DEBUG, logger="deephaven_mcp.config")
        config2 = await cm.get_config()
        assert config1 is config2
        assert "Using cached Deephaven MCP application configuration." in caplog.text
        aiofiles_open_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_config_unknown_top_level_key(monkeypatch, caplog):
    config_file_path = "/fake/path/config_unknown_key.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {"some_unknown_key": {}}

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    with patch("aiofiles.open", mock.MagicMock(return_value=mock_async_context_manager)):
        cm = CommunityServerConfigManager()
        with pytest.raises(
            ConfigurationError,
            match=re.escape(
                r"Error loading configuration file: Unknown keys at config path (): {'some_unknown_key'}"
            ),
        ):
            await cm.get_config()

    assert r"Unknown keys at config path (): {'some_unknown_key'}" in caplog.text


@pytest.mark.asyncio
async def test_get_config_invalid_community_session_schema_from_file(monkeypatch, caplog):
    config_file_path = "/fake/path/invalid_community_schema.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {
        "sessions": {
            "bad_session": {
                "host": 12345,  # Invalid type, should be string
                "port": "not-a-port",
            }
        }
    }

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    with patch("aiofiles.open", mock.MagicMock(return_value=mock_async_context_manager)):
        cm = CommunityServerConfigManager()
        expected_error_pattern = re.escape(
            "Error loading configuration file: Invalid configuration for sessions: Field 'host' in community session config for bad_session must be of type str, got int"
        )
        with pytest.raises(ConfigurationError, match=expected_error_pattern):
            await cm.get_config()


@pytest.mark.asyncio
async def test_config_manager_set_and_clear_cache():
    cm = CommunityServerConfigManager()
    await cm._set_config_cache({"sessions": {"a_session": {}}})
    cfg1 = await cm.get_config()
    assert "a_session" in cfg1["sessions"]
    await cm.clear_config_cache()
    await cm._set_config_cache({"sessions": {"b_session": {}}})
    cfg2 = await cm.get_config()
    assert "b_session" in cfg2["sessions"]
    assert "a_session" not in cfg2["sessions"]


@pytest.mark.asyncio
async def test_get_config_missing_env(monkeypatch):
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(
        RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"
    ):
        await CommunityServerConfigManager().get_config()


@pytest.mark.asyncio
async def test_validate_community_config_missing_required_key_runtime(monkeypatch, caplog):
    config_file_path = "/fake/path/missing_required_key.json"
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", config_file_path)
    config_data = {}  # Missing 'must_have_this'
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(
        return_value=json.dumps(config_data)
    )

    with (
        patch.object(
            _community_module,
            "_SCHEMA_PATHS",
            {
                **_community_module._SCHEMA_PATHS,
                ("must_have_this",): _community_module._ConfigPathSpec(
                    required=True, expected_type=dict, validator=None
                ),
            },
        ),
        patch("aiofiles.open", mock.Mock(return_value=aiofiles_open_ctx)),
    ):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        expected_error = re.escape(
            "Error loading configuration file: Missing required keys at config path (): {'must_have_this'}"
        )
        with pytest.raises(ConfigurationError, match=expected_error):
            await cm.get_config()


@pytest.mark.asyncio
async def test_get_config_no_community_sessions_key_from_file(monkeypatch, caplog):
    empty_config_json = "{}"

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/empty_config.json")
    mock_aiofiles_open = mock.MagicMock()
    mock_async_context_manager = mock.AsyncMock()
    mock_file_object = mock.AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = empty_config_json

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        with caplog.at_level("INFO"):
            cfg = await cm.get_config()
    assert cfg == {}
    assert cm._cache == {}
    log_text = caplog.text
    assert "Configuration validation passed." in log_text
    assert "Configuration summary:" in log_text
    assert "Loaded configuration:\n{}" in log_text
    session_names = _get_all_config_names(cfg, ["sessions"])
    assert session_names == []

    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['sessions', 'any_session_name'] does not exist in configuration"
        ),
    ):
        _get_config_section(cfg, ["sessions", "any_session_name"])


@pytest.mark.asyncio
async def test_config_file_not_found_error(monkeypatch):
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/nonexistent/path/config.json")

    cm = CommunityServerConfigManager()
    await cm.clear_config_cache()

    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        await cm.get_config()


@pytest.mark.asyncio
async def test_config_permission_error(monkeypatch):
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    with patch("aiofiles.open", side_effect=PermissionError("Permission denied")):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(
            ConfigurationError,
            match="Permission denied when trying to read configuration file",
        ):
            await cm.get_config()


@pytest.mark.asyncio
async def test_config_invalid_json_error(monkeypatch):
    invalid_json = "{ invalid json content"

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = invalid_json

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(
            ConfigurationError, match="Invalid JSON/JSON5 in configuration file"
        ):
            await cm.get_config()


@pytest.mark.asyncio
async def test_json_formatting_error_in_log_config_summary(monkeypatch, caplog):
    valid_config_json = '{"sessions": {}}'

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = valid_config_json

    def mock_json5_dumps(*args, **kwargs):
        raise TypeError("Object is not JSON serializable")

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("json5.dumps", side_effect=mock_json5_dumps):
            cm = CommunityServerConfigManager()
            await cm.clear_config_cache()
            with caplog.at_level("WARNING"):
                await cm.get_config()

    assert "Failed to format config as JSON" in caplog.text


@pytest.mark.asyncio
async def test_config_validation_error_in_load_and_validate(monkeypatch):
    invalid_config_json = '{"unknown_top_level_key": "invalid"}'

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = invalid_config_json

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(ConfigurationError, match="Error loading configuration file"):
            await cm.get_config()


@pytest.mark.asyncio
async def test_json5_with_single_line_comments(monkeypatch):
    json5_content = """{
        // Sessions section
        "sessions": {
            // Local development session
            "local": {
                "host": "localhost",  // Server hostname
                "port": 10000         // Server port
            }
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        result = await cm.get_config()

    assert "sessions" in result
    assert "local" in result["sessions"]
    assert result["sessions"]["local"]["host"] == "localhost"
    assert result["sessions"]["local"]["port"] == 10000


@pytest.mark.asyncio
async def test_json5_with_multi_line_comments(monkeypatch):
    json5_content = """{
        /*
         * Sessions configuration section
         */
        "sessions": {
            /* Production session configuration */
            "prod": {
                "host": "prod.example.com",
                "port": 10000
            }
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        result = await cm.get_config()

    assert "sessions" in result
    assert "prod" in result["sessions"]
    assert result["sessions"]["prod"]["host"] == "prod.example.com"


@pytest.mark.asyncio
async def test_json5_with_mixed_comments(monkeypatch):
    json5_content = """{
        // Session definitions
        "sessions": {
            "local": {
                "host": "localhost",
                "port": 10000  // Default port
            }
        },
        /*
         * Session creation defaults
         */
        "session_creation": {
            // Maximum concurrent sessions
            "max_concurrent_sessions": 5
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        result = await cm.get_config()

    assert "sessions" in result
    assert result["sessions"]["local"]["port"] == 10000
    assert result["session_creation"]["max_concurrent_sessions"] == 5


@pytest.mark.asyncio
async def test_json5_standard_json_still_works(monkeypatch):
    json_content = """{
        "sessions": {
            "local": {
                "host": "localhost",
                "port": 10000
            }
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        result = await cm.get_config()

    assert "sessions" in result
    assert result["sessions"]["local"]["host"] == "localhost"
    assert result["sessions"]["local"]["port"] == 10000


@pytest.mark.asyncio
async def test_json5_invalid_syntax_raises_error(monkeypatch):
    invalid_json5 = """{
        "sessions": {
            "local": {
                "host": "localhost"
            }
        // Missing closing bracket
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = invalid_json5

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(ConfigurationError, match="Invalid JSON/JSON5"):
            await cm.get_config()


@pytest.mark.asyncio
async def test_json5_trailing_commas_support(monkeypatch):
    json5_content = """{
        "sessions": {
            "local": {
                "host": "localhost",
                "port": 10000,
            },
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        result = await cm.get_config()

    assert "sessions" in result
    assert result["sessions"]["local"]["host"] == "localhost"


class TestCommunityServerConfigManager:
    """Integration tests for CommunityServerConfigManager."""

    @pytest.mark.asyncio
    async def test_get_config_returns_community_config_unchanged(self):
        manager = CommunityServerConfigManager(config_path="/fake/dhc.json")

        with patch(
            "aiofiles.open",
            mock.MagicMock(
                return_value=mock.MagicMock(
                    __aenter__=mock.AsyncMock(
                        return_value=mock.MagicMock(
                            read=mock.AsyncMock(
                                return_value='{"sessions": {"local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "secret"}}}'
                            )
                        )
                    ),
                    __aexit__=mock.AsyncMock(return_value=None),
                )
            ),
        ):
            result = await manager.get_config()

        assert result["sessions"]["local"]["host"] == "localhost"

    def test_is_distinct_type_from_enterprise_manager(self):
        from deephaven_mcp.config import EnterpriseServerConfigManager

        assert CommunityServerConfigManager is not EnterpriseServerConfigManager
        assert issubclass(CommunityServerConfigManager, ConfigManager)
        assert issubclass(EnterpriseServerConfigManager, ConfigManager)


# ---------------------------------------------------------------------------
# Nested-schema infrastructure coverage
#
# The current production schema (_SCHEMA_PATHS) is entirely flat — every key
# has length 1 — so the code paths that handle nested schemas are unreachable
# under normal operation. These tests patch in a synthetic nested schema to
# exercise that infrastructure and keep it from silently rotting.
# ---------------------------------------------------------------------------


def test_apply_redaction_to_config_nested_path_descends_into_parent():
    """Cover the inner-loop body of _apply_redaction_to_config.

    For a nested schema path like ``("outer", "inner")`` the loop
    ``for key in path_tuple[:-1]`` must descend into the parent dict before
    writing the redacted section at the leaf. With the current flat schema
    this line is never executed.
    """
    nested_redactor = mock.MagicMock(return_value={"redacted": True})
    synthetic_schema = {
        ("outer", "inner"): _community_module._ConfigPathSpec(
            required=False,
            expected_type=dict,
            redactor=nested_redactor,
        ),
    }
    config = {"outer": {"inner": {"secret": "abc", "other": 1}}}

    with patch.object(_community_module, "_SCHEMA_PATHS", synthetic_schema):
        result = _community_module._apply_redaction_to_config(config)

    # Redactor was called with the nested section only.
    nested_redactor.assert_called_once_with({"secret": "abc", "other": 1})
    # Redacted value was written back at the correct nested location.
    assert result == {"outer": {"inner": {"redacted": True}}}
    # Original dict is untouched (deepcopy).
    assert config == {"outer": {"inner": {"secret": "abc", "other": 1}}}


def test_validate_section_recurses_into_nested_dict():
    """Cover the recursive call inside _validate_section.

    _should_recurse_into_nested_dict returns True only when _SCHEMA_PATHS
    contains a child of the current path. With the flat production schema
    it always returns False, so the recursive call at ``_validate_section(
    value, current_path)`` is never executed. Patch in a two-level schema
    and verify recursion runs (and correctly validates the nested value).
    """
    synthetic_schema = {
        ("parent",): _community_module._ConfigPathSpec(
            required=False, expected_type=dict
        ),
        ("parent", "child"): _community_module._ConfigPathSpec(
            required=False, expected_type=str
        ),
    }

    with patch.object(_community_module, "_SCHEMA_PATHS", synthetic_schema):
        # Valid nested config — recursion should validate ``child`` as a str.
        _community_module._validate_community_config({"parent": {"child": "ok"}})

        # Recursion must actually enforce the nested spec: wrong type at
        # ``parent.child`` is caught during the recursive call, proving line
        # ``_validate_section(value, current_path)`` executed.
        with pytest.raises(
            ConfigurationError,
            match=re.escape(
                "Config path ('parent', 'child') must be of type str, got int"
            ),
        ):
            _community_module._validate_community_config(
                {"parent": {"child": 123}}
            )
