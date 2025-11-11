"""Tests for Deephaven MCP community session specific configuration logic."""

import logging
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import CommunitySessionConfigurationError
from deephaven_mcp.config._community_session import (
    redact_community_session_config,
    redact_community_session_creation_config,
    validate_community_session_creation_config,
    validate_community_sessions_config,
    validate_security_community_config,
    validate_single_community_session_config,
)

# --- Redaction Tests ---


def test_redact_community_session_config():
    """Test that all sensitive fields are redacted according to unified logic."""
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
    # Ensure original is not modified
    assert original_config["auth_token"] == "sensitive_token_value"
    assert original_config["tls_root_certs"] == b"binaryca"
    assert original_config["client_cert_chain"] == b"binarychain"
    assert original_config["client_private_key"] == b"binarykey"


def test_redact_community_session_config_comprehensive():
    # All sensitive keys, string and binary
    config = {
        "auth_token": "secret",
        "tls_root_certs": b"bytes",
        "client_cert_chain": b"chain-bytes",
        "client_private_key": b"key-bytes",
        "foo": "bar",
    }
    # Default: redact all sensitive fields
    redacted = redact_community_session_config(config)
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["tls_root_certs"] == "[REDACTED]"
    assert redacted["client_cert_chain"] == "[REDACTED]"
    assert redacted["client_private_key"] == "[REDACTED]"
    assert redacted["foo"] == "bar"
    # redact_binary_values=False: only auth_token is redacted, binary fields are not
    config = {"auth_token": "tok", "tls_root_certs": b"binary"}
    redacted = redact_community_session_config(config, redact_binary_values=False)
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["tls_root_certs"] == b"binary"


def test_redact_community_session_config_empty():
    # No sensitive values
    cfg = {"foo": "bar"}
    assert redact_community_session_config(cfg) == cfg


def test_redact_community_session_config_edge_cases():
    # Empty config
    assert redact_community_session_config({}) == {}
    # Unexpected types
    cfg = {"auth_token": 123, "tls_root_certs": None, "foo": b"bar"}
    result = redact_community_session_config(cfg)
    assert result["auth_token"] == "[REDACTED]"
    assert result["tls_root_certs"] is None
    assert result["foo"] == b"bar"

    """Test that all sensitive fields are redacted according to unified logic."""
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
    # Ensure original is not modified
    assert original_config["auth_token"] == "sensitive_token_value"
    assert original_config["tls_root_certs"] == b"binaryca"
    assert original_config["client_cert_chain"] == b"binarychain"
    assert original_config["client_private_key"] == b"binarykey"


def test_redact_community_session_config_no_token():
    """Test redaction when auth_token is not present."""
    original_config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "none",
    }
    redacted = redact_community_session_config(original_config)
    assert redacted == original_config  # Should be identical if no token
    assert "auth_token" not in redacted


# --- Validation Tests for individual community session configs ---


def test_validate_single_cs_valid_minimal():
    """Test a valid, minimal community session config passes."""
    config_item = {}  # Assuming _REQUIRED_FIELDS is empty by default
    try:
        validate_single_community_session_config("test_min_session", config_item)
    except ValueError as e:
        pytest.fail(f"Minimal valid config raised ValueError: {e}")


def test_validate_single_cs_valid_full():
    """Test a valid, full community session config passes."""
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
    """Test validation fails if the session config item itself is not a dictionary."""
    with pytest.raises(
        CommunitySessionConfigurationError,
        match=r"Community session config for session_not_dict must be a dictionary, got <class 'str'>",
    ):
        validate_single_community_session_config("session_not_dict", "not_a_dict")


def test_validate_single_cs_unknown_field_foo():
    """Test validation fails for an unknown field 'foo'. (Adapted from test_config.py)"""
    bad_config = {"host": "localhost", "foo": 1}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Unknown field 'foo' in community session config for local_session",
    ):
        validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_field_wrong_type_host():
    """Test validation fails if 'host' field has wrong type (int instead of str). (Adapted from test_config.py)"""
    bad_config = {"host": 123}  # host should be str
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'host' in community session config for local_session must be of type str, got int",
    ):
        validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_field_wrong_type_port():
    """Test validation fails if 'port' field has wrong type (str instead of int)."""
    bad_config = {"port": "10000"}  # port should be int
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'port' in community session config for local_session must be of type int, got str",
    ):
        validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_wrong_type_in_tuple_tls_root_certs():
    """Test validation fails if 'tls_root_certs' field has wrong type (int instead of str or None)."""
    bad_config = {"host": "localhost", "tls_root_certs": 123}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match=r"Field 'tls_root_certs' in community session config for test_session must be one of types \(str, NoneType\), got int",
    ):
        validate_single_community_session_config("test_session", bad_config)


def test_validate_single_cs_missing_required_field():
    """Test validation raises ValueError if a required field (e.g., 'host') is missing."""
    # Patch _REQUIRED_FIELDS in the actual module where it's defined and used.
    with patch("deephaven_mcp.config._community_session._REQUIRED_FIELDS", ["host"]):
        bad_config = {"port": 10000}  # Missing 'host'
        with pytest.raises(
            CommunitySessionConfigurationError,
            match="Missing required field 'host' in community session config for local_session",
        ):
            validate_single_community_session_config("local_session", bad_config)


def test_validate_single_cs_auth_token_and_env_var_exclusive():
    """Test validation fails if both 'auth_token' and 'auth_token_env_var' are provided."""
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
    """Test a valid config with only auth_token_env_var passes."""
    config_item = {
        "auth_token_env_var": "MY_TOKEN_VAR",
    }
    try:
        validate_single_community_session_config("test_env_var_only", config_item)
    except CommunitySessionConfigurationError as e:
        pytest.fail(f"Valid config with auth_token_env_var raised error: {e}")


# --- Validation Tests for the overall community_sessions map ---


def test_validate_community_sessions_valid():
    """Test that a valid community_sessions map passes validation."""
    # Temporarily ensure _REQUIRED_FIELDS is empty for this test
    with patch("deephaven_mcp.config._community_session._REQUIRED_FIELDS", []):
        valid_sessions_map = {
            "session1": {"host": "localhost"},
            "session2": {"host": "remote", "port": 9999},
        }
        try:
            validate_community_sessions_config(valid_sessions_map)
        except ValueError as e:
            pytest.fail(f"Valid community_sessions_map raised ValueError: {e}")


def test_validate_community_sessions_not_dict():
    """Test validation fails if community_sessions_map is not a dictionary (when provided)."""
    # Case: community_sessions_map is None (key was absent) - should pass (return None)
    try:
        validate_community_sessions_config(None)
    except ValueError as e:
        pytest.fail(f"validate_community_sessions_config(None) raised ValueError: {e}")

    # Case: community_sessions_map is not a dict (e.g., string) - should fail
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'community_sessions' must be a dictionary in Deephaven community session config",
    ):
        validate_community_sessions_config("not_a_dict")

    # Case: community_sessions_map is not a dict (e.g., list) - should fail
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'community_sessions' must be a dictionary in Deephaven community session config",
    ):
        validate_community_sessions_config([{"session1": {}}])


def test_validate_community_sessions_empty_dict():
    """Test validation passes if community_sessions_map is an empty dictionary."""
    validate_community_sessions_config({})  # Should not raise any exception


@patch(
    "deephaven_mcp.config._community_session.validate_single_community_session_config"
)
def test_validate_community_sessions_invalid_item(mock_validate_single):
    """Test validation fails if an individual session item is invalid."""
    sessions_map_with_invalid = {
        "valid_session": {"host": "localhost"},
        "invalid_session": {"port": "not_an_int"},
    }

    def side_effect_func(name, cfg):
        if name == "invalid_session":
            raise ValueError("Mocked single validation error for invalid_session")
        # For "valid_session", do nothing (implicitly returns None)

    mock_validate_single.side_effect = side_effect_func

    with pytest.raises(
        ValueError, match="Mocked single validation error for invalid_session"
    ):
        validate_community_sessions_config(sessions_map_with_invalid)

    assert mock_validate_single.call_count == 2
    mock_validate_single.assert_any_call("valid_session", {"host": "localhost"})
    mock_validate_single.assert_any_call("invalid_session", {"port": "not_an_int"})


def test_validate_single_cs_auth_type_known_values_no_warning(caplog):
    """Test that known auth_type values don't generate warnings."""
    with caplog.at_level(logging.WARNING):
        # Test known values
        validate_single_community_session_config(
            "test_session", {"auth_type": "Anonymous"}
        )
        validate_single_community_session_config("test_session", {"auth_type": "Basic"})

    # Should have no warnings
    assert len(caplog.records) == 0


def test_validate_single_cs_auth_type_unknown_value_warning(caplog):
    """Test that unknown auth_type values generate appropriate warnings."""
    with caplog.at_level(logging.WARNING):
        # Test unknown value (common typo)
        validate_single_community_session_config(
            "test_session", {"auth_type": "anonymous"}
        )

    # Should have exactly one warning
    assert len(caplog.records) == 1
    warning_message = caplog.records[0].message
    assert "auth_type='anonymous'" in warning_message
    assert "not a commonly known value" in warning_message
    assert "Anonymous, Basic" in warning_message
    assert "Custom authenticators are also valid" in warning_message


def test_validate_single_cs_auth_type_custom_authenticator_warning(caplog):
    """Test that custom authenticator strings generate warnings but are still valid."""
    with caplog.at_level(logging.WARNING):
        # Test custom authenticator (should warn but not fail)
        validate_single_community_session_config(
            "test_session", {"auth_type": "com.example.custom.AuthenticationHandler"}
        )

    # Should have exactly one warning
    assert len(caplog.records) == 1
    warning_message = caplog.records[0].message
    assert "com.example.custom.AuthenticationHandler" in warning_message
    assert "not a commonly known value" in warning_message
    assert "Custom authenticators are also valid" in warning_message


# --- Session Creation Configuration Tests ---


def test_session_creation_none_config_is_valid():
    """Test that None config is valid (session creation not configured)."""
    # Should not raise
    validate_community_session_creation_config(None)


def test_session_creation_empty_dict_is_valid():
    """Test that empty dict is valid (all fields optional)."""
    validate_community_session_creation_config({})


def test_session_creation_valid_minimal_config():
    """Test valid minimal configuration."""
    config = {
        "max_concurrent_sessions": 5,
    }
    validate_community_session_creation_config(config)


def test_session_creation_valid_full_config_docker():
    """Test valid full configuration with Docker launch method."""
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
    """Test valid full configuration with pip launch method."""
    config = {
        "max_concurrent_sessions": 3,
        "defaults": {
            "launch_method": "pip",
            "auth_type": "Anonymous",
            "heap_size_gb": 2,
            "startup_timeout_seconds": 30,
        },
    }
    validate_community_session_creation_config(config)


def test_session_creation_invalid_not_dict():
    """Test that non-dict config raises error."""
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'session_creation' must be a dictionary in community config",
    ):
        validate_community_session_creation_config("not a dict")


def test_session_creation_invalid_max_concurrent_sessions_negative():
    """Test that negative max_concurrent_sessions raises error."""
    config = {"max_concurrent_sessions": -1}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'max_concurrent_sessions' must be non-negative",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_max_concurrent_sessions_not_int():
    """Test that non-integer max_concurrent_sessions raises error."""
    config = {"max_concurrent_sessions": "five"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'max_concurrent_sessions' in session_creation config must be of type int",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_defaults_not_dict():
    """Test that non-dict defaults raises error."""
    config = {"defaults": "not a dict"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'defaults' in session_creation config must be of type dict",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_launch_method():
    """Test that invalid launch_method raises error."""
    config = {"defaults": {"launch_method": "invalid"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'launch_method' must be one of",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_auth_type_generates_warning(caplog):
    """Test that invalid auth_type generates warning (not error)."""
    config = {"defaults": {"auth_type": "InvalidAuth"}}
    # Should not raise, just warn
    validate_community_session_creation_config(config)
    assert "auth_type='InvalidAuth' which is not a commonly known value" in caplog.text


def test_session_creation_auth_token_and_env_var_mutually_exclusive():
    """Test that auth_token and auth_token_env_var are mutually exclusive."""
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
    """Test that negative heap_size_gb raises error."""
    config = {"defaults": {"heap_size_gb": -1}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'heap_size_gb' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_docker_memory_limit_negative():
    """Test that negative docker_memory_limit_gb raises error."""
    config = {"defaults": {"docker_memory_limit_gb": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'docker_memory_limit_gb' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_docker_cpu_limit_negative():
    """Test that negative docker_cpu_limit raises error."""
    config = {"defaults": {"docker_cpu_limit": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'docker_cpu_limit' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_startup_timeout_negative():
    """Test that negative startup_timeout_seconds raises error."""
    config = {"defaults": {"startup_timeout_seconds": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'startup_timeout_seconds' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_startup_check_interval_negative():
    """Test that negative startup_check_interval_seconds raises error."""
    config = {"defaults": {"startup_check_interval_seconds": -1.0}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'startup_check_interval_seconds' must be positive",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_startup_retries_negative():
    """Test that negative startup_retries raises error."""
    config = {"defaults": {"startup_retries": -1}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'startup_retries' must be non-negative",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_docker_volumes_not_list():
    """Test that non-list docker_volumes raises error."""
    config = {"defaults": {"docker_volumes": "not a list"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'docker_volumes' in session_creation.defaults must be of type list",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_extra_jvm_args_not_list():
    """Test that non-list extra_jvm_args raises error."""
    config = {"defaults": {"extra_jvm_args": "not a list"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'extra_jvm_args' in session_creation.defaults must be of type list",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_invalid_environment_vars_not_dict():
    """Test that non-dict environment_vars raises error."""
    config = {"defaults": {"environment_vars": "not a dict"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Field 'environment_vars' in session_creation.defaults must be of type dict",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_unknown_field_raises_error():
    """Test that unknown fields raise errors."""
    config = {"defaults": {"unknown_field": "value"}}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Unknown field 'unknown_field' in session_creation.defaults config",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_unknown_field_in_session_creation():
    """Test unknown field in session_creation."""
    config = {
        "unknown_field": "value",
    }

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Unknown field 'unknown_field' in session_creation config",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_field_wrong_type_tuple_allowed():
    """Test field with wrong type when tuple of types allowed."""
    config = {
        "defaults": {
            "docker_memory_limit_gb": "not_a_number",  # Should be float, int, or None
        }
    }

    with pytest.raises(
        CommunitySessionConfigurationError, match="must be one of types"
    ):
        validate_community_session_creation_config(config)


def test_session_creation_docker_volumes_item_not_string():
    """Test docker_volumes items must be strings."""
    config = {
        "defaults": {
            "docker_volumes": [
                "/valid/path",
                123,
                "/another/path",
            ],  # Item is not string
        }
    }

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'docker_volumes\\[1\\]' must be a string",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_extra_jvm_args_item_not_string():
    """Test extra_jvm_args items must be strings."""
    config = {
        "defaults": {
            "extra_jvm_args": ["-XX:+UseG1GC", 123, "-Xms1g"],  # Item is not string
        }
    }

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'extra_jvm_args\\[1\\]' must be a string",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_environment_vars_key_not_string():
    """Test environment_vars keys must be strings."""
    config = {
        "defaults": {
            "environment_vars": {123: "value"},  # Key is not string
        }
    }

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'environment_vars' key must be a string",
    ):
        validate_community_session_creation_config(config)


def test_session_creation_environment_vars_value_not_string():
    """Test environment_vars values must be strings."""
    config = {
        "defaults": {
            "environment_vars": {"KEY": 123},  # Value is not string
        }
    }

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'environment_vars\\[KEY\\]' value must be a string",
    ):
        validate_community_session_creation_config(config)


# --- Session Creation Redaction Tests ---


def test_session_creation_redact_empty_config_returns_empty():
    """Test that empty config returns empty dict."""
    assert redact_community_session_creation_config({}) == {}


def test_session_creation_redact_redacts_auth_token():
    """Test that auth_token is redacted."""
    config = {"defaults": {"auth_token": "secret-token"}}
    redacted = redact_community_session_creation_config(config)
    assert redacted["defaults"]["auth_token"] == "[REDACTED]"


def test_session_creation_redact_preserves_non_sensitive_fields():
    """Test that non-sensitive fields are preserved."""
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
    """Test that auth_token_env_var is NOT redacted (it's just a variable name)."""
    config = {"defaults": {"auth_token_env_var": "MY_TOKEN"}}
    redacted = redact_community_session_creation_config(config)
    # Environment variable names are not sensitive, only their values are
    assert redacted["defaults"]["auth_token_env_var"] == "MY_TOKEN"


def test_session_creation_redact_does_not_modify_original():
    """Test that redaction does not modify the original config."""
    config = {"defaults": {"auth_token": "secret"}}
    original_token = config["defaults"]["auth_token"]
    redact_community_session_creation_config(config)
    assert config["defaults"]["auth_token"] == original_token


# --- validate_security_community_config Tests ---


def test_validate_security_community_config_none():
    """Test that None security.community config is valid (optional section)."""
    validate_security_community_config(None)  # Should not raise


def test_validate_security_community_config_empty_dict():
    """Test that empty security.community config is valid."""
    validate_security_community_config({})  # Should not raise


def test_validate_security_community_config_mode_none():
    """Test that credential_retrieval_mode='none' is valid."""
    config = {"credential_retrieval_mode": "none"}
    validate_security_community_config(config)  # Should not raise


def test_validate_security_community_config_mode_dynamic_only():
    """Test that credential_retrieval_mode='dynamic_only' is valid."""
    config = {"credential_retrieval_mode": "dynamic_only"}
    validate_security_community_config(config)  # Should not raise


def test_validate_security_community_config_mode_static_only():
    """Test that credential_retrieval_mode='static_only' is valid."""
    config = {"credential_retrieval_mode": "static_only"}
    validate_security_community_config(config)  # Should not raise


def test_validate_security_community_config_mode_all():
    """Test that credential_retrieval_mode='all' is valid."""
    config = {"credential_retrieval_mode": "all"}
    validate_security_community_config(config)  # Should not raise


def test_validate_security_community_config_not_dict():
    """Test that non-dict security.community config raises error."""
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'security.community' must be a dictionary",
    ):
        validate_security_community_config("not_a_dict")


def test_validate_security_community_config_mode_not_string():
    """Test that credential_retrieval_mode must be a string."""
    config = {"credential_retrieval_mode": True}  # Boolean instead of string
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'security.community.credential_retrieval_mode' must be a string, got bool",
    ):
        validate_security_community_config(config)


def test_validate_security_community_config_invalid_mode():
    """Test that invalid credential_retrieval_mode value raises error."""
    config = {"credential_retrieval_mode": "invalid_mode"}
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="'security.community.credential_retrieval_mode' must be one of",
    ):
        validate_security_community_config(config)


def test_validate_security_community_config_with_other_fields():
    """Test that config with other fields is valid (future-proofing)."""
    config = {
        "credential_retrieval_mode": "dynamic_only",
        "future_security_field": "value",
    }
    validate_security_community_config(config)  # Should not raise
