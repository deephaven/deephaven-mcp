"""Tests for Deephaven MCP community session specific configuration logic."""

from unittest.mock import patch
import logging

import pytest

from deephaven_mcp._exceptions import CommunitySessionConfigurationError
from deephaven_mcp.config._community_session import (
    redact_community_session_config,
    validate_community_sessions_config,
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


import pytest


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
        validate_single_community_session_config("test_session", {"auth_type": "Anonymous"})
        validate_single_community_session_config("test_session", {"auth_type": "Basic"})
        
    # Should have no warnings
    assert len(caplog.records) == 0


def test_validate_single_cs_auth_type_unknown_value_warning(caplog):
    """Test that unknown auth_type values generate appropriate warnings."""
    with caplog.at_level(logging.WARNING):
        # Test unknown value (common typo)
        validate_single_community_session_config("test_session", {"auth_type": "anonymous"})
        
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
        validate_single_community_session_config("test_session", {
            "auth_type": "com.example.custom.AuthenticationHandler"
        })
        
    # Should have exactly one warning
    assert len(caplog.records) == 1
    warning_message = caplog.records[0].message
    assert "com.example.custom.AuthenticationHandler" in warning_message
    assert "not a commonly known value" in warning_message
    assert "Custom authenticators are also valid" in warning_message
