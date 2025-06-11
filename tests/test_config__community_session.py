"""Tests for Deephaven MCP community session specific configuration logic."""

from unittest.mock import patch

import pytest

from deephaven_mcp.config._community_session import (
    redact_community_session_config,
    validate_community_sessions_config,
    validate_single_community_session_config,
)
from deephaven_mcp.config._errors import CommunitySessionConfigurationError

# --- Redaction Tests ---


def test_redact_community_session_config():
    """Test that auth_token is redacted and other fields are not."""
    original_config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "token",
        "auth_token": "sensitive_token_value",
        "auth_token_env_var": "MY_ENV_VAR_NAME",
        "session_type": "python",
        "client_private_key": "/path/to/key.pem",  # Path, not sensitive content
    }
    redacted = redact_community_session_config(original_config)

    assert redacted["host"] == "localhost"
    assert redacted["port"] == 10000
    assert redacted["auth_type"] == "token"
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["auth_token_env_var"] == "MY_ENV_VAR_NAME"
    assert redacted["session_type"] == "python"
    assert redacted["client_private_key"] == "/path/to/key.pem"
    assert "sensitive_token_value" not in redacted.values()
    # Ensure original is not modified
    assert original_config["auth_token"] == "sensitive_token_value"


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


def test_validate_single_cs_missing_required_field(monkeypatch):
    """Test validation raises ValueError if a required field (e.g., 'host') is missing."""
    # Must patch _REQUIRED_FIELDS in the *actual* module where it's defined and used.
    monkeypatch.setattr(
        "deephaven_mcp.config._community_session._REQUIRED_FIELDS", ["host"]
    )
    bad_config = {"port": 10000}  # Missing 'host'
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Missing required field 'host' in community session config for local_session",
    ):
        validate_single_community_session_config("local_session", bad_config)
    # monkeypatch automatically undoes the change after the test.


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


def test_validate_community_sessions_valid(monkeypatch):
    """Test that a valid community_sessions map passes validation."""
    # Temporarily ensure _REQUIRED_FIELDS is empty for this test, or mock validate_single_community_session_config
    monkeypatch.setattr("deephaven_mcp.config._community_session._REQUIRED_FIELDS", [])
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
