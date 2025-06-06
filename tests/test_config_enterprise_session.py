"""Unit tests for the enterprise_session configuration validation."""
import pytest

from deephaven_mcp.config.enterprise_session import (
    validate_enterprise_sessions_config,
    EnterpriseSessionConfigurationError,
)


# --- Tests for validate_enterprise_sessions_config --- #

def test_validate_enterprise_sessions_valid_config():
    """Test with a basic valid enterprise_sessions configuration."""
    config = {
        "my_enterprise_session": {
            "connection_json_url": "https://enterprise.example.com/iris/connection.json",
            "auth_type": "api_key",
            "api_key_env_var": "MY_API_KEY_ENV"
        },
        "another_session": {
            "connection_json_url": "https://another.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user1",
            "password_env_var": "USER1_PASS_ENV"
        }
    }
    try:
        validate_enterprise_sessions_config(config)
    except EnterpriseSessionConfigurationError as e:
        pytest.fail(f"Validation failed for a valid config: {e}")

def test_validate_enterprise_sessions_none_is_valid():
    """Test that None (key not present) is valid."""
    try:
        validate_enterprise_sessions_config(None)
    except EnterpriseSessionConfigurationError as e:
        pytest.fail(f"Validation failed for None config: {e}")

def test_validate_enterprise_sessions_empty_dict_is_valid():
    """Test that an empty dictionary (no sessions configured) is valid."""
    try:
        validate_enterprise_sessions_config({})
    except EnterpriseSessionConfigurationError as e:
        pytest.fail(f"Validation failed for empty dict config: {e}")


def test_validate_enterprise_sessions_not_a_dict():
    """Test that non-dict for enterprise_sessions raises error."""
    with pytest.raises(EnterpriseSessionConfigurationError,
                       match="'enterprise_sessions' must be a dictionary"): 
        validate_enterprise_sessions_config(["not", "a", "dict"])


# --- Tests for _validate_single_enterprise_session (via validate_enterprise_sessions_config) --- #

@pytest.mark.parametrize(
    "session_name, session_config, error_match",
    [
        (
            123, # Invalid session name type
            {
                "connection_json_url": "http://test", 
                "auth_type": "api_key",
                "api_key_env_var": "DUMMY_ENV_VAR"
            },
            "Enterprise session name must be a string"
        ),
        (
            "test_session", 
            "not_a_dict", # Invalid session_config type
            "Configuration for enterprise session 'test_session' must be a dictionary"
        ),
        (
            "missing_conn_url",
            {"auth_type": "api_key", "api_key_env_var": "DUMMY_ENV_VAR"},
            "missing required key 'connection_json_url'"
        ),
        (
            "invalid_conn_url_type",
            {"connection_json_url": 123, "auth_type": "api_key", "api_key_env_var": "DUMMY_ENV_VAR"},
            "'connection_json_url'.*must be a string"
        ),
        (
            "missing_auth_type",
            {"connection_json_url": "http://test"},
            "missing required key 'auth_type'"
        ),
        (
            "invalid_auth_type_value",
            {"connection_json_url": "http://test", "auth_type": "bad_type"},
            "'auth_type'.*must be one of"
        ),
        # API Key Auth Type Tests
        (
            "api_key_missing_key_or_env",
            {"connection_json_url": "http://test", "auth_type": "api_key"},
            "requires 'api_key' or 'api_key_env_var'"
        ),
        (
            "api_key_invalid_key_type",
            {"connection_json_url": "http://test", "auth_type": "api_key", "api_key": 123},
            "'api_key'.*must be a string"
        ),
        (
            "api_key_invalid_env_var_type",
            {"connection_json_url": "http://test", "auth_type": "api_key", "api_key_env_var": 123},
            "'api_key_env_var'.*must be a string"
        ),
        # Password Auth Type Tests
        (
            "password_missing_username",
            {"connection_json_url": "http://test", "auth_type": "password"},
            "requires 'username'"
        ),
        (
            "password_invalid_username_type",
            {"connection_json_url": "http://test", "auth_type": "password", "username": 123},
            "'username'.*must be a string"
        ),
        (
            "password_missing_pass_or_env",
            {"connection_json_url": "http://test", "auth_type": "password", "username": "user"},
            "requires 'password' or 'password_env_var'"
        ),
        (
            "password_invalid_password_type",
            {"connection_json_url": "http://test", "auth_type": "password", "username": "user", "password": 123},
            "'password'.*must be a string"
        ),
        (
            "password_invalid_password_env_var_type",
            {"connection_json_url": "http://test", "auth_type": "password", "username": "user", "password_env_var": 123},
            "'password_env_var'.*must be a string"
        ),
        # Private Key Auth Type Tests
        (
            "private_key_missing_key_path",
            {"connection_json_url": "http://test", "auth_type": "private_key"},
            "requires 'private_key_path'"
        ),
        (
            "private_key_invalid_key_path_type",
            {"connection_json_url": "http://test", "auth_type": "private_key", "private_key_path": 123},
            "'private_key_path'.*must be a string"
        )
        # Unknown key warning (does not raise error)
    ]
)
def test_single_enterprise_session_invalid_configs(session_name, session_config, error_match):
    """Test various invalid single enterprise session configurations."""
    config_map = {session_name: session_config}
    # For session_name as int, the error is raised before _validate_single_enterprise_session
    if isinstance(session_name, int):
        config_map = {str(session_name): session_config} # Use a valid string key for the map itself
        # Then test the name validation directly if that's the intent for this specific case
        # However, the primary test here is for the content of session_config
        # The session_name type check is already in validate_enterprise_sessions_config
        # Let's adjust this particular test case if it's meant for session_name type
        if error_match == "Enterprise session name must be a string":
             with pytest.raises(EnterpriseSessionConfigurationError, match=error_match):
                validate_enterprise_sessions_config({session_name: session_config})
             return # End test here for this specific case

    with pytest.raises(EnterpriseSessionConfigurationError, match=error_match):
        validate_enterprise_sessions_config(config_map)

@pytest.mark.parametrize(
    "session_name, auth_type, config_override, unknown_key, expected_warning_match",
    [
        (
            "api_key_with_username", "api_key",
            {"api_key_env_var": "KEY", "username": "test_user"},
            "username",
            "Unknown key 'username' in enterprise session 'api_key_with_username' configuration (auth_type: api_key)"
        ),
        (
            "password_with_api_key", "password",
            {"username": "user", "password_env_var": "PASS", "api_key": "some_key"},
            "api_key",
            "Unknown key 'api_key' in enterprise session 'password_with_api_key' configuration (auth_type: password)"
        ),
        (
            "private_key_with_password", "private_key",
            {"private_key_path": "/path/key.pem", "password": "secret"},
            "password",
            "Unknown key 'password' in enterprise session 'private_key_with_password' configuration (auth_type: private_key)"
        ),
        (
            "api_key_with_multiple_unknown", "api_key",
            {"api_key_env_var": "KEY", "username": "user", "private_key_path": "path"},
            ["username", "private_key_path"], # Test one, the loop will catch others
            "Unknown key 'username' in enterprise session 'api_key_with_multiple_unknown' configuration (auth_type: api_key)"
        ),
    ]
)
def test_validate_single_session_unknown_key_warnings(
    caplog, session_name, auth_type, config_override, unknown_key, expected_warning_match
):
    """Test that unknown keys for a given auth_type log a warning but don't error."""
    base_config = {
        "connection_json_url": "http://valid.url",
        "auth_type": auth_type,
    }
    # Add required fields for the chosen auth_type to the base_config
    if auth_type == "api_key" and "api_key" not in config_override and "api_key_env_var" not in config_override:
        base_config["api_key_env_var"] = "DEFAULT_KEY_ENV" # Default if not in override
    elif auth_type == "password":
        if "username" not in config_override:
            base_config["username"] = "default_user"
        if "password" not in config_override and "password_env_var" not in config_override:
            base_config["password_env_var"] = "DEFAULT_PASS_ENV"
    elif auth_type == "private_key" and "private_key_path" not in config_override:
        base_config["private_key_path"] = "/default/path.pem"

    full_session_config = {**base_config, **config_override}
    config_map = {session_name: full_session_config}

    validate_enterprise_sessions_config(config_map)  # Should not raise error

    # Check if the specific expected warning is present
    # For multiple unknown keys, the current implementation logs them one by one.
    # We check for the first one if 'unknown_key' is a list, or the specific one if it's a string.
    key_to_check = unknown_key[0] if isinstance(unknown_key, list) else unknown_key
    
    found_specific_warning = False
    for record in caplog.records:
        if record.levelname == "WARNING" and f"Unknown key '{key_to_check}'" in record.message and f"(auth_type: {auth_type})" in record.message:
            assert expected_warning_match in record.message # Check full match if needed, or parts
            found_specific_warning = True
            break
    assert found_specific_warning, f"Expected warning for key '{key_to_check}' not found in logs: {caplog.text}"

    # Additionally, ensure no EnterpriseSessionConfigurationError was raised by mistake
    for record in caplog.records:
        assert "EnterpriseSessionConfigurationError" not in record.message, "Validation error was unexpectedly raised."
