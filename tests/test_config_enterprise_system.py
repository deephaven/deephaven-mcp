"""Unit tests for the enterprise_system configuration validation."""
import pytest

from deephaven_mcp.config.enterprise_system import (
    validate_enterprise_systems_config,
    EnterpriseSystemConfigurationError,
)


# --- Tests for validate_enterprise_systems_config --- #

def test_validate_enterprise_systems_valid_config():
    """Test with a basic valid enterprise_systems configuration."""
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
        validate_enterprise_systems_config(config)
    except EnterpriseSystemConfigurationError as e:
        pytest.fail(f"Validation failed for a valid config: {e}")

def test_validate_enterprise_systems_none_is_valid():
    """Test that None (key not present) is valid."""
    try:
        validate_enterprise_systems_config(None)
    except EnterpriseSystemConfigurationError as e:
        pytest.fail(f"Validation failed for None config: {e}")

def test_validate_enterprise_systems_empty_dict_is_valid():
    """Test that an empty dictionary (no systems configured) is valid."""
    try:
        validate_enterprise_systems_config({})
    except EnterpriseSystemConfigurationError as e:
        pytest.fail(f"Validation failed for empty dict config: {e}")


def test_validate_enterprise_systems_not_a_dict():
    """Test that non-dict for enterprise_systems raises error."""
    with pytest.raises(EnterpriseSystemConfigurationError,
                       match="'enterprise_systems' must be a dictionary"): 
        validate_enterprise_systems_config(["not", "a", "dict"])


# --- Tests for _validate_single_enterprise_system (via validate_enterprise_systems_config) --- #

@pytest.mark.parametrize(
    "system_name, system_config, error_match",
    [
        (
            123, # Invalid session name type
            {
                "connection_json_url": "http://test", 
                "auth_type": "api_key",
                "api_key_env_var": "DUMMY_ENV_VAR"
            },
            "Enterprise system name must be a string"
        ),
        (
            "test_system", 
            "not_a_dict", # Invalid system_config type
            "Configuration for enterprise system 'test_system' must be a dictionary"
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
def test_single_enterprise_system_invalid_configs(system_name, system_config, error_match):
    """Test various invalid single enterprise system configurations."""
    config_map = {system_name: system_config}
    # For system_name as int, the error is raised before _validate_single_enterprise_system
    if isinstance(system_name, int):
        config_map = {str(system_name): system_config} # Use a valid string key for the map itself
        # Then test the name validation directly if that's the intent for this specific case
        # However, the primary test here is for the content of system_config
        # The system_name type check is already in validate_enterprise_systems_config
        # Let's adjust this particular test case if it's meant for system_name type
        if error_match == "Enterprise system name must be a string":
             with pytest.raises(EnterpriseSystemConfigurationError, match=error_match):
                validate_enterprise_systems_config({system_name: system_config})
             return # End test here for this specific case

    with pytest.raises(EnterpriseSystemConfigurationError, match=error_match):
        validate_enterprise_systems_config(config_map)

@pytest.mark.parametrize(
    "system_name, auth_type, config_override, unknown_key, expected_warning_match",
    [
        (
            "api_key_with_username", "api_key",
            {"api_key_env_var": "KEY", "username": "test_user"},
            "username",
            "Unknown key 'username' in enterprise system 'api_key_with_username' configuration (auth_type: api_key)"
        ),
        (
            "password_with_api_key", "password",
            {"username": "user", "password_env_var": "PASS", "api_key": "some_key"},
            "api_key",
            "Unknown key 'api_key' in enterprise system 'password_with_api_key' configuration (auth_type: password)"
        ),
        (
            "private_key_with_password", "private_key",
            {"private_key_path": "/path/key.pem", "password": "secret"},
            "password",
            "Unknown key 'password' in enterprise system 'private_key_with_password' configuration (auth_type: private_key)"
        ),
        (
            "api_key_with_multiple_unknown", "api_key",
            {"api_key_env_var": "KEY", "username": "user", "private_key_path": "path"},
            ["username", "private_key_path"], # Test one, the loop will catch others
            "Unknown key 'username' in enterprise system 'api_key_with_multiple_unknown' configuration (auth_type: api_key)"
        ),
    ]
)
def test_validate_single_system_unknown_key_warnings(
    caplog, system_name, auth_type, config_override, unknown_key, expected_warning_match
):
    """Test that unknown keys for a given auth_type log a warning but don't error."""
    base_config = {
        "connection_json_url": "http://valid.url",
        "auth_type": auth_type,
    }
    
    # Add required fields based on auth_type to make base_config valid before adding unknowns
    if auth_type == "api_key":
        base_config["api_key_env_var"] = "DUMMY_KEY_ENV_VAR"
    elif auth_type == "password":
        base_config["username"] = "dummy_user"
        base_config["password_env_var"] = "DUMMY_PASS_ENV_VAR"
    elif auth_type == "private_key":
        base_config["private_key_path"] = "/dummy/path/to/key.pem"

    full_system_config = {**base_config, **config_override}
    config_to_validate = {system_name: full_system_config}

    validate_enterprise_systems_config(config_to_validate)

    found_warning = False
    for record in caplog.records:
        if record.levelname == "WARNING" and expected_warning_match in record.message:
            found_warning = True
            break
    assert found_warning, f"Expected warning '{expected_warning_match}' not found in logs: {caplog.text}"
