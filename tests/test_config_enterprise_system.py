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
            123,  # Invalid system name type
            {
                "connection_json_url": "http://test",
                "auth_type": "api_key",
                "api_key_env_var": "DUMMY_ENV_VAR"
            },
            r"Enterprise system name must be a string"
        ),
        (
            "test_system_not_dict",
            "not_a_dict",  # Invalid system_config type
            r"Enterprise system 'test_system_not_dict' configuration must be a dictionary"
        ),
        (
            "missing_conn_url",
            {"auth_type": "api_key", "api_key_env_var": "DUMMY_ENV_VAR"},
            r"Required field 'connection_json_url' missing in enterprise system 'missing_conn_url'"
        ),
        (
            "invalid_conn_url_type",
            {"connection_json_url": 123, "auth_type": "api_key", "api_key_env_var": "DUMMY_ENV_VAR"},
            r"Field 'connection_json_url' for enterprise system 'invalid_conn_url_type' must be of type str"
        ),
        (
            "missing_auth_type",
            {"connection_json_url": "http://test"},
            r"Required field 'auth_type' missing in enterprise system 'missing_auth_type'"
        ),
        (
            "invalid_auth_type_value",
            {"connection_json_url": "http://test", "auth_type": "bad_type"},
            r"'auth_type' for enterprise system 'invalid_auth_type_value' must be one of \['api_key', 'password', 'private_key'\]"
        ),
        # API Key Auth Type Tests
        (
            "api_key_missing_key_or_env",
            {"connection_json_url": "http://test", "auth_type": "api_key"},
            r"Enterprise system 'api_key_missing_key_or_env' with auth_type 'api_key' must define 'api_key' or 'api_key_env_var'"
        ),
        (
            "api_key_invalid_key_type",
            {"connection_json_url": "http://test", "auth_type": "api_key", "api_key": 123},
            r"Field 'api_key' for enterprise system 'api_key_invalid_key_type' \(auth_type: api_key\) must be of type str"
        ),
        (
            "api_key_invalid_env_var_type",
            {"connection_json_url": "http://test", "auth_type": "api_key", "api_key_env_var": 123},
            r"Field 'api_key_env_var' for enterprise system 'api_key_invalid_env_var_type' \(auth_type: api_key\) must be of type str"
        ),
        # Password Auth Type Tests
        (
            "password_missing_username",
            {"connection_json_url": "http://test", "auth_type": "password"},
            r"Enterprise system 'password_missing_username' with auth_type 'password' must define 'username'"
        ),
        (
            "password_invalid_username_type",
            {"connection_json_url": "http://test", "auth_type": "password", "username": 123},
            r"Field 'username' for enterprise system 'password_invalid_username_type' \(auth_type: password\) must be of type str"
        ),
        (
            "password_missing_pass_or_env",
            {"connection_json_url": "http://test", "auth_type": "password", "username": "user"},
            r"Enterprise system 'password_missing_pass_or_env' with auth_type 'password' must define 'password' or 'password_env_var'"
        ),
        (
            "password_invalid_password_type",
            {"connection_json_url": "http://test", "auth_type": "password", "username": "user", "password": 123},
            r"Field 'password' for enterprise system 'password_invalid_password_type' \(auth_type: password\) must be of type str"
        ),
        (
            "password_invalid_password_env_var_type",
            {"connection_json_url": "http://test", "auth_type": "password", "username": "user", "password_env_var": 123},
            r"Field 'password_env_var' for enterprise system 'password_invalid_password_env_var_type' \(auth_type: password\) must be of type str"
        ),
        # Private Key Auth Type Tests
        (
            "private_key_missing_key", # Renamed from private_key_missing_key_path
            {"connection_json_url": "http://test", "auth_type": "private_key"},
            r"Enterprise system 'private_key_missing_key' with auth_type 'private_key' must define 'private_key'"
        ),
        (
            "private_key_invalid_key_type", # Renamed from private_key_invalid_key_path_type
            {"connection_json_url": "http://test", "auth_type": "private_key", "private_key": 123},
            r"Field 'private_key' for enterprise system 'private_key_invalid_key_type' \(auth_type: private_key\) must be of type str"
        )
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
    "system_name, auth_type, config_override, unknown_key_to_check, expected_warning_substring",
    [
        (
            "api_key_with_username", "api_key",
            {"api_key_env_var": "KEY", "username": "test_user"},
            "username",
            "Unknown field 'username' in enterprise system 'api_key_with_username' configuration. It will be ignored."
        ),
        (
            "password_with_api_key", "password",
            {"username": "user", "password_env_var": "PASS", "api_key": "some_key"},
            "api_key",
            "Unknown field 'api_key' in enterprise system 'password_with_api_key' configuration. It will be ignored."
        ),
        (
            "private_key_with_password", "private_key",
            {"private_key": "/path/key.pem", "password": "secret"}, # Changed private_key_path to private_key
            "password",
            "Unknown field 'password' in enterprise system 'private_key_with_password' configuration. It will be ignored."
        ),
        (
            "api_key_with_multiple_unknown", "api_key",
            {"api_key_env_var": "KEY", "username": "user", "private_key": "path"}, # Changed private_key_path to private_key
            "username", # We'll check for one of the unknown keys' warning
            "Unknown field 'username' in enterprise system 'api_key_with_multiple_unknown' configuration. It will be ignored."
        ),
        (
            "api_key_with_multiple_unknown_check_other", "api_key",
            {"api_key_env_var": "KEY", "username": "user", "private_key": "path"}, # Changed private_key_path to private_key
            "private_key", # Check for the other unknown key's warning in a separate case
            "Unknown field 'private_key' in enterprise system 'api_key_with_multiple_unknown_check_other' configuration. It will be ignored."
        ),
    ]
)
def test_validate_single_system_unknown_key_warnings(
    caplog, system_name, auth_type, config_override, unknown_key_to_check, expected_warning_substring
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
        base_config["private_key"] = "/dummy/path/to/key.pem"

    full_system_config = {**base_config, **config_override}
    config_to_validate = {system_name: full_system_config}

    validate_enterprise_systems_config(config_to_validate)

    warning_found = any(
        record.levelname == "WARNING" and expected_warning_substring in record.message
        for record in caplog.records
    )
    assert warning_found, f"Expected warning substring '{expected_warning_substring}' not found in logs: {caplog.text}"

    # Additionally, ensure that for 'api_key_with_multiple_unknown', both warnings are present if that's the system_name
    if system_name == "api_key_with_multiple_unknown":
        expected_warning_private_key = f"Unknown field 'private_key' in enterprise system '{system_name}' configuration. It will be ignored."
        private_key_warning_found = any(
            record.levelname == "WARNING" and expected_warning_private_key in record.message
            for record in caplog.records
        )
        assert private_key_warning_found, f"Expected warning for 'private_key' not found for {system_name}: {caplog.text}"
