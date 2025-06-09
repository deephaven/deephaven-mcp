"""Unit tests for the enterprise_system configuration validation."""

import pytest

from deephaven_mcp.config.enterprise_system import (
    _AUTH_SPECIFIC_FIELDS,
    _BASE_ENTERPRISE_SYSTEM_FIELDS,
    EnterpriseSystemConfigurationError,
    redact_enterprise_system_config,
    redact_enterprise_systems_map,
    validate_enterprise_systems_config,
)

# --- Tests for validate_enterprise_systems_config --- #


def test_validate_enterprise_systems_valid_config():
    """Test with a basic valid enterprise_systems configuration."""
    config = {
        "my_enterprise_session": {
            "connection_json_url": "https://enterprise.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "my_user",
            "password_env_var": "MY_PASSWORD_ENV",
        },
        "another_session": {
            "connection_json_url": "https://another.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user1",
            "password_env_var": "USER1_PASS_ENV",
        },
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
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'enterprise_systems' must be a dictionary",
    ):
        validate_enterprise_systems_config(["not", "a", "dict"])


# --- Tests for _validate_single_enterprise_system (via validate_enterprise_systems_config) --- #


# --- Tests for redact_enterprise_systems_map --- #


def test_redact_enterprise_system_config_with_password():
    """Test redacting a config with a password field."""
    config = {
        "connection_json_url": "http://test",
        "auth_type": "password",
        "username": "test_user",
        "password": "sensitive_password",
    }
    redacted = redact_enterprise_system_config(config)

    # Password should be redacted
    assert redacted["password"] == "[REDACTED]"
    # Other fields should remain unchanged
    assert redacted["connection_json_url"] == "http://test"
    assert redacted["auth_type"] == "password"
    assert redacted["username"] == "test_user"
    # Original should not be modified
    assert config["password"] == "sensitive_password"


def test_redact_enterprise_system_config_without_password():
    """Test redacting a config without a password field."""
    config = {
        "connection_json_url": "http://test",
        "auth_type": "private_key",
        "private_key": "/path/to/key",
    }
    redacted = redact_enterprise_system_config(config)

    # Config should be unchanged
    assert redacted == config
    # Should be the same object (shallow copy)
    assert redacted is not config


def test_redact_enterprise_system_config_empty():
    """Test redacting an empty config."""
    config = {}
    redacted = redact_enterprise_system_config(config)

    assert redacted == {}
    assert redacted is not config


def test_redact_enterprise_systems_map_single_system_with_password():
    """Test redacting a single system with a password."""
    config_map = {
        "sys1": {
            "connection_json_url": "http://host1",
            "auth_type": "password",
            "username": "user1",
            "password": "secret123",
            "other_field": "value1",
        }
    }
    expected = {
        "sys1": {
            "connection_json_url": "http://host1",
            "auth_type": "password",
            "username": "user1",
            "password": "[REDACTED]",
            "other_field": "value1",
        }
    }
    assert redact_enterprise_systems_map(config_map) == expected


def test_redact_enterprise_systems_map_single_system_without_password():
    """Test a single system without a password field."""
    config_map = {
        "sys1": {
            "connection_json_url": "http://host1",
            "auth_type": "password",
            "username": "user1",
            "password_env_var": "ENV_VAR_PASS",
            "other_field": "value1",
        }
    }
    # Expect no changes as 'password' key is not present
    assert redact_enterprise_systems_map(config_map) == config_map


def test_redact_enterprise_systems_map_multiple_systems_mixed():
    """Test multiple systems, some with passwords, some without."""
    config_map = {
        "sys1_pw": {"password": "secret1", "id": 1},
        "sys2_no_pw": {"username": "user2", "id": 2},
        "sys3_pw": {"password": "secret3", "id": 3},
    }
    expected = {
        "sys1_pw": {"password": "[REDACTED]", "id": 1},
        "sys2_no_pw": {"username": "user2", "id": 2},
        "sys3_pw": {"password": "[REDACTED]", "id": 3},
    }
    assert redact_enterprise_systems_map(config_map) == expected


def test_redact_enterprise_systems_map_empty():
    """Test with an empty enterprise_systems map."""
    assert redact_enterprise_systems_map({}) == {}


def test_redact_enterprise_systems_map_item_not_a_dict():
    """Test when a system config item is not a dictionary (should be included as-is)."""
    config_map = {"sys1": "this_is_not_a_dict"}
    result = redact_enterprise_systems_map(config_map)
    assert result == {"sys1": "this_is_not_a_dict"}


def test_redact_enterprise_systems_map_item_is_none():
    """Test when a system config item is None (should be included as-is)."""
    config_map = {"sys1": None}
    result = redact_enterprise_systems_map(config_map)
    assert result == {"sys1": None}


# --- Tests for _validate_single_enterprise_system (via validate_enterprise_systems_config) --- #


@pytest.mark.parametrize(
    "system_name, system_config, error_match",
    [
        (
            123,  # Invalid system name type
            {
                "connection_json_url": "http://test",
                "auth_type": "password",
                "username": "dummy_user",
                "password_env_var": "DUMMY_ENV_VAR",
            },
            r"Enterprise system name must be a string",
        ),
        (
            "test_system_not_dict",
            "not_a_dict",  # Invalid system_config type
            r"Enterprise system 'test_system_not_dict' configuration must be a dictionary",
        ),
        (
            "missing_conn_url",
            {
                "auth_type": "password",
                "username": "dummy_user",
                "password_env_var": "DUMMY_ENV_VAR",
            },
            r"Required field 'connection_json_url' missing in enterprise system 'missing_conn_url'",
        ),
        (
            "invalid_conn_url_type",
            {
                "connection_json_url": 123,
                "auth_type": "password",
                "username": "dummy_user",
                "password_env_var": "DUMMY_ENV_VAR",
            },
            r"Field 'connection_json_url' for enterprise system 'invalid_conn_url_type' must be of type str",
        ),
        (
            "missing_auth_type",
            {"connection_json_url": "http://test"},
            r"Required field 'auth_type' missing in enterprise system 'missing_auth_type'",
        ),
        (
            "invalid_auth_type_value",
            {"connection_json_url": "http://test", "auth_type": "bad_type"},
            r"'auth_type' for enterprise system 'invalid_auth_type_value' must be one of \['password', 'private_key'\]",
        ),
        # Password Auth Type Tests
        (
            "password_missing_username",
            {"connection_json_url": "http://test", "auth_type": "password"},
            r"Enterprise system 'password_missing_username' with auth_type 'password' must define 'username'",
        ),
        (
            "password_invalid_username_type",
            {
                "connection_json_url": "http://test",
                "auth_type": "password",
                "username": 123,
            },
            r"Field 'username' for enterprise system 'password_invalid_username_type' \(auth_type: password\) must be of type str",
        ),
        (
            "password_missing_pass_or_env",
            {
                "connection_json_url": "http://test",
                "auth_type": "password",
                "username": "user",
            },
            r"Enterprise system 'password_missing_pass_or_env' with auth_type 'password' must define 'password' or 'password_env_var'",
        ),
        (
            "password_invalid_password_type",
            {
                "connection_json_url": "http://test",
                "auth_type": "password",
                "username": "user",
                "password": 123,
            },
            r"Field 'password' for enterprise system 'password_invalid_password_type' \(auth_type: password\) must be of type str",
        ),
        (
            "password_invalid_password_env_var_type",
            {
                "connection_json_url": "http://test",
                "auth_type": "password",
                "username": "user",
                "password_env_var": 123,
            },
            r"Field 'password_env_var' for enterprise system 'password_invalid_password_env_var_type' \(auth_type: password\) must be of type str",
        ),
        # Private Key Auth Type Tests
        (
            "private_key_missing_key",  # Renamed from private_key_missing_key_path
            {"connection_json_url": "http://test", "auth_type": "private_key"},
            r"Enterprise system 'private_key_missing_key' with auth_type 'private_key' must define 'private_key'",
        ),
        (
            "private_key_invalid_key_type",  # Renamed from private_key_invalid_key_path_type
            {
                "connection_json_url": "http://test",
                "auth_type": "private_key",
                "private_key": 123,
            },
            r"Field 'private_key' for enterprise system 'private_key_invalid_key_type' \(auth_type: private_key\) must be of type str",
        ),
        # Test case for unknown fields (they should log warnings but not fail validation)
        # This is a valid config that should pass validation with warnings
        (
            "unknown_field_with_valid_auth",
            {
                "connection_json_url": "http://test",
                "auth_type": "password",
                "username": "test_user",
                "password_env_var": "TEST_PASS_ENV",
                "unknown_field1": 123,
                "unknown_field2": "value",
            },
            None,  # No error expected, just warnings
        ),
        (
            "pw_auth_both_passwords",
            {
                "connection_json_url": "http://c",
                "auth_type": "password",
                "username": "user_both_pw",
                "password": "actual_password_value",
                "password_env_var": "PASSWORD_ENV_VAR_NAME",
            },
            r"Enterprise system 'pw_auth_both_passwords' with auth_type 'password' must not define both 'password' and 'password_env_var'. Specify one.",
        ),
    ],
)
def test_single_enterprise_system_invalid_configs(
    system_name, system_config, error_match, caplog
):
    """Test various invalid single enterprise system configurations."""
    config_map = {system_name: system_config}
    # For system_name as int, the error is raised before _validate_single_enterprise_system
    if isinstance(system_name, int):
        config_map = {
            str(system_name): system_config
        }  # Use a valid string key for the map itself
        # Then test the name validation directly if that's the intent for this specific case
        # However, the primary test here is for the content of system_config
        # The system_name type check is already in validate_enterprise_systems_config
        # Let's adjust this particular test case if it's meant for system_name type
        if error_match == "Enterprise system name must be a string":
            with pytest.raises(EnterpriseSystemConfigurationError, match=error_match):
                validate_enterprise_systems_config({system_name: system_config})
            return  # End test here for this specific case

    if error_match is None:
        # This is a valid config that should pass validation (may log warnings)
        validate_enterprise_systems_config(config_map)
        # Verify that unknown fields were logged as warnings
        if any(field.startswith("unknown_field") for field in system_config):
            assert any(
                "Unknown field 'unknown_field" in record.message
                for record in caplog.records
            )
    else:
        with pytest.raises(EnterpriseSystemConfigurationError, match=error_match):
            validate_enterprise_systems_config(config_map)


@pytest.mark.parametrize(
    "system_name, auth_type, config_override, unknown_key_to_check, expected_warning_substring",
    [
        (
            "private_key_with_password",
            "private_key",
            {
                "private_key": "/path/key.pem",
                "password": "secret",
            },  # Changed private_key_path to private_key
            "password",
            "Unknown field 'password' in enterprise system 'private_key_with_password' configuration. It will be ignored.",
        ),
        (
            "password_with_api_key",
            "password",
            {"username": "user", "password_env_var": "PASS", "api_key": "some_key"},
            "api_key",
            "Unknown field 'api_key' in enterprise system 'password_with_api_key' configuration. It will be ignored.",
        ),
    ],
)
def test_validate_single_system_unknown_key_warnings(
    caplog,
    system_name,
    auth_type,
    config_override,
    unknown_key_to_check,
    expected_warning_substring,
):
    """Test that unknown keys for a given auth_type log a warning but don't error."""
    base_config = {
        "connection_json_url": "http://valid.url",
        "auth_type": auth_type,
    }

    # Add required fields based on auth_type to make base_config valid before adding unknowns
    if auth_type == "password":
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
    assert (
        warning_found
    ), f"Expected warning substring '{expected_warning_substring}' not found in logs: {caplog.text}"


def test_validate_single_system_with_optional_fields():
    """Test that optional fields with tuple types are properly validated."""
    # This test covers the tuple type validation in _validate_single_enterprise_system
    # which is used for optional fields (like tls_root_cert_path if it were added)
    # Since we don't have any optional fields right now, we'll test the code path
    # by temporarily adding a test field

    # Save original state
    original_base_fields = dict(_BASE_ENTERPRISE_SYSTEM_FIELDS)
    original_auth_fields = dict(_AUTH_SPECIFIC_FIELDS)

    try:
        # Add a test optional field
        _BASE_ENTERPRISE_SYSTEM_FIELDS["test_optional_field"] = (str, type(None))

        # Test with None value (should pass)
        config = {
            "connection_json_url": "http://test",
            "auth_type": "password",
            "username": "test_user",
            "password_env_var": "TEST_PASS_ENV",
            "test_optional_field": None,
        }
        validate_enterprise_systems_config({"test_system": config})

        # Test with string value (should pass)
        config["test_optional_field"] = "test_value"
        validate_enterprise_systems_config({"test_system": config})

        # Test with invalid type (should fail)
        config["test_optional_field"] = 123
        with pytest.raises(
            EnterpriseSystemConfigurationError,
            match=r"Field 'test_optional_field' for enterprise system 'test_system' must be one of types \(str, NoneType\), but got int",
        ):
            validate_enterprise_systems_config({"test_system": config})
    finally:
        # Restore original state
        _BASE_ENTERPRISE_SYSTEM_FIELDS.clear()
        _BASE_ENTERPRISE_SYSTEM_FIELDS.update(original_base_fields)
        _AUTH_SPECIFIC_FIELDS.clear()
        _AUTH_SPECIFIC_FIELDS.update(original_auth_fields)


def test_validate_system_with_unknown_fields(caplog):
    """Test that unknown fields are logged as warnings but don't fail validation."""
    # This test covers the unknown field warning in _validate_single_enterprise_system
    config = {
        "connection_json_url": "http://test",
        "auth_type": "password",
        "username": "test_user",
        "password_env_var": "TEST_PASS_ENV",
        "unknown_field1": "value1",
        "unknown_field2": 123,
    }

    # Should not raise an exception
    validate_enterprise_systems_config({"test_system": config})

    # Check that warnings were logged for the unknown fields
    warning_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    assert any("unknown_field1" in msg for msg in warning_messages)
    assert any("unknown_field2" in msg for msg in warning_messages)
    assert all("will be ignored" in msg for msg in warning_messages)


def test_tuple_type_validation_error_message():
    """Test that tuple type validation produces the correct error message format for base fields."""
    # Save original state
    original_base_fields = dict(_BASE_ENTERPRISE_SYSTEM_FIELDS)
    original_auth_fields = dict(_AUTH_SPECIFIC_FIELDS)

    try:
        # Add a test field with tuple type
        _BASE_ENTERPRISE_SYSTEM_FIELDS["test_tuple_field"] = (str, int, type(None))

        # Test with invalid type
        config = {
            "connection_json_url": "http://test",
            "auth_type": "password",
            "username": "test_user",
            "password_env_var": "TEST_PASS_ENV",
            "test_tuple_field": 3.14,  # float is not in (str, int, NoneType)
        }

        with pytest.raises(
            EnterpriseSystemConfigurationError,
            match=r"Field 'test_tuple_field' for enterprise system 'test_system' "
            r"must be one of types \(str, int, NoneType\), but got float\.",
        ):
            validate_enterprise_systems_config({"test_system": config})
    finally:
        # Restore original state
        _BASE_ENTERPRISE_SYSTEM_FIELDS.clear()
        _BASE_ENTERPRISE_SYSTEM_FIELDS.update(original_base_fields)
        _AUTH_SPECIFIC_FIELDS.clear()
        _AUTH_SPECIFIC_FIELDS.update(original_auth_fields)


def test_tuple_type_validation_error_message_auth_specific():
    """Test that tuple type validation produces the correct error message format for auth-specific fields."""
    # Save original state
    original_base_fields = dict(_BASE_ENTERPRISE_SYSTEM_FIELDS)
    original_auth_fields = dict(_AUTH_SPECIFIC_FIELDS)
    try:
        # Add a test field with tuple type to private_key auth
        _AUTH_SPECIFIC_FIELDS["private_key"]["test_tuple_field"] = (
            str,
            int,
            type(None),
        )
        config = {
            "connection_json_url": "http://test",
            "auth_type": "private_key",
            "private_key": "/dummy/path",
            "test_tuple_field": 3.14,  # float is not in (str, int, NoneType)
        }
        with pytest.raises(
            EnterpriseSystemConfigurationError,
            match=r"Field 'test_tuple_field' for enterprise system 'test_system' \(auth_type: private_key\) must be one of types \(str, int, NoneType\), but got float\.",
        ):
            validate_enterprise_systems_config({"test_system": config})
    finally:
        _BASE_ENTERPRISE_SYSTEM_FIELDS.clear()
        _BASE_ENTERPRISE_SYSTEM_FIELDS.update(original_base_fields)
        _AUTH_SPECIFIC_FIELDS.clear()
        _AUTH_SPECIFIC_FIELDS.update(original_auth_fields)
