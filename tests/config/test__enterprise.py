"""Unit tests for deephaven_mcp.config._enterprise — enterprise config validation and EnterpriseServerConfigManager."""

import os
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import (
    ConfigurationError,
    EnterpriseSystemConfigurationError,
)
from deephaven_mcp.config import (
    CONFIG_ENV_VAR,
    ConfigManager,
    EnterpriseServerConfigManager,
)
from deephaven_mcp.config._enterprise import (
    _AUTH_SPECIFIC_FIELDS,
    _BASE_ENTERPRISE_SYSTEM_FIELDS,
    _validate_field_type,
    _validate_optional_fields,
    _validate_required_fields,
    redact_enterprise_system_config,
    validate_enterprise_config,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _valid_config(**kwargs) -> dict:
    """Return a minimal valid flat enterprise config, with optional overrides."""
    base = {
        "system_name": "test_system",
        "connection_json_url": "https://test.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# validate_enterprise_config — top-level
# ---------------------------------------------------------------------------


def test_valid_password_config():
    """A complete valid password-auth config should pass without error."""
    validate_enterprise_config(_valid_config())


def test_valid_private_key_config():
    """A complete valid private_key-auth config should pass without error."""
    validate_enterprise_config(
        {
            "system_name": "prod",
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "private_key",
            "private_key_path": "/path/to/priv-key.base64.txt",
        }
    )


def test_config_not_dict():
    """Raises when config is not a dictionary."""
    with pytest.raises(
        EnterpriseSystemConfigurationError, match="must be a dictionary"
    ):
        validate_enterprise_config("not_a_dict")


def test_config_not_dict_list():
    """Raises when config is a list."""
    with pytest.raises(
        EnterpriseSystemConfigurationError, match="must be a dictionary"
    ):
        validate_enterprise_config([1, 2, 3])


def test_missing_system_name():
    """Raises when system_name field is absent."""
    cfg = _valid_config()
    del cfg["system_name"]
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="Required field 'system_name' is missing",
    ):
        validate_enterprise_config(cfg)


def test_system_name_wrong_type():
    """Raises when system_name is not a string."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'system_name'.*must be of type str",
    ):
        validate_enterprise_config(_valid_config(system_name=42))


def test_missing_connection_json_url():
    """Raises when connection_json_url is missing."""
    cfg = _valid_config()
    del cfg["connection_json_url"]
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="Required field 'connection_json_url' missing",
    ):
        validate_enterprise_config(cfg)


def test_invalid_connection_json_url_type():
    """Raises when connection_json_url is not a string."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'connection_json_url'.*must be of type str",
    ):
        validate_enterprise_config(_valid_config(connection_json_url=123))


def test_missing_auth_type():
    """Raises when auth_type is missing."""
    cfg = _valid_config()
    del cfg["auth_type"]
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="Required field 'auth_type' missing",
    ):
        validate_enterprise_config(cfg)


def test_invalid_auth_type_type():
    """Raises when auth_type is not a string."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'auth_type'.*must be of type str",
    ):
        validate_enterprise_config(_valid_config(auth_type=1))


def test_unknown_auth_type_value():
    """Raises when auth_type has an unsupported value."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'auth_type'.*must be one of \['password', 'private_key'\]",
    ):
        validate_enterprise_config(_valid_config(auth_type="badtype"))


# --- password auth ---


def test_password_auth_missing_username():
    """Raises when password auth is missing username."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="must define 'username'",
    ):
        validate_enterprise_config(
            {
                "system_name": "sys",
                "connection_json_url": "https://test.example.com/iris/connection.json",
                "auth_type": "password",
                "password": "p",
            }
        )


def test_password_auth_invalid_username_type():
    """Raises when username is not a string."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'username'.*must be of type str",
    ):
        validate_enterprise_config(_valid_config(username=1))


def test_password_auth_missing_password_keys():
    """Raises when neither password nor password_env_var is present."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="must define 'password' or 'password_env_var'",
    ):
        validate_enterprise_config(
            {
                "system_name": "sys",
                "connection_json_url": "https://test.example.com/iris/connection.json",
                "auth_type": "password",
                "username": "u",
            }
        )


def test_password_auth_invalid_password_type():
    """Raises when password field has wrong type."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'password'.*must be of type str",
    ):
        validate_enterprise_config(_valid_config(password=1))


def test_password_auth_invalid_password_env_var_type():
    """Raises when password_env_var field has wrong type."""
    cfg = _valid_config()
    del cfg["password"]
    cfg["password_env_var"] = 123  # should be str
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'password_env_var'.*must be of type str",
    ):
        validate_enterprise_config(cfg)


def test_password_auth_both_passwords_present():
    """Raises when both password and password_env_var are present."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="must not define both 'password' and 'password_env_var'",
    ):
        validate_enterprise_config(_valid_config(password_env_var="ENV"))


def test_password_env_var_valid():
    """password_env_var alone (without password) is valid."""
    validate_enterprise_config(
        {
            "system_name": "sys",
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "u",
            "password_env_var": "MY_PASSWORD",
        }
    )


# --- private_key auth ---


def test_private_key_auth_missing_key():
    """Raises when private_key auth is missing private_key_path."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="must define 'private_key_path'",
    ):
        validate_enterprise_config(
            {
                "system_name": "sys",
                "connection_json_url": "https://test.example.com/iris/connection.json",
                "auth_type": "private_key",
            }
        )


def test_private_key_auth_invalid_key_type():
    """Raises when private_key_path has wrong type."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="'private_key_path'.*must be of type str",
    ):
        validate_enterprise_config(
            {
                "system_name": "sys",
                "connection_json_url": "https://test.example.com/iris/connection.json",
                "auth_type": "private_key",
                "private_key_path": 123,
            }
        )


def test_auth_type_logic_unknown_type_is_noop():
    """auth types without specific validation rules are silently skipped.

    _validate_enterprise_system_auth_type_logic is designed to be extended with
    additional elif branches; unknown auth types fall through without raising.
    """
    from deephaven_mcp.config._enterprise import (
        _validate_enterprise_system_auth_type_logic,
    )

    _validate_enterprise_system_auth_type_logic("sys", {}, "saml")


# --- unknown fields ---


def test_unknown_key_logs_warning_not_error(caplog):
    """Unknown fields log a warning but do not raise."""
    validate_enterprise_config(_valid_config(totally_unknown_field="xyz"))
    assert any(
        "totally_unknown_field" in r.message
        for r in caplog.records
        if r.levelname == "WARNING"
    )


def test_unknown_key_warning_for_private_key_with_password(caplog):
    """For private_key auth, 'password' is an unknown field and generates a warning."""
    validate_enterprise_config(
        {
            "system_name": "sys",
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "private_key",
            "private_key_path": "/path/key.pem",
            "password": "secret",
        }
    )
    assert any(
        "Unknown field 'password'" in r.message
        for r in caplog.records
        if r.levelname == "WARNING"
    )


# --- connection_timeout ---


def test_connection_timeout_valid_int():
    """connection_timeout with integer value passes."""
    validate_enterprise_config(_valid_config(connection_timeout=5))


def test_connection_timeout_valid_float():
    """connection_timeout with float value passes."""
    validate_enterprise_config(_valid_config(connection_timeout=10.5))


def test_connection_timeout_missing_is_valid():
    """Missing connection_timeout is valid (optional)."""
    validate_enterprise_config(_valid_config())


def test_connection_timeout_zero_invalid():
    """connection_timeout=0 raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="must be positive, but got 0",
    ):
        validate_enterprise_config(_valid_config(connection_timeout=0))


def test_connection_timeout_negative_invalid():
    """Negative connection_timeout raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="must be positive, but got -5",
    ):
        validate_enterprise_config(_valid_config(connection_timeout=-5))


def test_connection_timeout_wrong_type_string():
    """String connection_timeout raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Optional field 'connection_timeout'.*must be one of types \(int, float\), but got str",
    ):
        validate_enterprise_config(_valid_config(connection_timeout="10"))


def test_connection_timeout_wrong_type_bool():
    """Bool connection_timeout raises (bool is a subclass of int but explicitly rejected)."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'connection_timeout'.*must be a number \(int or float\), but got bool",
    ):
        validate_enterprise_config(_valid_config(connection_timeout=True))


def test_connection_timeout_very_small_float_valid():
    """Very small positive float is valid."""
    validate_enterprise_config(_valid_config(connection_timeout=0.01))


# --- session_creation ---


def test_session_creation_absent_is_valid():
    """Missing session_creation is valid — the section is optional."""
    validate_enterprise_config(_valid_config())


def test_session_creation_missing_defaults_raises():
    """session_creation present but without defaults raises — defaults is required when section present."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'session_creation.defaults' is required.*but is missing",
    ):
        validate_enterprise_config(_valid_config(session_creation={}))


def test_session_creation_missing_heap_size_raises():
    """session_creation.defaults without heap_size_gb raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'session_creation.defaults.heap_size_gb' is required.*but is missing",
    ):
        validate_enterprise_config(_valid_config(session_creation={"defaults": {}}))


def test_session_creation_valid_minimal():
    """session_creation with heap_size_gb passes."""
    validate_enterprise_config(
        _valid_config(session_creation={"defaults": {"heap_size_gb": 4}})
    )


def test_session_creation_valid_with_max_workers():
    """session_creation with max_concurrent_sessions passes."""
    validate_enterprise_config(
        _valid_config(
            session_creation={
                "max_concurrent_sessions": 10,
                "defaults": {"heap_size_gb": 4},
            }
        )
    )


def test_session_creation_valid_max_workers_zero():
    """session_creation max_concurrent_sessions=0 (disable) is valid."""
    validate_enterprise_config(
        _valid_config(
            session_creation={
                "max_concurrent_sessions": 0,
                "defaults": {"heap_size_gb": 4},
            }
        )
    )


def test_session_creation_invalid_max_workers_negative():
    """Negative max_concurrent_sessions raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'max_concurrent_sessions'.*must be a non-negative integer, but got -1",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "max_concurrent_sessions": -1,
                    "defaults": {"heap_size_gb": 4},
                }
            )
        )


def test_session_creation_invalid_not_dict():
    """session_creation that is not a dict raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Optional field 'session_creation'.*must be of type dict",
    ):
        validate_enterprise_config(_valid_config(session_creation="bad"))


def test_session_creation_invalid_defaults_not_dict():
    """session_creation.defaults that is not a dict raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'defaults' in session_creation.*must be a dictionary",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={"max_concurrent_sessions": 5, "defaults": "bad"}
            )
        )


def test_session_creation_invalid_heap_size_wrong_type():
    """Invalid heap_size_gb type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'heap_size_gb'.*must be one of types \(int, float\)",
    ):
        validate_enterprise_config(
            _valid_config(session_creation={"defaults": {"heap_size_gb": "invalid"}})
        )


def test_session_creation_valid_heap_size_float():
    """Float heap_size_gb passes."""
    validate_enterprise_config(
        _valid_config(session_creation={"defaults": {"heap_size_gb": 2.5}})
    )


def test_session_creation_invalid_extra_jvm_args_wrong_type():
    """Invalid extra_jvm_args type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'extra_jvm_args'.*must be of type list",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "extra_jvm_args": "bad"}
                }
            )
        )


def test_session_creation_invalid_admin_groups_wrong_type():
    """Invalid admin_groups type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'admin_groups'.*must be of type list",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "admin_groups": "bad"}
                }
            )
        )


def test_session_creation_invalid_viewer_groups_wrong_type():
    """Invalid viewer_groups type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'viewer_groups'.*must be of type list",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "viewer_groups": "bad"}
                }
            )
        )


def test_session_creation_invalid_timeout_seconds_wrong_type():
    """Invalid timeout_seconds type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'timeout_seconds'.*must be one of types \(int, float\)",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "timeout_seconds": "bad"}
                }
            )
        )


def test_session_creation_invalid_session_arguments_wrong_type():
    """Invalid session_arguments type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'session_arguments'.*must be of type dict",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "session_arguments": "bad"}
                }
            )
        )


def test_session_creation_invalid_extra_environment_vars_wrong_type():
    """Invalid extra_environment_vars type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'extra_environment_vars'.*must be of type list",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "extra_environment_vars": "bad"}
                }
            )
        )


def test_session_creation_invalid_programming_language_wrong_type():
    """Invalid programming_language type raises."""
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'programming_language'.*must be of type str",
    ):
        validate_enterprise_config(
            _valid_config(
                session_creation={
                    "defaults": {"heap_size_gb": 4, "programming_language": 123}
                }
            )
        )


def test_session_creation_valid_all_defaults():
    """session_creation with all valid default parameters passes."""
    validate_enterprise_config(
        _valid_config(
            session_creation={
                "max_concurrent_sessions": 3,
                "defaults": {
                    "heap_size_gb": 8,
                    "auto_delete_timeout": 600,
                    "server": "gpu-server-1",
                    "engine": "DeephavenCommunity",
                    "extra_jvm_args": ["-XX:+UseG1GC"],
                    "extra_environment_vars": ["PYTHONPATH=/custom/libs"],
                    "admin_groups": ["deephaven-admins"],
                    "viewer_groups": ["analysts"],
                    "timeout_seconds": 120.0,
                    "session_arguments": {"custom_setting": "example_value"},
                    "programming_language": "Groovy",
                },
            }
        )
    )


def test_session_creation_valid_session_arguments():
    """session_arguments dict with various values passes."""
    validate_enterprise_config(
        _valid_config(
            session_creation={
                "defaults": {"heap_size_gb": 4, "session_arguments": {"a": 1, "b": "x"}}
            }
        )
    )


# --- tuple-type validation in _validate_required_fields ---


def test_validate_enterprise_config_tuple_type_in_base_field(monkeypatch):
    """Tuple type validation in _validate_required_fields raises correct message."""
    monkeypatch.setitem(
        _BASE_ENTERPRISE_SYSTEM_FIELDS, "test_tuple_field", (str, type(None))
    )
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'test_tuple_field'.*must be one of types \(str, NoneType\), but got int",
    ):
        validate_enterprise_config(_valid_config(test_tuple_field=123))


def test_validate_enterprise_config_tuple_type_validation_error_optional(monkeypatch):
    """Tuple type validation in _validate_optional_fields raises correct message."""
    from deephaven_mcp.config import _enterprise

    monkeypatch.setitem(
        _enterprise._OPTIONAL_ENTERPRISE_SYSTEM_FIELDS, "test_tuple_field", (str, int)
    )
    with pytest.raises(
        EnterpriseSystemConfigurationError, match="must be one of types"
    ):
        validate_enterprise_config(_valid_config(test_tuple_field=[]))


def test_validate_enterprise_config_tuple_type_auth_specific(monkeypatch):
    """Tuple type validation in auth-specific fields raises correct message."""
    monkeypatch.setitem(
        _AUTH_SPECIFIC_FIELDS["private_key"], "test_tuple_field", (str, int, type(None))
    )
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"'test_tuple_field'.*must be one of types \(str, int, NoneType\), but got float",
    ):
        validate_enterprise_config(
            {
                "system_name": "sys",
                "connection_json_url": "https://test.example.com/iris/connection.json",
                "auth_type": "private_key",
                "private_key_path": "/path/key",
                "test_tuple_field": 3.14,
            }
        )


# ---------------------------------------------------------------------------
# redact_enterprise_system_config
# ---------------------------------------------------------------------------


def test_redact_enterprise_system_config_with_password():
    """Redacts password field."""
    config = {
        "connection_json_url": "http://test",
        "auth_type": "password",
        "username": "test_user",
        "password": "sensitive_password",
    }
    redacted = redact_enterprise_system_config(config)
    assert redacted["password"] == "[REDACTED]"
    assert redacted["connection_json_url"] == "http://test"
    assert redacted["auth_type"] == "password"
    assert redacted["username"] == "test_user"
    assert config["password"] == "sensitive_password"  # original unchanged


def test_redact_enterprise_system_config_without_password():
    """No-op redaction when password field absent."""
    config = {
        "connection_json_url": "http://test",
        "auth_type": "private_key",
        "private_key_path": "/path/to/key",
    }
    redacted = redact_enterprise_system_config(config)
    assert redacted == config
    assert redacted is not config  # shallow copy


def test_redact_enterprise_system_config_empty():
    """Redacting empty config returns empty dict (not same object)."""
    config = {}
    redacted = redact_enterprise_system_config(config)
    assert redacted == {}
    assert redacted is not config


# ---------------------------------------------------------------------------
# _validate_field_type
# ---------------------------------------------------------------------------


def test_validate_field_type_valid_string():
    _validate_field_type("sys", "f", "value", str)


def test_validate_field_type_valid_int():
    _validate_field_type("sys", "f", 42, int)


def test_validate_field_type_valid_list():
    _validate_field_type("sys", "f", ["a"], list)


def test_validate_field_type_valid_union():
    _validate_field_type("sys", "f", "value", (str, list))
    _validate_field_type("sys", "f", ["a"], (str, list))


def test_validate_field_type_invalid_single():
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Field 'f' for enterprise system 'sys' must be of type str, but got int\.",
    ):
        _validate_field_type("sys", "f", 42, str)


def test_validate_field_type_invalid_union():
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Field 'f' for enterprise system 'sys' must be one of types \(str, list\), but got int\.",
    ):
        _validate_field_type("sys", "f", 42, (str, list))


# ---------------------------------------------------------------------------
# _validate_required_fields
# ---------------------------------------------------------------------------


def test_validate_required_fields_valid():
    config = {
        "system_name": "test_system",
        "connection_json_url": "https://test.com",
        "auth_type": "password",
        "username": "user",
    }
    _validate_required_fields("test_system", config)


def test_validate_required_fields_missing_field():
    config = {"system_name": "test_system", "connection_json_url": "https://test.com"}
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Required field 'auth_type' missing in enterprise system 'test_system'\.",
    ):
        _validate_required_fields("test_system", config)


def test_validate_required_fields_wrong_type():
    config = {
        "system_name": "test_system",
        "connection_json_url": "https://test.com",
        "auth_type": 123,
    }
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Field 'auth_type' for enterprise system 'test_system' must be of type str, but got int\.",
    ):
        _validate_required_fields("test_system", config)


# ---------------------------------------------------------------------------
# _validate_optional_fields
# ---------------------------------------------------------------------------


def test_validate_optional_fields_valid():
    config = {
        "connection_json_url": "https://test.com",
        "auth_type": "password",
        "session_creation": {"max_concurrent_sessions": 5},
    }
    _validate_optional_fields("sys", config)


def test_validate_optional_fields_wrong_type():
    config = {
        "connection_json_url": "https://test.com",
        "auth_type": "password",
        "session_creation": "not_a_dict",
    }
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=r"Optional field 'session_creation' for enterprise system 'sys' must be of type dict, but got str\.",
    ):
        _validate_optional_fields("sys", config)


def test_validate_optional_fields_ignores_missing():
    """Missing optional fields are silently ignored."""
    config = {"connection_json_url": "https://test.com", "auth_type": "password"}
    _validate_optional_fields("sys", config)


# ---------------------------------------------------------------------------
# EnterpriseServerConfigManager Tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_env():
    old = os.environ.get(CONFIG_ENV_VAR)
    if CONFIG_ENV_VAR in os.environ:
        del os.environ[CONFIG_ENV_VAR]
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


def _flat_config(**kwargs) -> dict:
    base = {
        "system_name": "prod",
        "connection_json_url": "https://dhe.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
    }
    base.update(kwargs)
    return base


@pytest.mark.asyncio
async def test_get_config_returns_flat_config_directly():
    manager = EnterpriseServerConfigManager(config_path="/fake/path.json")
    flat_config = _flat_config()

    with patch(
        "deephaven_mcp.config._base._load_config_from_file",
        return_value=flat_config,
    ):
        result = await manager.get_config()

    assert result == flat_config


@pytest.mark.asyncio
async def test_get_config_caches_result():
    manager = EnterpriseServerConfigManager(config_path="/fake/path.json")
    flat_config = _flat_config()

    with (
        patch(
            "deephaven_mcp.config._base._load_config_from_file",
            return_value=flat_config,
        ) as mock_load,
        patch("deephaven_mcp.config._enterprise.validate_enterprise_config"),
    ):
        first = await manager.get_config()
        second = await manager.get_config()

    assert first is second
    mock_load.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_config_calls_validate_enterprise_config():
    manager = EnterpriseServerConfigManager(config_path="/fake/path.json")
    flat_config = _flat_config()

    with (
        patch(
            "deephaven_mcp.config._base._load_config_from_file",
            return_value=flat_config,
        ),
        patch(
            "deephaven_mcp.config._enterprise.validate_enterprise_config",
        ) as mock_validate,
    ):
        await manager.get_config()

    mock_validate.assert_called_once_with(flat_config)


@pytest.mark.asyncio
async def test_get_config_raises_when_system_name_missing():
    flat_config = {
        "connection_json_url": "https://dhe.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password": "pass",
    }
    manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

    with (
        patch(
            "deephaven_mcp.config._base._load_config_from_file",
            return_value=flat_config,
        ),
        pytest.raises(ConfigurationError),
    ):
        await manager.get_config()


@pytest.mark.asyncio
async def test_get_config_raises_when_system_name_wrong_type():
    flat_config = _flat_config(system_name=42)
    manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

    with (
        patch(
            "deephaven_mcp.config._base._load_config_from_file",
            return_value=flat_config,
        ),
        pytest.raises(ConfigurationError),
    ):
        await manager.get_config()


@pytest.mark.asyncio
async def test_get_config_falls_back_to_env_var_when_no_path(monkeypatch):
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/env/path.json")
    flat_config = _flat_config()
    manager = EnterpriseServerConfigManager()  # no config_path

    with (
        patch(
            "deephaven_mcp.config._base._load_config_from_file",
            return_value=flat_config,
        ) as mock_load,
        patch("deephaven_mcp.config._enterprise.validate_enterprise_config"),
    ):
        await manager.get_config()

    call_path = mock_load.call_args[0][0]
    assert call_path == "/env/path.json"


@pytest.mark.asyncio
async def test_get_config_does_not_log_password_in_plaintext():
    flat_config = _flat_config(password="supersecret")
    manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

    logged_calls = []

    def capture_log_summary(config, label=None, redactor=None):
        logged_calls.append({"config": config, "label": label, "redactor": redactor})

    with (
        patch(
            "deephaven_mcp.config._base._load_config_from_file",
            return_value=flat_config,
        ),
        patch(
            "deephaven_mcp.config._enterprise._log_config_summary",
            side_effect=capture_log_summary,
        ),
    ):
        await manager.get_config()

    assert len(logged_calls) == 1
    call = logged_calls[0]
    assert (
        call["redactor"] is not None
    ), "redactor must be passed to _log_config_summary"
    redacted = call["redactor"](call["config"])
    assert (
        redacted.get("password") == "[REDACTED]"
    ), "redactor must mask password before logging"


@pytest.mark.asyncio
async def test_set_config_cache_accepts_valid_enterprise_config():
    valid_config = _flat_config()
    manager = EnterpriseServerConfigManager()
    await manager._set_config_cache(valid_config)

    result = await manager.get_config()
    assert result == valid_config


@pytest.mark.asyncio
async def test_set_config_cache_rejects_invalid_enterprise_config():
    manager = EnterpriseServerConfigManager()
    with pytest.raises(EnterpriseSystemConfigurationError):
        await manager._set_config_cache({})


@pytest.mark.asyncio
async def test_set_config_cache_rejects_community_format():
    manager = EnterpriseServerConfigManager()
    with pytest.raises(EnterpriseSystemConfigurationError):
        await manager._set_config_cache({"sessions": {}})
