"""
Comprehensive test suite for deephaven_mcp.config (public/private functions, 100% coverage, up-to-date with latest refactor).
"""

import asyncio
import json
import logging
import os
import re
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import aiofiles
import pytest

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    ConfigurationError,
    EnterpriseSystemConfigurationError,
)
import deephaven_mcp.config._community as _community_module

from deephaven_mcp.config import (
    CONFIG_ENV_VAR,
    ConfigManager,
    CommunityServerConfigManager,
    _load_config_from_file,
    _log_config_summary,
)
from deephaven_mcp.config._community import (
    _get_all_config_names,
    _get_config_section,
    _validate_community_config,
)

# Aliases to match old public API names used throughout tests
get_all_config_names = _get_all_config_names
get_config_section = _get_config_section
validate_community_config = _validate_community_config


# --- Fixtures and helpers ---
@pytest.fixture
def valid_community_config():
    return {
        "community": {
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
    }



@pytest.fixture(autouse=True)
def clear_env():
    old = os.environ.get(CONFIG_ENV_VAR)
    if CONFIG_ENV_VAR in os.environ:
        del os.environ[CONFIG_ENV_VAR]
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


# --- Top-level config validation ---
def test_validate_community_config_accepts_empty():
    assert validate_community_config({}) == {}


def test_validate_community_config_accepts_community_only(valid_community_config):
    assert validate_community_config(valid_community_config) == valid_community_config


def test_validate_community_config_rejects_enterprise_key():
    """Community server config must not contain an 'enterprise' key."""
    with pytest.raises(ConfigurationError):
        validate_community_config({"enterprise": {"systems": {}}})


def test_validate_community_config_rejects_unknown_top_level():
    with pytest.raises(ConfigurationError):
        validate_community_config({"foo": {}})


# --- Community session validation ---
from deephaven_mcp.config._community import (
    redact_community_session_config,
    validate_community_sessions_config,
    validate_single_community_session_config,
)


def test_validate_community_sessions_config_valid_empty_sessions(caplog):
    """Tests that validate_community_sessions_config allows a dict with an empty sessions map."""
    validate_community_sessions_config({"sessions": {}})


def test_validate_community_sessions_config_missing_sessions_key(caplog):
    """Tests that validate_community_sessions_config fails if 'sessions' key is missing."""
    with pytest.raises(CommunitySessionConfigurationError):
        validate_community_sessions_config({"foo": "bar"})


def test_validate_community_sessions_config_invalid_session_item(caplog):
    """Tests that validate_community_sessions_config fails for an invalid session item."""
    with pytest.raises(CommunitySessionConfigurationError):
        validate_community_sessions_config({"sessions": {"foo": []}})


def test_validate_community_sessions_config_unknown_field(caplog):
    """Tests that validate_community_sessions_config fails for an unknown field."""
    with pytest.raises(CommunitySessionConfigurationError):
        validate_community_sessions_config(
            {"sessions": {"foo": {"host": "localhost", "bad": 1}}}
        )


def test_community_sessions_wrong_type():
    with pytest.raises(CommunitySessionConfigurationError):
        validate_single_community_session_config("foo", {"host": 1})


def test_community_sessions_mutual_exclusive_auth_token():
    with pytest.raises(CommunitySessionConfigurationError):
        validate_single_community_session_config(
            "foo", {"auth_token": "a", "auth_token_env_var": "b"}
        )


def test_community_sessions_redact():
    d = {"auth_token": "secret", "host": "localhost"}
    redacted = redact_community_session_config(d)
    assert redacted["auth_token"] == "[REDACTED]"
    assert redacted["host"] == "localhost"


# --- Enterprise system validation ---
from deephaven_mcp.config._enterprise import (
    _validate_and_get_auth_type,
    redact_enterprise_system_config,
    validate_enterprise_config,
)


def test_enterprise_systems_missing_connection_json_url():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "auth_type": "password"}
        )


def test_enterprise_systems_invalid_connection_json_url_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": 1, "auth_type": "password"}
        )


def test_enterprise_systems_missing_auth_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url"}
        )


def test_enterprise_systems_invalid_auth_type_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": 1}
        )


def test_enterprise_systems_unknown_auth_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "badtype"}
        )


def test_enterprise_systems_unknown_key():
    # Should log a warning but not raise
    validate_enterprise_config(
        {
            "system_name": "foo",
            "connection_json_url": "url",
            "auth_type": "password",
            "username": "u",
            "password": "p",
            "bad": 1,
        }
    )


def test_enterprise_systems_password_auth_missing_username():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "password", "password": "p"}
        )


def test_enterprise_systems_password_auth_invalid_username_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "password", "username": 1, "password": "p"}
        )


def test_enterprise_systems_password_auth_missing_password_keys():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "password", "username": "u"}
        )


def test_enterprise_systems_password_auth_invalid_password_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "password", "username": "u", "password": 1}
        )


def test_enterprise_systems_password_auth_invalid_password_env_var_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "password", "username": "u", "password_env_var": 1}
        )


def test_enterprise_systems_password_auth_both_passwords_present():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "password", "username": "u", "password": "p", "password_env_var": "env"}
        )


def test_enterprise_systems_private_key_auth_missing_key():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "private_key"}
        )


def test_enterprise_systems_private_key_auth_invalid_key_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config(
            {"system_name": "foo", "connection_json_url": "url", "auth_type": "private_key", "private_key_path": 1}
        )


def test_enterprise_systems_redact():
    d = {"password": "secret", "connection_json_url": "url"}
    redacted = redact_enterprise_system_config(d)
    assert redacted["password"] == "[REDACTED]"
    assert redacted["connection_json_url"] == "url"


def test_validate_and_get_auth_type_invalid():
    with pytest.raises(EnterpriseSystemConfigurationError):
        _validate_and_get_auth_type(
            "foo", {"connection_json_url": "url", "auth_type": "badtype"}
        )


# --- validate_enterprise_config coverage ---


def test_enterprise_config_non_dict_rejected():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_config("not a dict")


def test_enterprise_config_missing_system_name():
    with pytest.raises(EnterpriseSystemConfigurationError, match="system_name"):
        validate_enterprise_config(
            {"connection_json_url": "u", "auth_type": "password", "username": "u", "password": "p"}
        )


def test_enterprise_config_invalid_system_name_type():
    with pytest.raises(EnterpriseSystemConfigurationError, match="system_name"):
        validate_enterprise_config(
            {"system_name": 42, "connection_json_url": "u", "auth_type": "password", "username": "u", "password": "p"}
        )


def test_enterprise_config_valid_password_auth():
    result = validate_enterprise_config(
        {
            "system_name": "prod",
            "connection_json_url": "https://host/iris/connection.json",
            "auth_type": "password",
            "username": "u",
            "password": "p",
        }
    )
    assert result["system_name"] == "prod"


def test_enterprise_config_valid_private_key_auth():
    result = validate_enterprise_config(
        {
            "system_name": "prod",
            "connection_json_url": "https://host/iris/connection.json",
            "auth_type": "private_key",
            "private_key_path": "/key.pem",
        }
    )
    assert result["system_name"] == "prod"


def test_enterprise_config_connection_timeout_valid():
    validate_enterprise_config(
        {
            "system_name": "s",
            "connection_json_url": "u",
            "auth_type": "private_key",
            "private_key_path": "/k",
            "connection_timeout": 30.0,
        }
    )


def test_enterprise_config_connection_timeout_bool_rejected():
    with pytest.raises(EnterpriseSystemConfigurationError, match="connection_timeout"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "connection_timeout": True,
            }
        )


def test_enterprise_config_connection_timeout_string_rejected():
    with pytest.raises(EnterpriseSystemConfigurationError, match="connection_timeout"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "connection_timeout": "30",
            }
        )


def test_enterprise_config_connection_timeout_zero_rejected():
    with pytest.raises(EnterpriseSystemConfigurationError, match="connection_timeout"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "connection_timeout": 0,
            }
        )


def test_enterprise_config_session_creation_non_dict_rejected():
    with pytest.raises(EnterpriseSystemConfigurationError, match="session_creation"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "session_creation": "bad",
            }
        )


def test_enterprise_config_session_creation_max_sessions_invalid():
    with pytest.raises(EnterpriseSystemConfigurationError, match="max_concurrent_sessions"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "session_creation": {"max_concurrent_sessions": "5"},
            }
        )


def test_enterprise_config_session_creation_max_sessions_negative():
    with pytest.raises(EnterpriseSystemConfigurationError, match="max_concurrent_sessions"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "session_creation": {"max_concurrent_sessions": -1},
            }
        )


def test_enterprise_config_session_creation_defaults_non_dict():
    with pytest.raises(EnterpriseSystemConfigurationError, match="defaults"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "session_creation": {"defaults": "bad"},
            }
        )


def test_enterprise_config_session_creation_defaults_invalid_heap_size():
    with pytest.raises(EnterpriseSystemConfigurationError, match="heap_size_gb"):
        validate_enterprise_config(
            {
                "system_name": "s",
                "connection_json_url": "u",
                "auth_type": "private_key",
                "private_key_path": "/k",
                "session_creation": {"defaults": {"heap_size_gb": "4"}},
            }
        )


def test_enterprise_config_session_creation_valid():
    validate_enterprise_config(
        {
            "system_name": "s",
            "connection_json_url": "u",
            "auth_type": "private_key",
            "private_key_path": "/k",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4, "programming_language": "Python"},
            },
        }
    )


# --- Coverage gap: community validator error wrapping ---


def test_validate_community_config_wraps_session_error_as_config_error():
    """CommunitySessionConfigurationError from nested validator is wrapped as ConfigurationError."""
    with pytest.raises(ConfigurationError):
        validate_community_config({"community": {"sessions": {"x": {"host": 1}}}})


def test_validate_community_config_raises_on_wrong_type_for_schema_key():
    """ConfigurationError raised when a schema-defined key has the wrong type."""
    with pytest.raises(ConfigurationError, match="must be of type dict"):
        validate_community_config({"community": "not_a_dict"})


# --- Coverage gap: _log_config_summary JSON serialization failure ---


def test_log_config_summary_handles_json_serialization_failure(caplog):
    with patch("deephaven_mcp.config.json5.dumps", side_effect=TypeError("not serializable")):
        with caplog.at_level("WARNING"):
            _log_config_summary({})
    assert "Failed to format config as JSON" in caplog.text


# --- ConfigManager cache/async/IO ---


@pytest.mark.asyncio
async def test_get_config_other_os_error_on_read(monkeypatch, caplog):
    config_file_path = "/fake/path/config_for_os_error_read.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)

    mock_file_read = mock.AsyncMock(side_effect=os.error("Simulated OS error on read"))
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

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
    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_validate_community_config_missing_required_key_bytes_mode(caplog, monkeypatch):
    # Variant that reads bytes from the file mock (exercises the bytes→str path through json5.loads)
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
        cm = config.CommunityServerConfigManager()
        invalid_config_data = {
            "community": {"sessions": {}}
        }  # Missing 'must_have_this'

        config_file_path = "/fake/path/config_missing_req.json"
        monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)

        mock_file_read_content = mock.AsyncMock(
            return_value=json.dumps(invalid_config_data).encode("utf-8")
        )
        mock_async_context_manager_req = mock.AsyncMock()
        mock_async_context_manager_req.__aenter__.return_value.read = (
            mock_file_read_content
        )

        original_aio_open_req = aiofiles.open
        aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager_req)

        with pytest.raises(
            ConfigurationError,
            match=re.escape(
                "Error loading configuration file: Missing required keys at config path (): {'must_have_this'}"
            ),
        ):
            await cm.get_config()  # This will load, then validate

        assert (
            "Missing required keys at config path (): {'must_have_this'}" in caplog.text
        )

        aiofiles.open = original_aio_open_req


@pytest.mark.asyncio
async def test_get_config_uses_cache_and_logs(monkeypatch, caplog):
    config_file_path = "/fake/path/config_for_cache_test.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)
    valid_config_data = {
        "community": {"sessions": {"test_session": {"host": "localhost"}}}
    }

    # Mock aiofiles.open to be called only once for the read
    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(valid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    # Keep track of the original aiofiles.open
    original_aio_open = aiofiles.open
    # Create a mock for aiofiles.open that we can assert call counts on
    aiofiles_open_mock = mock.MagicMock(return_value=mock_async_context_manager)
    aiofiles.open = aiofiles_open_mock

    cm = CommunityServerConfigManager()
    # First call - should load from file
    config1 = await cm.get_config()
    assert valid_config_data == config1
    assert (
        "Returning cached Deephaven MCP application configuration." not in caplog.text
    )
    aiofiles_open_mock.assert_called_once_with(config_file_path)

    caplog.clear()
    # Set log level to DEBUG for the relevant logger to capture the cache message
    caplog.set_level(logging.DEBUG, logger="deephaven_mcp.config")
    # Second call - should use cache
    config2 = await cm.get_config()
    assert config1 is config2  # Should be the same object from cache
    assert "Using cached Deephaven MCP application configuration." in caplog.text
    # Ensure aiofiles.open was not called again
    aiofiles_open_mock.assert_called_once()  # Still called only once in total

    # Restore original aiofiles.open
    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_get_config_unknown_top_level_key(monkeypatch, caplog):
    config_file_path = "/fake/path/config_unknown_key.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {"some_unknown_key": {}, "community": {"sessions": {}}}

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = CommunityServerConfigManager()
    with pytest.raises(
        ConfigurationError,
        match=re.escape(
            r"Error loading configuration file: Unknown keys at config path (): {'some_unknown_key'}"
        ),
    ):
        await cm.get_config()

    assert r"Unknown keys at config path (): {'some_unknown_key'}" in caplog.text

    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_get_config_invalid_community_session_schema_from_file(
    monkeypatch, caplog
):
    config_file_path = "/fake/path/invalid_community_schema.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {
        "community": {
            "sessions": {
                "bad_session": {
                    "host": 12345,  # Invalid type, should be string
                    "port": "not-a-port",
                }
            }
        }
    }

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = CommunityServerConfigManager()
    # The error will now come from validate_community_sessions_config via validate_community_config
    expected_error_pattern = re.escape(
        "Error loading configuration file: Invalid configuration for community.sessions: Field 'host' in community session config for bad_session must be of type str, got int"
    )

    with pytest.raises(
        ConfigurationError,
        match=expected_error_pattern,
    ):
        await cm.get_config()

    aiofiles.open = original_aio_open


# --- Cache and worker config tests ---

import pytest


def test_config_manager_base_is_abstract():
    """ConfigManager is abstract — direct instantiation must raise TypeError."""
    with pytest.raises(TypeError):
        ConfigManager()


def test_get_config_section_invalid_section():
    cm = CommunityServerConfigManager()
    cm._cache = {"community_sessions": {}}
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['not_a_section', 'foo'] does not exist in configuration"
        ),
    ):
        get_config_section(cm._cache, ["not_a_section", "foo"])


@pytest.mark.asyncio
async def test_config_manager_set_and_clear_cache():
    cm = CommunityServerConfigManager()
    await cm._set_config_cache({"community": {"sessions": {"a_session": {}}}})
    cfg1 = await cm.get_config()
    assert "a_session" in cfg1["community"]["sessions"]
    await cm.clear_config_cache()
    await cm._set_config_cache({"community": {"sessions": {"b_session": {}}}})
    cfg2 = await cm.get_config()
    assert "b_session" in cfg2["community"]["sessions"]
    assert "a_session" not in cfg2["community"]["sessions"]


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
    config_data = {
        "community": {"sessions": {}}
    }  # Use nested format, missing 'must_have_this'
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
        with pytest.raises(
            ConfigurationError,
            match=expected_error,
        ):
            await cm.get_config()


def test_get_all_config_names_returns_keys():
    cfg = {
        "community": {
            "sessions": {"a": {"host": "localhost"}, "b": {"host": "localhost"}}
        }
    }
    names = get_all_config_names(cfg, ["community", "sessions"])
    assert set(names) == {"a", "b"}

    cfg2 = {"community": {"sessions": {}}}
    names2 = get_all_config_names(cfg2, ["community", "sessions"])
    assert names2 == []

    cfg3 = {"community": {"sessions": {}}}
    names3 = get_all_config_names(cfg3, ["enterprise", "systems"])
    assert names3 == []


def test_get_all_config_names_not_dict_raises():
    cfg = {"community_sessions": "not_a_dict"}
    result = get_all_config_names(cfg, ["community_sessions"])
    assert result == []  # Should return empty list for non-dict sections


def test_named_config_missing():
    cfg = {"community": {"sessions": {"foo": {"host": "localhost"}}}}
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['community', 'sessions', 'bar'] does not exist in configuration"
        ),
    ):
        get_config_section(cfg, ["community", "sessions", "bar"])


def test_get_all_config_names_returns_empty_for_non_dict_section(caplog):
    cfg = {"not_a_section": "not_a_dict"}
    caplog.set_level("WARNING", logger="deephaven_mcp.config._community")
    result = get_all_config_names(cfg, ["not_a_section"])
    assert result == []
    assert (
        "Section at path ['not_a_section'] is not a dictionary, returning empty list of names."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_get_config_no_community_sessions_key_from_file(monkeypatch, caplog):
    # Prepare an empty config JSON string
    empty_config_json = "{}"

    # Patch environment variable
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
    assert cfg == {}  # Expect an empty dictionary
    assert cm._cache == {}
    # Check for the new log messages for empty config
    log_text = caplog.text
    assert "Configuration validation passed." in log_text
    assert "Configuration summary:" in log_text
    assert "Loaded configuration:\n{}" in log_text
    session_names = get_all_config_names(cfg, ["community", "sessions"])
    assert session_names == []

    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['community', 'sessions', 'any_session_name'] does not exist in configuration"
        ),
    ):
        get_config_section(cfg, ["community", "sessions", "any_session_name"])


# --- New tests for uncovered exception handling paths ---


@pytest.mark.asyncio
async def test_config_file_not_found_error(monkeypatch):
    """Test FileNotFoundError handling in _load_config_from_file."""
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/nonexistent/path/config.json")

    cm = CommunityServerConfigManager()
    await cm.clear_config_cache()

    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        await cm.get_config()


@pytest.mark.asyncio
async def test_config_permission_error(monkeypatch):
    """Test PermissionError handling in _load_config_from_file."""
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
    """Test JSONDecodeError handling in _load_config_from_file."""
    # Invalid JSON content
    invalid_json = "{ invalid json content"

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    # Mock aiofiles.open
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


def test_validate_community_config_non_dict():
    """Test validation error when config is not a dictionary."""
    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        validate_community_config("not a dict")

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        validate_community_config(123)

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        validate_community_config(["list", "not", "dict"])


@pytest.mark.asyncio
async def test_json_formatting_error_in_log_config_summary(monkeypatch, caplog):
    """Test JSON formatting error handling in _log_config_summary."""
    # Valid config JSON
    valid_config_json = '{"community": {"sessions": {}}}'

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = valid_config_json

    # Mock json5.dumps to raise an error (simulating non-serializable config)
    def mock_json5_dumps(*args, **kwargs):
        raise TypeError("Object is not JSON serializable")

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("json5.dumps", side_effect=mock_json5_dumps):
            cm = CommunityServerConfigManager()
            await cm.clear_config_cache()
            with caplog.at_level("WARNING"):
                await cm.get_config()

    # Verify the warning was logged
    assert "Failed to format config as JSON" in caplog.text


@pytest.mark.asyncio
async def test_config_validation_error_in_load_and_validate(monkeypatch):
    """Test configuration validation error handling in load_and_validate_community_config."""
    # Create invalid config that will fail validation
    invalid_config_json = '{"unknown_top_level_key": "invalid"}'

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = invalid_config_json

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(
            ConfigurationError, match="Error loading configuration file"
        ):
            await cm.get_config()


# --- JSON5 support tests ---
@pytest.mark.asyncio
async def test_json5_with_single_line_comments(monkeypatch):
    """Test loading JSON5 configuration with single-line (//) comments."""
    # JSON5 with single-line comments
    json5_content = """{
        // This is a comment about the community section
        "community": {
            "sessions": {
                // Local development session
                "local": {
                    "host": "localhost",  // Server hostname
                    "port": 10000         // Server port
                }
            }
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        config = await cm.get_config()

    # Verify the config was parsed correctly
    assert "community" in config
    assert "sessions" in config["community"]
    assert "local" in config["community"]["sessions"]
    assert config["community"]["sessions"]["local"]["host"] == "localhost"
    assert config["community"]["sessions"]["local"]["port"] == 10000


@pytest.mark.asyncio
async def test_json5_with_multi_line_comments(monkeypatch):
    """Test loading JSON5 configuration with multi-line (/* */) comments."""
    # JSON5 with multi-line comments
    json5_content = """{
        /*
         * Community configuration section
         * Contains all community sessions
         */
        "community": {
            "sessions": {
                /* Production session configuration */
                "prod": {
                    "host": "prod.example.com",
                    "port": 10000
                }
            }
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        config = await cm.get_config()

    # Verify the config was parsed correctly
    assert "community" in config
    assert "sessions" in config["community"]
    assert "prod" in config["community"]["sessions"]
    assert config["community"]["sessions"]["prod"]["host"] == "prod.example.com"


@pytest.mark.asyncio
async def test_json5_with_mixed_comments(monkeypatch):
    """Test loading JSON5 configuration with both single-line and multi-line comments."""
    # JSON5 with mixed comment styles
    json5_content = """{
        /* Community configuration */
        "community": {
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
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        config = await cm.get_config()

    # Verify the config was parsed correctly
    assert "community" in config
    assert config["community"]["sessions"]["local"]["port"] == 10000
    assert config["community"]["session_creation"]["max_concurrent_sessions"] == 5


@pytest.mark.asyncio
async def test_json5_standard_json_still_works(monkeypatch):
    """Test that standard JSON (without comments) still works correctly with json5 parser."""
    # Standard JSON without comments
    json_content = """{
        "community": {
            "sessions": {
                "local": {
                    "host": "localhost",
                    "port": 10000
                }
            }
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        config = await cm.get_config()

    # Verify the config was parsed correctly
    assert "community" in config
    assert config["community"]["sessions"]["local"]["host"] == "localhost"
    assert config["community"]["sessions"]["local"]["port"] == 10000


@pytest.mark.asyncio
async def test_json5_invalid_syntax_raises_error(monkeypatch):
    """Test that invalid JSON5 syntax raises ConfigurationError."""
    # Invalid JSON with unclosed bracket
    invalid_json5 = """{
        "community": {
            "sessions": {
                "local": {
                    "host": "localhost"
                }
            // Missing closing bracket
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    # Mock aiofiles.open
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
    """Test that JSON5 allows trailing commas."""
    # JSON5 with trailing commas
    json5_content = """{
        "community": {
            "sessions": {
                "local": {
                    "host": "localhost",
                    "port": 10000,
                },
            },
        }
    }"""

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json5")

    # Mock aiofiles.open
    mock_aiofiles_open = MagicMock()
    mock_async_context_manager = AsyncMock()
    mock_file_object = AsyncMock()

    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = json5_content

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = CommunityServerConfigManager()
        await cm.clear_config_cache()
        config = await cm.get_config()

    # Verify the config was parsed correctly despite trailing commas
    assert "community" in config
    assert config["community"]["sessions"]["local"]["host"] == "localhost"


# --- EnterpriseServerConfigManager tests ---

class TestEnterpriseServerConfigManager:
    """Tests for EnterpriseServerConfigManager."""

    @pytest.mark.asyncio
    async def test_get_config_returns_flat_config_directly(self):
        """get_config() returns flat enterprise config directly without wrapping."""
        from deephaven_mcp.config import (
            EnterpriseServerConfigManager,
        )

        flat_config = {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

        with patch(
            "deephaven_mcp.config._load_config_from_file",
            return_value=flat_config,
        ):
            result = await manager.get_config()

        assert result == flat_config

    @pytest.mark.asyncio
    async def test_get_config_caches_result(self):
        """Repeated calls return cached config without re-loading the file."""
        from deephaven_mcp.config import (
            EnterpriseServerConfigManager,
        )

        flat_config = {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

        with (
            patch(
                "deephaven_mcp.config._load_config_from_file",
                return_value=flat_config,
            ) as mock_load,
            patch(
                "deephaven_mcp.config.validate_enterprise_config",
            ),
        ):
            first = await manager.get_config()
            second = await manager.get_config()

        assert first is second
        mock_load.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_config_calls_validate_enterprise_config(self):
        """get_config() calls validate_enterprise_config with the flat config."""
        from deephaven_mcp.config import EnterpriseServerConfigManager

        flat_config = {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

        with (
            patch(
                "deephaven_mcp.config._load_config_from_file",
                return_value=flat_config,
            ),
            patch(
                "deephaven_mcp.config.validate_enterprise_config",
            ) as mock_validate,
        ):
            await manager.get_config()

        mock_validate.assert_called_once_with(flat_config)

    @pytest.mark.asyncio
    async def test_get_config_raises_when_system_name_missing(self):
        """get_config() raises ConfigurationError when system_name is absent."""
        from deephaven_mcp.config import EnterpriseServerConfigManager

        flat_config = {
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

        with (
            patch(
                "deephaven_mcp.config._load_config_from_file",
                return_value=flat_config,
            ),
            pytest.raises(ConfigurationError),
        ):
            await manager.get_config()

    @pytest.mark.asyncio
    async def test_get_config_raises_when_system_name_wrong_type(self):
        """get_config() raises ConfigurationError when system_name is not a string."""
        from deephaven_mcp.config import EnterpriseServerConfigManager

        flat_config = {
            "system_name": 42,
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

        with (
            patch(
                "deephaven_mcp.config._load_config_from_file",
                return_value=flat_config,
            ),
            pytest.raises(ConfigurationError),
        ):
            await manager.get_config()

    @pytest.mark.asyncio
    async def test_get_config_falls_back_to_env_var_when_no_path(self, monkeypatch):
        """get_config() uses DH_MCP_CONFIG_FILE env var when config_path is None."""
        from deephaven_mcp.config import (
            EnterpriseServerConfigManager,
        )

        monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/env/path.json")

        flat_config = {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager()  # no config_path

        with (
            patch(
                "deephaven_mcp.config._load_config_from_file",
                return_value=flat_config,
            ) as mock_load,
            patch(
                "deephaven_mcp.config.validate_enterprise_config",
            ),
        ):
            await manager.get_config()

        # The path passed to _load_config_from_file should be the env var value
        call_path = mock_load.call_args[0][0]
        assert call_path == "/env/path.json"

    @pytest.mark.asyncio
    async def test_get_config_does_not_log_password_in_plaintext(self):
        """get_config() must call _log_config_summary with a redactor that hides passwords."""
        from deephaven_mcp.config import EnterpriseServerConfigManager

        flat_config = {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "supersecret",
        }

        manager = EnterpriseServerConfigManager(config_path="/fake/path.json")

        logged_calls = []

        def capture_log_summary(config, label=None, redactor=None):
            logged_calls.append({"config": config, "label": label, "redactor": redactor})

        with (
            patch(
                "deephaven_mcp.config._load_config_from_file",
                return_value=flat_config,
            ),
            patch(
                "deephaven_mcp.config._log_config_summary",
                side_effect=capture_log_summary,
            ),
        ):
            await manager.get_config()

        assert len(logged_calls) == 1
        call = logged_calls[0]
        assert call["redactor"] is not None, "redactor must be passed to _log_config_summary"
        redacted = call["redactor"](call["config"])
        assert redacted.get("password") == "[REDACTED]", (
            "redactor must mask password before logging"
        )

    @pytest.mark.asyncio
    async def test_set_config_cache_accepts_valid_enterprise_config(self):
        """_set_config_cache() accepts a valid flat enterprise config and caches it."""
        from deephaven_mcp.config import EnterpriseServerConfigManager

        valid_config = {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }

        manager = EnterpriseServerConfigManager()
        await manager._set_config_cache(valid_config)

        # get_config() should return the cached value without any file I/O
        result = await manager.get_config()
        assert result == valid_config

    @pytest.mark.asyncio
    async def test_set_config_cache_rejects_invalid_enterprise_config(self):
        """_set_config_cache() raises EnterpriseSystemConfigurationError for an invalid config."""
        from deephaven_mcp._exceptions import EnterpriseSystemConfigurationError
        from deephaven_mcp.config import EnterpriseServerConfigManager

        manager = EnterpriseServerConfigManager()
        with pytest.raises(EnterpriseSystemConfigurationError):
            # Missing all required fields
            await manager._set_config_cache({})

    @pytest.mark.asyncio
    async def test_set_config_cache_rejects_community_format(self):
        """_set_config_cache() rejects community-format config on EnterpriseServerConfigManager."""
        from deephaven_mcp._exceptions import EnterpriseSystemConfigurationError
        from deephaven_mcp.config import EnterpriseServerConfigManager

        manager = EnterpriseServerConfigManager()
        with pytest.raises(EnterpriseSystemConfigurationError):
            # Community format — none of the required enterprise fields are present
            await manager._set_config_cache({"community": {"sessions": {}}})


# --- CommunityServerConfigManager tests ---

class TestCommunityServerConfigManager:
    """Tests for CommunityServerConfigManager."""

    @pytest.mark.asyncio
    async def test_get_config_returns_community_config_unchanged(self):
        """CommunityServerConfigManager.get_config() returns community config as-is."""
        from deephaven_mcp.config import CommunityServerConfigManager

        community_config = {
            "community": {
                "sessions": {
                    "local": {
                        "host": "localhost",
                        "port": 10000,
                        "auth_type": "PSK",
                        "auth_token": "secret",
                    }
                }
            }
        }

        manager = CommunityServerConfigManager(config_path="/fake/dhc.json")

        with patch(
            "aiofiles.open",
            mock.MagicMock(
                return_value=mock.MagicMock(
                    __aenter__=mock.AsyncMock(
                        return_value=mock.MagicMock(
                            read=mock.AsyncMock(
                                return_value='{"community": {"sessions": {"local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "secret"}}}}'
                            )
                        )
                    ),
                    __aexit__=mock.AsyncMock(return_value=None),
                )
            ),
        ):
            result = await manager.get_config()

        assert result["community"]["sessions"]["local"]["host"] == "localhost"

    def test_is_distinct_type_from_enterprise_manager(self):
        """CommunityServerConfigManager is a distinct type from EnterpriseServerConfigManager."""
        from deephaven_mcp.config import (
            CommunityServerConfigManager,
            EnterpriseServerConfigManager,
        )

        assert CommunityServerConfigManager is not EnterpriseServerConfigManager
        assert issubclass(CommunityServerConfigManager, ConfigManager)
        assert issubclass(EnterpriseServerConfigManager, ConfigManager)
