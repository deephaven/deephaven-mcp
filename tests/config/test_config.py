"""
Comprehensive test suite for deephaven_mcp.config (public/private functions, 100% coverage, up-to-date with latest refactor).
"""

import asyncio
import json
import logging
import os
import re
from unittest import mock
from unittest.mock import patch

import aiofiles
import pytest

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    ConfigurationError,
    EnterpriseSystemConfigurationError,
)
from deephaven_mcp.config import (
    CONFIG_ENV_VAR,
    ConfigManager,
    _load_config_from_file,
    _log_config_summary,
    get_all_config_names,
    get_config_path,
    get_config_section,
    load_and_validate_config,
    validate_config,
)


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


@pytest.fixture
def valid_enterprise_config():
    return {
        "enterprise": {
            "systems": {
                "prod": {
                    "connection_json_url": "https://foo",
                    "auth_type": "password",
                    "username": "u",
                    "password": "p",
                },
                "staging": {
                    "connection_json_url": "https://bar",
                    "auth_type": "private_key",
                    "private_key_path": "key.pem",
                },
            }
        }
    }


@pytest.fixture
def valid_full_config(valid_community_config, valid_enterprise_config):
    # Merge nested 'community' and 'enterprise' dicts
    return {
        "community": valid_community_config["community"],
        "enterprise": valid_enterprise_config["enterprise"],
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
def test_validate_config_accepts_empty():
    assert validate_config({}) == {}


def test_validate_config_accepts_community_only(valid_community_config):
    assert validate_config(valid_community_config) == valid_community_config


def test_validate_config_accepts_enterprise_only(valid_enterprise_config):
    assert validate_config(valid_enterprise_config) == valid_enterprise_config


def test_validate_config_accepts_full(valid_full_config):
    assert validate_config(valid_full_config) == valid_full_config


def test_validate_config_rejects_unknown_top_level():
    with pytest.raises(ConfigurationError):
        validate_config({"foo": {}})


# --- Community session validation ---
from deephaven_mcp.config._community_session import (
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
from deephaven_mcp.config._enterprise_system import (
    _validate_and_get_auth_type,
    redact_enterprise_system_config,
    validate_enterprise_systems_config,
    validate_single_enterprise_system,
)


def test_enterprise_systems_accepts_empty():
    validate_enterprise_systems_config({})


def test_enterprise_systems_rejects_non_dict():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_systems_config([])


def test_enterprise_systems_rejects_non_dict_item():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_systems_config({"foo": []})


def test_enterprise_systems_invalid_system_name_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_enterprise_systems_config({1: {}})


def test_enterprise_systems_missing_connection_json_url():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system("foo", {"auth_type": "password"})


def test_enterprise_systems_invalid_connection_json_url_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo", {"connection_json_url": 1, "auth_type": "password"}
        )


def test_enterprise_systems_missing_auth_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system("foo", {"connection_json_url": "url"})


def test_enterprise_systems_invalid_auth_type_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo", {"connection_json_url": "url", "auth_type": 1}
        )


def test_enterprise_systems_unknown_auth_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo", {"connection_json_url": "url", "auth_type": "badtype"}
        )


def test_enterprise_systems_unknown_key():
    # Should log a warning but not raise
    validate_single_enterprise_system(
        "foo",
        {
            "connection_json_url": "url",
            "auth_type": "password",
            "username": "u",
            "password": "p",
            "bad": 1,
        },
    )


def test_enterprise_systems_password_auth_missing_username():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {"connection_json_url": "url", "auth_type": "password", "password": "p"},
        )


def test_enterprise_systems_password_auth_invalid_username_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {
                "connection_json_url": "url",
                "auth_type": "password",
                "username": 1,
                "password": "p",
            },
        )


def test_enterprise_systems_password_auth_missing_password_keys():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {"connection_json_url": "url", "auth_type": "password", "username": "u"},
        )


def test_enterprise_systems_password_auth_invalid_password_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {
                "connection_json_url": "url",
                "auth_type": "password",
                "username": "u",
                "password": 1,
            },
        )


def test_enterprise_systems_password_auth_invalid_password_env_var_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {
                "connection_json_url": "url",
                "auth_type": "password",
                "username": "u",
                "password_env_var": 1,
            },
        )


def test_enterprise_systems_password_auth_both_passwords_present():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {
                "connection_json_url": "url",
                "auth_type": "password",
                "username": "u",
                "password": "p",
                "password_env_var": "env",
            },
        )


def test_enterprise_systems_private_key_auth_missing_key():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo", {"connection_json_url": "url", "auth_type": "private_key"}
        )


def test_enterprise_systems_private_key_auth_invalid_key_type():
    with pytest.raises(EnterpriseSystemConfigurationError):
        validate_single_enterprise_system(
            "foo",
            {
                "connection_json_url": "url",
                "auth_type": "private_key",
                "private_key": 1,
            },
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


# --- ConfigManager cache/async/IO ---


@pytest.mark.asyncio
async def test_get_config_other_os_error_on_read(monkeypatch, caplog):
    import os
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/config_for_os_error_read.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)

    # Mock the async file object's read method
    mock_file_read = mock.AsyncMock(side_effect=os.error("Simulated OS error on read"))

    # Mock the async context manager returned by aiofiles.open
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
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
async def test_validate_config_missing_required_key_runtime(caplog, monkeypatch):
    import json  # For dumping test config
    from unittest import mock  # For mocking

    import aiofiles  # For mocking

    from deephaven_mcp import config

    # Temporarily add a required key to _SCHEMA_PATHS
    original_schema = config._SCHEMA_PATHS.copy()
    test_schema = original_schema.copy()
    test_schema[("must_have_this",)] = config._ConfigPathSpec(
        required=True, expected_type=dict, validator=None
    )

    with patch.object(
        config,
        "_SCHEMA_PATHS",
        {
            **config._SCHEMA_PATHS,
            ("must_have_this",): config._ConfigPathSpec(
                required=True, expected_type=dict, validator=None
            ),
        },
    ):
        cm = config.ConfigManager()
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
    import json
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/config_for_cache_test.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
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

    cm = config.ConfigManager()
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
    import json
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/config_unknown_key.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {"some_unknown_key": {}, "community": {"sessions": {}}}

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
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
    import json
    import re
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/invalid_community_schema.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
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

    cm = config.ConfigManager()
    # The error will now come from validate_community_sessions_config via validate_config
    expected_error_pattern = re.escape(
        "Error loading configuration file: Invalid configuration for community.sessions: Field 'host' in community session config for bad_session must be of type str, got int"
    )

    with pytest.raises(
        ConfigurationError,
        match=expected_error_pattern,
    ):
        await cm.get_config()

    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_validate_enterprise_systems_config_logs_non_dict_map(
    monkeypatch, caplog
):
    """
    Tests that validate_enterprise_systems_config correctly logs and raises an error
    when 'enterprise_systems' is not a dictionary, ensuring the logging redaction
    path for non-dict maps is covered.
    """
    import json
    import logging  # Added for caplog.set_level
    import re
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/enterprise_non_dict_map.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    # 'enterprise_systems' is a list, not a dict
    invalid_config_data = {
        "enterprise": {
            "systems": [
                {
                    "name": "sys1",
                    "auth_type": "password",
                    "username": "user1",
                    "password": "key1",
                }
            ]
        }
    }

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
    caplog.set_level(
        logging.DEBUG, logger="deephaven_mcp.config"
    )  # For validate_enterprise_systems_config debug log

    # The error will now come from validate_config checking _SCHEMA_PATHS
    expected_error_pattern = re.escape(
        "Error loading configuration file: Config path ('enterprise', 'systems') must be of type dict, got list"
    )

    with pytest.raises(
        ConfigurationError,
        match=expected_error_pattern,
    ):
        await cm.get_config()

    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_validate_enterprise_systems_config_logs_non_dict_item_in_map(
    monkeypatch, caplog
):
    """
    Tests that validate_enterprise_systems_config correctly logs and raises an error
    when an item within 'enterprise_systems' is not a dictionary, ensuring the
    logging redaction path for non-dict items in the map is covered.
    """
    import json
    import logging  # Added for caplog.set_level
    import re
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/enterprise_non_dict_item.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {
        "enterprise": {
            "systems": {
                "good_system": {
                    "connection_json_url": "http://good",
                    "auth_type": "password",
                    "username": "gooduser",
                    "password": "goodpass",
                },
                "bad_system_item": "this is not a dict",  # Malformed item
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

    cm = config.ConfigManager()
    caplog.set_level(logging.DEBUG, logger="deephaven_mcp.config")

    # Error from validate_single_enterprise_system for 'bad_system_item'
    specific_error_detail = "Enterprise system 'bad_system_item' configuration must be a dictionary, but got str."

    with pytest.raises(
        ConfigurationError,
        match=re.escape(
            "Error loading configuration file: Invalid configuration for enterprise.systems: Enterprise system 'bad_system_item' configuration must be a dictionary, but got str."
        ),
    ):
        await cm.get_config()

    # Check the debug log from validate_enterprise_systems_config
    # 'good_system' should be redacted, 'bad_system_item' should be as-is.
    expected_log_map_str = "{'good_system': {'connection_json_url': 'http://good', 'auth_type': 'password', 'username': 'gooduser', 'password': '[REDACTED]'}, 'bad_system_item': 'this is not a dict'}"
    assert (
        f"Validating enterprise_systems configuration: {expected_log_map_str}"
        in caplog.text
    )
    assert (
        specific_error_detail in caplog.text
    )  # From validate_single_enterprise_system
    assert (
        f"Error loading configuration file {config_file_path}: Invalid configuration for enterprise.systems: {specific_error_detail}"
        in caplog.text
    )  # From get_config

    aiofiles.open = original_aio_open


def test_validate_enterprise_systems_config_is_none_direct_call(caplog):
    """
    Tests that validate_enterprise_systems_config handles the case where
    'enterprise_systems' key is not present (evaluates to None) when called directly.
    This should be a valid scenario and log a specific DEBUG message.
    """
    import logging

    from deephaven_mcp.config._enterprise_system import (
        validate_enterprise_systems_config,
    )

    caplog.set_level(logging.DEBUG)

    # Directly call the function being tested
    validate_enterprise_systems_config(None)

    expected_log_message = "'enterprise_systems' key is not present, which is valid."
    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "DEBUG"
            and expected_log_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected DEBUG log message '{expected_log_message}' not found from enterprise_system logger. Logs: {caplog.text}"


@pytest.mark.asyncio  # Marking async for consistency, though not strictly needed by this test's direct call
async def test_validate_enterprise_systems_config_invalid_system_name_type(caplog):
    """
    Tests that validate_enterprise_systems_config raises an error if a system name
    (key in enterprise_systems map) is not a string, when called directly.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_enterprise_systems_config,
    )

    caplog.set_level(
        logging.DEBUG
    )  # Capture all debug logs, including from enterprise_system

    invalid_enterprise_map = {
        123: {
            "connection_json_url": "http://example.com",
            "auth_type": "none",
        }  # Invalid system name (int)
    }

    specific_error_detail = "Enterprise system name must be a string, but got int: 123."

    with pytest.raises(
        EnterpriseSystemConfigurationError,  # Expecting the direct error from the validation function
        match=re.escape(specific_error_detail),
    ):
        validate_enterprise_systems_config(invalid_enterprise_map)

    # Verify that the specific error was logged by the enterprise_system logger
    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and specific_error_detail in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{specific_error_detail}' not found from enterprise_system logger."


def testvalidate_single_enterprise_system_missing_connection_json_url(caplog):
    """
    Tests validate_single_enterprise_system when 'connection_json_url' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)  # Capture all logs for thorough checking
    system_name = "test_system_no_url"
    # Config missing 'connection_json_url'
    invalid_config = {"auth_type": "none"}
    expected_error_message = f"Required field 'connection_json_url' missing in enterprise system '{system_name}'."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_invalid_connection_json_url_type(caplog):
    """
    Tests validate_single_enterprise_system when 'connection_json_url' is not a string.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_bad_url_type"
    # Config with 'connection_json_url' of wrong type
    invalid_config = {
        "connection_json_url": 12345,  # Not a string
        "auth_type": "password",
        "username": "dummy_user",
        "password": "dummy_key_for_valid_auth",
    }
    # Ensure the type name in the message matches Python's output for int
    url_type_name = type(invalid_config["connection_json_url"]).__name__
    expected_error_message = f"Field 'connection_json_url' for enterprise system '{system_name}' must be of type str, but got {url_type_name}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_missing_auth_type(caplog):
    """
    Tests validate_single_enterprise_system when 'auth_type' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_no_auth_type"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json"
        # 'auth_type' is missing
    }
    expected_error_message = (
        f"Required field 'auth_type' missing in enterprise system '{system_name}'."
    )

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_invalid_auth_type_type(caplog):
    """
    Tests validate_single_enterprise_system when 'auth_type' is not a string.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_bad_auth_type_type"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": 123,  # Not a string
    }
    auth_type_val = invalid_config["auth_type"]
    auth_type_name = type(auth_type_val).__name__
    # This tests when auth_type itself is not a string, so it's a base field type error
    expected_error_message = f"Field 'auth_type' for enterprise system '{system_name}' must be of type str, but got {auth_type_name}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_unknown_auth_type_value(caplog):
    """
    Tests validate_single_enterprise_system when 'auth_type' is an unknown string value.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        _AUTH_SPECIFIC_FIELDS,
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_unknown_auth_value"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "unknown_auth_method",  # Unknown string value
    }
    allowed_types_str = sorted(list(_AUTH_SPECIFIC_FIELDS.keys()))
    expected_error_message = f"'auth_type' for enterprise system '{system_name}' must be one of {allowed_types_str}, but got '{invalid_config['auth_type']}'."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_unknown_key(caplog):
    """
    Tests validate_single_enterprise_system logs a warning for an unknown key.
    """
    import logging

    from deephaven_mcp.config._enterprise_system import (
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.WARNING)  # We only care about the warning here
    system_name = "test_system_unknown_key"
    config_with_unknown_key = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        "username": "dummy_user",
        "password": "dummy_key_for_valid_auth",
        "some_unknown_field": "some_value",
    }
    expected_warning_message = f"Unknown field 'some_unknown_field' in enterprise system '{system_name}' configuration. It will be ignored."

    # This should not raise an error, only log a warning
    validate_single_enterprise_system(system_name, config_with_unknown_key)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "WARNING"
            and expected_warning_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected WARNING log message '{expected_warning_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_base_field_invalid_tuple_type(caplog):
    """
    Tests validate_single_enterprise_system when a base field expects a tuple of types
    and an invalid type is provided.
    """
    import logging
    import re

    from deephaven_mcp.config import enterprise_system  # Import the module itself
    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_base_tuple_type_fail"

    # Modify _BASE_ENTERPRISE_SYSTEM_FIELDS for this test
    original_base_fields = enterprise_system._BASE_ENTERPRISE_SYSTEM_FIELDS
    patched_base_fields = original_base_fields.copy()
    patched_base_fields["test_base_tuple_field"] = (str, int)  # Expects str OR int
    with patch.object(
        enterprise_system, "_BASE_ENTERPRISE_SYSTEM_FIELDS", patched_base_fields
    ):
        invalid_config = {
            "connection_json_url": "http://example.com/connection.json",
            "auth_type": "password",  # Use a valid auth_type
            "username": "dummy_user",
            "password": "dummy_key_for_test",  # Satisfy 'password' auth type requirements
            "test_base_tuple_field": [
                1.0,
                2.0,
            ],  # Use a type not str or int (e.g., list)
        }

    field_value = invalid_config["test_base_tuple_field"]
    expected_types_str = ", ".join(
        t.__name__ for t in patched_base_fields["test_base_tuple_field"]
    )
    expected_error_message = f"Field 'test_base_tuple_field' for enterprise system '{system_name}' must be one of types ({expected_types_str}), but got {type(field_value).__name__}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_auth_specific_field_invalid_tuple_type(
    caplog,
):
    """
    Tests validate_single_enterprise_system when an auth-specific field expects a tuple of types
    and an invalid type is provided.
    """
    import logging
    import re

    from deephaven_mcp.config import enterprise_system  # Import the module itself
    from deephaven_mcp.config._enterprise_system import (
        _AUTH_SPECIFIC_FIELDS,
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_auth_tuple_type_fail"
    auth_type_to_test = "password"

    # Modify _AUTH_SPECIFIC_FIELDS for this test
    original_auth_fields = enterprise_system._AUTH_SPECIFIC_FIELDS
    patched_auth_fields = {
        k: v.copy() for k, v in original_auth_fields.items()
    }  # Deep copy for safety
    if auth_type_to_test not in patched_auth_fields:
        patched_auth_fields[auth_type_to_test] = {}
    patched_auth_fields[auth_type_to_test]["test_auth_tuple_field"] = (
        str,
        int,
    )  # Expects str OR int
    with patch.object(enterprise_system, "_AUTH_SPECIFIC_FIELDS", patched_auth_fields):
        invalid_config = {
            "connection_json_url": "http://example.com/connection.json",
            "auth_type": auth_type_to_test,
            "password": "dummy_pass_value",  # Satisfy password auth presence
            "test_auth_tuple_field": [1.0, 2.0],  # Invalid type (list)
        }

    field_value = invalid_config["test_auth_tuple_field"]
    expected_types_str = ", ".join(
        t.__name__
        for t in patched_auth_fields[auth_type_to_test]["test_auth_tuple_field"]
    )
    expected_error_message = f"Field 'test_auth_tuple_field' for enterprise system '{system_name}' (auth_type: {auth_type_to_test}) must be one of types ({expected_types_str}), but got {type(field_value).__name__}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_password_auth_missing_username(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'password'
    when 'username' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pw_auth_no_user"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        # 'username' is missing
    }
    expected_error_message = f"Enterprise system '{system_name}' with auth_type 'password' must define 'username'."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_password_auth_invalid_username_type(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'password'
    when 'username' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pw_auth_bad_user_type"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        "username": 12345,  # Not a string
    }
    key_type_name = type(invalid_config["username"]).__name__
    expected_error_message = f"Field 'username' for enterprise system '{system_name}' (auth_type: password) must be of type str, but got {key_type_name}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_password_auth_missing_password_keys(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'password'
    when both 'password' and 'password_env_var' are missing.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pw_auth_no_pw_keys"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        "username": "testuser",
        # 'password' and 'password_env_var' are missing
    }
    expected_error_message = f"Enterprise system '{system_name}' with auth_type 'password' must define 'password' or 'password_env_var'."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_password_auth_invalid_password_type(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'password'
    when 'password' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pw_auth_bad_pw_type"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        "username": "testuser",
        "password": 12345,  # Not a string
    }
    key_type_name = type(invalid_config["password"]).__name__
    expected_error_message = f"Field 'password' for enterprise system '{system_name}' (auth_type: password) must be of type str, but got {key_type_name}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_password_auth_invalid_password_env_var_type(
    caplog,
):
    """
    Tests validate_single_enterprise_system for auth_type 'password'
    when 'password_env_var' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pw_auth_bad_pw_env_type"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        "username": "testuser",
        "password_env_var": 12345,  # Not a string
    }
    key_type_name = type(invalid_config["password_env_var"]).__name__
    expected_error_message = f"Field 'password_env_var' for enterprise system '{system_name}' (auth_type: password) must be of type str, but got {key_type_name}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_private_key_auth_missing_key(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'private_key'
    when 'private_key' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pk_auth_no_path"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "private_key",
        # 'private_key' is missing
    }
    expected_error_message = f"Enterprise system '{system_name}' with auth_type 'private_key' must define 'private_key'."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_private_key_auth_invalid_key_type(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'private_key'
    when 'private_key' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pk_auth_bad_path_type"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "private_key",
        "private_key": 12345,  # Not a string
    }
    key_type_name = type(invalid_config["private_key"]).__name__
    expected_error_message = f"Field 'private_key' for enterprise system '{system_name}' (auth_type: private_key) must be of type str, but got {key_type_name}."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def testvalidate_single_enterprise_system_password_auth_both_passwords_present(caplog):
    """
    Tests validate_single_enterprise_system for auth_type 'password'
    when both 'password' and 'password_env_var' are present.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_pw_both_passwords"
    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",
        "username": "testuser",
        "password": "some_password",
        "password_env_var": "SOME_PW_ENV_VAR",
    }
    expected_error_message = f"Enterprise system '{system_name}' with auth_type 'password' must not define both 'password' and 'password_env_var'. Specify one."

    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match=re.escape(expected_error_message),
    ):
        validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


@pytest.mark.asyncio  # Marking async for consistency, though not strictly needed by this test's direct call
async def test_validate_enterprise_systems_config_invalid_system_name_type(caplog):
    """
    Tests that validate_enterprise_systems_config raises an error if a system name
    (key in enterprise_systems map) is not a string, when called directly.
    """
    import logging
    import re

    from deephaven_mcp.config._enterprise_system import (
        EnterpriseSystemConfigurationError,
        validate_enterprise_systems_config,
    )

    caplog.set_level(
        logging.DEBUG
    )  # Capture all debug logs, including from enterprise_system

    invalid_enterprise_map = {
        123: {
            "connection_json_url": "http://example.com",
            "auth_type": "none",
        }  # Invalid system name (int)
    }

    specific_error_detail = "Enterprise system name must be a string, but got int: 123."

    with pytest.raises(
        EnterpriseSystemConfigurationError,  # Expecting the direct error from the validation function
        match=re.escape(specific_error_detail),
    ):
        validate_enterprise_systems_config(invalid_enterprise_map)

    # Verify that the specific error was logged by the enterprise_system logger
    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config._enterprise_system"
            and record.levelname == "ERROR"
            and specific_error_detail in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{specific_error_detail}' not found from enterprise_system logger."


@pytest.mark.asyncio
async def test_get_config_no_community_sessions_key_from_file(monkeypatch, caplog):
    import importlib
    import json
    from unittest import mock

    from deephaven_mcp import config

    # Prepare an empty config JSON string
    empty_config_json = "{}"

    # Patch environment variable
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/empty_config.json")
    # Mock for aiofiles.open
    mock_aiofiles_open = mock.MagicMock()
    # Mock for the async context manager returned by aiofiles.open()
    mock_async_context_manager = mock.AsyncMock()
    # Mock for the file object yielded by the context manager
    mock_file_object = mock.AsyncMock()

    # Configure the mocks
    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = empty_config_json

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = config.ConfigManager()
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


# --- Cache and worker config tests ---

import pytest


@pytest.mark.asyncio
async def test_get_config_section_invalid_section():
    cm = ConfigManager()
    cm._cache = {"community_sessions": {}}
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['not_a_section', 'foo'] does not exist in configuration"
        ),
    ):
        get_config_section(cm._cache, ["not_a_section", "foo"])


@pytest.mark.asyncio
async def test_get_config_section_invalid_name_enterprise_systems():
    cm = ConfigManager()
    cm._cache = {
        "enterprise_systems": {
            "foo": {
                "connection_json_url": "url",
                "auth_type": "api_key",
                "api_key": "SECRET",
            }
        }
    }
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['enterprise', 'systems', 'bar'] does not exist in configuration"
        ),
    ):
        get_config_section(cm._cache, ["enterprise", "systems", "bar"])


@pytest.mark.asyncio
async def test_config_manager_set_and_clear_cache():
    from deephaven_mcp import config

    cm = config.ConfigManager()
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
    from deephaven_mcp import config

    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(
        RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"
    ):
        await config.ConfigManager().get_config()


@pytest.mark.asyncio
async def test_validate_config_missing_required_key_runtime(monkeypatch, caplog):
    import json
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

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
            config,
            "_SCHEMA_PATHS",
            {
                **config._SCHEMA_PATHS,
                ("must_have_this",): config._ConfigPathSpec(
                    required=True, expected_type=dict, validator=None
                ),
            },
        ),
        patch("aiofiles.open", mock.Mock(return_value=aiofiles_open_ctx)),
    ):
        cm = config.ConfigManager()
        await cm.clear_config_cache()
        expected_error = re.escape(
            "Error loading configuration file: Missing required keys at config path (): {'must_have_this'}"
        )
        with pytest.raises(
            ConfigurationError,
            match=expected_error,
        ):
            await cm.get_config()


@pytest.mark.asyncio
async def test_get_all_config_names_returns_keys():
    config = {
        "community": {
            "sessions": {"a": {"host": "localhost"}, "b": {"host": "localhost"}}
        }
    }
    names = get_all_config_names(config, ["community", "sessions"])
    assert set(names) == {"a", "b"}

    config2 = {"community": {"sessions": {}}}
    names2 = get_all_config_names(config2, ["community", "sessions"])
    assert names2 == []

    config3 = {"community": {"sessions": {}}}
    names3 = get_all_config_names(config3, ["enterprise", "systems"])
    assert names3 == []


@pytest.mark.asyncio
async def test_get_all_config_names_not_dict_raises():
    config = {"community_sessions": "not_a_dict"}
    result = get_all_config_names(config, ["community_sessions"])
    assert result == []  # Should return empty list for non-dict sections


@pytest.mark.asyncio
async def test_named_config_missing():
    config = {"community": {"sessions": {"foo": {"host": "localhost"}}}}
    with pytest.raises(
        KeyError,
        match=re.escape(
            "Section path ['community', 'sessions', 'bar'] does not exist in configuration"
        ),
    ):
        get_config_section(config, ["community", "sessions", "bar"])


@pytest.mark.asyncio
async def test_get_all_config_names_returns_empty_for_non_dict_section(caplog):
    from deephaven_mcp import config

    config = {"not_a_section": "not_a_dict"}
    caplog.set_level("WARNING", logger="deephaven_mcp.config.__init__")
    # Call with a non-dict section
    result = get_all_config_names(config, ["not_a_section"])
    assert result == []
    assert (
        "Section at path ['not_a_section'] is not a dictionary, returning empty list of names."
        in caplog.text
    )
    config = {"not_a_section": "not_a_dict"}
    caplog.set_level("WARNING", logger="deephaven_mcp.config.__init__")
    # Call with a non-dict section
    result = get_all_config_names(config, ["not_a_section"])
    assert result == []
    assert (
        "Section at path ['not_a_section'] is not a dictionary, returning empty list of names."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_get_config_no_community_sessions_key_from_file(monkeypatch, caplog):
    import importlib
    import json
    from unittest import mock

    from deephaven_mcp import config

    # Prepare an empty config JSON string
    empty_config_json = "{}"

    # Patch environment variable
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/empty_config.json")
    # Mock for aiofiles.open
    mock_aiofiles_open = mock.MagicMock()
    # Mock for the async context manager returned by aiofiles.open()
    mock_async_context_manager = mock.AsyncMock()
    # Mock for the file object yielded by the context manager
    mock_file_object = mock.AsyncMock()

    # Configure the mocks
    mock_aiofiles_open.return_value = mock_async_context_manager
    mock_async_context_manager.__aenter__.return_value = mock_file_object
    mock_file_object.read.return_value = empty_config_json

    with patch("aiofiles.open", mock_aiofiles_open):
        cm = config.ConfigManager()
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
    from deephaven_mcp._exceptions import ConfigurationError
    from deephaven_mcp.config import ConfigManager

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/nonexistent/path/config.json")

    cm = ConfigManager()
    await cm.clear_config_cache()

    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        await cm.get_config()


@pytest.mark.asyncio
async def test_config_permission_error(monkeypatch):
    """Test PermissionError handling in _load_config_from_file."""
    import os
    from unittest.mock import patch

    from deephaven_mcp._exceptions import ConfigurationError
    from deephaven_mcp.config import ConfigManager

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")

    with patch("aiofiles.open", side_effect=PermissionError("Permission denied")):
        cm = ConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(
            ConfigurationError,
            match="Permission denied when trying to read configuration file",
        ):
            await cm.get_config()


@pytest.mark.asyncio
async def test_config_invalid_json_error(monkeypatch):
    """Test JSONDecodeError handling in _load_config_from_file."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from deephaven_mcp._exceptions import ConfigurationError
    from deephaven_mcp.config import ConfigManager

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
        cm = ConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(
            ConfigurationError, match="Invalid JSON in configuration file"
        ):
            await cm.get_config()


def test_validate_config_non_dict():
    """Test validation error when config is not a dictionary."""
    from deephaven_mcp._exceptions import ConfigurationError
    from deephaven_mcp.config import validate_config

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        validate_config("not a dict")

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        validate_config(123)

    with pytest.raises(ConfigurationError, match="Configuration must be a dictionary"):
        validate_config(["list", "not", "dict"])


@pytest.mark.asyncio
async def test_config_validation_error_in_load_and_validate(monkeypatch):
    """Test configuration validation error handling in load_and_validate_config."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from deephaven_mcp._exceptions import ConfigurationError
    from deephaven_mcp.config import ConfigManager

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
        cm = ConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            await cm.get_config()


@pytest.mark.asyncio
async def test_json_formatting_error_in_log_config_summary(monkeypatch, caplog):
    """Test JSON formatting error handling in _log_config_summary."""
    import json
    from unittest.mock import AsyncMock, MagicMock, patch

    from deephaven_mcp.config import ConfigManager

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

    # Mock json.dumps to raise an error (simulating non-serializable config)
    def mock_json_dumps(*args, **kwargs):
        raise TypeError("Object is not JSON serializable")

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("json.dumps", side_effect=mock_json_dumps):
            cm = ConfigManager()
            await cm.clear_config_cache()
            with caplog.at_level("WARNING"):
                await cm.get_config()

    # Verify the warning was logged
    assert "Failed to format config as JSON" in caplog.text


@pytest.mark.asyncio
async def test_config_validation_error_in_load_and_validate(monkeypatch):
    """Test configuration validation error handling in load_and_validate_config."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from deephaven_mcp._exceptions import ConfigurationError
    from deephaven_mcp.config import ConfigManager

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
        cm = ConfigManager()
        await cm.clear_config_cache()

        with pytest.raises(
            ConfigurationError, match="Error loading configuration file"
        ):
            await cm.get_config()
