"""Tests for Deephaven MCP configuration management."""

import importlib
import json
import logging
import os
import re
import sys
from unittest import mock

import pytest
import pytest_asyncio

from deephaven_mcp.config import (
    CommunitySessionConfigurationError,
    EnterpriseSystemConfigurationError,
)

# TODO: needed?
# Ensure local source is used for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# --- Constants and helpers ---
VALID_COMMUNITY_SESSIONS_CONFIG = {
    "community_sessions": {
        "local": {
            "host": "localhost",
            "port": 10000,
            "auth_type": "token",
            "auth_token": "tokenval",
            "never_timeout": True,
            "session_type": "single",
            "use_tls": False,
        }
    },
}


MINIMAL_COMMUNITY_SESSIONS_CONFIG = {"community_sessions": {"local_session": {}}}


# --- Fixtures ---
@pytest_asyncio.fixture(autouse=True)
async def cleanup_config_cache():
    """Ensure config cache is cleared before and after each test using a local ConfigManager."""
    # No-op: Use local ConfigManager in each test, so no global cache to clear
    yield


# --- Validation tests ---
def test_validate_config_unknown_top_level_key():
    from deephaven_mcp import config

    bad_config = {
        "extra": "some_value"
    }  # This key is not defined in REQUIRED_TOP_LEVEL_KEYS or OPTIONAL_TOP_LEVEL_KEYS
    cm = config.ConfigManager()
    with pytest.raises(
        config.McpConfigurationError,
        match=r"Unknown top-level keys in Deephaven MCP config: {'extra'}",
    ):
        cm.validate_config(bad_config)


def test_validate_config_missing_required_key(monkeypatch):
    from deephaven_mcp import config

    # Temporarily set required keys for this test
    monkeypatch.setattr(
        config.ConfigManager, "_REQUIRED_TOP_LEVEL_KEYS", {"must_have_this"}
    )

    # Config missing the temporarily required key
    bad_config = {"community_sessions": {}}
    with pytest.raises(
        config.McpConfigurationError,
        match="Missing required top-level keys in Deephaven MCP config: {'must_have_this'}",
    ):
        config.ConfigManager.validate_config(bad_config)

    # Reset to default after test if necessary, though monkeypatch handles cleanup
    # monkeypatch.setattr(config.ConfigManager, "_REQUIRED_TOP_LEVEL_KEYS", set())


def test_validate_config_allows_empty_and_various_valid_shapes():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    # Empty config is valid
    assert cm.validate_config({}) == {}
    # Only community_sessions is valid
    assert cm.validate_config({"community_sessions": {}}) == {"community_sessions": {}}
    # Only enterprise_systems is valid
    assert cm.validate_config({"enterprise_systems": {}}) == {"enterprise_systems": {}}
    # Both are valid
    both = {"community_sessions": {}, "enterprise_systems": {}}
    assert cm.validate_config(both) == both
    # Empty dicts with one session/system
    assert cm.validate_config({"community_sessions": {"foo": {}}, "enterprise_systems": {}})["community_sessions"]["foo"] == {}
    # An enterprise system config with missing required fields should raise an error
    import pytest
    with pytest.raises(Exception):
        cm.validate_config({"community_sessions": {}, "enterprise_systems": {"bar": {}}})


def test_validate_config_rejects_unknown_top_level_keys():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    bad = {"community_sessions": {}, "enterprise_systems": {}, "extra": 1}
    with pytest.raises(config.McpConfigurationError, match=r"Unknown top-level keys in Deephaven MCP config: {'extra'}"):
        cm.validate_config(bad)


def test_validate_config_rejects_unknown_fields_in_community_sessions():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    bad = {"community_sessions": {"foo": {"host": "x", "unknown_field": 1}}}
    with pytest.raises(config.CommunitySessionConfigurationError, match=r"Unknown field 'unknown_field' in community session config for foo"):
        cm.validate_config(bad)


def test_validate_config_enforces_field_types():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    bad = {"community_sessions": {"foo": {"host": 123}}}
    with pytest.raises(config.CommunitySessionConfigurationError, match=r"Field 'host' in community session config for foo must be of type str, got int"):
        cm.validate_config(bad)


def test_validate_config_mutual_exclusivity_auth_token_fields():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    # Both auth_token and auth_token_env_var present
    bad = {"community_sessions": {"foo": {"auth_token": "a", "auth_token_env_var": "ENV"}}}
    with pytest.raises(config.CommunitySessionConfigurationError, match=r"In community session config for 'foo', both 'auth_token' and 'auth_token_env_var' are set\. Please use only one\."):
        cm.validate_config(bad)


# --- Config loading tests ---
@pytest.mark.asyncio
async def test_get_config_valid():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_COMMUNITY_SESSIONS_CONFIG)
    cfg = await cm.get_config()
    assert "community_sessions" in cfg
    assert "local" in cfg["community_sessions"]


@pytest.mark.asyncio
async def test_get_config_sets_cache_and_logs(monkeypatch, caplog):
    import importlib
    import json
    from unittest import mock

    from deephaven_mcp import config

    # Prepare a valid config JSON string
    valid_config = {
        "community_sessions": {
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
    config_json = json.dumps(valid_config)

    # Patch environment variable
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    # Patch aiofiles.open to return our config JSON
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(
        return_value=config_json
    )
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(
        importlib.import_module("aiofiles").__dict__, "open", aiofiles_mock.open
    )

    cm = config.ConfigManager()
    await cm.clear_config_cache()
    with caplog.at_level("INFO"):
        cfg = await cm.get_config()
    assert cfg == valid_config
    assert cm._cache == valid_config

    # Check for the new log messages
    log_text = caplog.text
    assert "Successfully loaded and validated Deephaven MCP application configuration" in log_text
    assert "Configured Community Sessions:" in log_text
    # Construct expected redacted session string carefully
    expected_session_details = valid_config["community_sessions"]["local"].copy()
    expected_session_details["auth_token"] = "[REDACTED]"
    assert f"  Session 'local': {expected_session_details}" in log_text
    assert "No Enterprise Systems configured." in log_text


@pytest.mark.asyncio
async def test_get_config_logs_enterprise_systems(monkeypatch, caplog):
    import importlib
    import json
    from unittest import mock

    from deephaven_mcp import config

    # Prepare a valid config JSON string with both community and enterprise systems
    valid_config_with_enterprise = {
        "community_sessions": {
            "comm_local": {
                "host": "localhost",
                "port": 10000,
                "auth_type": "token",
                "auth_token": "comm_token_val",
            }
        },
        "enterprise_systems": {
            "ent_prod": {
                "connection_json_url": "https://enterprise.example.com/iris/connection.json",
                "auth_type": "password",
                "username": "prod_user",
                "password": "prod_password_value",
            },
            "ent_staging": {
                "connection_json_url": "https://staging.example.com/connection.json",
                "auth_type": "password",
                "username": "staging_user",
                "password": "staging_password_value",
            },
        },
    }
    config_json = json.dumps(valid_config_with_enterprise)

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config_with_enterprise.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(
        return_value=config_json
    )
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(
        importlib.import_module("aiofiles").__dict__, "open", aiofiles_mock.open
    )

    cm = config.ConfigManager()
    await cm.clear_config_cache()
    with caplog.at_level("INFO"):
        cfg = await cm.get_config()

    assert cfg == valid_config_with_enterprise
    assert cm._cache == valid_config_with_enterprise

    log_text = caplog.text
    assert "Successfully loaded and validated Deephaven MCP application configuration" in log_text

    # Check community session logs
    assert "Configured Community Sessions:" in log_text
    expected_comm_session_details = valid_config_with_enterprise["community_sessions"][
        "comm_local"
    ].copy()
    expected_comm_session_details["auth_token"] = "[REDACTED]"
    assert f"  Session 'comm_local': {expected_comm_session_details}" in log_text

    # Check enterprise system logs
    assert "Configured Enterprise Systems:" in log_text

    expected_ent_prod_details = valid_config_with_enterprise["enterprise_systems"][
        "ent_prod"
    ].copy()
    expected_ent_prod_details["password"] = "[REDACTED]"
    assert f"  System 'ent_prod': {expected_ent_prod_details}" in log_text

    expected_ent_staging_details = valid_config_with_enterprise["enterprise_systems"][
        "ent_staging"
    ].copy()
    expected_ent_staging_details["password"] = "[REDACTED]"
    assert f"  System 'ent_staging': {expected_ent_staging_details}" in log_text


@pytest.mark.asyncio
async def test_get_config_missing_env(monkeypatch):
    from deephaven_mcp import config

    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(
        RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"
    ):
        await config.ConfigManager().get_config()


@pytest.mark.asyncio
async def test_get_config_invalid_json(monkeypatch):
    from deephaven_mcp import config

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(
        return_value=b"not json"
    )
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(
        importlib.import_module("aiofiles").__dict__, "open", aiofiles_mock.open
    )
    cm = config.ConfigManager()
    with pytest.raises(config.McpConfigurationError, match="Invalid JSON in configuration file"):
        await cm.get_config()


@pytest.mark.asyncio
async def test_get_config_file_not_found(monkeypatch, caplog):
    import os

    from deephaven_mcp import config

    non_existent_path = "/tmp/this/path/should/not/exist/config.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, non_existent_path)
    cm = config.ConfigManager()
    with pytest.raises(
        config.McpConfigurationError,
        match=f"Configuration file not found: {non_existent_path}",
    ):
        await cm.get_config()
    assert f"Configuration file not found: {non_existent_path}" in caplog.text


@pytest.mark.asyncio
async def test_get_config_general_value_error(monkeypatch, caplog):
    # Patch validate_config to raise ValueError (not a config error subclass)
    import importlib
    from unittest import mock
    from deephaven_mcp import config

    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value="{}")
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module("aiofiles").__dict__, "open", aiofiles_mock.open)

    # Patch validate_config to raise ValueError
    monkeypatch.setattr(config.ConfigManager, "validate_config", staticmethod(lambda _: (_ for _ in ()).throw(ValueError("some general validation error!"))))
    cm = config.ConfigManager()
    with pytest.raises(config.McpConfigurationError, match="General configuration validation error: some general validation error!"):
        await cm.get_config()
    assert "General configuration validation error for /fake/path/config.json: some general validation error!" in caplog.text


@pytest.mark.asyncio
async def test_get_config_permission_error(tmp_path, monkeypatch, caplog):
    import os
    import stat

    from deephaven_mcp import config

    unreadable_config_file = tmp_path / "unreadable_config.json"
    unreadable_config_file.write_text("{}")  # Create the file
    unreadable_config_file.chmod(0o000)  # Make it unreadable

    monkeypatch.setenv(config.CONFIG_ENV_VAR, str(unreadable_config_file))
    cm = config.ConfigManager()

    # The exact error message for PermissionError can vary by OS,
    # so we match a substring.
    with pytest.raises(
        config.McpConfigurationError,
        match=f"Permission denied when trying to read configuration file: {str(unreadable_config_file)}",
    ):
        await cm.get_config()
    assert (
        f"Permission denied when trying to read configuration file: {str(unreadable_config_file)}"
        in caplog.text
    )

    # Clean up by making it writable so it can be deleted by tmp_path
    unreadable_config_file.chmod(stat.S_IWUSR | stat.S_IRUSR)


@pytest.mark.asyncio
async def test_get_config_other_os_error_on_read(monkeypatch, caplog):
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/config_for_os_error_read.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)

    # Mock the async file object's read method
    mock_file_read = mock.AsyncMock(side_effect=OSError("Simulated OS error on read"))

    # Mock the async context manager returned by aiofiles.open
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
    with pytest.raises(
        config.McpConfigurationError,
        match=rf"Unexpected error loading or parsing config file {re.escape(config_file_path)}: Simulated OS error on read",
    ):
        await cm.get_config()

    assert (
        f"Unexpected error loading or parsing config file {config_file_path}: Simulated OS error on read"
        in caplog.text
    )
    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_validate_config_missing_required_key_runtime(monkeypatch, caplog):
    import json  # For dumping test config
    from unittest import mock  # For mocking

    import aiofiles  # For mocking

    from deephaven_mcp import config

    # Temporarily add a required key
    # original_required_keys = config.ConfigManager._REQUIRED_TOP_LEVEL_KEYS # monkeypatch handles restoration
    monkeypatch.setattr(
        config.ConfigManager, "_REQUIRED_TOP_LEVEL_KEYS", {"must_have_this"}
    )

    cm = config.ConfigManager()
    invalid_config_data = {"community_sessions": {}}  # Missing 'must_have_this'

    config_file_path = "/fake/path/config_missing_req.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager_req = mock.AsyncMock()
    mock_async_context_manager_req.__aenter__.return_value.read = mock_file_read_content

    original_aio_open_req = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager_req)

    with pytest.raises(
        config.McpConfigurationError,
        match=r"Missing required top-level keys in Deephaven MCP config: {'must_have_this'}",
    ):
        await cm.get_config()  # This will load, then validate

    assert (
        r"Missing required top-level keys in Deephaven MCP config: {'must_have_this'}"
        in caplog.text
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
    valid_config_data = {"community_sessions": {"test_session": {"host": "localhost"}}}

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
    invalid_config_data = {"some_unknown_key": {}, "community_sessions": {}}

    mock_file_read_content = mock.AsyncMock(
        return_value=json.dumps(invalid_config_data).encode("utf-8")
    )
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content

    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
    with pytest.raises(
        config.McpConfigurationError,
        match=r"Unknown top-level keys in Deephaven MCP config: {'some_unknown_key'}",
    ):
        await cm.get_config()

    assert (
        r"Unknown top-level keys in Deephaven MCP config: {'some_unknown_key'}"
        in caplog.text
    )

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
        "community_sessions": {
            "bad_session": {
                "host": 12345,  # Invalid type, should be string
                "port": "not-a-port",
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
    specific_error_detail = "Field 'host' in community session config for bad_session must be of type str, got int"
    # This is the message from CommunitySessionConfigurationError
    expected_mcp_error_message = f"Configuration validation failed: {specific_error_detail}"
    final_match_regex = re.escape(expected_mcp_error_message)

    with pytest.raises(
        config.McpConfigurationError,
        match=final_match_regex,
    ):
        await cm.get_config()

    # Check for the original error log from validate_community_sessions_config
    assert specific_error_detail in caplog.text
    # Check for the new log from get_config when it catches the specific error
    assert (
        f"Configuration validation failed for {config_file_path}: {specific_error_detail}"
        in caplog.text
    )

    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_get_config_invalid_enterprise_system_schema_from_file(
    monkeypatch, caplog
):
    import json
    import re
    from unittest import mock

    import aiofiles

    from deephaven_mcp import config

    config_file_path = "/fake/path/invalid_enterprise_schema.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {
        "enterprise_systems": {
            "bad_system": {
                "connection_json_url": 12345,  # Invalid type
                "auth_type": "invalid_auth_type",
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
    # The 'connection_json_url' error is raised first by validate_enterprise_systems_config
    specific_error_detail = "Field 'connection_json_url' for enterprise system 'bad_system' must be of type str, but got int."
    # This is the message from EnterpriseSystemConfigurationError
    expected_mcp_error_message = f"Configuration validation failed: {specific_error_detail}"
    final_match_regex = re.escape(expected_mcp_error_message)

    with pytest.raises(
        config.McpConfigurationError,
        match=final_match_regex,
    ):
        await cm.get_config()

    # Check for the original error log from validate_enterprise_systems_config
    assert specific_error_detail in caplog.text
    # Check for the new log from get_config when it catches the specific error
    assert (
        f"Configuration validation failed for {config_file_path}: {specific_error_detail}"
        in caplog.text
    )

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
        "enterprise_systems": [
            {
                "name": "sys1",
                "auth_type": "password",
                "username": "user1",
                "password": "key1",
            }
        ]
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

    specific_error_detail = (
        "'enterprise_systems' must be a dictionary, but got type list."
    )
    expected_mcp_error_message = f"Configuration validation failed: {specific_error_detail}"
    final_match_regex = re.escape(expected_mcp_error_message)

    with pytest.raises(
        config.McpConfigurationError,
        match=final_match_regex,
    ):
        await cm.get_config()

    # Check the debug log from validate_enterprise_systems_config
    # It should log the string representation of the list as passed.
    assert (
        "Validating enterprise_systems configuration: [{'name': 'sys1', 'auth_type': 'password', 'username': 'user1', 'password': 'key1'}]"
        in caplog.text
    )
    assert (
        specific_error_detail in caplog.text
    )  # From validate_enterprise_systems_config
    assert (
        f"Configuration validation failed for {config_file_path}: {specific_error_detail}"
        in caplog.text
    )  # From get_config

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
        "enterprise_systems": {
            "good_system": {
                "connection_json_url": "http://good",
                "auth_type": "password",
                "username": "gooduser",
                "password": "secretkey",
            },
            "bad_system_item": "this is not a dict",  # Malformed item
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

    # Error from _validate_single_enterprise_system for 'bad_system_item'
    specific_error_detail = "Enterprise system 'bad_system_item' configuration must be a dictionary, but got str."

    with pytest.raises(
        config.McpConfigurationError,
        match=r"Configuration validation failed: Enterprise system 'bad_system_item' configuration must be a dictionary, but got str.",
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
    )  # From _validate_single_enterprise_system
    assert (
        f"Configuration validation failed for {config_file_path}: {specific_error_detail}"
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

    from deephaven_mcp.config.enterprise_system import (
        validate_enterprise_systems_config,
    )

    caplog.set_level(logging.DEBUG)

    # Directly call the function being tested
    validate_enterprise_systems_config(None)

    expected_log_message = "'enterprise_systems' key is not present, which is valid."
    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
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

    from deephaven_mcp.config.enterprise_system import (
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
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and specific_error_detail in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{specific_error_detail}' not found from enterprise_system logger."


def test_validate_single_enterprise_system_missing_connection_json_url(caplog):
    """
    Tests _validate_single_enterprise_system when 'connection_json_url' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_invalid_connection_json_url_type(caplog):
    """
    Tests _validate_single_enterprise_system when 'connection_json_url' is not a string.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_missing_auth_type(caplog):
    """
    Tests _validate_single_enterprise_system when 'auth_type' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_invalid_auth_type_type(caplog):
    """
    Tests _validate_single_enterprise_system when 'auth_type' is not a string.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_unknown_auth_type_value(caplog):
    """
    Tests _validate_single_enterprise_system when 'auth_type' is an unknown string value.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        _AUTH_SPECIFIC_FIELDS,
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_unknown_key(caplog):
    """
    Tests _validate_single_enterprise_system logs a warning for an unknown key.
    """
    import logging

    from deephaven_mcp.config.enterprise_system import (
        _validate_single_enterprise_system,
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
    _validate_single_enterprise_system(system_name, config_with_unknown_key)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "WARNING"
            and expected_warning_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected WARNING log message '{expected_warning_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_base_field_invalid_tuple_type(
    monkeypatch, caplog
):
    """
    Tests _validate_single_enterprise_system when a base field expects a tuple of types
    and an invalid type is provided.
    """
    import logging
    import re

    from deephaven_mcp.config import enterprise_system  # Import the module itself
    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
    )

    caplog.set_level(logging.DEBUG)
    system_name = "test_system_base_tuple_type_fail"

    # Modify _BASE_ENTERPRISE_SYSTEM_FIELDS for this test
    original_base_fields = enterprise_system._BASE_ENTERPRISE_SYSTEM_FIELDS
    patched_base_fields = original_base_fields.copy()
    patched_base_fields["test_base_tuple_field"] = (str, int)  # Expects str OR int
    monkeypatch.setattr(
        enterprise_system, "_BASE_ENTERPRISE_SYSTEM_FIELDS", patched_base_fields
    )

    invalid_config = {
        "connection_json_url": "http://example.com/connection.json",
        "auth_type": "password",  # Use a valid auth_type
        "username": "dummy_user",
        "password": "dummy_key_for_test",  # Satisfy 'password' auth type requirements
        "test_base_tuple_field": [1.0, 2.0],  # Use a type not str or int (e.g., list)
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"

    # Restore original fields to avoid affecting other tests
    monkeypatch.setattr(
        enterprise_system, "_BASE_ENTERPRISE_SYSTEM_FIELDS", original_base_fields
    )


def test_validate_single_enterprise_system_auth_specific_field_invalid_tuple_type(
    monkeypatch, caplog
):
    """
    Tests _validate_single_enterprise_system when an auth-specific field expects a tuple of types
    and an invalid type is provided.
    """
    import logging
    import re

    from deephaven_mcp.config import enterprise_system  # Import the module itself
    from deephaven_mcp.config.enterprise_system import (
        _AUTH_SPECIFIC_FIELDS,
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
    monkeypatch.setattr(enterprise_system, "_AUTH_SPECIFIC_FIELDS", patched_auth_fields)

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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"

    # Restore original fields to avoid affecting other tests
    monkeypatch.setattr(
        enterprise_system, "_AUTH_SPECIFIC_FIELDS", original_auth_fields
    )


def test_validate_single_enterprise_system_password_auth_missing_username(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'password'
    when 'username' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_password_auth_invalid_username_type(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'password'
    when 'username' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_password_auth_missing_password_keys(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'password'
    when both 'password' and 'password_env_var' are missing.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_password_auth_invalid_password_type(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'password'
    when 'password' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_password_auth_invalid_password_env_var_type(
    caplog,
):
    """
    Tests _validate_single_enterprise_system for auth_type 'password'
    when 'password_env_var' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_private_key_auth_missing_key(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'private_key'
    when 'private_key' is missing.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_private_key_auth_invalid_key_type(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'private_key'
    when 'private_key' has an invalid type.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
            and record.levelname == "ERROR"
            and expected_error_message in record.message
        ):
            found_log = True
            break
    assert (
        found_log
    ), f"Expected ERROR log message '{expected_error_message}' not found. Logs: {caplog.text}"


def test_validate_single_enterprise_system_password_auth_both_passwords_present(caplog):
    """
    Tests _validate_single_enterprise_system for auth_type 'password'
    when both 'password' and 'password_env_var' are present.
    """
    import logging
    import re

    from deephaven_mcp.config.enterprise_system import (
        EnterpriseSystemConfigurationError,
        _validate_single_enterprise_system,
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
        _validate_single_enterprise_system(system_name, invalid_config)

    found_log = False
    for record in caplog.records:
        if (
            record.name == "deephaven_mcp.config.enterprise_system"
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

    from deephaven_mcp.config.enterprise_system import (
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
            record.name == "deephaven_mcp.config.enterprise_system"
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
    # Patch aiofiles.open to return our empty config JSON
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(
        return_value=empty_config_json
    )
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(
        importlib.import_module("aiofiles").__dict__, "open", aiofiles_mock.open
    )

    cm = config.ConfigManager()
    await cm.clear_config_cache()
    with caplog.at_level("INFO"):
        cfg = await cm.get_config()
    assert cfg == {}  # Expect an empty dictionary
    assert cm._cache == {}
    # Check for the new log messages for empty config
    log_text = caplog.text
    assert "Successfully loaded and validated Deephaven MCP application configuration" in log_text
    assert "No Community Sessions configured." in log_text
    assert "No Enterprise Systems configured." in log_text

    session_names = await cm.get_community_session_names()
    assert session_names == []

    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Community session any_session_name not found in configuration",
    ):
        await cm.get_community_session_config("any_session_name")


# --- Cache and worker config tests ---
@pytest.mark.asyncio
async def test_clear_config_cache_community_sessions_1():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache({"community_sessions": {"a_session": {}}})
    cfg1 = await cm.get_config()
    assert "a_session" in cfg1["community_sessions"]
    await cm.clear_config_cache()
    await cm.set_config_cache({"community_sessions": {"b_session": {}}})
    cfg2 = await cm.get_config()
    assert "b_session" in cfg2["community_sessions"]
    assert "a_session" not in cfg2["community_sessions"]


@pytest.mark.asyncio
async def test_get_community_session_names():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_COMMUNITY_SESSIONS_CONFIG)
    names = await cm.get_community_session_names()
    assert "local" in names


@pytest.mark.asyncio
async def test_get_community_session_config_no_community_sessions_key():
    from deephaven_mcp import config

    empty_config = {}
    cm = config.ConfigManager()
    await cm.set_config_cache(empty_config)  # Should not raise an error

    # Check that getting session names returns an empty list
    session_names = await cm.get_community_session_names()
    assert session_names == []

    # Check that trying to get a specific session config raises an error
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Community session any_session_name not found in configuration",
    ):
        await cm.get_community_session_config("any_session_name")


@pytest.mark.asyncio
async def test_get_config_missing_env(monkeypatch):
    from deephaven_mcp import config

    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(
        RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"
    ):
        await config.ConfigManager().get_config()


@pytest.mark.asyncio
async def test_clear_config_cache_community_sessions_2():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache({"community_sessions": {"a_session": {}}})
    cfg1 = await cm.get_config()
    assert "a_session" in cfg1["community_sessions"]
    await cm.clear_config_cache()
    await cm.set_config_cache({"community_sessions": {"b_session": {}}})
    cfg2 = await cm.get_config()
    assert "b_session" in cfg2["community_sessions"]
    assert "a_session" not in cfg2["community_sessions"]


@pytest.mark.asyncio
async def test_get_community_session_config_2():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_COMMUNITY_SESSIONS_CONFIG)
    cfg = await cm.get_community_session_config("local")
    assert cfg["host"] == "localhost"
    with pytest.raises(
        CommunitySessionConfigurationError,
        match="Community session nonexistent not found in configuration",
    ):
        await cm.get_community_session_config("nonexistent")


@pytest.mark.asyncio
async def test_get_community_session_names_2():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_COMMUNITY_SESSIONS_CONFIG)
    names = await cm.get_community_session_names()
    assert "local" in names


# --- Enterprise System Config Access Tests ---

VALID_ENTERPRISE_SYSTEM_CONFIG_SECTION = {
    "enterprise_systems": {
        "prod_enterprise": {
            "connection_json_url": "https://enterprise.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "prod_user_env",
            "password_env_var": "PROD_PASSWORD_KEY",
        },
        "dev_enterprise": {
            "connection_json_url": "https://dev.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "testuser",
            "password_env_var": "DEV_PASS_ENV",
        },
    }
}


@pytest.mark.asyncio
async def test_get_enterprise_system_config_valid():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    # VALID_COMMUNITY_SESSIONS_CONFIG is defined globally in this test file
    full_config = {
        "community_sessions": VALID_COMMUNITY_SESSIONS_CONFIG.get(
            "community_sessions", {}
        ),
        **VALID_ENTERPRISE_SYSTEM_CONFIG_SECTION,
    }
    await cm.set_config_cache(full_config)

    system_config = await cm.get_enterprise_system_config("prod_enterprise")
    assert (
        system_config["connection_json_url"]
        == "https://enterprise.example.com/iris/connection.json"
    )
    assert system_config["auth_type"] == "password"


@pytest.mark.asyncio
async def test_get_enterprise_system_config_not_found():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_ENTERPRISE_SYSTEM_CONFIG_SECTION)
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="Enterprise system 'non_existent_session' not found",
    ):
        await cm.get_enterprise_system_config("non_existent_session")


@pytest.mark.asyncio
async def test_get_enterprise_system_config_key_missing():  # When 'enterprise_systems' itself is missing
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(
        {}
    )  # Empty config, so "enterprise_systems" key is missing
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="Enterprise system 'any_system' not found in configuration.",
    ):
        await cm.get_enterprise_system_config("any_system")


@pytest.mark.asyncio
async def test_get_enterprise_system_config_not_a_dict():  # When 'enterprise_systems' is not a dict
    from unittest import mock

    from deephaven_mcp import config

    cm = config.ConfigManager()
    # Mock get_config to return a config where 'enterprise_systems' is not a dictionary
    bad_config_data = {"enterprise_systems": "not_a_dictionary"}
    async_mock_get_config = mock.AsyncMock(return_value=bad_config_data)

    with mock.patch.object(cm, "get_config", new=async_mock_get_config):
        with pytest.raises(
            EnterpriseSystemConfigurationError,
            match="Enterprise system 'any_session' not found",
        ):
            await cm.get_enterprise_system_config("any_session")


@pytest.mark.asyncio
async def test_get_enterprise_system_names_valid(caplog):
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_ENTERPRISE_SYSTEM_CONFIG_SECTION)
    with caplog.at_level("DEBUG"):
        system_names = await cm.get_all_enterprise_system_names()
    # Sort for comparison as dict key order is not guaranteed for older Pythons
    assert sorted(system_names) == sorted(["dev_enterprise", "prod_enterprise"])
    assert "Found 2 enterprise system(s)" in caplog.text


@pytest.mark.asyncio
async def test_get_enterprise_system_names_empty_config():  # When 'enterprise_systems' is an empty dict
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache({"enterprise_systems": {}})
    names = await cm.get_all_enterprise_system_names()
    assert names == []


@pytest.mark.asyncio
async def test_get_enterprise_system_names_key_missing():  # When 'enterprise_systems' key is absent
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(
        {}
    )  # Empty config, so 'enterprise_systems' key is missing
    names = await cm.get_all_enterprise_system_names()
    assert names == []


@pytest.mark.asyncio
async def test_get_enterprise_system_names_not_a_dict(
    caplog,
):  # When 'enterprise_systems' is not a dict
    from unittest import mock

    from deephaven_mcp import config

    cm = config.ConfigManager()
    # Mock get_config to return a config where 'enterprise_systems' is not a dictionary
    bad_config_data = {"enterprise_systems": "not_a_dictionary"}
    async_mock_get_config = mock.AsyncMock(return_value=bad_config_data)

    with mock.patch.object(cm, "get_config", new=async_mock_get_config):
        with caplog.at_level("WARNING"):
            system_names = await cm.get_all_enterprise_system_names()

    assert system_names == []
    assert (
        "'enterprise_systems' is not a dictionary, returning empty list of names."
        in caplog.text
    )
