"""Tests for Deephaven MCP configuration management."""

import importlib
import json
import os
import sys
import re
import logging
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

    bad_config = {"extra": "some_value"}  # This key is not defined in REQUIRED_TOP_LEVEL_KEYS or OPTIONAL_TOP_LEVEL_KEYS
    cm = config.ConfigManager()
    with pytest.raises(
        ValueError,
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
        ValueError,
        match="Missing required top-level keys in Deephaven MCP config: {'must_have_this'}",
    ):
        config.ConfigManager.validate_config(bad_config)

    # Reset to default after test if necessary, though monkeypatch handles cleanup
    # monkeypatch.setattr(config.ConfigManager, "_REQUIRED_TOP_LEVEL_KEYS", set())


def test_validate_config_invalid_schema():
    from deephaven_mcp import config

    # Case: Minimal config is valid
    valid_config = MINIMAL_COMMUNITY_SESSIONS_CONFIG.copy()
    assert config.ConfigManager.validate_config(valid_config) == valid_config
    # Case: Empty config (missing community_sessions key) is also valid
    empty_config = {}
    assert config.ConfigManager.validate_config(empty_config) == empty_config


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
    assert any(
        "Deephaven community session configuration loaded and validated successfully"
        in r
        for r in caplog.text.splitlines()
    )


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
    import importlib
    from unittest import mock

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
    with pytest.raises(ValueError):
        await cm.get_config()


@pytest.mark.asyncio
async def test_get_config_file_not_found(monkeypatch, caplog):
    from deephaven_mcp import config
    import os

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
async def test_get_config_permission_error(tmp_path, monkeypatch, caplog):
    from deephaven_mcp import config
    import os
    import stat

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
    assert f"Permission denied when trying to read configuration file: {str(unreadable_config_file)}" in caplog.text

    # Clean up by making it writable so it can be deleted by tmp_path
    unreadable_config_file.chmod(stat.S_IWUSR | stat.S_IRUSR)


@pytest.mark.asyncio
async def test_get_config_other_os_error_on_read(monkeypatch, caplog):
    from deephaven_mcp import config
    from unittest import mock
    import aiofiles

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
    
    assert f"Unexpected error loading or parsing config file {config_file_path}: Simulated OS error on read" in caplog.text
    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_validate_config_missing_required_key_runtime(monkeypatch, caplog):
    from deephaven_mcp import config
    import aiofiles # For mocking
    from unittest import mock # For mocking
    import json # For dumping test config

    # Temporarily add a required key
    # original_required_keys = config.ConfigManager._REQUIRED_TOP_LEVEL_KEYS # monkeypatch handles restoration
    monkeypatch.setattr(config.ConfigManager, '_REQUIRED_TOP_LEVEL_KEYS', {'must_have_this'})

    cm = config.ConfigManager()
    invalid_config_data = {"community_sessions": {}} # Missing 'must_have_this'
    
    config_file_path = "/fake/path/config_missing_req.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    
    mock_file_read_content = mock.AsyncMock(return_value=json.dumps(invalid_config_data).encode('utf-8'))
    mock_async_context_manager_req = mock.AsyncMock()
    mock_async_context_manager_req.__aenter__.return_value.read = mock_file_read_content
    
    original_aio_open_req = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager_req)

    with pytest.raises(
            config.McpConfigurationError,
            match=r"Missing required top-level keys in Deephaven MCP config: {'must_have_this'}",
    ):
        await cm.get_config() # This will load, then validate

    assert r"Missing required top-level keys in Deephaven MCP config: {'must_have_this'}" in caplog.text
    
    aiofiles.open = original_aio_open_req


@pytest.mark.asyncio
async def test_get_config_uses_cache_and_logs(monkeypatch, caplog):
    from deephaven_mcp import config
    import aiofiles
    import json
    from unittest import mock

    config_file_path = "/fake/path/config_for_cache_test.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    valid_config_data = {"community_sessions": {"test_session": {"host": "localhost"}}}

    # Mock aiofiles.open to be called only once for the read
    mock_file_read_content = mock.AsyncMock(return_value=json.dumps(valid_config_data).encode('utf-8'))
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
    assert "Returning cached Deephaven MCP application configuration." not in caplog.text
    aiofiles_open_mock.assert_called_once_with(config_file_path, mode="r")

    caplog.clear()
    # Set log level to DEBUG for the relevant logger to capture the cache message
    caplog.set_level(logging.DEBUG, logger="deephaven_mcp.config")
    # Second call - should use cache
    config2 = await cm.get_config()
    assert config1 is config2  # Should be the same object from cache
    assert "Using cached Deephaven MCP application configuration." in caplog.text
    # Ensure aiofiles.open was not called again
    aiofiles_open_mock.assert_called_once() # Still called only once in total

    # Restore original aiofiles.open
    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_get_config_unknown_top_level_key(monkeypatch, caplog):
    from deephaven_mcp import config
    import aiofiles
    import json
    from unittest import mock

    config_file_path = "/fake/path/config_unknown_key.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {"some_unknown_key": {}, "community_sessions": {}}

    mock_file_read_content = mock.AsyncMock(return_value=json.dumps(invalid_config_data).encode('utf-8'))
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
    
    assert r"Unknown top-level keys in Deephaven MCP config: {'some_unknown_key'}" in caplog.text

    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_get_config_invalid_community_session_schema_from_file(monkeypatch, caplog):
    from deephaven_mcp import config
    import aiofiles
    import json
    from unittest import mock
    import re

    config_file_path = "/fake/path/invalid_community_schema.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {
        "community_sessions": {
            "bad_session": {
                "host": 12345,  # Invalid type, should be string
                "port": "not-a-port"
            }
        }
    }

    mock_file_read_content = mock.AsyncMock(return_value=json.dumps(invalid_config_data).encode('utf-8'))
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content
    
    original_aio_open = aiofiles.open 
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
    specific_error_detail = "Field 'host' in community session config for bad_session must be of type str, got int"
    # This is the message from CommunitySessionConfigurationError
    expected_mcp_error_message = f"Configuration validation failed for {config_file_path}: {specific_error_detail}"
    final_match_regex = re.escape(expected_mcp_error_message)

    with pytest.raises(
        config.McpConfigurationError,
        match=final_match_regex,
    ):
        await cm.get_config()
    
    # Check for the original error log from validate_community_sessions_config
    assert specific_error_detail in caplog.text 
    # Check for the new log from get_config when it catches the specific error
    assert f"Configuration validation failed for {config_file_path} due to specific session/system error: {specific_error_detail}" in caplog.text

    aiofiles.open = original_aio_open


@pytest.mark.asyncio
async def test_get_config_invalid_enterprise_system_schema_from_file(monkeypatch, caplog):
    from deephaven_mcp import config
    import aiofiles
    import json
    from unittest import mock
    import re

    config_file_path = "/fake/path/invalid_enterprise_schema.json"
    monkeypatch.setenv(config.CONFIG_ENV_VAR, config_file_path)
    invalid_config_data = {
        "enterprise_systems": {
            "bad_system": {
                "connection_json_url": 12345,  # Invalid type
                "auth_type": "invalid_auth_type"
            }
        }
    }

    mock_file_read_content = mock.AsyncMock(return_value=json.dumps(invalid_config_data).encode('utf-8'))
    mock_async_context_manager = mock.AsyncMock()
    mock_async_context_manager.__aenter__.return_value.read = mock_file_read_content
    
    original_aio_open = aiofiles.open
    aiofiles.open = mock.MagicMock(return_value=mock_async_context_manager)

    cm = config.ConfigManager()
    # The 'connection_json_url' error is raised first by validate_enterprise_systems_config
    specific_error_detail = "'connection_json_url' for enterprise system 'bad_system' must be a string, but got int."
    # This is the message from EnterpriseSystemConfigurationError
    expected_mcp_error_message = f"Configuration validation failed for {config_file_path}: {specific_error_detail}"
    final_match_regex = re.escape(expected_mcp_error_message)

    with pytest.raises(
        config.McpConfigurationError,
        match=final_match_regex,
    ):
        await cm.get_config()
    
    # Check for the original error log from validate_enterprise_systems_config
    assert specific_error_detail in caplog.text
    # Check for the new log from get_config when it catches the specific error
    assert f"Configuration validation failed for {config_file_path} due to specific session/system error: {specific_error_detail}" in caplog.text

    aiofiles.open = original_aio_open


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
    assert any(
        "Deephaven community session configuration loaded and validated successfully"
        in r
        for r in caplog.text.splitlines()
    )

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
            "auth_type": "api_key",
            "api_key_env_var": "PROD_API_KEY"
        },
        "dev_enterprise": {
            "connection_json_url": "https://dev.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "testuser",
            "password_env_var": "DEV_PASS_ENV"
        }
    }
}

@pytest.mark.asyncio
async def test_get_enterprise_system_config_valid():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    # VALID_COMMUNITY_SESSIONS_CONFIG is defined globally in this test file
    full_config = {
        "community_sessions": VALID_COMMUNITY_SESSIONS_CONFIG.get("community_sessions", {}), 
        **VALID_ENTERPRISE_SYSTEM_CONFIG_SECTION
    }
    await cm.set_config_cache(full_config)

    system_config = await cm.get_enterprise_system_config("prod_enterprise")
    assert system_config["connection_json_url"] == "https://enterprise.example.com/iris/connection.json"
    assert system_config["auth_type"] == "api_key"

@pytest.mark.asyncio
async def test_get_enterprise_system_config_not_found():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_ENTERPRISE_SYSTEM_CONFIG_SECTION) 
    with pytest.raises(EnterpriseSystemConfigurationError, match="Enterprise system 'non_existent_session' not found"):
        await cm.get_enterprise_system_config("non_existent_session")

@pytest.mark.asyncio
async def test_get_enterprise_system_config_key_missing(): # When 'enterprise_systems' itself is missing
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache({}) # Empty config, so "enterprise_systems" key is missing
    with pytest.raises(
        EnterpriseSystemConfigurationError,
        match="Enterprise system 'any_system' not found in configuration.",
    ):    await cm.get_enterprise_system_config("any_system")

@pytest.mark.asyncio
async def test_get_enterprise_system_config_not_a_dict(): # When 'enterprise_systems' is not a dict
    from deephaven_mcp import config
    from unittest import mock
    cm = config.ConfigManager()
    # Mock get_config to return a config where 'enterprise_systems' is not a dictionary
    bad_config_data = {"enterprise_systems": "not_a_dictionary"}
    async_mock_get_config = mock.AsyncMock(return_value=bad_config_data)

    with mock.patch.object(cm, 'get_config', new=async_mock_get_config):
        with pytest.raises(EnterpriseSystemConfigurationError, match="Enterprise system 'any_session' not found"):
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
async def test_get_enterprise_system_names_empty_config(): # When 'enterprise_systems' is an empty dict
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache({"enterprise_systems": {}})
    names = await cm.get_all_enterprise_system_names()
    assert names == []

@pytest.mark.asyncio
async def test_get_enterprise_system_names_key_missing(): # When 'enterprise_systems' key is absent
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache({}) # Empty config, so 'enterprise_systems' key is missing
    names = await cm.get_all_enterprise_system_names()
    assert names == []

@pytest.mark.asyncio
async def test_get_enterprise_system_names_not_a_dict(caplog): # When 'enterprise_systems' is not a dict
    from deephaven_mcp import config
    from unittest import mock

    cm = config.ConfigManager()
    # Mock get_config to return a config where 'enterprise_systems' is not a dictionary
    bad_config_data = {"enterprise_systems": "not_a_dictionary"}
    async_mock_get_config = mock.AsyncMock(return_value=bad_config_data)

    with mock.patch.object(cm, 'get_config', new=async_mock_get_config):
        with caplog.at_level("WARNING"):
            system_names = await cm.get_all_enterprise_system_names()
    
    assert system_names == []
    assert (
        "'enterprise_systems' is not a dictionary, returning empty list of names."
        in caplog.text
    )
