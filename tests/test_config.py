"""Tests for Deephaven MCP configuration management."""

import importlib
import json
import os
import sys
from unittest import mock

import pytest
import pytest_asyncio

from deephaven_mcp.config import (
    CommunitySessionConfigurationError,
    EnterpriseSessionConfigurationError,
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

    bad_config = {"community_sessions": {}, "extra": 1}
    cm = config.ConfigManager()
    with pytest.raises(
        ValueError,
        match="Unknown top-level keys in Deephaven MCP config: {'extra'}",
    ):
        config.ConfigManager().validate_config(bad_config)
        config.ConfigManager.validate_config(bad_config)


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


# --- Enterprise Session Config Access Tests ---

VALID_ENTERPRISE_CONFIG_SECTION = {
    "enterprise_sessions": {
        "prod_cluster": {
            "connection_json_url": "https://enterprise.example.com/iris/connection.json",
            "auth_type": "api_key",
            "api_key_env_var": "PROD_API_KEY"
        },
        "staging_cluster": {
            "connection_json_url": "https://staging.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "testuser",
            "password_env_var": "STAGING_PASS_ENV"
        }
    }
}

@pytest.mark.asyncio
async def test_get_enterprise_session_config_valid():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    # VALID_COMMUNITY_SESSIONS_CONFIG is defined globally in this test file
    full_config = {
        "community_sessions": VALID_COMMUNITY_SESSIONS_CONFIG.get("community_sessions", {}), 
        **VALID_ENTERPRISE_CONFIG_SECTION
    }
    await cm.set_config_cache(full_config)

    session_config = await cm.get_enterprise_session_config("prod_cluster")
    assert session_config["connection_json_url"] == "https://enterprise.example.com/iris/connection.json"
    assert session_config["auth_type"] == "api_key"

@pytest.mark.asyncio
async def test_get_enterprise_session_config_not_found():
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_ENTERPRISE_CONFIG_SECTION) 
    with pytest.raises(EnterpriseSessionConfigurationError, match="Enterprise session 'non_existent_session' not found"):
        await cm.get_enterprise_session_config("non_existent_session")

@pytest.mark.asyncio
async def test_get_enterprise_session_config_key_missing(): # When 'enterprise_sessions' itself is missing
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache({}) # Empty config
    with pytest.raises(EnterpriseSessionConfigurationError, match="Enterprise session 'any_session' not found"):
        await cm.get_enterprise_session_config("any_session")

@pytest.mark.asyncio
async def test_get_enterprise_session_config_not_a_dict(): # When 'enterprise_sessions' is not a dict
    from deephaven_mcp import config
    from unittest import mock

    cm = config.ConfigManager()
    # Mock get_config to return a config where 'enterprise_sessions' is not a dictionary
    bad_config_data = {"enterprise_sessions": "not_a_dictionary"}
    async_mock_get_config = mock.AsyncMock(return_value=bad_config_data)

    with mock.patch.object(cm, 'get_config', new=async_mock_get_config):
        with pytest.raises(EnterpriseSessionConfigurationError, match="Enterprise session 'any_session' not found"):
            await cm.get_enterprise_session_config("any_session")

@pytest.mark.asyncio
async def test_get_enterprise_session_names_valid(caplog):
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_ENTERPRISE_CONFIG_SECTION)
    with caplog.at_level("DEBUG"):
        session_names = await cm.get_enterprise_session_names()
    # Sort for comparison as dict key order is not guaranteed for older Pythons
    assert sorted(session_names) == sorted(["prod_cluster", "staging_cluster"])
    assert "Found 2 enterprise session(s)" in caplog.text

@pytest.mark.asyncio
async def test_get_enterprise_session_names_empty_config(): # When 'enterprise_sessions' is an empty dict
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache({"enterprise_sessions": {}})
    session_names = await cm.get_enterprise_session_names()
    assert session_names == []

@pytest.mark.asyncio
async def test_get_enterprise_session_names_key_missing(): # When 'enterprise_sessions' key is absent
    from deephaven_mcp import config
    cm = config.ConfigManager()
    await cm.set_config_cache({}) 
    session_names = await cm.get_enterprise_session_names()
    assert session_names == []

@pytest.mark.asyncio
async def test_get_enterprise_session_names_not_a_dict(caplog): # When 'enterprise_sessions' is not a dict
    from deephaven_mcp import config
    from unittest import mock

    cm = config.ConfigManager()
    # Mock get_config to return a config where 'enterprise_sessions' is not a dictionary
    bad_config_data = {"enterprise_sessions": "not_a_dictionary"}
    async_mock_get_config = mock.AsyncMock(return_value=bad_config_data)

    with mock.patch.object(cm, 'get_config', new=async_mock_get_config):
        with caplog.at_level("WARNING"):
            session_names = await cm.get_enterprise_session_names()
    
    assert session_names == []
    assert "'enterprise_sessions' is not a dictionary, returning empty list of names." in caplog.text

