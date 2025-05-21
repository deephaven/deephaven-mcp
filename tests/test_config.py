"""Tests for Deephaven MCP configuration management."""

import importlib
import json
import os
import sys
from unittest import mock

import pytest
import pytest_asyncio

from deephaven_mcp.config import WorkerConfigurationError

# TODO: needed?
# Ensure local source is used for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# --- Constants and helpers ---
VALID_CONFIG = {
    "workers": {
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


MINIMAL_CONFIG = {"workers": {"local": {}}}


# --- Fixtures ---
@pytest_asyncio.fixture(autouse=True)
async def cleanup_config_cache():
    """Ensure config cache is cleared before and after each test using a local ConfigManager."""
    # No-op: Use local ConfigManager in each test, so no global cache to clear
    yield


# --- Validation tests ---
def test_validate_config_unknown_top_level_key():
    from deephaven_mcp import config

    bad_config = {"workers": {}, "extra": 1}
    cm = config.ConfigManager()
    with pytest.raises(
        ValueError, match="Unknown top-level keys in Deephaven worker config: {'extra'}"
    ):
        config.ConfigManager().validate_config(bad_config)
        config.ConfigManager.validate_config(bad_config)


def test_validate_config_missing_required_worker_field(monkeypatch):
    from deephaven_mcp import config

    monkeypatch.setattr(config, "_REQUIRED_FIELDS", ["host"])
    bad_config = {"workers": {"local": {}}}
    with pytest.raises(
        ValueError,
        match=r"Missing required fields in worker config for local: \['host'\]",
    ):
        config.ConfigManager.validate_config(bad_config)
    monkeypatch.setattr(config, "_REQUIRED_FIELDS", [])


def test_validate_config_invalid_schema():
    from deephaven_mcp import config

    # Case: Minimal config is valid
    valid_config = MINIMAL_CONFIG.copy()
    assert config.ConfigManager.validate_config(valid_config) == valid_config
    # Case: Missing workers key
    bad_config = {}
    with pytest.raises(ValueError):
        config.ConfigManager.validate_config(bad_config)


# --- Config loading tests ---
@pytest.mark.asyncio
async def test_get_config_valid():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_CONFIG)
    cfg = await cm.get_config()
    assert "workers" in cfg
    assert "local" in cfg["workers"]


@pytest.mark.asyncio
async def test_get_config_sets_cache_and_logs(monkeypatch, caplog):
    import importlib
    import json
    from unittest import mock

    from deephaven_mcp import config

    # Prepare a valid config JSON string
    valid_config = {
        "workers": {
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
        "Deephaven worker configuration loaded and validated successfully" in r
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


# --- Cache and worker config tests ---
@pytest.mark.asyncio
async def test_clear_config_cache():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache({"workers": {"a": {}}})
    cfg1 = await cm.get_config()
    assert "a" in cfg1["workers"]
    await cm.clear_config_cache()
    await cm.set_config_cache({"workers": {"b": {}}})
    cfg2 = await cm.get_config()
    assert "b" in cfg2["workers"]
    assert "a" not in cfg2["workers"]


@pytest.mark.asyncio
async def test_get_worker_config():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_CONFIG)
    cfg = await cm.get_worker_config("local")
    assert cfg["host"] == "localhost"
    with pytest.raises(RuntimeError):
        await cm.get_worker_config("nonexistent")


@pytest.mark.asyncio
async def test_get_worker_names():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_CONFIG)
    names = await cm.get_worker_names()
    assert "local" in names


@pytest.mark.asyncio
async def test_get_worker_config_no_workers_key():
    from deephaven_mcp import config

    bad_config = {}
    with pytest.raises(
        ValueError,
        match="Missing required top-level keys in Deephaven worker config: {'workers'}",
    ):
        await config.ConfigManager().set_config_cache(bad_config)


@pytest.mark.asyncio
async def test_get_config_missing_env(monkeypatch):
    from deephaven_mcp import config

    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(
        RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"
    ):
        await config.ConfigManager().get_config()


def test_validate_config_workers_not_dict():
    from deephaven_mcp import config

    bad_config = {"workers": ["not", "a", "dict"]}
    with pytest.raises(
        ValueError, match="'workers' must be a dictionary in Deephaven worker config"
    ):
        config.ConfigManager.validate_config(bad_config)


def test_validate_config_workers_empty():
    from deephaven_mcp import config

    bad_config = {"workers": {}}
    with pytest.raises(
        ValueError, match="No workers defined in Deephaven worker config"
    ):
        config.ConfigManager.validate_config(bad_config)


def test_validate_config_worker_config_not_dict():
    from deephaven_mcp import config

    bad_config = {"workers": {"local": "not_a_dict"}}
    with pytest.raises(
        ValueError, match="Worker config for local must be a dictionary"
    ):
        config.ConfigManager.validate_config(bad_config)


def test_validate_config_unknown_worker_field():
    from deephaven_mcp import config

    bad_config = {"workers": {"local": {"foo": 1}}}
    with pytest.raises(
        ValueError, match="Unknown field 'foo' in worker config for local"
    ):
        config.ConfigManager.validate_config(bad_config)


def test_validate_config_worker_field_wrong_type():
    from deephaven_mcp import config

    bad_config = {"workers": {"local": {"host": 123}}}
    with pytest.raises(
        ValueError, match="Field 'host' in worker config for local must be of type"
    ):
        config.ConfigManager.validate_config(bad_config)


@pytest.mark.asyncio
async def test_clear_config_cache():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache({"workers": {"a": {}}})
    cfg1 = await cm.get_config()
    assert "a" in cfg1["workers"]
    await cm.clear_config_cache()
    await cm.set_config_cache({"workers": {"b": {}}})
    cfg2 = await cm.get_config()
    assert "b" in cfg2["workers"]
    assert "a" not in cfg2["workers"]


@pytest.mark.asyncio
async def test_get_worker_config():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_CONFIG)
    cfg = await cm.get_worker_config("local")
    assert cfg["host"] == "localhost"
    with pytest.raises(WorkerConfigurationError):
        await cm.get_worker_config("nonexistent")


@pytest.mark.asyncio
async def test_get_worker_names():
    from deephaven_mcp import config

    cm = config.ConfigManager()
    await cm.set_config_cache(VALID_CONFIG)
    names = await cm.get_worker_names()
    assert "local" in names
