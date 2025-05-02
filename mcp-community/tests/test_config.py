"""Tests for Deephaven MCP configuration management."""

import sys
import os
import pytest
import pytest_asyncio
from unittest import mock
import importlib
import json

# Ensure local source is used for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

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
    "default_worker": "local",
}


MINIMAL_CONFIG = {
    "workers": {"local": {}},
    "default_worker": "local",
}

# --- Fixtures ---
@pytest_asyncio.fixture(autouse=True)
async def cleanup_config_cache():
    """Ensure config cache is cleared before and after each test."""
    from deephaven_mcp import config
    await config.clear_config_cache()
    yield
    await config.clear_config_cache()

# --- Validation tests ---
def test_validate_config_unknown_top_level_key():
    from deephaven_mcp import config
    bad_config = {"workers": {}, "default_worker": "local", "extra": 1}
    with pytest.raises(ValueError, match="Unknown top-level keys in Deephaven worker config: {'extra'}"):
        config.validate_config(bad_config)

def test_validate_config_missing_required_worker_field(monkeypatch):
    from deephaven_mcp import config
    monkeypatch.setattr(config, '_REQUIRED_FIELDS', ['host'])
    bad_config = {"workers": {"local": {}}, "default_worker": "local"}
    with pytest.raises(ValueError, match=r"Missing required fields in worker config for local: \['host'\]"):
        config.validate_config(bad_config)
    monkeypatch.setattr(config, '_REQUIRED_FIELDS', [])

def test_validate_config_invalid_schema():
    from deephaven_mcp import config
    bad_config = MINIMAL_CONFIG.copy()
    bad_config.pop("default_worker")  # Remove required key
    with pytest.raises(ValueError):
        config.validate_config(bad_config)

def test_validate_config_missing_default_worker():
    from deephaven_mcp import config
    bad_config = {"workers": {}}
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        config.validate_config(bad_config)

# --- Config loading tests ---
@pytest.mark.asyncio
async def test_get_config_valid():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    cfg = await config.get_config()
    assert cfg["default_worker"] == "local"
    assert "local" in cfg["workers"]

@pytest.mark.asyncio
async def test_get_config_missing_env(monkeypatch):
    from deephaven_mcp import config
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"):
        await config.get_config()

@pytest.mark.asyncio
async def test_get_config_invalid_json(monkeypatch):
    from deephaven_mcp import config
    import importlib
    from unittest import mock
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=b"not json")
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    with pytest.raises(ValueError):
        await config.get_config()

# --- Cache and worker config tests ---
@pytest.mark.asyncio
async def test_clear_config_cache():
    from deephaven_mcp import config
    await config.set_config_cache({"workers": {"a": {}}, "default_worker": "a"})
    cfg1 = await config.get_config()
    assert "a" in cfg1["workers"]
    await config.clear_config_cache()
    await config.set_config_cache({"workers": {"b": {}}, "default_worker": "b"})
    cfg2 = await config.get_config()
    assert "b" in cfg2["workers"]
    assert "a" not in cfg2["workers"]

@pytest.mark.asyncio
async def test_get_worker_config():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    cfg = await config.get_worker_config("local")
    assert cfg["host"] == "localhost"
    with pytest.raises(RuntimeError):
        await config.get_worker_config("nonexistent")

@pytest.mark.asyncio
async def test_get_worker_names():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    names = await config.get_worker_names()
    assert "local" in names

@pytest.mark.asyncio
async def test_get_worker_name_default():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    name = await config.get_worker_name_default()
    assert name == "local"

@pytest.mark.asyncio
async def test_resolve_worker_name():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    name = await config.resolve_worker_name("local")
    assert name == "local"
    name = await config.resolve_worker_name(None)
    assert name == "local"

@pytest.mark.asyncio
async def test_resolve_worker_name_no_default():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": {}}}
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        await config.set_config_cache(bad_config)

@pytest.mark.asyncio
async def test_get_worker_config_no_workers_key():
    from deephaven_mcp import config
    bad_config = {"default_worker": "local"}
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'workers'}"):
        await config.set_config_cache(bad_config)

@pytest.mark.asyncio
async def test_get_config_missing_env(monkeypatch):
    from deephaven_mcp import config
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    with pytest.raises(RuntimeError, match="Environment variable DH_MCP_CONFIG_FILE is not set"):
        await config.get_config()

@pytest.mark.asyncio
async def test_get_config_invalid_json(monkeypatch):
    from deephaven_mcp import config
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=b"not json")
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    with pytest.raises(ValueError):
        await config.get_config()

def test_validate_config_invalid_schema():
    from deephaven_mcp import config
    bad_config = {
        "workers": {
            "local": {"host": "localhost", "port": 10000}
        }
        # Missing default_worker
    }
    with pytest.raises(ValueError):
        config.validate_config(bad_config)

def test_validate_config_workers_not_dict():
    from deephaven_mcp import config
    bad_config = {"workers": ["not", "a", "dict"], "default_worker": "local"}
    with pytest.raises(ValueError, match="'workers' must be a dictionary in Deephaven worker config"):
        config.validate_config(bad_config)

def test_validate_config_workers_empty():
    from deephaven_mcp import config
    bad_config = {"workers": {}, "default_worker": "local"}
    with pytest.raises(ValueError, match="No workers defined in Deephaven worker config"):
        config.validate_config(bad_config)

def test_validate_config_worker_config_not_dict():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": "not_a_dict"}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Worker config for local must be a dictionary"):
        config.validate_config(bad_config)

def test_validate_config_unknown_worker_field():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": {"host": "localhost", "foo": 1}}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Unknown field 'foo' in worker config for local"):
        config.validate_config(bad_config)

def test_validate_config_worker_field_wrong_type():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": {"host": 123}}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Field 'host' in worker config for local must be of type"):
        config.validate_config(bad_config)

def test_validate_config_default_worker_not_in_workers():
    from deephaven_mcp import config
    bad_config = {"workers": {"remote": {}}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Default worker 'local' is not defined in workers"):
        config.validate_config(bad_config)

@pytest.mark.asyncio
async def test_resolve_worker_name_missing_default(monkeypatch):
    from deephaven_mcp import config
    # Patch get_config to return a config without default_worker
    async def fake_get_config():
        return {"workers": {"local": {}}}
    monkeypatch.setattr(config, "get_config", fake_get_config)
    with pytest.raises(RuntimeError, match="No worker name specified and no default_worker in config"):
        await config.resolve_worker_name(None)

@pytest.mark.asyncio
async def test_get_config_loads_and_validates(monkeypatch):
    from deephaven_mcp import config
    # Set up environment variable
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    # Mock aiofiles.open to return valid JSON config
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=json.dumps(VALID_CONFIG))
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    # Should load and validate config
    result = await config.get_config()
    assert result["default_worker"] == VALID_CONFIG["default_worker"]
    assert "local" in result["workers"]

@pytest.mark.asyncio
async def test_clear_config_cache():
    from deephaven_mcp import config
    await config.set_config_cache({"workers": {"a": {}}, "default_worker": "a"})
    cfg1 = await config.get_config()
    assert "a" in cfg1["workers"]
    await config.clear_config_cache()
    await config.set_config_cache({"workers": {"b": {}}, "default_worker": "b"})
    cfg2 = await config.get_config()
    assert "b" in cfg2["workers"]
    assert "a" not in cfg2["workers"]

@pytest.mark.asyncio
async def test_get_worker_config():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    cfg = await config.get_worker_config("local")
    assert cfg["host"] == "localhost"
    with pytest.raises(RuntimeError):
        await config.get_worker_config("nonexistent")

@pytest.mark.asyncio
async def test_get_worker_names():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    names = await config.get_worker_names()
    assert "local" in names

@pytest.mark.asyncio
async def test_get_worker_name_default():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    name = await config.get_worker_name_default()
    assert name == "local"

@pytest.mark.asyncio
async def test_resolve_worker_name():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    name = await config.resolve_worker_name("local")
    assert name == "local"
    name = await config.resolve_worker_name(None)
    assert name == "local"

def test_validate_config_missing_default_worker():
    from deephaven_mcp import config
    bad_config = {"workers": {}}
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        config.validate_config(bad_config)
