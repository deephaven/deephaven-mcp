"""Tests for Deephaven MCP configuration management."""

import pytest
import pytest_asyncio
from unittest import mock
import importlib

def make_minimal_valid_config(worker_name: str = "local") -> dict:
    """Helper to create a minimal valid config for testing."""
    return {
        "workers": {worker_name: {}},
        "default_worker": worker_name,
    }

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

@pytest.mark.asyncio
async def test_get_config_valid():
    from deephaven_mcp import config
    await config.set_config_cache(VALID_CONFIG)
    cfg = await config.get_config()
    assert cfg["default_worker"] == "local"
    assert "local" in cfg["workers"]

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

INVALID_CONFIG_MISSING_DEFAULT = {
    "workers": {
        "local": {"host": "localhost", "port": 10000}
    }
    # Missing default_worker
}

@pytest_asyncio.fixture(autouse=True)
async def cleanup_config_cache():
    from deephaven_mcp import config
    await config.clear_config_cache()
    yield
    await config.clear_config_cache()

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
    with pytest.raises(ValueError):
        config.validate_config(INVALID_CONFIG_MISSING_DEFAULT)

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
