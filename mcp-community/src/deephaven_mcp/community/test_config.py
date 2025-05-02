"""Tests for Deephaven MCP configuration management."""

import pytest
import pytest_asyncio

# The following imports are only used for tests that simulate file I/O or environment variable errors.
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
    # Patch _REQUIRED_FIELDS to require 'host'
    monkeypatch.setattr(config, '_REQUIRED_FIELDS', ['host'])
    bad_config = {"workers": {"local": {}}, "default_worker": "local"}
    with pytest.raises(ValueError, match=r"Missing required fields in worker config for local: \['host'\]"):
        config.validate_config(bad_config)
    # Restore _REQUIRED_FIELDS to empty
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
    bad_config = {"default_worker": "local"}  # workers key missing
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
    # Set initial config
    await config.set_config_cache({"workers": {"a": {}}, "default_worker": "a"})
    cfg1 = await config.get_config()
    assert "a" in cfg1["workers"]
    # Clear and set a new config
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
    # Should raise if worker doesn't exist
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
    # Explicit
    name = await config.resolve_worker_name("local")
    assert name == "local"
    # Default
    name = await config.resolve_worker_name(None)
    assert name == "local"

def test_validate_config_missing_default_worker():
    from deephaven_mcp import config
    bad_config = {"workers": {}}
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        config.validate_config(bad_config)

def test_validate_config_no_workers():
    from deephaven_mcp import config
    bad_config = {"workers": {}, "default_worker": "local"}
    with pytest.raises(ValueError, match="No workers defined in Deephaven worker config"):
        config.validate_config(bad_config)

def test_validate_config_workers_not_dict():
    from deephaven_mcp import config
    bad_config = {"workers": [], "default_worker": "local"}
    with pytest.raises(ValueError, match="'workers' must be a dictionary in Deephaven worker config"):
        config.validate_config(bad_config)

def test_validate_config_worker_config_not_dict():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": []}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Worker config for local must be a dictionary"):
        config.validate_config(bad_config)

def test_validate_config_unknown_worker_field():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": {"host": "localhost", "unknown_field": 123}}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Unknown field 'unknown_field' in worker config for local"):
        config.validate_config(bad_config)

def test_validate_config_worker_field_wrong_type():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": {"host": 123}}, "default_worker": "local"}
    with pytest.raises(ValueError, match="Field 'host' in worker config for local must be of type <class 'str'>"):
        config.validate_config(bad_config)

@pytest.mark.asyncio
async def test_get_worker_config_no_workers():
    from deephaven_mcp import config
    bad_config = {"workers": {}, "default_worker": "local"}
    with pytest.raises(ValueError, match="No workers defined in Deephaven worker config"):
        await config.set_config_cache(bad_config)

@pytest.mark.asyncio
async def test_get_worker_config_worker_not_found_error():
    from deephaven_mcp import config
    bad_config = {"workers": {"local": {}}, "default_worker": "local"}
    await config.set_config_cache(bad_config)
    with pytest.raises(RuntimeError, match="Worker nonexistent not found in configuration"):
        await config.get_worker_config("nonexistent")

@pytest.mark.asyncio
async def test_get_worker_config_worker_not_found():
    from deephaven_mcp import config
    # Config with only 'local' worker
    test_config = {"workers": {"local": VALID_CONFIG["workers"]["local"]}, "default_worker": "local"}
    await config.set_config_cache(test_config)
    with pytest.raises(RuntimeError, match="Worker nonexistent not found in configuration"):
        await config.get_worker_config("nonexistent")

@pytest.mark.asyncio
async def test_get_worker_name_default_none():
    from deephaven_mcp import config
    # Config missing default_worker
    bad_config = {"workers": {"local": VALID_CONFIG["workers"]["local"]}}
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        await config.set_config_cache(bad_config)
