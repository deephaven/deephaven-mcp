import os
import pytest
import pytest_asyncio
import json
from unittest import mock

import importlib

VALID_CONFIG = {
    "workers": {
        "local": {
            "host": "localhost",
            "port": 10000,
            "auth_type": "token",
            "auth_token": "tokenval",
            "never_timeout": True,
            "session_type": "single",
            "use_tls": False
        }
    },
    "default_worker": "local"
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

@pytest.fixture
def patch_aiofiles(monkeypatch):
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(
        return_value=json.dumps(VALID_CONFIG).encode()
    )
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    return aiofiles_mock, aiofiles_open_ctx

@pytest.mark.asyncio
async def test_get_config_valid(monkeypatch, patch_aiofiles):
    from deephaven_mcp import config
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock, _ = patch_aiofiles
    cfg = await config.get_config()
    assert cfg["default_worker"] == "local"
    assert "local" in cfg["workers"]
    aiofiles_mock.open.assert_called_once_with("/fake/path/config.json", "r")

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

@pytest.mark.asyncio
async def test_get_config_invalid_schema(monkeypatch):
    from deephaven_mcp import config
    os.environ["DH_MCP_CONFIG_FILE"] = "/fake/path/config.json"
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=json.dumps(INVALID_CONFIG_MISSING_DEFAULT).encode())
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    with pytest.raises(ValueError):
        await config.get_config()

@pytest.mark.asyncio
async def test_clear_config_cache(monkeypatch, patch_aiofiles):
    from deephaven_mcp import config
    os.environ["DH_MCP_CONFIG_FILE"] = "/fake/path/config.json"
    await config.get_config()
    await config.clear_config_cache()
    # After clearing, a new call should reload (so open is called again)
    await config.get_config()
    patch_aiofiles[0].open.assert_called_with("/fake/path/config.json", "r")

@pytest.mark.asyncio
async def test_get_worker_config(monkeypatch, patch_aiofiles):
    from deephaven_mcp import config
    os.environ["DH_MCP_CONFIG_FILE"] = "/fake/path/config.json"
    await config.clear_config_cache()
    cfg = await config.get_worker_config("local")
    assert cfg["host"] == "localhost"
    # Should raise if worker doesn't exist
    with pytest.raises(RuntimeError):
        await config.get_worker_config("nonexistent")

@pytest.mark.asyncio
async def test_get_worker_names(monkeypatch, patch_aiofiles):
    from deephaven_mcp import config
    os.environ["DH_MCP_CONFIG_FILE"] = "/fake/path/config.json"
    await config.clear_config_cache()
    names = await config.get_worker_names()
    assert "local" in names

@pytest.mark.asyncio
async def test_get_worker_name_default(monkeypatch, patch_aiofiles):
    from deephaven_mcp import config
    os.environ["DH_MCP_CONFIG_FILE"] = "/fake/path/config.json"
    await config.clear_config_cache()
    name = await config.get_worker_name_default()
    assert name == "local"

@pytest.mark.asyncio
async def test_resolve_worker_name(monkeypatch, patch_aiofiles):
    from deephaven_mcp import config
    os.environ["DH_MCP_CONFIG_FILE"] = "/fake/path/config.json"
    await config.clear_config_cache()
    # Explicit
    name = await config.resolve_worker_name("local")
    assert name == "local"
    # Default
    name = await config.resolve_worker_name(None)
    assert name == "local"

@pytest.mark.asyncio
async def test_resolve_worker_name_no_default(monkeypatch):
    from deephaven_mcp import config
    # Config missing default_worker
    bad_config = {"workers": {}}
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=json.dumps(bad_config).encode())
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    await config.clear_config_cache()
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        await config.resolve_worker_name(None)

@pytest.mark.asyncio
async def test_get_worker_config_no_workers(monkeypatch):
    from deephaven_mcp import config
    # Config missing workers
    bad_config = {"workers": {}, "default_worker": "local"}
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=json.dumps(bad_config).encode())
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    await config.clear_config_cache()
    with pytest.raises(ValueError, match="Default worker 'local' is not defined in workers"):
        await config.get_worker_config("local")

@pytest.mark.asyncio
async def test_get_worker_config_worker_not_found(monkeypatch):
    from deephaven_mcp import config
    # Config with only 'local' worker
    test_config = {"workers": {"local": VALID_CONFIG["workers"]["local"]}, "default_worker": "local"}
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=json.dumps(test_config).encode())
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    await config.clear_config_cache()
    with pytest.raises(RuntimeError, match="Worker nonexistent not found in configuration"):
        await config.get_worker_config("nonexistent")

@pytest.mark.asyncio
async def test_get_worker_name_default_none(monkeypatch):
    from deephaven_mcp import config
    # Config missing default_worker
    bad_config = {"workers": {"local": VALID_CONFIG["workers"]["local"]}}
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/fake/path/config.json")
    aiofiles_mock = mock.Mock()
    aiofiles_open_ctx = mock.AsyncMock()
    aiofiles_open_ctx.__aenter__.return_value.read = mock.AsyncMock(return_value=json.dumps(bad_config).encode())
    aiofiles_mock.open = mock.Mock(return_value=aiofiles_open_ctx)
    monkeypatch.setitem(importlib.import_module('aiofiles').__dict__, 'open', aiofiles_mock.open)
    await config.clear_config_cache()
    with pytest.raises(ValueError, match="Missing required top-level keys in Deephaven worker config: {'default_worker'}"):
        await config.get_worker_name_default()
