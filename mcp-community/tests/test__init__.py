import sys
import os
import logging
import types
import builtins
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

def test_module_all_exports():
    import deephaven_mcp.community as mod
    # __all__ should include mcp_server and run_server
    assert hasattr(mod, 'mcp_server')
    assert hasattr(mod, 'run_server')
    assert 'mcp_server' in mod.__all__
    assert 'run_server' in mod.__all__

def test_run_server_async_finally(monkeypatch):
    import deephaven_mcp.community as mod
    monkeypatch.setattr(mod, '_LOGGER', MagicMock())
    monkeypatch.setattr(mod, 'os', MagicMock())
    monkeypatch.setattr(mod, 'sys', MagicMock())
    # Patch _CONFIG_MANAGER.get_config and mcp_server.run to be async mocks
    mock_config = MagicMock()
    mock_config.get_config = AsyncMock()
    monkeypatch.setattr(mod, '_CONFIG_MANAGER', mock_config)
    mock_server = MagicMock()
    # Make mcp_server.run raise an exception to trigger finally
    def raise_exc(*a, **kw):
        raise RuntimeError("fail")
    mock_server.run = raise_exc
    mock_server.name = 'testserver'
    monkeypatch.setattr(mod, 'mcp_server', mock_server)
    mock_asyncio_run = MagicMock()
    # Actually run the async function synchronously for coverage
    def fake_asyncio_run(coro):
        try:
            import asyncio
            asyncio.run(coro)
        except RuntimeError:
            # If already in event loop (pytest-asyncio), just await
            import asyncio
            return asyncio.ensure_future(coro)
    monkeypatch.setattr(mod, 'asyncio', MagicMock(run=fake_asyncio_run))
    mod.os.getenv.return_value = 'INFO'
    with pytest.raises(RuntimeError, match="fail"):
        mod.run_server('stdio')
    # _LOGGER.info should be called for both start and stop
    assert mod._LOGGER.info.call_count >= 2

def test_run_server_stdio(monkeypatch):
    import deephaven_mcp.community as mod
    # Patch all side effects
    monkeypatch.setattr(mod, '_LOGGER', MagicMock())
    monkeypatch.setattr(mod, 'os', MagicMock())
    monkeypatch.setattr(mod, 'sys', MagicMock())
    monkeypatch.setattr(mod, '_CONFIG_MANAGER', MagicMock())
    monkeypatch.setattr(mod, 'mcp_server', MagicMock())
    mock_asyncio = MagicMock()
    monkeypatch.setattr(mod, 'asyncio', mock_asyncio)
    # Setup mocks
    mod.os.getenv.return_value = 'INFO'
    mod.mcp_server.name = 'testserver'
    mock_logger = MagicMock()
    monkeypatch.setattr(mod, '_LOGGER', mock_logger)
    # Call run_server
    monkeypatch.setattr(mod.logging, "basicConfig", MagicMock())
    mod.run_server('stdio')
    mock_logger.info.assert_any_call(f"Starting MCP server 'testserver' with transport=stdio")
    mock_asyncio.run.assert_called()
    # Should use sys.stderr for stdio
    assert mod.sys.stderr == mod.logging.basicConfig.call_args.kwargs['stream']

def test_run_server_sse(monkeypatch):
    import deephaven_mcp.community as mod
    monkeypatch.setattr(mod, '_LOGGER', MagicMock())
    monkeypatch.setattr(mod, 'os', MagicMock())
    monkeypatch.setattr(mod, 'sys', MagicMock())
    monkeypatch.setattr(mod, '_CONFIG_MANAGER', MagicMock())
    monkeypatch.setattr(mod, 'mcp_server', MagicMock())
    mock_asyncio = MagicMock()
    monkeypatch.setattr(mod, 'asyncio', mock_asyncio)
    mod.os.getenv.return_value = 'INFO'
    mod.mcp_server.name = 'testserver'
    monkeypatch.setattr(mod.logging, "basicConfig", MagicMock())
    mod.run_server('sse')
    # Should use sys.stdout for sse
    assert mod.sys.stdout == mod.logging.basicConfig.call_args.kwargs['stream']

def test_run_server_async_logic(monkeypatch):
    import deephaven_mcp.community as mod
    monkeypatch.setattr(mod, '_LOGGER', MagicMock())
    monkeypatch.setattr(mod, 'os', MagicMock())
    monkeypatch.setattr(mod, 'sys', MagicMock())
    mock_config = MagicMock()
    monkeypatch.setattr(mod, '_CONFIG_MANAGER', mock_config)
    mock_server = MagicMock()
    monkeypatch.setattr(mod, 'mcp_server', mock_server)
    mock_asyncio = MagicMock()
    monkeypatch.setattr(mod, 'asyncio', mock_asyncio)
    mod.os.getenv.return_value = 'INFO'
    mod.mcp_server.name = 'testserver'
    # Simulate the async run logic
    mod.run_server('stdio')
    # The async function should be passed to asyncio.run
    assert mock_asyncio.run.called
    # The config manager's get_config should be awaited inside the async function
    # (We can't await here, but we can check that the async function was constructed)
    # This is a smoke test for coverage
