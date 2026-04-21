"""Tests for deephaven_mcp.config._base — shared ConfigManager infrastructure."""

import os
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import aiofiles
import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config._base import (
    CONFIG_ENV_VAR,
    ConfigManager,
    _get_config_path,
    _load_and_validate_config,
    _load_config_from_file,
    _log_config_summary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_env():
    old = os.environ.get(CONFIG_ENV_VAR)
    if CONFIG_ENV_VAR in os.environ:
        del os.environ[CONFIG_ENV_VAR]
    yield
    if old is not None:
        os.environ[CONFIG_ENV_VAR] = old


def _make_aiofiles_mock(content: str):
    """Return an aiofiles.open mock that yields `content` on .read()."""
    mock_file = AsyncMock()
    mock_file.read.return_value = content
    ctx = AsyncMock()
    ctx.__aenter__.return_value = mock_file
    return MagicMock(return_value=ctx)


# ---------------------------------------------------------------------------
# CONFIG_ENV_VAR
# ---------------------------------------------------------------------------

def test_config_env_var_value():
    assert CONFIG_ENV_VAR == "DH_MCP_CONFIG_FILE"


# ---------------------------------------------------------------------------
# ConfigManager abstract base
# ---------------------------------------------------------------------------

def test_config_manager_is_abstract():
    """ConfigManager cannot be instantiated directly."""
    with pytest.raises(TypeError):
        ConfigManager()  # type: ignore[abstract]


def test_config_manager_subclass_must_implement_get_config():
    """Subclass missing get_config raises TypeError."""
    class Partial(ConfigManager):
        async def _set_config_cache(self, config):
            pass

    with pytest.raises(TypeError):
        Partial()  # type: ignore[abstract]


def test_config_manager_subclass_must_implement_set_config_cache():
    """Subclass missing _set_config_cache raises TypeError."""
    class Partial(ConfigManager):
        async def get_config(self):
            return {}

    with pytest.raises(TypeError):
        Partial()  # type: ignore[abstract]


@pytest.mark.asyncio
async def test_config_manager_clear_cache():
    """clear_config_cache sets _cache to None."""
    class Concrete(ConfigManager):
        async def get_config(self):
            return {}
        async def _set_config_cache(self, config):
            self._cache = config

    mgr = Concrete()
    mgr._cache = {"key": "value"}
    await mgr.clear_config_cache()
    assert mgr._cache is None


@pytest.mark.asyncio
async def test_config_manager_init_with_explicit_path():
    """ConfigManager stores an explicit config_path."""
    class Concrete(ConfigManager):
        async def get_config(self):
            return {}
        async def _set_config_cache(self, config):
            pass

    mgr = Concrete(config_path="/some/path.json")
    assert mgr._config_path == "/some/path.json"


# ---------------------------------------------------------------------------
# _get_config_path
# ---------------------------------------------------------------------------

def test_get_config_path_returns_env_var(monkeypatch):
    monkeypatch.setenv(CONFIG_ENV_VAR, "/etc/config.json")
    assert _get_config_path() == "/etc/config.json"


def test_get_config_path_raises_when_unset(monkeypatch):
    monkeypatch.delenv(CONFIG_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match="DH_MCP_CONFIG_FILE is not set"):
        _get_config_path()


# ---------------------------------------------------------------------------
# _load_config_from_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_load_config_from_file_success(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {"local": {"host": "localhost"}}}')
    result = await _load_config_from_file(str(cfg_file))
    assert result["sessions"]["local"]["host"] == "localhost"


@pytest.mark.asyncio
async def test_load_config_from_file_json5_comments(tmp_path):
    cfg_file = tmp_path / "config.json5"
    cfg_file.write_text('{\n  // comment\n  "sessions": {}\n}')
    result = await _load_config_from_file(str(cfg_file))
    assert result == {"sessions": {}}


@pytest.mark.asyncio
async def test_load_config_from_file_not_found():
    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        await _load_config_from_file("/nonexistent/path/config.json")


@pytest.mark.asyncio
async def test_load_config_from_file_permission_error():
    with patch("aiofiles.open", side_effect=PermissionError("denied")):
        with pytest.raises(ConfigurationError, match="Permission denied"):
            await _load_config_from_file("/any/path.json")


@pytest.mark.asyncio
async def test_load_config_from_file_invalid_json():
    with patch("aiofiles.open", _make_aiofiles_mock("{ invalid json }")):
        with pytest.raises(ConfigurationError, match="Invalid JSON/JSON5"):
            await _load_config_from_file("/any/path.json")


@pytest.mark.asyncio
async def test_load_config_from_file_generic_exception():
    with patch("aiofiles.open", side_effect=OSError("disk error")):
        with pytest.raises(ConfigurationError, match="Unexpected error"):
            await _load_config_from_file("/any/path.json")


# ---------------------------------------------------------------------------
# _load_and_validate_config
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_load_and_validate_config_success(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')
    validator = lambda d: d  # identity
    result = await _load_and_validate_config(str(cfg_file), validator, "test")
    assert result == {"sessions": {}}


@pytest.mark.asyncio
async def test_load_and_validate_config_validator_error_wrapped(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"sessions": {}}')

    def bad_validator(d):
        raise ValueError("bad config")

    with pytest.raises(ConfigurationError, match="Error loading configuration file"):
        await _load_and_validate_config(str(cfg_file), bad_validator, "test")


@pytest.mark.asyncio
async def test_load_and_validate_config_load_error_wrapped():
    with pytest.raises(ConfigurationError, match="Error loading configuration file"):
        await _load_and_validate_config(
            "/nonexistent.json", lambda d: d, "test"
        )


# ---------------------------------------------------------------------------
# _log_config_summary
# ---------------------------------------------------------------------------

def test_log_config_summary_with_redactor(caplog):
    redactor = lambda c: {k: "[R]" if k == "password" else v for k, v in c.items()}
    with caplog.at_level("INFO", logger="deephaven_mcp.config._base"):
        _log_config_summary({"password": "secret", "host": "x"}, redactor=redactor)
    assert "[R]" in caplog.text
    assert "secret" not in caplog.text


def test_log_config_summary_without_redactor_logs_config(caplog):
    with caplog.at_level("INFO", logger="deephaven_mcp.config._base"):
        _log_config_summary({"key": "value"})
    assert "key" in caplog.text


def test_log_config_summary_json_serialization_failure(caplog):
    with patch("deephaven_mcp.config._base.json5.dumps", side_effect=TypeError("not serializable")):
        with caplog.at_level("WARNING", logger="deephaven_mcp.config._base"):
            _log_config_summary({"x": object()})
    assert "Failed to format config as JSON" in caplog.text
