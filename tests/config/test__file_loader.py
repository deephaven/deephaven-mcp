"""Tests for ``deephaven_mcp.config._file_loader``.

Covers the async JSON/JSON5 file loader, including its
``ConfigurationError`` wrapping for I/O and parse failures.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config._file_loader import load_config_from_file

# ---------------------------------------------------------------------------
# load_config_from_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_config_from_file_success(tmp_path):
    f = tmp_path / "ok.json"
    f.write_text('{"a": 1, "b": [1, 2]}')
    out = await load_config_from_file(str(f))
    assert out == {"a": 1, "b": [1, 2]}


@pytest.mark.asyncio
async def test_load_config_from_file_missing_raises(tmp_path):
    missing = tmp_path / "does-not-exist.json"
    with pytest.raises(ConfigurationError, match="not found"):
        await load_config_from_file(str(missing))


@pytest.mark.asyncio
async def test_load_config_from_file_permission_denied(tmp_path):
    f = tmp_path / "x.json"
    f.write_text("{}")
    with (
        patch("aiofiles.open", side_effect=PermissionError("denied")),
        pytest.raises(ConfigurationError, match="Permission denied"),
    ):
        await load_config_from_file(str(f))


@pytest.mark.asyncio
async def test_load_config_from_file_invalid_json_raises(tmp_path):
    f = tmp_path / "bad.json"
    f.write_text("{ this is not valid json")
    with pytest.raises(ConfigurationError, match="Invalid JSON"):
        await load_config_from_file(str(f))


@pytest.mark.asyncio
async def test_load_config_from_file_unexpected_error(tmp_path):
    f = tmp_path / "x.json"
    f.write_text("{}")
    with (
        patch("aiofiles.open", side_effect=RuntimeError("boom")),
        pytest.raises(ConfigurationError, match="Unexpected error"),
    ):
        await load_config_from_file(str(f))


@pytest.mark.asyncio
async def test_load_config_from_file_rejects_non_object_top_level_list(tmp_path):
    """A JSON array at the top level is rejected with a clear message."""
    f = tmp_path / "list.json"
    f.write_text("[1, 2, 3]")
    with pytest.raises(ConfigurationError, match="must contain a JSON object"):
        await load_config_from_file(str(f))


@pytest.mark.asyncio
async def test_load_config_from_file_rejects_non_object_top_level_scalar(tmp_path):
    """A JSON scalar at the top level is rejected with a clear message."""
    f = tmp_path / "scalar.json"
    f.write_text('"just a string"')
    with pytest.raises(ConfigurationError, match="must contain a JSON object"):
        await load_config_from_file(str(f))


@pytest.mark.asyncio
async def test_load_config_from_file_rejects_top_level_null(tmp_path):
    """JSON ``null`` at the top level is rejected."""
    f = tmp_path / "null.json"
    f.write_text("null")
    with pytest.raises(ConfigurationError, match="must contain a JSON object"):
        await load_config_from_file(str(f))


@pytest.mark.asyncio
async def test_load_config_from_file_invalid_json_wrapped(tmp_path):
    """``json.JSONDecodeError`` (a ``ValueError``) is wrapped as ConfigurationError."""
    import json

    f = tmp_path / "decodeerror.json"
    f.write_text("{}")

    def _raise_decode_error(*_args, **_kwargs):
        raise json.JSONDecodeError("bad", "", 0)

    with (
        patch("json5.loads", side_effect=_raise_decode_error),
        pytest.raises(ConfigurationError, match="Invalid JSON"),
    ):
        await load_config_from_file(str(f))
