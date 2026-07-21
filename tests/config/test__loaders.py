"""Tests for ``deephaven_mcp.config._loaders``.

Covers the shared JSON-to-Pydantic loader helpers used by both the
``dhcli`` CLI (``cli.json``) and every per-section loader under
``deephaven_mcp.config.schema``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from pydantic import BaseModel

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config._loaders import load_named_json, load_named_json_with_stem


class _Model(BaseModel):
    """Minimal test model with one required and one optional field."""

    name: str
    count: int = 0


def _write(path: Path, payload: dict[str, object]) -> None:
    """Helper: write ``payload`` as JSON to ``path``."""
    path.write_text(json.dumps(payload))


# ---------------------------------------------------------------------------
# load_named_json
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_named_json_success(tmp_path: Path) -> None:
    """Happy path: JSON file → validated model instance."""
    f = tmp_path / "cfg.json"
    _write(f, {"name": "alpha", "count": 3})
    model = await load_named_json(
        _Model,
        path=f,
        config_dir=tmp_path,
        error_label="cfg.json",
        log_label="test:cfg",
        logger=logging.getLogger("test"),
    )
    assert isinstance(model, _Model)
    assert model.name == "alpha"
    assert model.count == 3


@pytest.mark.asyncio
async def test_load_named_json_validation_error_wrapped_as_configuration_error(
    tmp_path: Path,
) -> None:
    """A pydantic ``ValidationError`` is wrapped as ``ConfigurationError``."""
    f = tmp_path / "bad.json"
    _write(f, {"count": "not-an-int"})  # missing required `name`, wrong type
    with pytest.raises(ConfigurationError) as excinfo:
        await load_named_json(
            _Model,
            path=f,
            config_dir=tmp_path,
            error_label="bad.json",
            log_label="test:bad",
            logger=logging.getLogger("test"),
        )
    # The error_label appears in the wrapped message so operators can find
    # the offending file.
    assert "bad.json" in str(excinfo.value)


@pytest.mark.asyncio
async def test_load_named_json_propagates_file_loader_failure(
    tmp_path: Path,
) -> None:
    """Missing files surface as ``ConfigurationError`` via the file loader."""
    missing = tmp_path / "absent.json"
    with pytest.raises(ConfigurationError):
        await load_named_json(
            _Model,
            path=missing,
            config_dir=tmp_path,
            error_label="absent.json",
            log_label="test:absent",
            logger=logging.getLogger("test"),
        )


# ---------------------------------------------------------------------------
# load_named_json_with_stem
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_named_json_with_stem_injects_filename_stem(
    tmp_path: Path,
) -> None:
    """The filename stem becomes the model's ``name`` field."""
    f = tmp_path / "demo.json"
    _write(f, {"count": 7})  # no `name` in the file
    model = await load_named_json_with_stem(
        _Model,
        path=f,
        config_dir=tmp_path,
        error_label="demo.json",
        log_label="test:demo",
        logger=logging.getLogger("test"),
    )
    assert model.name == "demo"
    assert model.count == 7


@pytest.mark.asyncio
async def test_load_named_json_with_stem_file_name_overrides_stem(
    tmp_path: Path,
) -> None:
    """An explicit ``name`` in the file overrides the stem.

    Per the docstring, this is permitted — downstream schemas may
    validate the filename-vs-name match separately.
    """
    f = tmp_path / "stem-says-this.json"
    _write(f, {"name": "file-says-this", "count": 1})
    model = await load_named_json_with_stem(
        _Model,
        path=f,
        config_dir=tmp_path,
        error_label="stem-says-this.json",
        log_label="test:stem",
        logger=logging.getLogger("test"),
    )
    assert model.name == "file-says-this"


@pytest.mark.asyncio
async def test_load_named_json_with_stem_validation_error_wrapped(
    tmp_path: Path,
) -> None:
    """``ValidationError`` from a stem-loader path is wrapped too."""
    f = tmp_path / "broken.json"
    _write(f, {"count": "not-an-int"})
    with pytest.raises(ConfigurationError) as excinfo:
        await load_named_json_with_stem(
            _Model,
            path=f,
            config_dir=tmp_path,
            error_label="broken.json",
            log_label="test:broken",
            logger=logging.getLogger("test"),
        )
    assert "broken.json" in str(excinfo.value)
