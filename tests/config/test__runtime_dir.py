"""Tests for :mod:`deephaven_mcp.config._runtime_dir`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

from deephaven_mcp.config._data_root import DATA_DIR_ENV_VAR, _default_data_root
from deephaven_mcp.config._runtime_dir import (
    daemon_dir,
    instances_dir,
    resolve_runtime_dir,
)

# ---------------------------------------------------------------------------
# resolve_runtime_dir - precedence
# ---------------------------------------------------------------------------


def test_resolve_explicit_argument_wins(tmp_path: Path) -> None:
    """An explicit Path overrides the env-var-driven data root."""
    explicit = tmp_path / "explicit"
    with patch.dict(os.environ, {DATA_DIR_ENV_VAR: str(tmp_path / "env")}):
        assert resolve_runtime_dir(explicit) == explicit


def test_resolve_uses_data_root_env_var_when_explicit_none(tmp_path: Path) -> None:
    """``$DH_MCP_DATA_DIR/runtime`` is used when no explicit Path is given."""
    root = str(tmp_path / "data_root")
    with patch.dict(os.environ, {DATA_DIR_ENV_VAR: root}):
        assert resolve_runtime_dir(None) == Path(root) / "runtime"


def test_resolve_falls_back_to_default_when_env_unset() -> None:
    """With no env var, the platform default data root + ``runtime``."""
    env = {k: v for k, v in os.environ.items() if k != DATA_DIR_ENV_VAR}
    with patch.dict(os.environ, env, clear=True):
        assert resolve_runtime_dir(None) == _default_data_root() / "runtime"


def test_resolve_expands_tilde_in_explicit_argument() -> None:
    """A ``~``-prefixed explicit argument expands to the user's home."""
    explicit = Path("~/.deephaven/ai/runtime")
    resolved = resolve_runtime_dir(explicit)
    assert resolved == Path.home() / ".deephaven" / "ai" / "runtime"
    assert "~" not in str(resolved)


# ---------------------------------------------------------------------------
# daemon_dir
# ---------------------------------------------------------------------------


def test_daemon_dir_appends_daemon_segment(tmp_path: Path) -> None:
    """``daemon_dir`` appends a ``daemon`` segment to the runtime root."""
    assert daemon_dir(tmp_path) == tmp_path / "daemon"


def test_daemon_dir_does_not_create_path(tmp_path: Path) -> None:
    """The helper is path-construction only; it does not touch disk."""
    result = daemon_dir(tmp_path / "nonexistent")
    assert not result.exists()


# ---------------------------------------------------------------------------
# instances_dir
# ---------------------------------------------------------------------------


def test_instances_dir_appends_instances_segment(tmp_path: Path) -> None:
    """``instances_dir`` appends an ``instances`` segment to the runtime root."""
    assert instances_dir(tmp_path) == tmp_path / "instances"


def test_instances_dir_does_not_create_path(tmp_path: Path) -> None:
    """The helper is path-construction only; it does not touch disk."""
    result = instances_dir(tmp_path / "nonexistent")
    assert not result.exists()
