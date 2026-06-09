"""Tests for :mod:`deephaven_mcp.config._data_root`."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deephaven_mcp.config._data_root import (
    DATA_DIR_ENV_VAR,
    _default_data_root,
    resolve_data_root,
)

# ---------------------------------------------------------------------------
# _default_data_root — per-platform default
# ---------------------------------------------------------------------------


def test_default_root_posix() -> None:
    if sys.platform == "win32":
        pytest.skip("POSIX path only")
    assert _default_data_root() == Path.home() / ".deephaven" / "ai"


def test_default_root_windows_with_appdata(tmp_path: Path) -> None:
    fake_appdata = str(tmp_path)
    with (
        patch("deephaven_mcp.config._data_root.sys") as fake_sys,
        patch.dict(os.environ, {"APPDATA": fake_appdata}, clear=False),
    ):
        fake_sys.platform = "win32"
        assert _default_data_root() == Path(fake_appdata) / "Deephaven" / "ai"


def test_default_root_windows_without_appdata() -> None:
    env = {k: v for k, v in os.environ.items() if k != "APPDATA"}
    with (
        patch("deephaven_mcp.config._data_root.sys") as fake_sys,
        patch.dict(os.environ, env, clear=True),
    ):
        fake_sys.platform = "win32"
        # Falls through to home-based form when APPDATA is unset.
        assert _default_data_root() == Path.home() / ".deephaven" / "ai"


# ---------------------------------------------------------------------------
# resolve_data_root — env override or default
# ---------------------------------------------------------------------------


def test_resolve_uses_env_var_when_set(tmp_path: Path) -> None:
    """``$DH_MCP_DATA_DIR`` overrides the platform default."""
    env_value = str(tmp_path / "from_env")
    with patch.dict(os.environ, {DATA_DIR_ENV_VAR: env_value}):
        assert resolve_data_root() == Path(env_value)


def test_resolve_falls_back_to_default_when_env_unset() -> None:
    """With no env var, the platform default applies."""
    env = {k: v for k, v in os.environ.items() if k != DATA_DIR_ENV_VAR}
    with patch.dict(os.environ, env, clear=True):
        assert resolve_data_root() == _default_data_root()


def test_resolve_expands_tilde_in_env_var() -> None:
    """A ``~``-prefixed ``$DH_MCP_DATA_DIR`` expands to the user's home."""
    with patch.dict(os.environ, {DATA_DIR_ENV_VAR: "~/some/where"}):
        resolved = resolve_data_root()
    assert resolved == Path.home() / "some" / "where"
    assert "~" not in str(resolved)


def test_env_var_constant_is_stable() -> None:
    """The env-var name is part of the public contract."""
    assert DATA_DIR_ENV_VAR == "DH_MCP_DATA_DIR"
