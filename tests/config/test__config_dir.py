"""Tests for :mod:`deephaven_mcp.config._config_dir`."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deephaven_mcp.config._config_dir import (
    CONFIG_DIR_ENV_VAR,
    default_config_dir,
    resolve_config_dir,
)

# ---------------------------------------------------------------------------
# default_config_dir — per-platform default
# ---------------------------------------------------------------------------


def test_default_dir_posix() -> None:
    if sys.platform == "win32":
        pytest.skip("POSIX path only")
    expected = Path.home() / ".deephaven" / "ai" / "config"
    assert default_config_dir() == expected


def test_default_dir_windows_with_appdata(tmp_path: Path) -> None:
    fake_appdata = str(tmp_path)
    with (
        patch("deephaven_mcp.config._config_dir.sys") as fake_sys,
        patch.dict(os.environ, {"APPDATA": fake_appdata}, clear=False),
    ):
        fake_sys.platform = "win32"
        assert (
            default_config_dir() == Path(fake_appdata) / "Deephaven" / "ai" / "config"
        )


def test_default_dir_windows_without_appdata() -> None:
    env = {k: v for k, v in os.environ.items() if k != "APPDATA"}
    with (
        patch("deephaven_mcp.config._config_dir.sys") as fake_sys,
        patch.dict(os.environ, env, clear=True),
    ):
        fake_sys.platform = "win32"
        # Falls through to home-based form when APPDATA is unset.
        assert default_config_dir() == Path.home() / ".deephaven" / "ai" / "config"


# ---------------------------------------------------------------------------
# resolve_config_dir - precedence
# ---------------------------------------------------------------------------


def test_resolve_explicit_argument_wins(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit"
    with patch.dict(os.environ, {CONFIG_DIR_ENV_VAR: str(tmp_path / "env")}):
        assert resolve_config_dir(explicit) == explicit


def test_resolve_uses_env_var_when_explicit_none(tmp_path: Path) -> None:
    env_value = str(tmp_path / "from_env")
    with patch.dict(os.environ, {CONFIG_DIR_ENV_VAR: env_value}):
        assert resolve_config_dir(None) == Path(env_value)


def test_resolve_falls_back_to_default_when_env_unset() -> None:
    env = {k: v for k, v in os.environ.items() if k != CONFIG_DIR_ENV_VAR}
    with patch.dict(os.environ, env, clear=True):
        assert resolve_config_dir(None) == default_config_dir()


def test_resolve_expands_tilde_in_explicit_argument() -> None:
    """A ``~``-prefixed explicit argument expands to the user's home."""
    explicit = Path("~/.deephaven/ai/config")
    resolved = resolve_config_dir(explicit)
    assert resolved == Path.home() / ".deephaven" / "ai" / "config"
    assert "~" not in str(resolved)


def test_resolve_expands_tilde_in_env_var() -> None:
    """A ``~``-prefixed ``$DH_MCP_CONFIG_DIR`` expands to the user's home."""
    with patch.dict(os.environ, {CONFIG_DIR_ENV_VAR: "~/some/where"}):
        resolved = resolve_config_dir(None)
    assert resolved == Path.home() / "some" / "where"
    assert "~" not in str(resolved)
