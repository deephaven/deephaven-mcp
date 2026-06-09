"""Unit tests for :mod:`deephaven_mcp.config._dir_permissions`.

Covers the startup *policy*: existence / not-a-directory checks, the
clean-pass path, aggregation of audit violations into a single
``ConfigurationError``, and propagation of the unsupported-OS error
from the audit layer. The per-OS audit mechanics it delegates to
(``audit_tree``) are tested in
:mod:`tests._platform.test_dir_permissions`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import ConfigurationError, InternalError
from deephaven_mcp.config._dir_permissions import verify_config_directory_permissions


def _chmod_owner_only(path: Path) -> None:
    """Make ``path`` user-only (mode 700 for dirs, 600 for files)."""
    if path.is_dir():
        path.chmod(0o700)
    else:
        path.chmod(0o600)


@pytest.fixture
def clean_dir(tmp_path: Path) -> Path:
    """Return a freshly created user-only directory and a user-only file inside it."""
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    (config_dir / "server.json").write_text("{}")
    for path in [config_dir, config_dir / "server.json"]:
        _chmod_owner_only(path)
    return config_dir


def test_missing_directory_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope"
    with pytest.raises(ConfigurationError, match="does not exist"):
        verify_config_directory_permissions(missing)


def test_path_not_directory_raises(tmp_path: Path) -> None:
    path = tmp_path / "file.json"
    path.write_text("{}")
    with pytest.raises(ConfigurationError, match="not a directory"):
        verify_config_directory_permissions(path)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_clean_tree_passes(clean_dir: Path) -> None:
    """A user-private tree produces no violations and returns cleanly."""
    verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_violations_aggregated_into_configuration_error(clean_dir: Path) -> None:
    """Audit violations are surfaced as a ``ConfigurationError``.

    The policy layer turns the non-empty violation list from
    ``audit_tree`` into a single aggregated error naming the offending
    paths.
    """
    clean_dir.chmod(0o755)
    with pytest.raises(ConfigurationError) as exc:
        verify_config_directory_permissions(clean_dir)
    msg = str(exc.value)
    assert "permission audit failed" in msg
    assert "permits group/other access" in msg


def test_unsupported_os_error_propagates(tmp_path: Path) -> None:
    """An unknown ``os.name`` from the audit layer propagates unchanged.

    The policy does not swallow the ``InternalError`` the audit raises
    on an unrecognised platform; security code must fail loud rather
    than silently degrade.
    """
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    with patch.object(os, "name", "java"):
        with pytest.raises(InternalError, match="java"):
            verify_config_directory_permissions(config_dir)
