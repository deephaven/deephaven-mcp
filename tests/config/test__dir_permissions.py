"""Unit tests for :mod:`deephaven_mcp.config._dir_permissions`."""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import ConfigurationError
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


# ---------------------------------------------------------------------------
# Cross-platform behavior
# ---------------------------------------------------------------------------


def test_missing_directory_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope"
    with pytest.raises(ConfigurationError, match="does not exist"):
        verify_config_directory_permissions(missing)


def test_path_not_directory_raises(tmp_path: Path) -> None:
    path = tmp_path / "file.json"
    path.write_text("{}")
    with pytest.raises(ConfigurationError, match="not a directory"):
        verify_config_directory_permissions(path)


# ---------------------------------------------------------------------------
# POSIX behavior
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_clean_tree_passes(clean_dir: Path) -> None:
    verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_directory_world_readable_fails(clean_dir: Path) -> None:
    clean_dir.chmod(0o755)
    with pytest.raises(ConfigurationError, match="permits group/other access"):
        verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_file_world_readable_fails(clean_dir: Path) -> None:
    target = clean_dir / "server.json"
    target.chmod(0o644)
    with pytest.raises(ConfigurationError, match="permits group/other access"):
        verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_nested_file_violation_reported(clean_dir: Path) -> None:
    nested = clean_dir / "enterprise"
    nested.mkdir()
    leaf = nested / "prod.json"
    leaf.write_text("{}")
    leaf.chmod(0o644)
    nested.chmod(0o700)
    with pytest.raises(ConfigurationError) as exc:
        verify_config_directory_permissions(clean_dir)
    assert str(leaf) in str(exc.value)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_wrong_owner_fails(clean_dir: Path) -> None:
    real_stat = Path.stat
    expected_uid = os.getuid()

    def fake_stat(self: Path, *args: object, **kwargs: object) -> os.stat_result:
        st = real_stat(self, *args, **kwargs)
        # Only forge on the audit's follow-target stat() call. ``lstat``-
        # shaped calls (``follow_symlinks=False``) pass through so internal
        # ``is_symlink`` checks during the walk still work.
        if self.name == "server.json" and kwargs.get("follow_symlinks", True):
            fields = list(st)
            uid_index = 4  # st_uid index in stat_result
            fields[uid_index] = expected_uid + 1
            return os.stat_result(fields)
        return st

    with patch.object(Path, "stat", fake_stat):
        with pytest.raises(ConfigurationError, match="owned by UID"):
            verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_unstattable_path_reported(clean_dir: Path) -> None:
    real_stat = Path.stat
    bad = clean_dir / "server.json"

    def fake_stat(self: Path, *args: object, **kwargs: object) -> os.stat_result:
        # Only raise on the audit's follow-target stat() call. ``lstat``-
        # shaped calls (``follow_symlinks=False``) pass through so internal
        # ``is_symlink`` checks during the walk still work.
        if self == bad and kwargs.get("follow_symlinks", True):
            raise OSError("simulated stat failure")
        return real_stat(self, *args, **kwargs)

    with patch.object(Path, "stat", fake_stat):
        with pytest.raises(ConfigurationError, match="cannot stat"):
            verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_irregular_path_type_reported(clean_dir: Path) -> None:
    """A non-regular non-directory path (e.g. FIFO) is reported."""
    fifo = clean_dir / "pipe"
    os.mkfifo(fifo)
    fifo.chmod(0o600)
    with pytest.raises(ConfigurationError, match="not a regular file or directory"):
        verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_accepts_file_symlink_to_safe_target(
    clean_dir: Path, tmp_path: Path
) -> None:
    """A symlinked file whose target is user-private (0600, owned by user) passes.

    The audit checks the *target*'s mode and owner via ``Path.stat``,
    not the symlink itself. This is what makes the audit compatible
    with Kubernetes ConfigMap mounts, Vault Agent atomic swaps, and
    cert-manager live cert paths — all of which route through symlinks.
    """
    target = tmp_path / "real.json"
    target.write_text("{}")
    target.chmod(0o600)
    link = clean_dir / "link.json"
    os.symlink(target, link)
    # Must not raise.
    verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_rejects_file_symlink_to_world_readable_target(
    clean_dir: Path, tmp_path: Path
) -> None:
    """A file symlink whose *target* permits group/other access is rejected.

    Verifies the audit follows the symlink and inspects the target's
    mode rather than the symlink's own mode (which is meaningless —
    ``chmod`` is a no-op on symlinks on most filesystems).
    """
    target = tmp_path / "loose.json"
    target.write_text("{}")
    target.chmod(0o644)  # world-readable target
    link = clean_dir / "link.json"
    os.symlink(target, link)
    with pytest.raises(ConfigurationError) as exc:
        verify_config_directory_permissions(clean_dir)
    msg = str(exc.value)
    # The diagnostic refers to the symlink path (what the audit walked),
    # and the mode complaint reflects the target's mode.
    assert str(link) in msg
    assert "group/other access" in msg


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_rejects_symlinked_subdirectory(clean_dir: Path, tmp_path: Path) -> None:
    """Any subdirectory symlink under ``config_dir`` is a hard error.

    The loader's ``Path.glob`` would follow such a symlink when
    listing a section dir, but the audit cannot safely descend through
    it. To avoid the resulting audit hole the policy is: subdirectory
    symlinks are forbidden anywhere below the root. The error message
    points the operator at the workable alternatives (root-level
    symlink, or symlink individual files).
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    outside.chmod(
        0o700
    )  # target perms are irrelevant — symlink itself is the violation
    link = clean_dir / "link"
    os.symlink(outside, link)
    with pytest.raises(ConfigurationError) as exc:
        verify_config_directory_permissions(clean_dir)
    msg = str(exc.value)
    assert str(link) in msg
    assert "symlinked subdirectories are not supported" in msg
    # Remediation hint mentions the supported alternatives.
    assert "config root" in msg or "individual files" in msg


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_rejects_symlinked_subdirectory_even_with_safe_target(
    clean_dir: Path, tmp_path: Path
) -> None:
    """Even a perfectly-permissioned target dir is rejected when reached via a subdir symlink.

    The policy is uniform: the symlink itself is the violation, not
    the target's permissions. This prevents the audit hole regardless
    of target permissions and avoids confusing operators with
    "sometimes a subdir symlink works".
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    outside.chmod(0o700)  # impeccable target
    link = clean_dir / "link_to_safe"
    os.symlink(outside, link)
    with pytest.raises(ConfigurationError, match="symlinked subdirectories"):
        verify_config_directory_permissions(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_accepts_symlinked_config_dir_with_safe_target(
    clean_dir: Path, tmp_path: Path
) -> None:
    """``config_dir`` itself being a symlink to a user-private dir passes audit.

    Common in real deployments: dotfile managers, XDG redirects, or
    container bind paths where the operator sets
    ``DH_MCP_CONFIG_DIR=~/symlink-to-real-config``. The audit follows
    the symlink at the root via ``Path.exists`` / ``Path.is_dir`` (both
    follow) and via ``os.walk(top=link, ...)`` (also follows the root).
    """
    link = tmp_path / "config_link"
    os.symlink(clean_dir, link)
    # Must not raise — target is the standard user-private clean_dir.
    verify_config_directory_permissions(link)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_rejects_symlinked_config_dir_with_permissive_target(
    tmp_path: Path,
) -> None:
    """A symlinked ``config_dir`` whose target is loose-perm is rejected with the qualifier.

    The violation must include the ``(via symlink target)`` qualifier
    so an operator who ``ls -l``s the symlinked root (and sees
    ``lrwxrwxrwx``) understands that the reported mode is the
    target's, not the symlink's.
    """
    target = tmp_path / "loose_dir"
    target.mkdir()
    target.chmod(0o755)  # world-readable target
    link = tmp_path / "ai"
    os.symlink(target, link)
    with pytest.raises(ConfigurationError) as exc:
        verify_config_directory_permissions(link)
    msg = str(exc.value)
    assert str(link) in msg
    assert "group/other access" in msg
    assert "via symlink target" in msg


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_reports_dangling_symlink(clean_dir: Path, tmp_path: Path) -> None:
    """A broken (dangling) symlink surfaces as a cannot-stat violation.

    ``Path.stat()`` follows the symlink and raises ``FileNotFoundError``
    when the target does not exist; that maps to a ``cannot stat``
    violation, so an operator sees a clear message instead of a silent
    hole in the audit.
    """
    link = clean_dir / "link.json"
    os.symlink(tmp_path / "does-not-exist", link)
    with pytest.raises(ConfigurationError, match="cannot stat"):
        verify_config_directory_permissions(clean_dir)


# ---------------------------------------------------------------------------
# Windows behavior
# ---------------------------------------------------------------------------


def test_windows_under_home_passes(tmp_path: Path) -> None:
    """Force the Windows code path and verify a path under HOME passes."""
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    with (
        patch("deephaven_mcp.config._dir_permissions.sys") as fake_sys,
        patch.object(Path, "home", return_value=tmp_path),
    ):
        fake_sys.platform = "win32"
        verify_config_directory_permissions(config_dir)


def test_windows_outside_home_fails(tmp_path: Path) -> None:
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    other_home = tmp_path / "elsewhere"
    other_home.mkdir()
    with (
        patch("deephaven_mcp.config._dir_permissions.sys") as fake_sys,
        patch.object(Path, "home", return_value=other_home),
    ):
        fake_sys.platform = "win32"
        with pytest.raises(
            ConfigurationError, match="not under the current user profile"
        ):
            verify_config_directory_permissions(config_dir)
