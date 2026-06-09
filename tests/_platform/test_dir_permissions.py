"""Unit tests for :mod:`deephaven_mcp._platform.dir_permissions`.

Covers the OS-dispatched mechanics: ``audit_tree`` (POSIX owner/mode
walk, Windows profile containment, unsupported-OS rejection) and
``harden_private_dir``. The config-startup *policy* that consumes
``audit_tree`` is tested in
:mod:`tests.config.test__dir_permissions`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._platform.dir_permissions import audit_tree, harden_private_dir


def _chmod_owner_only(path: Path) -> None:
    """Make ``path`` user-only (mode 700 for dirs, 600 for files)."""
    if path.is_dir():
        path.chmod(0o700)
    else:
        path.chmod(0o600)


def _violations(path: Path) -> str:
    """Run the audit and join its violations into one searchable string."""
    return "\n".join(audit_tree(path))


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
# audit_tree — POSIX behavior
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_clean_tree_has_no_violations(clean_dir: Path) -> None:
    assert audit_tree(clean_dir) == []


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_directory_world_readable_reported(clean_dir: Path) -> None:
    clean_dir.chmod(0o755)
    assert "permits group/other access" in _violations(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_file_world_readable_reported(clean_dir: Path) -> None:
    target = clean_dir / "server.json"
    target.chmod(0o644)
    assert "permits group/other access" in _violations(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_nested_file_violation_reported(clean_dir: Path) -> None:
    nested = clean_dir / "enterprise"
    nested.mkdir()
    leaf = nested / "prod.json"
    leaf.write_text("{}")
    leaf.chmod(0o644)
    nested.chmod(0o700)
    assert str(leaf) in _violations(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_wrong_owner_reported(clean_dir: Path) -> None:
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
        assert "owned by UID" in _violations(clean_dir)


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
        assert "cannot stat" in _violations(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_irregular_path_type_reported(clean_dir: Path) -> None:
    """A non-regular non-directory path (e.g. FIFO) is reported."""
    fifo = clean_dir / "pipe"
    os.mkfifo(fifo)
    fifo.chmod(0o600)
    assert "not a regular file or directory" in _violations(clean_dir)


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
    assert audit_tree(clean_dir) == []


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
    msg = _violations(clean_dir)
    # The diagnostic refers to the symlink path (what the audit walked),
    # and the mode complaint reflects the target's mode.
    assert str(link) in msg
    assert "group/other access" in msg


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_rejects_symlinked_subdirectory(clean_dir: Path, tmp_path: Path) -> None:
    """Any subdirectory symlink under the root is a hard error.

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
    msg = _violations(clean_dir)
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
    assert "symlinked subdirectories" in _violations(clean_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_accepts_symlinked_root_with_safe_target(
    clean_dir: Path, tmp_path: Path
) -> None:
    """The audited root itself being a symlink to a user-private dir passes.

    Common in real deployments: dotfile managers, XDG redirects, or
    container bind paths where the operator sets
    ``DH_MCP_DATA_DIR=~/symlink-to-real-config``. The audit follows
    the symlink at the root via ``os.walk(top=link, ...)``.
    """
    link = tmp_path / "config_link"
    os.symlink(clean_dir, link)
    assert audit_tree(link) == []


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_rejects_symlinked_root_with_permissive_target(
    tmp_path: Path,
) -> None:
    """A symlinked root whose target is loose-perm is rejected with the qualifier.

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
    msg = _violations(link)
    assert str(link) in msg
    assert "group/other access" in msg
    assert "via symlink target" in msg


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_checks_every_path_in_multi_level_tree(tmp_path: Path) -> None:
    """Completeness: every reachable path in the tree is permission-audited.

    Build a 3-level tree containing the root, a subdir, a deeper subdir,
    regular files at every depth, and a hidden (dotfile) entry. With
    every path tightened to user-only the audit passes; loosening *any
    single one* in turn must produce a violation naming that path. If a
    path were ever silently skipped, this test would pass for that path
    while permissions on it were loose — exactly the silent-hole class
    of bug.
    """
    root = tmp_path / "ai"
    root.mkdir()
    sub = root / "sub"
    sub.mkdir()
    deep = sub / "deep"
    deep.mkdir()
    root_file = root / "a.json"
    root_file.write_text("{}")
    sub_file = sub / "b.json"
    sub_file.write_text("{}")
    deep_file = deep / "c.json"
    deep_file.write_text("{}")
    hidden_file = root / ".hidden"
    hidden_file.write_text("{}")

    all_paths = [root, sub, deep, root_file, sub_file, deep_file, hidden_file]
    for p in all_paths:
        _chmod_owner_only(p)

    # Baseline: a fully-tightened tree audits clean.
    assert audit_tree(root) == []

    # Loosen each path one at a time; each must be reported by name.
    for victim in all_paths:
        original_mode = victim.stat().st_mode & 0o777
        victim.chmod(original_mode | 0o040)  # add group-read
        try:
            assert str(victim) in _violations(
                root
            ), f"Audit failed to report loosened path: {victim}"
        finally:
            victim.chmod(original_mode)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only checks")
def test_audit_reports_unwalkable_subdirectory(clean_dir: Path) -> None:
    """An unlistable subdirectory must be reported, not silently skipped.

    ``os.walk``'s default behaviour swallows ``PermissionError`` from
    ``scandir`` calls. A subdir whose mode strips the user's read bit
    (here ``0o100`` — traverse but not list; not group/other accessible
    so it would otherwise pass the bit-mask check) could therefore
    leave its contents unchecked. The audit's ``onerror`` callback
    turns that case into an explicit violation so the operator sees it.
    """
    locked = clean_dir / "locked"
    locked.mkdir()
    (locked / "secret.json").write_text("{}")
    # ``0o100`` has neither user-read nor group/other access, so the
    # mode-bit check passes but the running user cannot list contents.
    locked.chmod(0o100)
    try:
        msg = _violations(clean_dir)
        assert "cannot list directory contents" in msg
        assert str(locked) in msg
    finally:
        # Restore so pytest's tmp_path cleanup can recurse in.
        locked.chmod(0o700)


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
    assert "cannot stat" in _violations(clean_dir)


# ---------------------------------------------------------------------------
# audit_tree — Windows behavior
# ---------------------------------------------------------------------------


def test_windows_under_home_passes(tmp_path: Path) -> None:
    """Force the Windows code path and verify a path under HOME passes."""
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    with (
        patch.object(os, "name", "nt"),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        assert audit_tree(config_dir) == []


def test_windows_outside_home_fails(tmp_path: Path) -> None:
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    other_home = tmp_path / "elsewhere"
    other_home.mkdir()
    with (
        patch.object(os, "name", "nt"),
        patch.object(Path, "home", return_value=other_home),
    ):
        assert "not under the current user profile" in _violations(config_dir)


# ---------------------------------------------------------------------------
# audit_tree — unsupported OS dispatch
# ---------------------------------------------------------------------------


def test_audit_tree_unsupported_os_raises_internal_error(tmp_path: Path) -> None:
    """An unknown ``os.name`` must fail loud rather than silently auditing.

    Security code does not silently degrade to a default branch on an
    unrecognised platform; the operator must be told to add an explicit
    audit strategy.
    """
    config_dir = tmp_path / "ai"
    config_dir.mkdir()
    with patch.object(os, "name", "java"):
        with pytest.raises(InternalError, match="java"):
            audit_tree(config_dir)


# ---------------------------------------------------------------------------
# harden_private_dir
# ---------------------------------------------------------------------------


def test_harden_private_dir_creates_missing_directory(tmp_path: Path) -> None:
    target = tmp_path / "fresh"
    assert not target.exists()
    harden_private_dir(target)
    assert target.is_dir()
    if os.name == "posix":
        assert (target.stat().st_mode & 0o777) == 0o700


def test_harden_private_dir_creates_parents(tmp_path: Path) -> None:
    """``parents=True`` is set; nested missing parents are created.

    The intermediate parents are *not* hardened (they may legitimately
    be shared, e.g. ``~/.deephaven``); only the final leaf is.
    """
    target = tmp_path / "a" / "b" / "c"
    harden_private_dir(target)
    assert target.is_dir()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only mode test")
def test_harden_private_dir_tightens_existing_loose_perms(tmp_path: Path) -> None:
    """An existing 0o755 dir is tightened to 0o700 idempotently."""
    target = tmp_path / "loose"
    target.mkdir()
    target.chmod(0o755)
    harden_private_dir(target)
    assert (target.stat().st_mode & 0o777) == 0o700


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only mode test")
def test_harden_private_dir_is_idempotent(tmp_path: Path) -> None:
    """Calling twice on an already-private dir does not error or change perms."""
    target = tmp_path / "private"
    harden_private_dir(target)
    harden_private_dir(target)
    assert (target.stat().st_mode & 0o777) == 0o700


def test_harden_private_dir_windows_skips_chmod(tmp_path: Path) -> None:
    """On Windows the function only creates the directory; no chmod is attempted.

    The ACL hardening for user-private trees on Windows is tracked
    under the Windows-support follow-up; the helper degrades to a
    plain ``mkdir`` so it remains usable cross-platform. The audit
    provides the Windows-side security guarantee by rejecting paths
    outside the user profile.
    """
    target = tmp_path / "win_target"
    with (
        patch.object(os, "name", "nt"),
        # ``os.chmod`` must not be called on the Windows path; assert
        # explicitly rather than relying on the absence of an error.
        patch("deephaven_mcp._platform.dir_permissions.os.chmod") as mock_chmod,
    ):
        harden_private_dir(target)
        mock_chmod.assert_not_called()
    assert target.is_dir()


def test_harden_private_dir_unsupported_os_raises_internal_error(
    tmp_path: Path,
) -> None:
    """An unknown ``os.name`` must raise rather than silently no-op.

    Security code must refuse on an unrecognised OS instead of creating
    the directory with whatever default mode the umask happened to
    grant and proceeding.
    """
    target = tmp_path / "future_os_target"
    with patch.object(os, "name", "plan9"):
        with pytest.raises(InternalError, match="plan9"):
            harden_private_dir(target)
