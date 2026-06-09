"""Per-OS private-directory hardening and permission auditing.

These are the platform mechanics behind the project's user-private
directory guarantees. The config-startup *policy* that consumes the
audit (refuse-to-start, error aggregation) lives in
:mod:`deephaven_mcp.config._dir_permissions`.

Platform support
----------------

Dispatch is keyed on :data:`os.name`. Only ``"posix"`` and ``"nt"`` are
implemented; any other value raises :class:`InternalError` rather than
falling through to a default — this is security code and silent degrade
is not an option.

POSIX (``os.name == "posix"``; Linux, macOS, *BSD)
    Hardening tightens the directory to ``0o700``. The audit requires
    every regular file and every directory under the root to:

    - Be owned by the running UID, and
    - Have mode bits satisfying ``mode & 0o077 == 0`` (no group or other
      access of any kind).

    Symlink policy:

    - The audited root itself may be a symlink; the audit follows it and
      verifies the target's owner and mode.
    - Individual **file** symlinks at any depth are accepted; the
      target's owner and mode are audited.
    - **Subdirectory** symlinks are rejected. They are forbidden
      because the loader's ``Path.glob`` follows them when listing
      section directories but the audit cannot safely descend through
      them (would either escape the tree or require bounded follow with
      cycle detection). The asymmetry would produce an audit hole where
      files inside a symlinked subdirectory are loaded without permission
      verification. Restructure the tree so that only the root or
      individual files are symlinks.

    Symlink targets are checked, not the symlink's own bits, because
    ``chmod`` on a symlink is a no-op on most filesystems.

Windows (``os.name == "nt"``)
    Hardening performs only ``mkdir`` (POSIX-style mode bits do not map
    to Windows ACLs; a faithful ACL apply requires ``pywin32``, tracked
    under the Windows-support follow-up). The audit is a best-effort
    containment check, no extra dependencies: the audited root must
    resolve to a descendant of the current user's profile directory
    (``Path.home()`` / ``%USERPROFILE%``). The default
    ``%APPDATA%/Deephaven/ai/`` location satisfies this; anything outside
    the profile is rejected. Bit-mode checks are skipped because Windows
    ACLs cannot be faithfully expressed via :func:`os.stat`'s ``st_mode``
    field. Security on Windows therefore relies on the per-user ACL
    inherited from ``%APPDATA%``; placing the configuration anywhere else
    is rejected so that inheritance is guaranteed.
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path

from deephaven_mcp._platform._os_support import unsupported_os_error

__all__ = ["audit_tree", "harden_private_dir"]

_LOGGER = logging.getLogger(__name__)

_FORBIDDEN_MODE_BITS = 0o077
"""Mode bits that must be clear on every audited file and directory.

Equivalent to disallowing every group and other permission. The audit
fails any path whose ``st_mode & _FORBIDDEN_MODE_BITS`` is non-zero.
"""


def _harden_posix(path: Path) -> None:
    """Create ``path`` if missing and chmod it to ``0o700`` (POSIX).

    The chmod runs whether or not the directory already existed so a
    looser-umask creation by an earlier run, or an operator who ran
    ``chmod`` after creation, cannot widen access mid-run. Tightening
    from a looser mode is logged at ``INFO`` so operators see the change.

    Args:
        path (Path): Directory to create and harden. Parent directories
            are created as needed but *not* tightened.
    """
    path.mkdir(parents=True, exist_ok=True)
    current_mode = stat.S_IMODE(path.stat().st_mode)
    if current_mode != 0o700:
        _LOGGER.info(
            f"[_platform.dir_permissions:_harden_posix] Tightening "
            f"{path} mode from {oct(current_mode)} to 0o700"
        )
        os.chmod(path, 0o700)


def _harden_nt(path: Path) -> None:
    """Create ``path`` if missing (Windows); does **not** modify ACLs.

    POSIX-style mode bits do not map to Windows ACLs, and a faithful
    user-private ACL apply requires :mod:`pywin32` (tracked under the
    Windows-support follow-up). This helper therefore performs only
    the ``mkdir`` step; the security guarantee on Windows comes from
    the per-user ACL inherited from ``%APPDATA%`` (the standard
    placement), enforced at audit time by :func:`audit_tree` rejecting
    paths outside the user profile.

    Args:
        path (Path): Directory to create. Parent directories are
            created as needed.
    """
    path.mkdir(parents=True, exist_ok=True)


def harden_private_dir(path: Path) -> None:
    """Create ``path`` if missing and lock it down to user-private mode.

    Idempotent. Dispatches by :data:`os.name`:

    - ``"posix"``: see :func:`_harden_posix` — ``mkdir`` + chmod to ``0o700``.
    - ``"nt"``: see :func:`_harden_nt` — ``mkdir`` only; security comes
      from inherited ``%APPDATA%`` ACLs, enforced at audit time.
    - anything else: :class:`InternalError` is raised. A new platform
      must add an explicit hardening strategy before the code is allowed
      to run on it; silent fallthrough is not acceptable here.

    Used at every site that creates a per-user-private directory in this
    project: the resolved ``runtime_dir`` (in
    :func:`deephaven_mcp.cli._runtime.load_runtime`) and the ``daemon``
    subdirectory beneath it (at the daemon entry point and the
    spawn-coordinator).

    Args:
        path (Path): The directory to create and harden. Parent
            directories are created as needed; their permissions are
            *not* tightened (they may legitimately be shared, e.g.
            ``~/.deephaven``).

    Raises:
        InternalError: If :data:`os.name` is not in ``{"posix", "nt"}``.
    """
    if os.name == "posix":
        _harden_posix(path)
    elif os.name == "nt":
        _harden_nt(path)
    else:
        raise unsupported_os_error("harden_private_dir")


def audit_tree(root: Path) -> list[str]:
    """Return per-path permission violations under ``root``, dispatched by os.name.

    The dispatch keys on :data:`os.name` (not :data:`sys.platform`) so
    the rule is symmetric with :func:`harden_private_dir` and an unknown
    platform raises explicitly rather than silently running the POSIX
    branch with semantics that may not apply.

    Args:
        root (Path): The directory tree to audit. Assumed to exist and
            be a directory (the caller validates this).

    Returns:
        list[str]: One human-readable message per offending path. Empty
            when the tree satisfies the platform's rule.

    Raises:
        InternalError: If :data:`os.name` is not in ``{"posix", "nt"}``.
    """
    if os.name == "posix":
        return _audit_posix(root)
    elif os.name == "nt":
        return _audit_windows(root)
    else:
        raise unsupported_os_error("audit_tree")


def _audit_posix(config_dir: Path) -> list[str]:  # noqa: C901
    """Return per-path POSIX permission violations under ``config_dir``.

    Traversal uses :func:`os.walk` with ``followlinks=False`` so the
    audit cannot escape the tree through a directory symlink. The
    root ``config_dir`` may itself be a symlink (a common
    dotfile-manager or container bind pattern) and file symlinks at
    any depth are accepted — both are stat'd via :func:`Path.stat`
    and the target's owner and mode are checked. The symlink's own
    bits are not checked because ``chmod`` is a no-op on symlinks on
    most filesystems.

    Subdirectory symlinks anywhere below the root are **rejected**.
    The loader's ``Path.glob`` follows them when listing section
    directories, but the audit cannot safely descend through them
    without bounded-follow + cycle detection. Rather than accept the
    resulting audit hole the audit refuses; the operator's recourse
    is to make the subdirectory real, symlink at the config root
    instead, or symlink individual files within it.

    Completeness guarantee:

    Every path reachable from ``config_dir`` is included in the
    audit:

    - The root itself (always, even if it is a symlink).
    - Every real subdirectory (each level descended into).
    - Every regular file at every depth.
    - Every file symlink (target's owner and mode are audited).
    - Every subdirectory symlink (recorded as a violation; not
      descended into, but the audit fails before the loader can
      reach what is behind it).

    Paths that cannot be enumerated (e.g. a subdirectory the running
    user cannot list) are recorded via the ``os.walk`` error callback
    as ``cannot list directory contents`` violations, so an audit
    blind spot becomes a failure rather than a silent pass.

    Args:
        config_dir (Path): The configuration directory to walk; included
            in the audit.

    Returns:
        list[str]: One human-readable message per offending path. Empty
            when the tree is clean.
    """
    expected_uid = os.getuid()
    violations: list[str] = []

    paths_to_check: list[Path] = [config_dir]
    symlink_paths: set[Path] = set()
    # ``config_dir`` is added to ``paths_to_check`` unconditionally; if
    # it is itself a symlink (a common dotfile-manager or container
    # bind pattern), tag it so any violation on the root carries the
    # ``(via symlink target)`` qualifier consistent with every other
    # symlinked path in the tree.
    if config_dir.is_symlink():
        symlink_paths.add(config_dir)

    def _on_walk_error(exc: OSError) -> None:
        # ``os.walk``'s default behaviour (``onerror=None``) is to
        # silently drop a directory whose contents cannot be listed
        # (typically ``PermissionError``). That would leave an audit
        # blind spot: the directory itself is still stat-checked at
        # the parent's iteration, but the operator would never learn
        # that the contents were unverifiable. Record it instead.
        violations.append(
            f"{exc.filename}: cannot list directory contents ({exc}); "
            "audit cannot verify children. Fix the directory's "
            "permissions (e.g. `chmod 700`) so the audit can descend."
        )

    for dirpath, dirnames, filenames in os.walk(
        str(config_dir), followlinks=False, onerror=_on_walk_error
    ):
        dirpath_p = Path(dirpath)
        # ``followlinks=False`` keeps the walk inside the tree.
        # Subdirectory symlinks are reported as violations and not
        # descended into; see the module docstring for why (loader
        # would follow them while the audit cannot safely descend).
        kept_dirs: list[str] = []
        for d in dirnames:
            child = dirpath_p / d
            if child.is_symlink():
                violations.append(
                    f"{child}: symlinked subdirectories are not supported "
                    f"(symlink the config root or individual files instead, "
                    f"or restructure so this is a real directory)"
                )
                continue
            paths_to_check.append(child)
            kept_dirs.append(d)
        dirnames[:] = kept_dirs
        for f in filenames:
            child = dirpath_p / f
            paths_to_check.append(child)
            if child.is_symlink():
                symlink_paths.add(child)

    for path in paths_to_check:
        try:
            info = path.stat()
        except OSError as exc:
            violations.append(f"{path}: cannot stat ({exc})")
            continue
        # Symlink-target qualifier surfaces in every violation message
        # for a path that was reached via a symlink, so an operator who
        # ``ls -l``s the offending path (and sees the symlink's own
        # ``lrwxrwxrwx``) understands that the reported owner/mode are
        # the *target*'s, not the symlink's.
        via_symlink = " (via symlink target)" if path in symlink_paths else ""
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
            violations.append(
                f"{path}: not a regular file or directory "
                f"(mode={stat.filemode(info.st_mode)}){via_symlink}"
            )
            continue
        if info.st_uid != expected_uid:
            violations.append(
                f"{path}: owned by UID {info.st_uid}, expected {expected_uid} "
                f"(run `chown {expected_uid} {path}` as root to fix)"
                f"{via_symlink}"
            )
        forbidden = stat.S_IMODE(info.st_mode) & _FORBIDDEN_MODE_BITS
        if forbidden:
            kind = "directory" if stat.S_ISDIR(info.st_mode) else "file"
            remediation = "chmod 700" if kind == "directory" else "chmod 600"
            violations.append(
                f"{path}: mode {stat.filemode(info.st_mode)} permits "
                f"group/other access (run `{remediation} {path}` to fix)"
                f"{via_symlink}"
            )

    return violations


def _audit_windows(config_dir: Path) -> list[str]:
    """Return Windows containment violations for ``config_dir``.

    The check verifies the resolved configuration directory is a
    descendant of the current user's profile (``Path.home()``). Anything
    outside the profile is rejected because POSIX-style mode bits do not
    represent Windows ACLs reliably and a more thorough check would
    require a third-party dependency.

    Args:
        config_dir (Path): The configuration directory to validate.

    Returns:
        list[str]: One human-readable message if the directory is not
            under the user profile; empty otherwise.
    """
    home = Path.home().resolve()
    resolved = config_dir.resolve()
    try:
        resolved.relative_to(home)
    except ValueError:
        return [
            f"{resolved}: not under the current user profile {home}. "
            "On Windows, place the configuration directory under your user "
            "profile (e.g. %APPDATA%/Deephaven/ai/) so the default ACLs "
            "restrict access to your account."
        ]
    return []
