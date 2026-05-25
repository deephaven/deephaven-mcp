"""Filesystem permission audit for the Deephaven MCP configuration directory.

The configuration directory holds files containing PSKs, passwords, and
private-key paths. Before any JSON parsing happens at startup, this module
verifies the directory tree is locked down to the running user.

Two platform-specific strategies are used:

POSIX (Linux, macOS)
    Strict, refuse-to-start enforcement. Every regular file and every
    directory under the root must:

    - Be owned by the running UID, and
    - Have mode bits satisfying ``mode & 0o077 == 0`` (no group or other
      access of any kind).

    Symlink policy:

    - The configuration root itself (``DH_MCP_CONFIG_DIR``) may be a
      symlink; the audit follows it and verifies the target's owner
      and mode.
    - Individual **file** symlinks at any depth are accepted; the
      target's owner and mode are audited.
    - **Subdirectory** symlinks are rejected. They are forbidden
      because the loader's ``Path.glob`` follows them when listing
      section directories (``community/sessions/``,
      ``enterprise/systems/``) but the audit cannot safely descend
      through them (would either escape the tree or require bounded
      follow with cycle detection). The asymmetry would produce an
      audit hole where files inside a symlinked subdirectory are
      loaded without permission verification. Restructure the tree
      so that only the root or individual files are symlinks.

    Symlink targets are checked, not the symlink's own bits, because
    ``chmod`` on a symlink is a no-op on most filesystems.

Windows
    Best-effort, no extra dependencies. The configuration root must
    resolve to a descendant of the current user's profile directory
    (``Path.home()`` / ``%USERPROFILE%``). The default
    ``%APPDATA%/Deephaven/ai/`` location satisfies this; anything outside
    the profile is rejected. Bit-mode checks are skipped because Windows
    ACLs cannot be faithfully expressed via :func:`os.stat`'s ``st_mode``
    field.

All violations encountered during a single audit are collected into one
:class:`ConfigurationError` with a remediation hint per offender; the
message is also emitted at ``ERROR`` level via the module logger.
"""

from __future__ import annotations

import logging
import os
import stat
import sys
from pathlib import Path

from deephaven_mcp._exceptions import ConfigurationError

__all__ = ["verify_config_directory_permissions"]

_LOGGER = logging.getLogger(__name__)

_FORBIDDEN_MODE_BITS = 0o077
"""Mode bits that must be clear on every config file and directory.

Equivalent to disallowing every group and other permission. The audit
fails any path whose ``st_mode & _FORBIDDEN_MODE_BITS`` is non-zero.
"""


def verify_config_directory_permissions(config_dir: Path) -> None:
    """Refuse to start if the config directory tree is not user-private.

    Args:
        config_dir (Path): Resolved path to the configuration directory
            (typically ``~/.deephaven/ai/`` on POSIX or
            ``%APPDATA%/Deephaven/ai/`` on Windows). The directory itself
            and every regular file or subdirectory beneath it is audited.
            Symlinks at the root and for individual files are followed
            for owner/mode checks (target is what matters); symlinked
            subdirectories below the root are rejected. See the module
            docstring for the full policy.

    Raises:
        ConfigurationError: When the directory does not exist, is not a
            directory, or any audited path violates the platform's rule.
            All violations from a single call are aggregated into one
            message so the operator can fix them in a single pass.
    """
    if not config_dir.exists():
        msg = f"Configuration directory does not exist: {config_dir}"
        _LOGGER.error(f"[verify_config_directory_permissions] {msg}")
        raise ConfigurationError(msg)
    if not config_dir.is_dir():
        msg = f"Configuration path is not a directory: {config_dir}"
        _LOGGER.error(f"[verify_config_directory_permissions] {msg}")
        raise ConfigurationError(msg)

    if sys.platform == "win32":
        violations = _audit_windows(config_dir)
    else:
        violations = _audit_posix(config_dir)

    if violations:
        joined = "\n".join(f"  - {v}" for v in violations)
        msg = (
            "Configuration directory permission audit failed. "
            "Files in the configuration directory may contain credentials "
            "and must not be accessible to other users.\n"
            f"Offending paths under {config_dir}:\n{joined}"
        )
        _LOGGER.error(f"[verify_config_directory_permissions] {msg}")
        raise ConfigurationError(msg)

    _LOGGER.info(
        f"[verify_config_directory_permissions] Permission audit passed for {config_dir}."
    )


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
    for dirpath, dirnames, filenames in os.walk(
        str(config_dir), followlinks=False, onerror=None
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
