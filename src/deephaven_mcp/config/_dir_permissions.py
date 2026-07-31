"""Startup permission policy for the Deephaven MCP configuration directory.

The configuration directory holds files containing PSKs, passwords, and
private-key paths. Before any JSON parsing happens at startup,
:func:`verify_config_directory_permissions` refuses to proceed unless the
directory tree is locked down to the running user.

This module owns only the *policy* (existence checks, error aggregation,
refuse-to-start). The per-OS audit mechanics it dispatches to live in
:func:`deephaven_mcp._platform.dir_permissions.audit_tree` (strict
owner/mode walk on POSIX; user-profile containment on Windows); see that
module for the full platform rules and symlink policy.

All violations encountered during a single audit are collected into one
:class:`ConfigurationError` with a remediation hint per offender; the
message is also emitted at ``ERROR`` level via the module logger.
"""

from __future__ import annotations

import logging
from pathlib import Path

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._platform.dir_permissions import audit_tree

__all__ = ["verify_config_directory_permissions"]

_LOGGER = logging.getLogger(__name__)


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

    violations = audit_tree(config_dir)

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
