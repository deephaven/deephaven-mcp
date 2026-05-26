"""Resolution of the MCP configuration directory.

The configuration directory holds the per-file tree that
:class:`~deephaven_mcp.config.MultiSystemConfigManager` reads at
startup. Resolution precedence (highest first):

1. Explicit ``config_dir`` argument passed to the manager.
2. ``$DH_MCP_CONFIG_DIR`` environment variable.
3. :func:`default_config_dir` (per-platform default).
"""

from __future__ import annotations

__all__ = [
    "CONFIG_DIR_ENV_VAR",
    "default_config_dir",
    "resolve_config_dir",
]

import os
import sys
from pathlib import Path

CONFIG_DIR_ENV_VAR = "DH_MCP_CONFIG_DIR"
"""Environment variable that overrides :func:`default_config_dir`."""


def default_config_dir() -> Path:
    """Return the default configuration directory for this platform.

    Returns:
        Path: ``~/.deephaven/ai/config`` on macOS and Linux;
            ``%APPDATA%/Deephaven/ai/config`` on Windows. Falls back to
            the home-directory form when ``%APPDATA%`` is unset.
    """
    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "Deephaven" / "ai" / "config"
    return Path.home() / ".deephaven" / "ai" / "config"


def resolve_config_dir(explicit: Path | None) -> Path:
    """Resolve the configuration directory using documented precedence.

    Args:
        explicit (Path | None): When not ``None``, overrides every
            other source. The leading ``~`` is expanded via
            :meth:`Path.expanduser` so callers may pass
            ``~/.deephaven/...`` from a CLI or config file.

    Returns:
        Path: Resolution order is ``explicit`` argument, then
            ``$DH_MCP_CONFIG_DIR``, then :func:`default_config_dir`.
            The first two sources are passed through
            :meth:`Path.expanduser` so a leading ``~`` resolves to
            the running user's home directory.
    """
    if explicit is not None:
        return explicit.expanduser()
    env_value = os.environ.get(CONFIG_DIR_ENV_VAR)
    if env_value:
        return Path(env_value).expanduser()
    return default_config_dir()
