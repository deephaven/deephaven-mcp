"""Single source of truth for the Deephaven MCP user-data root.

Every directory the MCP server family reads from or writes to lives
under one *user-data root*: configuration files, the daemon registry,
the daemon log, and any future mutable state. This module owns:

- The platform default
  (``~/.deephaven/ai`` on POSIX; ``%APPDATA%/Deephaven/ai`` on
  Windows).
- The single environment variable that overrides it
  (:data:`DATA_DIR_ENV_VAR` = ``DH_AI_DATA_DIR``).
- :func:`resolve_data_root` — the only function that reads the env
  var. Per-subdirectory helpers (``resolve_config_dir``,
  ``resolve_runtime_dir``) call this internally; they do *not* read
  any per-subdir env var of their own.

Design intent:

- **One operator knob.** Setting ``DH_AI_DATA_DIR=/opt/deephaven/ai``
  moves *every* MCP-managed directory at once. There is no
  per-subdir env var.
- **Per-subdir explicit overrides remain.** CLI flags (``--config-dir``,
  ``--runtime-dir``) and tests still pass concrete :class:`Path`
  arguments through the per-subdir resolvers; those arguments win
  unconditionally and bypass :func:`resolve_data_root`.
- **No CLI ``--data-dir`` flag today.** Operators who want to move
  the root use the env var; the CLI surfaces only the per-subdir
  flags because those satisfy targeted-override needs (e.g. tests
  pointing one subdir at a ``tmp_path`` while the other stays
  default).
"""

from __future__ import annotations

__all__ = [
    "DATA_DIR_ENV_VAR",
    "resolve_data_root",
]

import os
import sys
from pathlib import Path

DATA_DIR_ENV_VAR = "DH_AI_DATA_DIR"
"""Environment variable that overrides the platform-default data root.

When set, :func:`resolve_data_root` returns ``Path($DH_AI_DATA_DIR)``
(after :meth:`Path.expanduser`); all per-subdir resolvers
(:func:`~deephaven_mcp.config.resolve_config_dir`,
:func:`~deephaven_mcp.config.resolve_runtime_dir`) compose their
results under this root.
"""


def _default_data_root() -> Path:
    """Return the platform-default user-data root.

    Returns:
        Path: ``~/.deephaven/ai`` on macOS and Linux;
            ``%APPDATA%/Deephaven/ai`` on Windows. Falls back to the
            home-directory form when ``%APPDATA%`` is unset on
            Windows.
    """
    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "Deephaven" / "ai"
    return Path.home() / ".deephaven" / "ai"


def resolve_data_root() -> Path:
    """Resolve the user-data root using documented precedence.

    Returns:
        Path: ``$DH_AI_DATA_DIR`` (with a leading ``~`` expanded via
            :meth:`Path.expanduser`) when the env var is set and
            non-empty; otherwise :func:`_default_data_root`.
    """
    env_value = os.environ.get(DATA_DIR_ENV_VAR)
    if env_value:
        return Path(env_value).expanduser()
    return _default_data_root()
