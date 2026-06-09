"""Resolution of the MCP configuration directory.

The configuration directory holds the per-file tree that
:class:`~deephaven_mcp.config.tree.ConfigTreeLoader`
reads at startup. It is one of the subdirectories that lives under
the shared user-data root managed by
:mod:`deephaven_mcp.config._data_root`.

Resolution precedence (highest first):

1. Explicit ``config_dir`` argument passed by the caller (CLI
   ``--config-dir`` flag, test fixtures, etc.).
2. ``$DH_MCP_DATA_DIR / "config"`` — i.e. the env-overridden data
   root with a fixed ``"config"`` subdirectory.
3. The platform default data root (see
   :func:`deephaven_mcp.config._data_root._default_data_root`) plus
   ``"config"``.

There is intentionally **no** ``DH_MCP_CONFIG_DIR`` env var: a single
``DH_MCP_DATA_DIR`` knob moves every MCP-managed directory at once,
which is the operator use case that motivated this design.
"""

from __future__ import annotations

__all__ = ["resolve_config_dir"]

from pathlib import Path

from ._data_root import resolve_data_root


def resolve_config_dir(explicit: Path | None) -> Path:
    """Resolve the configuration directory using documented precedence.

    Args:
        explicit (Path | None): When not ``None``, overrides every
            other source. The leading ``~`` is expanded via
            :meth:`Path.expanduser` so callers may pass
            ``~/.deephaven/...`` from a CLI flag or config file.

    Returns:
        Path: The ``explicit`` argument (after ``~`` expansion) when
            supplied; otherwise :func:`resolve_data_root` ``/ "config"``.
    """
    if explicit is not None:
        return explicit.expanduser()
    return resolve_data_root() / "config"
