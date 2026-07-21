"""Resolution of the daemon's runtime directory.

The runtime directory holds *mutable* per-user state owned by the
running daemon — most notably the ``daemon/`` subtree containing
``daemon.json`` (the registry handshake), ``daemon.lock`` (the
advisory spawn lock), and ``daemon.log`` (captured stdout/stderr).
Configuration files (read-only, hand-edited) live next door under
:func:`deephaven_mcp.config.resolve_config_dir`; both subdirectories
live under the shared user-data root managed by
:mod:`deephaven_mcp.config._data_root`.

Resolution precedence (highest first):

1. Explicit ``runtime_dir`` argument passed by the caller (CLI
   ``--runtime-dir`` flag, test fixtures, etc.).
2. ``$DH_AI_DATA_DIR / "runtime"`` — i.e. the env-overridden data
   root with a fixed ``"runtime"`` subdirectory.
3. The platform default data root plus ``"runtime"``.

There is intentionally **no** ``DH_AI_RUNTIME_DIR`` env var: a
single ``DH_AI_DATA_DIR`` knob moves every MCP-managed directory at
once.

The :func:`daemon_dir` helper returns the subdirectory under the
runtime root that owns the daemon's registry, lock, and log files.
The :func:`instances_dir` helper returns the subdirectory that owns
the per-instance metadata files written by the systems-server's
instance tracker. Both are exposed alongside :func:`resolve_runtime_dir`
because callers need to compose these exact paths.
"""

from __future__ import annotations

__all__ = [
    "daemon_dir",
    "instances_dir",
    "resolve_runtime_dir",
]

from pathlib import Path

from ._data_root import resolve_data_root


def resolve_runtime_dir(explicit: Path | None) -> Path:
    """Resolve the runtime directory using documented precedence.

    Args:
        explicit (Path | None): When not ``None``, overrides every
            other source. The leading ``~`` is expanded via
            :meth:`Path.expanduser`.

    Returns:
        Path: The ``explicit`` argument (after ``~`` expansion) when
            supplied; otherwise :func:`resolve_data_root` ``/ "runtime"``.
    """
    if explicit is not None:
        return explicit.expanduser()
    return resolve_data_root() / "runtime"


def daemon_dir(runtime_dir: Path) -> Path:
    """Return the daemon-state subdirectory of ``runtime_dir``.

    This is where the daemon writes ``daemon.json``, ``daemon.lock``,
    and ``daemon.log``. The path is deterministic and always one
    level deep, so callers can construct it without consulting any
    env var or platform conditional.

    Args:
        runtime_dir (Path): The resolved runtime directory, typically
            from :func:`resolve_runtime_dir`.

    Returns:
        Path: ``runtime_dir / "daemon"``. The path is returned even
            if it does not exist; callers that need the directory on
            disk are responsible for creating it.
    """
    return runtime_dir / "daemon"


def instances_dir(runtime_dir: Path) -> Path:
    """Return the instance-metadata subdirectory of ``runtime_dir``.

    This is where the systems-server's instance tracker writes one
    ``{uuid}.json`` file per running process. The path is deterministic
    and always one level deep, so callers can construct it without
    consulting any env var or platform conditional.

    Args:
        runtime_dir (Path): The resolved runtime directory, typically
            from :func:`resolve_runtime_dir`.

    Returns:
        Path: ``runtime_dir / "instances"``. The path is returned even
            if it does not exist; callers that need the directory on
            disk are responsible for creating it.
    """
    return runtime_dir / "instances"
