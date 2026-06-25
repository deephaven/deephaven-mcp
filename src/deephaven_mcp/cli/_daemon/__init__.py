"""CLI daemon lifecycle subsystem — find an existing daemon or spawn one.

The ``dh-mcp`` command layer obtains daemon connection details through this
package's public surface; every CLI subcommand reaches a live daemon via
:func:`get_or_start_daemon` (directly, or through the shared acquire helper in
:mod:`deephaven_mcp.cli._commands._acquire`), and the ``daemon`` verb group
also calls :func:`stop_daemon`.

The subsystem is split by concern across private submodules:

- :mod:`._context` — :class:`DaemonContext` (the spawn-path input value object,
  built from a :class:`~deephaven_mcp.cli._runtime.Runtime` via
  :meth:`DaemonContext.from_runtime`).
- :mod:`._reuse` — the pure per-field reuse-policy engine.
- :mod:`._lifecycle` — the orchestration core (:func:`get_or_start_daemon`,
  :func:`stop_daemon`), including PID-reuse-safe termination.

The daemon lifecycle exceptions live in :mod:`deephaven_mcp._exceptions` per
the project's exception-organization rule and are re-exported here for caller
convenience.
"""

from __future__ import annotations

__all__ = [
    "DaemonClientError",
    "DaemonContext",
    "DaemonReuseRefusedError",
    "DaemonStartupTimeoutError",
    "get_or_start_daemon",
    "stop_daemon",
]

from deephaven_mcp._exceptions import (
    DaemonClientError,
    DaemonReuseRefusedError,
    DaemonStartupTimeoutError,
)

from ._context import DaemonContext
from ._lifecycle import get_or_start_daemon, stop_daemon
