"""Spawn-path inputs for the daemon lifecycle, and the :class:`Runtime` bridge.

Carries the :class:`DaemonContext` value object consumed by
:func:`deephaven_mcp.cli._daemon._lifecycle.get_or_start_daemon`. Its
:meth:`DaemonContext.from_runtime` constructor is the one place that depends on
:class:`~deephaven_mcp.cli._runtime.Runtime`. The command layer builds a
:class:`DaemonContext` once per verb invocation and reads the lifecycle
tunables (``auto_start``, ``startup_deadline_seconds``, ``kill_after_seconds``)
directly off ``runtime.config.cli.daemon``. Keeping the lifecycle functions off
:class:`Runtime` lets tests call them with focused scalar arguments rather than
constructing a full runtime.
"""

from __future__ import annotations

__all__ = [
    "DaemonContext",
]

import sys
from dataclasses import dataclass
from pathlib import Path

from deephaven_mcp.daemon_registry import DaemonDirectory

from .._runtime import Runtime


@dataclass(frozen=True, slots=True)
class DaemonContext:
    """Spawn-path inputs to :func:`get_or_start_daemon`.

    Carries everything needed to *spawn* a daemon. :func:`stop_daemon`
    needs no spawn parameters, so it takes a bare
    :class:`~deephaven_mcp.daemon_registry.DaemonDirectory` instead of
    a :class:`DaemonContext`.
    """

    directory: DaemonDirectory
    """Typed handle to ``<runtime_dir>/daemon/`` exposing the
    registry / lock / log paths and atomic registry CRUD."""

    spawn_argv: list[str]
    """Argv list that launches a fresh daemon process."""

    spawn_cwd: Path
    """Working directory the spawned daemon process inherits."""

    @classmethod
    def from_runtime(cls, runtime: Runtime) -> DaemonContext:
        """Build a :class:`DaemonContext` from a :class:`Runtime`.

        The lifecycle tunables (``auto_start``, ``startup_deadline_seconds``,
        ``kill_after_seconds``) are read directly off
        ``runtime.config.cli.daemon`` at the call site and passed as keyword
        arguments to :func:`get_or_start_daemon` / :func:`stop_daemon`.

        Args:
            runtime (Runtime): The CLI's pre-resolved runtime.

        Returns:
            DaemonContext: The spawn-path input for
                :func:`get_or_start_daemon`.
        """
        return cls(
            directory=runtime.daemon_dir,
            spawn_argv=_build_spawn_command(runtime),
            spawn_cwd=runtime.runtime_dir,
        )


def _build_spawn_command(runtime: Runtime) -> list[str]:
    """Return the argv that launches ``dh-mcp-systems-server --daemon``.

    Built from :data:`sys.executable` rather than the
    ``dh-mcp-systems-server`` entry-point script so the spawned
    daemon runs under the same Python interpreter as the caller.

    Args:
        runtime (Runtime): The CLI's pre-resolved runtime context.
            Read for ``config_dir`` and ``runtime_dir`` only.

    Returns:
        list[str]: The argv list, suitable for
            :class:`DaemonContext.spawn_argv`.
    """
    return [
        sys.executable,
        "-m",
        "deephaven_mcp.mcp_systems_server",
        "--daemon",
        "--config-dir",
        str(runtime.config_dir),
        "--runtime-dir",
        str(runtime.runtime_dir),
    ]
