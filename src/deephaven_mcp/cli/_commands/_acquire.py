"""Daemon acquisition for ``dh-mcp`` command modules.

Adapts the ``cli/_daemon.py`` lifecycle layer for command use: acquire a
running daemon (get-or-start) and translate daemon-lifecycle exceptions into
:class:`CliError` with stable error codes and an operator recovery hint. The
error mapping is supplied by the caller, so each command sets its own policy.
"""

from __future__ import annotations

from collections.abc import Callable

from deephaven_mcp.cli._daemon import (
    DaemonClientError,
    DaemonStartupTimeoutError,
    build_daemon_context,
    get_or_start_daemon,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import DaemonRegistryEntry, RegistryCorruptError

__all__ = ["acquire_daemon", "registry_corrupt_message"]


def registry_corrupt_message(exc: RegistryCorruptError, *, retry_command: str) -> str:
    """Format a corrupt-registry error with the recovery procedure.

    Args:
        exc (RegistryCorruptError): The underlying corruption error.
        retry_command (str): The command rendered into the final
            recovery step for the operator to re-run.

    Returns:
        str: The error message followed by the numbered recovery hint.
    """
    return (
        f"{exc}. The daemon process (if any) is unaffected. To recover:\n"
        "  1. Run `dh-mcp daemon status` to confirm whether a daemon is still running.\n"
        "  2. If a daemon is running, run `dh-mcp daemon stop` first.\n"
        "  3. Run `dh-mcp daemon reset` to quarantine the corrupt file.\n"
        f"  4. Re-run `{retry_command}`."
    )


async def acquire_daemon(
    runtime: Runtime,
    *,
    auto_start: bool,
    client_error_code: ErrorCode,
    on_registry_corrupt: Callable[[RegistryCorruptError], CliError],
) -> DaemonRegistryEntry:
    """Acquire a live daemon, remapping lifecycle exceptions to ``CliError``.

    Builds the :class:`DaemonContext` from ``runtime`` and calls
    :func:`get_or_start_daemon`, translating each daemon-lifecycle
    exception into a :class:`CliError` carrying a stable :class:`ErrorCode`.

    Args:
        runtime (Runtime): The active CLI runtime.
        auto_start (bool): Whether to spawn a daemon when none is running.
        client_error_code (ErrorCode): The code attached when
            :class:`DaemonClientError` is raised.
        on_registry_corrupt (Callable[[RegistryCorruptError], CliError]):
            Builds the :class:`CliError` raised for a corrupt registry,
            so each command supplies its own recovery hint.

    Returns:
        DaemonRegistryEntry: The validated registry entry for the
            running daemon.

    Raises:
        CliError: Wrapping :class:`DaemonStartupTimeoutError`,
            :class:`DaemonClientError`, or :class:`RegistryCorruptError`.
    """
    ctx = build_daemon_context(runtime)
    try:
        return await get_or_start_daemon(
            ctx,
            auto_start=auto_start,
            startup_deadline_seconds=runtime.config.cli.daemon.timeouts.startup_deadline_seconds,
        )
    except DaemonStartupTimeoutError as exc:
        raise CliError(str(exc), code=ErrorCode.DAEMON_STARTUP_TIMEOUT) from exc
    except DaemonClientError as exc:
        raise CliError(str(exc), code=client_error_code) from exc
    except RegistryCorruptError as exc:
        raise on_registry_corrupt(exc) from exc
