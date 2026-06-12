"""Structured-error contract for the ``dh-mcp`` CLI.

Subcommand handlers signal user-facing failure by raising
:class:`CliError` with a stable :class:`ErrorCode` enum value. The
root command's exception handler renders the error in the active
``-o/--output`` mode:

- ``human`` → ``<command>: <message>`` on stderr.
- ``json`` / ``yaml`` → a structured payload on stderr containing
  ``error``, ``error_code``, ``exit_code``, and ``command``.

The enum is the single source of truth for the agent-facing
``error_code`` registry; ``docs/CLI.md`` mirrors it for end users.
"""

from __future__ import annotations

__all__ = [
    "CliError",
    "ErrorCode",
    "ExitCode",
    "render_error",
    "render_warning",
]

import json
import sys
from enum import IntEnum, StrEnum
from typing import TextIO, assert_never

import click
import yaml

from deephaven_mcp.cli._format import OutputMode


class ExitCode(IntEnum):
    """Stable process exit codes for the ``dh-mcp`` CLI.

    Each member carries its agent-facing help text intrinsically via the
    ``(value, help_text)`` tuple — adding a member without a help string
    is a ``TypeError`` at class-construction time, the same guard
    :class:`ErrorCode` uses. Read ``ec.help_text`` for the explanation
    and ``ec.value`` (or the member itself, an ``int``) for the code.
    """

    SUCCESS = (0, "success")
    USER_ERROR = (2, "user-facing failure")
    TOOL_ERROR = (3, "the invoked MCP tool returned isError=True")

    help_text: str

    def __new__(cls, value: int, help_text: str) -> ExitCode:
        """Bind the numeric exit code and its help text together.

        ``IntEnum``'s default ``__new__`` accepts a single integer
        value. Extending it to a ``(value, help_text)`` tuple makes the
        help text a first-class attribute of each member; adding a
        member without help fails at class-construction time when this
        initializer raises ``TypeError`` for the missing argument.
        """
        member = int.__new__(cls, value)
        member._value_ = value
        member.help_text = help_text
        return member


class ErrorCode(StrEnum):
    """Stable identifiers for every user-facing CLI failure mode.

    Each member carries its agent-facing help text and process exit
    code intrinsically via the ``(value, help_text[, exit_code])``
    tuple — adding a new member without a help string is a
    ``TypeError`` at class-construction time, which keeps the
    ``introspect`` manifest's ``error_codes`` section from drifting
    silently. ``exit_code`` defaults to ``2``; only failures that must
    exit ``3`` declare it. Read ``ec.help_text`` for the explanation,
    ``ec.exit_code`` for the process code, and ``ec.value`` for the
    stable wire identifier.
    """

    DAEMON_STARTUP_TIMEOUT = (
        "daemon_startup_timeout",
        "The daemon was spawned but did not publish a registry entry in time.",
    )
    DAEMON_NOT_RUNNING = (
        "daemon_not_running",
        (
            "No running daemon was found: either none is registered and "
            "--no-auto-start was specified, or a command that needs the "
            "daemon's files (e.g. 'daemon logs') found none yet."
        ),
    )
    DAEMON_CLIENT_ERROR = (
        "daemon_client_error",
        "A client-side daemon-management failure (signal denied, etc.).",
    )
    DAEMON_REGISTRY_CORRUPT = (
        "daemon_registry_corrupt",
        (
            "The on-disk daemon.json exists but cannot be parsed. Distinct "
            "from daemon_not_running so the operator gets an actionable "
            "diagnostic rather than the misleading 'no daemon running' the CLI "
            "would otherwise emit after silently treating a corrupt file as "
            "absent."
        ),
    )
    DAEMON_REGISTRY_LIVE = (
        "daemon_registry_live",
        (
            "'dh-mcp daemon reset' refused to quarantine daemon.json "
            "because a live daemon is still registered. Run 'dh-mcp daemon "
            "stop' first to avoid orphaning the running process."
        ),
    )
    MCP_REQUEST_FAILED = (
        "mcp_request_failed",
        "The MCP transport reported an error (connect, timeout, parse).",
    )
    TOOL_NOT_FOUND = (
        "tool_not_found",
        "'dh-mcp tool show' or 'tool call' referenced an unknown tool name.",
    )
    TOOL_RETURNED_ERROR = (
        "tool_returned_error",
        "The invoked tool returned isError=true. Exit code 3.",
        3,
    )
    ARG_PARSE_ERROR = (
        "arg_parse_error",
        "A key=value token (--arg, --env, --session-arg) was malformed.",
    )
    OPTION_NOT_APPLICABLE = (
        "option_not_applicable",
        (
            "An option/argument combination is invalid for the selected "
            "--system type: an option that does not apply (e.g. a Docker option "
            "with an Enterprise system), or a missing required one (e.g. no "
            "session name for a Community session)."
        ),
    )
    BROWSER_LAUNCH_FAILED = (
        "browser_launch_failed",
        (
            "The default web browser could not be launched; the URL is included "
            "in the error message so it can be opened manually."
        ),
    )
    CONFIG_INVALID = (
        "config_invalid",
        "The configuration tree failed validation.",
    )
    INTERNAL_ERROR = (
        "internal_error",
        "An unexpected internal failure not attributable to a specific subsystem.",
    )

    help_text: str
    exit_code: int

    def __new__(cls, value: str, help_text: str, exit_code: int = 2) -> ErrorCode:
        """Bind the wire value, help text, and exit code together.

        ``StrEnum``'s default ``__new__`` accepts a single string
        value. Extending it to a ``(value, help_text[, exit_code])``
        tuple makes the help text and exit code first-class
        attributes of each member; adding a new member without help
        would fail at class-construction time when this initializer
        raises ``TypeError`` for missing positional args.
        """
        member = str.__new__(cls, value)
        member._value_ = value
        member.help_text = help_text
        member.exit_code = exit_code
        return member


class CliError(click.ClickException):
    """Structured user-facing CLI error.

    Subclasses :class:`click.ClickException` so click's normal
    dispatch machinery routes it to ``show()``; the root command's
    custom ``result_callback`` re-renders it according to the
    active output mode before exiting with ``exit_code``.
    """

    def __init__(self, message: str, *, code: ErrorCode) -> None:
        """Capture the message and the stable code.

        Args:
            message (str): Operator-actionable description.
            code (ErrorCode): The :class:`ErrorCode` enum value. Its
                ``exit_code`` becomes this error's process exit code.
        """
        super().__init__(message)
        self.code = code
        self.exit_code = code.exit_code

    def format_message(self) -> str:
        """Return the plain message; ``human`` mode prints this verbatim."""
        return self.message


def render_error(
    err: CliError,
    *,
    output: OutputMode,
    command: str,
    stream: TextIO | None = None,
) -> None:
    """Render ``err`` to stderr in the requested output mode.

    Args:
        err (CliError): The exception to render.
        output (OutputMode): Active output mode (``human``, ``json``,
            ``yaml``).
        command (str): Dotted command path (e.g. ``daemon start``)
            for the structured ``command`` field.
        stream (TextIO | None): Stream to write to. ``None`` falls
            back to :data:`sys.stderr`. Useful for tests.
    """
    target = stream if stream is not None else sys.stderr
    payload = {
        "error": err.message,
        "error_code": err.code.value,
        "exit_code": err.exit_code,
        "command": command,
    }
    match output:
        case "human":
            target.write(f"{command}: {err.message}\n")
        case "json":
            target.write(json.dumps(payload, indent=2, sort_keys=True))
            target.write("\n")
        case "yaml":
            target.write(yaml.safe_dump(payload, sort_keys=True))
        case _ as unexpected:
            # Statically unreachable thanks to the ``OutputMode``
            # ``Literal``; the runtime ``assert_never`` is the safety
            # net for callers that bypassed type checking.
            assert_never(unexpected)


def render_warning(
    message: str,
    *,
    output: OutputMode,
    details: dict[str, str] | None = None,
    stream: TextIO | None = None,
) -> None:
    """Render a non-fatal warning to stderr in the requested output mode.

    Stdout carries the command's result; warnings (e.g. partial-result or
    discovery diagnostics) go to stderr so they never pollute ``-o json`` /
    ``-o yaml`` stdout.

    Args:
        message (str): Human-readable warning summary.
        output (OutputMode): Active output mode (``human``, ``json``,
            ``yaml``).
        details (dict[str, str] | None): Optional per-item detail (e.g.
            ``{system: error}``); listed beneath the message in ``human``
            mode and nested under ``details`` in ``json`` / ``yaml`` mode.
        stream (TextIO | None): Stream to write to. ``None`` falls back to
            :data:`sys.stderr`. Useful for tests.
    """
    target = stream if stream is not None else sys.stderr
    payload: dict[str, object] = {"warning": message}
    if details:
        payload["details"] = details
    match output:
        case "human":
            target.write(f"warning: {message}\n")
            for name, detail in (details or {}).items():
                target.write(f"  {name}: {detail}\n")
        case "json":
            target.write(json.dumps(payload, indent=2, sort_keys=True))
            target.write("\n")
        case "yaml":
            target.write(yaml.safe_dump(payload, sort_keys=True))
        case _ as unexpected:
            assert_never(unexpected)
