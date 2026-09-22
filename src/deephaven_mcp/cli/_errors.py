"""Structured-error contract for the ``dhcli`` CLI.

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

import sys
from enum import IntEnum, StrEnum
from typing import TextIO, assert_never

import click

from deephaven_mcp.cli._format import OutputMode, format_output


class ExitCode(IntEnum):
    """Stable process exit codes for the ``dhcli`` CLI.

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
    ``agents`` manifest's ``error_codes`` section from drifting
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
    # Kept distinct from DAEMON_NOT_RUNNING deliberately: treating a corrupt
    # registry as absent would emit the misleading 'no daemon running' instead
    # of an actionable diagnostic. Do not merge the two.
    DAEMON_REGISTRY_CORRUPT = (
        "daemon_registry_corrupt",
        (
            "The on-disk daemon.json exists but cannot be parsed. Recover "
            "with 'dhcli daemon repair'."
        ),
    )
    DAEMON_REGISTRY_LIVE = (
        "daemon_registry_live",
        (
            "'dhcli daemon repair' refused to move daemon.json aside "
            "because a live daemon is still registered. Run 'dhcli daemon "
            "stop' first to avoid orphaning the running process."
        ),
    )
    DAEMON_REUSE_REFUSED = (
        "daemon_reuse_refused",
        (
            "The running daemon is a different build than this CLI (its "
            "version, virtualenv, or source fingerprint differs) and the "
            "daemon.reuse policy resolved to 'refuse'. Run 'dhcli daemon "
            "restart' to replace it, or adjust the configured "
            "cli.daemon.reuse."
        ),
    )
    MCP_REQUEST_FAILED = (
        "mcp_request_failed",
        "The MCP transport reported an error (connect, parse, server failure).",
    )
    MCP_REQUEST_TIMEOUT = (
        "mcp_request_timeout",
        (
            "The MCP request timed out. The server may still finish "
            "processing the request — if the operation changes state, verify "
            "the result before retrying. Allow more time with --timeout, or "
            "raise the configured cli.request.timeouts.default_seconds "
            "(cli.docs.timeouts.request_seconds for the 'docs' commands)."
        ),
    )
    TOOL_NOT_FOUND = (
        "tool_not_found",
        "'dhcli tool show' or 'tool call' referenced an unknown tool name.",
    )
    TOOL_RETURNED_ERROR = (
        "tool_returned_error",
        "The invoked tool returned isError=true. Exit code 3.",
        3,
    )
    ARG_PARSE_ERROR = (
        "arg_parse_error",
        "An option value could not be parsed: a malformed key=value token "
        "(--arg, --env, --session-arg), or malformed JSON (--history).",
    )
    COMMAND_NOT_FOUND = (
        "command_not_found",
        "'dhcli agents command PATH' referenced a command path that does not exist.",
    )
    MISSING_ARGUMENT = (
        "missing_argument",
        (
            "A required positional argument or option was not provided, or "
            "was provided as a blank string. No dhcli parameter accepts a "
            "blank value; note that a KEY=VALUE option such as --env "
            "'DEBUG=' is not blank and remains valid."
        ),
    )
    MUTUALLY_EXCLUSIVE_OPTIONS = (
        "mutually_exclusive_options",
        "Two or more options that cannot be combined were supplied together.",
    )
    FILE_READ_FAILED = (
        "file_read_failed",
        "A local file passed on the command line could not be read.",
    )
    OPTION_NOT_APPLICABLE = (
        "option_not_applicable",
        (
            "An option/argument combination is invalid for the selected "
            "--system or --auth type: an option that does not apply (e.g. a "
            "Docker option with an Enterprise system, or --token with "
            "--auth password), or a missing required one (e.g. no session "
            "name for a Community session)."
        ),
    )
    BROWSER_LAUNCH_FAILED = (
        "browser_launch_failed",
        (
            "The default web browser could not be launched; the URL is included "
            "in the error message so it can be opened manually. For 'session "
            "open' that URL omits the auth token unless --reveal-secrets was "
            "passed, so opening it will prompt for credentials; 'dhcli session "
            "url' returns one that logs in."
        ),
    )
    SYSTEM_NOT_FOUND = (
        "system_not_found",
        "The named Enterprise system is not configured (run 'dhcli system list').",
    )
    CONTEXT_NOT_SET = (
        "context_not_set",
        (
            "A session/system/PQ id was omitted and no sticky context supplies "
            "one: pass it explicitly, run 'dhcli context set <key> <value>' to "
            "establish a default, or check 'dhcli context show' if one was "
            "expected. Context fallback is skipped entirely when --no-context "
            "was given or the configured cli.context.enabled is false."
        ),
    )
    CONFIG_INVALID = (
        "config_invalid",
        "The configuration tree failed validation.",
    )
    NO_SYSTEMS_CONFIGURED = (
        "no_systems_configured",
        (
            "Every configuration file is individually valid, but no system "
            "is declared: there is no community session file, no "
            "session_creation block in community/settings.json, and no "
            "enterprise system file. Add a system ('dhcli config session "
            "add', 'dhcli config system add', or 'dhcli config init'), or check "
            "that --config-dir / DH_AI_DATA_DIR points at the intended "
            "directory."
        ),
    )
    CONFIG_PATH_INVALID = (
        "config_path_invalid",
        (
            "A configuration path argument is malformed or does not name a "
            "known location. Run 'dhcli config files' to list files and "
            "'dhcli config keys' to list settable paths."
        ),
    )
    MISSING_REQUIRED_OPTION = (
        "missing_required_option",
        (
            "A required option was not provided and interactive prompting is "
            "unavailable (stdin is not a TTY, or --no-input was given). The "
            "error message names the exact flag(s) to supply."
        ),
    )
    ALREADY_EXISTS = (
        "already_exists",
        (
            "The target configuration file already exists and the command "
            "refuses to overwrite it. Remove a session or system first with "
            "'dhcli config session/system remove'; change an existing file "
            "in place with 'dhcli config set' or 'dhcli config edit'."
        ),
    )
    NOT_FOUND = (
        "not_found",
        (
            "The named configuration entity or file does not exist, or the "
            "addressed field has no value set."
        ),
    )
    CONFIG_NOT_REWRITABLE = (
        "config_not_rewritable",
        (
            "The target file uses JSON5-only syntax (comments, trailing "
            "commas) that a programmatic read-modify-write would silently "
            "destroy. 'config set'/'unset' refuse to touch it; edit the "
            "file directly, or with 'dhcli config edit', instead."
        ),
    )
    NO_TTY = (
        "no_tty",
        (
            "The command is interactive-only ('config edit') but stdin is not "
            "a TTY or --no-input was given. Use the non-interactive "
            "equivalents ('config set', 'config session add' with flags) "
            "instead."
        ),
    )
    # The 'config' authoring verbs serialize on a per-directory advisory lock
    # so concurrent writers cannot clobber each other.
    CONFIG_LOCKED = (
        "config_locked",
        (
            "Another process holds the configuration write lock, so this "
            "authoring command could not acquire it before timing out; retry "
            "once the other invocation finishes."
        ),
    )
    OPERATION_CANCELED = (
        "operation_canceled",
        (
            "The operator answered no to an interactive confirmation "
            "prompt, so a destructive action was not performed. A Ctrl-C "
            "interruption exits 130 instead, so a script can tell a "
            "deliberate refusal from a signal."
        ),
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
        # click declares ``exit_code`` as a class variable; shadowing it
        # per-instance is intentional and what click reads at exit time.
        self.exit_code = code.exit_code  # type: ignore[misc]

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
            ``json-pretty``, ``yaml``).
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
        case "json" | "json-pretty" | "yaml":
            target.write(format_output(payload, output=output))
            target.write("\n")
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
            ``json-pretty``, ``yaml``).
        details (dict[str, str] | None): Optional per-item detail (e.g.
            ``{system: error}``); listed beneath the message in ``human``
            mode and nested under ``details`` in the structured modes.
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
        case "json" | "json-pretty" | "yaml":
            target.write(format_output(payload, output=output))
            target.write("\n")
        case _ as unexpected:
            assert_never(unexpected)
