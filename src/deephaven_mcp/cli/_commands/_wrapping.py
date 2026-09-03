"""Shared flow for ``dhcli`` commands that wrap MCP tools.

Every runtime command that fronts an MCP tool follows the same flow:
acquire the daemon, call the tool over MCP, unwrap the structured
result, and render it. :func:`call_for_payload` is the fetch half,
:func:`~deephaven_mcp.cli._echo.echo_payload` the render half, and
:func:`call_and_echo` their composition for the common whole-payload
case. These helpers centralize the flow so each wrapper stays a thin,
declarative mapping of flags to tool arguments. The companion
``ref-cli-tool-wrapping`` skill and ``docs/design/CLI_TOOL_WRAPPING.md``
describe the wrapper categories that build on this module.
"""

from __future__ import annotations

__all__ = [
    "TABULAR_OUTPUT_FIELDS",
    "TABULAR_OUTPUT_NOTE",
    "acquire",
    "call_and_echo",
    "call_and_echo_field",
    "call_and_echo_table",
    "call_for_payload",
    "call_tool",
    "parse_key_value",
    "read_local_script",
    "require_success",
    "tool_payload",
    "wrapper_error_codes",
    "yes_option",
]

import json
import sys
from collections.abc import Collection
from pathlib import Path
from typing import Any

import click
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli._commands._acquire import (
    acquire_daemon,
    registry_corrupt_message,
)
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, render_warning
from deephaven_mcp.cli._help import OutputField
from deephaven_mcp.cli._mcp_client import (
    McpClient,
    McpClientError,
    McpRequestTimeoutError,
)
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.tree import no_systems_configured_message
from deephaven_mcp.daemon_registry import DaemonRegistryEntry

_BOOKKEEPING_KEYS = frozenset({"success", "isError"})
"""Envelope keys stripped from a tool payload before it is rendered."""

TABULAR_OUTPUT_FIELDS: tuple[OutputField, ...] = (
    OutputField("id", "string", "The session id, echoed back."),
    OutputField("table_name", "string", "The table read, echoed back."),
    OutputField(
        "format",
        "string",
        "Serialization of 'data'; always 'json-row', which these commands "
        "request so 'data' is a JSON array of row objects.",
    ),
    OutputField(
        "schema",
        "array",
        "One entry per column: name and type (PyArrow type name, e.g. "
        "'int64', 'timestamp[ns]'). Read this before parsing 'data'.",
    ),
    OutputField("row_count", "integer", "Number of rows in 'data'."),
    OutputField(
        "is_complete",
        "boolean",
        "False when the row cap truncated the result — the table holds more "
        "rows than were returned. Check this before concluding anything "
        "about the table as a whole.",
    ),
    OutputField(
        "data", "array", "The rows: one JSON object per row, keyed by column name."
    ),
)
"""Output fields of the tabular tools, shared by the commands that render one
envelope through :func:`call_and_echo_table`.

Describing the envelope once keeps those help surfaces from drifting from
each other and from the tools.
"""

TABULAR_OUTPUT_NOTE = (
    "Keys are emitted in reading order (identity, format, schema, counts, "
    "data) rather than sorted. In -o human the 'format' field is dropped as "
    "noise and 'data' is redrawn as an aligned table; -o json / -o yaml keep "
    "the full envelope."
)
"""Rendering note shared by the tabular verbs' output specs."""

_ACQUIRE_ERROR_CODES: tuple[ErrorCode, ...] = (
    ErrorCode.NO_SYSTEMS_CONFIGURED,
    ErrorCode.DAEMON_NOT_RUNNING,
    ErrorCode.DAEMON_STARTUP_TIMEOUT,
    ErrorCode.DAEMON_REGISTRY_CORRUPT,
    ErrorCode.DAEMON_REUSE_REFUSED,
    ErrorCode.MCP_REQUEST_FAILED,
    ErrorCode.MCP_REQUEST_TIMEOUT,
)
"""Error codes the shared acquire + tool-call flow can raise."""


def reveal_secrets_option(f: Any) -> Any:
    """Attach the shared ``--reveal-secrets`` option to a command.

    One flag spelling and one wording for every verb that can emit a
    plaintext secret but is not *for* emitting one, so the opt-in cannot
    drift between them. A verb whose entire output is a credential
    (``session credentials``, ``session url``) takes no such flag: there
    the disclosure is the request, gated by
    ``community.settings.security.credential_retrieval_mode`` instead.
    """
    return click.option(
        "--reveal-secrets",
        "reveal_secrets",
        is_flag=True,
        default=False,
        help=(
            "Include plaintext secret values in the output. Without this "
            "flag each command withholds them in whatever way suits its "
            "output: 'config get' prints [REDACTED], 'session open' "
            "reports the URL without its auth token. Treat the result "
            "like a password."
        ),
    )(f)


def yes_option(f: Any) -> Any:
    """Attach the shared ``--yes`` option to a destructive command.

    Skips the confirmation that
    :func:`~deephaven_mcp.cli._context.require_context_target` asks when
    the target came from the sticky context and ``cli.json``'s
    ``context.confirm_destructive`` is enabled. Shared so the six
    destructive verbs cannot drift in wording.

    The help states that the confirmation is skipped for a
    non-interactive caller, because a machine caller never sees the
    prompt and so must not treat it as a guard: an agent reading only
    "a confirmation is asked" would conclude a wrong target gets caught.
    """
    return click.option(
        "--yes",
        "yes",
        is_flag=True,
        default=False,
        help=(
            "Skip the confirmation asked when the target comes from the "
            "sticky context (only asked when the configured "
            "cli.context.confirm_destructive is true). The confirmation is "
            "skipped anyway when prompting is unavailable — stdin is not a "
            "TTY, or --no-input was given — so a non-interactive caller is "
            "never protected by it and must pass the target explicitly."
        ),
    )(f)


def wrapper_error_codes(
    *, tool_error: bool = True, request_timeout: bool = True, no_systems: bool = True
) -> tuple[ErrorCode, ...]:
    """Return the error codes a tool-wrapping command surfaces in its help.

    Args:
        tool_error (bool): Whether to include
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.TOOL_RETURNED_ERROR`
            (exit 3). Pass ``False`` for a wrapper whose tool never reports
            failure (e.g. ``system list``).
        request_timeout (bool): Whether to include
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.MCP_REQUEST_TIMEOUT`.
            Pass ``False`` for a verb that only lists tools (``tool list`` /
            ``tool show``): ``list_tools`` applies no per-request read
            timeout, so the timeout code is unreachable there.
        no_systems (bool): Whether to include
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.NO_SYSTEMS_CONFIGURED`.
            Pass ``False`` for the discovery verbs that short-circuit to an
            empty list on a zero-system tree (``system list`` /
            ``session list``, via ``empty_on_no_systems``): they never reach
            the ``acquire`` guard that raises it.

    Returns:
        tuple[ErrorCode, ...]: ``TOOL_RETURNED_ERROR`` (when ``tool_error``)
            followed by the error codes the shared acquire + tool-call flow
            can raise.
    """
    codes = _ACQUIRE_ERROR_CODES
    if not request_timeout:
        codes = tuple(ec for ec in codes if ec is not ErrorCode.MCP_REQUEST_TIMEOUT)
    if not no_systems:
        codes = tuple(ec for ec in codes if ec is not ErrorCode.NO_SYSTEMS_CONFIGURED)
    if tool_error:
        return (ErrorCode.TOOL_RETURNED_ERROR, *codes)
    return codes


async def acquire(runtime: Runtime, *, retry_command: str) -> DaemonRegistryEntry:
    """Acquire a live daemon for a tool-wrapping command.

    Args:
        runtime (Runtime): The active CLI runtime.
        retry_command (str): The command rendered into the
            corrupt-registry recovery hint for the operator to re-run.

    Returns:
        DaemonRegistryEntry: The validated registry entry for the
            running daemon.

    Raises:
        CliError: ``no_systems_configured`` when the loaded tree declares
            no servable system (the daemon serves systems, so acquiring
            one is refused up front), or when the daemon cannot be
            acquired (startup timeout, client error, or corrupt registry).
    """
    if not runtime.config.has_usable_system():
        raise CliError(
            no_systems_configured_message(runtime.config_dir),
            code=ErrorCode.NO_SYSTEMS_CONFIGURED,
        )
    return await acquire_daemon(
        runtime,
        auto_start=runtime.config.cli.daemon.auto_start,
        client_error_code=ErrorCode.DAEMON_NOT_RUNNING,
        on_registry_corrupt=lambda exc: CliError(
            registry_corrupt_message(exc, retry_command=retry_command),
            code=ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ),
    )


async def call_tool(
    handle: DaemonRegistryEntry,
    runtime: Runtime,
    name: str,
    arguments: dict[str, Any],
) -> CallToolResult:
    """Invoke one MCP tool over a fresh client connection.

    Args:
        handle (DaemonRegistryEntry): The acquired daemon handle.
        runtime (Runtime): The active CLI runtime, for the request timeout.
        name (str): The MCP tool name to invoke.
        arguments (dict[str, Any]): The tool arguments.

    Returns:
        CallToolResult: The raw MCP call result.

    Raises:
        CliError: When the MCP transport reports an error —
            ``mcp_request_timeout`` for a request timeout,
            ``mcp_request_failed`` for everything else.
    """
    try:
        async with McpClient.for_daemon(
            handle,
            request_timeout_seconds=runtime.config.cli.request.timeouts.default_seconds,
        ) as client:
            return await client.call_tool(name, arguments)
    except McpRequestTimeoutError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_TIMEOUT) from exc
    except McpClientError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_FAILED) from exc


def tool_payload(result: CallToolResult) -> dict[str, Any]:
    """Extract the JSON dict payload from a tool result.

    Prefers the structured result; falls back to parsing the first
    JSON object text block when the server does not populate one.

    Args:
        result (CallToolResult): The raw MCP call result.

    Returns:
        dict[str, Any]: The decoded payload dict.

    Raises:
        CliError: When no structured dict payload can be recovered.
    """
    if isinstance(result.structuredContent, dict):
        return result.structuredContent
    for block in result.content:
        if isinstance(block, TextContent):
            try:
                parsed = json.loads(block.text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
    raise CliError(
        "MCP tool returned no structured result.",
        code=ErrorCode.MCP_REQUEST_FAILED,
    )


def parse_key_value(raw: str, *, decode_json: bool) -> tuple[str, Any]:
    """Parse a ``key=value`` option token into a ``(key, value)`` pair.

    Used by repeatable dict-valued options. With ``decode_json`` the value
    is JSON-decoded when possible (so ``n=42`` yields the integer ``42``),
    falling back to the raw string; without it the value stays a string.

    Args:
        raw (str): The raw ``key=value`` token.
        decode_json (bool): Whether to JSON-decode the value.

    Returns:
        tuple[str, Any]: The parsed key and value.

    Raises:
        CliError: With :attr:`ErrorCode.ARG_PARSE_ERROR` when the token has
            no ``=`` or an empty key.
    """
    if "=" not in raw:
        raise CliError(
            f"Expected key=value; got {raw!r}.", code=ErrorCode.ARG_PARSE_ERROR
        )
    key, _, value = raw.partition("=")
    if not key:
        raise CliError(f"Empty key in {raw!r}.", code=ErrorCode.ARG_PARSE_ERROR)
    if not decode_json:
        return key, value
    try:
        return key, json.loads(value)
    except json.JSONDecodeError:
        return key, value


def read_local_script(script_path: str) -> str:
    """Read a script source from a local file or standard input.

    The locality contract for CLI path flags: a local script file is read
    by the CLI itself — a relative path resolves against the invoking
    shell's working directory, never the daemon's — and forwarded to the
    tool as inline source. ``"-"`` reads standard input instead. A
    leading ``~`` is expanded via :meth:`Path.expanduser`, matching the
    ``--config-dir`` / ``--runtime-dir`` convention, so a quoted
    ``'~/job.py'`` works.

    Args:
        script_path (str): Path to a local script file (``~`` expanded),
            or ``"-"`` for standard input.

    Returns:
        str: The script source text.

    Raises:
        CliError: With :attr:`ErrorCode.MISSING_ARGUMENT` when stdin is
            selected but empty, or :attr:`ErrorCode.FILE_READ_FAILED` when
            the file cannot be read or is not valid UTF-8.
    """
    if script_path == "-":
        script = sys.stdin.read()
        if not script:
            raise CliError(
                "Standard input was empty; provide the script on stdin "
                "or pass a file path.",
                code=ErrorCode.MISSING_ARGUMENT,
            )
        return script
    try:
        return Path(script_path).expanduser().read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise CliError(
            f"Could not read script file {script_path!r}: {exc}",
            code=ErrorCode.FILE_READ_FAILED,
        ) from exc


def require_success(payload: dict[str, Any], *, tool: str) -> dict[str, Any]:
    """Return the payload's content, or raise an exit-3 error on failure.

    Deephaven MCP tools wrap their result in a ``success`` flag (and an
    optional ``error`` message / ``isError`` marker). This converts a
    ``success=False`` payload into a :class:`CliError` carrying
    :attr:`ErrorCode.TOOL_RETURNED_ERROR` (exit 3) and otherwise returns
    the payload with the envelope bookkeeping keys removed.

    Args:
        payload (dict[str, Any]): The decoded tool payload.
        tool (str): The tool name, used in the fallback error message.

    Returns:
        dict[str, Any]: ``payload`` without the ``success`` / ``isError``
            keys.

    Raises:
        CliError: With :attr:`ErrorCode.TOOL_RETURNED_ERROR` when
            ``payload`` does not report success.
    """
    if not payload.get("success", False):
        raise CliError(
            str(payload.get("error") or f"{tool!r} reported failure."),
            code=ErrorCode.TOOL_RETURNED_ERROR,
        )
    return {k: v for k, v in payload.items() if k not in _BOOKKEEPING_KEYS}


async def call_for_payload(
    runtime: Runtime, tool: str, *, retry_command: str, arguments: dict[str, Any]
) -> dict[str, Any]:
    """Acquire the daemon, invoke ``tool``, and return its success payload.

    Composes :func:`acquire`, :func:`call_tool`, :func:`tool_payload`, and
    :func:`require_success` into the fetch half of the wrapper flow. Use this
    when the command shapes the payload before rendering; otherwise use
    :func:`call_and_echo`.

    Args:
        runtime (Runtime): The active CLI runtime.
        tool (str): The MCP tool name to invoke.
        retry_command (str): The command rendered into the corrupt-registry
            recovery hint.
        arguments (dict[str, Any]): The tool arguments.

    Returns:
        dict[str, Any]: The tool payload, envelope bookkeeping keys removed.

    Raises:
        CliError: When the daemon cannot be acquired, the MCP transport
            fails, or the tool reports ``success=False`` (exit 3).
    """
    handle = await acquire(runtime, retry_command=retry_command)
    result = await call_tool(handle, runtime, tool, arguments)
    return require_success(tool_payload(result), tool=tool)


async def call_and_echo(
    runtime: Runtime,
    tool: str,
    *,
    retry_command: str,
    arguments: dict[str, Any],
    sort_keys: bool = True,
    human_exclude: Collection[str] = (),
) -> None:
    """Acquire, invoke ``tool``, and print its whole success payload.

    The common case: :func:`call_for_payload` followed by :func:`echo_payload`.
    Use :func:`call_for_payload` directly when the command shapes the payload
    before rendering.

    Args:
        runtime (Runtime): The active CLI runtime.
        tool (str): The MCP tool name to invoke.
        retry_command (str): The command rendered into the corrupt-registry
            recovery hint.
        arguments (dict[str, Any]): The tool arguments.
        sort_keys (bool): Forwarded to :func:`echo_payload`. Pass ``False`` for
            payloads whose key order is meaningful.
        human_exclude (Collection[str]): Forwarded to :func:`echo_payload`. Keys
            dropped from a dict payload in ``human`` mode only.

    Raises:
        CliError: When the daemon cannot be acquired, the MCP transport
            fails, or the tool reports ``success=False`` (exit 3).
    """
    payload = await call_for_payload(
        runtime, tool, retry_command=retry_command, arguments=arguments
    )
    echo_payload(runtime, payload, sort_keys=sort_keys, human_exclude=human_exclude)


async def call_and_echo_table(
    runtime: Runtime, tool: str, *, retry_command: str, arguments: dict[str, Any]
) -> None:
    """Acquire, invoke a tabular ``tool``, and print its envelope in reading order.

    The tools already emit their envelope keys in reading order (identity,
    summary, ``format``, schema, data); this echoes them with ``sort_keys=False``
    so ``json``/``yaml`` preserve that order instead of alphabetizing. In
    ``human`` mode two keys are dropped (via ``human_exclude``) as noise to a
    terminal reader:

    - ``format``, which always reports ``json-row`` (the serialization these
      commands request so the human renderer can re-draw ``data`` as an
      aligned table). Every current caller emits it.
    - ``columns``, a bare column-name list that would merely restate the
      headers of the rendered ``data`` table. Defensive: the tool routed
      through this helper today (``session_table_data``) describes its
      columns under ``schema``, which
      carries types and is left intact; the exclusion covers a tabular tool
      that reports the thinner key instead.

    ``json``/``yaml`` keep both keys for machine consumers. The commands
    describe this envelope to users through :data:`TABULAR_OUTPUT_FIELDS` and
    :data:`TABULAR_OUTPUT_NOTE`.

    Args:
        runtime (Runtime): The active CLI runtime.
        tool (str): The MCP tool name to invoke.
        retry_command (str): The command rendered into the corrupt-registry
            recovery hint.
        arguments (dict[str, Any]): The tool arguments.

    Raises:
        CliError: When the daemon cannot be acquired, the MCP transport
            fails, or the tool reports ``success=False`` (exit 3).
    """
    await call_and_echo(
        runtime,
        tool,
        retry_command=retry_command,
        arguments=arguments,
        sort_keys=False,
        human_exclude=("format", "columns"),
    )


def _warn_if_incomplete(
    runtime: Runtime,
    payload: dict[str, Any],
    *,
    reasons_in_rows: bool = False,
    truncation_hint: str | None = None,
) -> None:
    """Warn on stderr when a successful result is flagged incomplete.

    Two incompleteness signals are recognized:

    - ``is_complete: false`` — a truncating tool (e.g.
      ``catalog_namespaces_list``) trimmed the result to a caller-chosen row
      cap. Warns with a generic truncation message, extended by
      ``truncation_hint`` when the verb names its own recovery flags.
    - ``partial_result`` — discovery tools (e.g. ``sessions_list``) attach
      this block only when the result may be missing data — discovery is
      still running, or a system failed to connect. Set by
      ``format_partial_result`` in ``mcp_systems_server/_tools/shared.py``,
      the block is ``{"phase": <discovery phase>, "detail": <message>,
      "errors": {<system>: <message>}}`` (``errors`` only on failures), and
      the key is absent when the result is complete. A shaping verb prints
      one field and drops the rest, so this re-emits the block's
      human-readable ``detail`` — and the per-system ``errors`` map — as a
      stderr warning.

    Args:
        runtime (Runtime): The active CLI runtime.
        payload (dict[str, Any]): The full tool response.
        reasons_in_rows (bool, optional): Set ``True`` when the printed rows
            already attribute each per-system reason (e.g. ``system status``
            promotes the exception type into ``liveness_detail``). A
            ``"completed"`` ``partial_result`` is then suppressed entirely — its
            phase ``detail`` ("had connection issues") would only restate the
            table. ``"loading"`` / ``"failed"`` phases still warn and still
            carry the ``errors`` map, because the rows show only the short
            reason and the full message would otherwise be unreachable.
        truncation_hint (str | None, optional): Sentence appended to the
            ``is_complete: false`` warning naming the verb's recovery flags
            (e.g. ``"Raise --max-rows or narrow with --filter."``).
    """
    if payload.get("is_complete") is False:
        message = "Result truncated by the row cap."
        if truncation_hint:
            message = f"{message} {truncation_hint}"
        render_warning(message, output=runtime.config.cli.output.format)
    incomplete = payload.get("partial_result")
    if incomplete is None:
        return
    if reasons_in_rows and incomplete.get("phase") == "completed":
        # Reasons are attributed per-row; the only remaining signal would be
        # the "had connection issues" detail, which restates the table.
        return
    render_warning(
        incomplete["detail"],
        output=runtime.config.cli.output.format,
        details=incomplete.get("errors"),
    )


async def call_and_echo_field(
    runtime: Runtime,
    tool: str,
    *,
    retry_command: str,
    arguments: dict[str, Any],
    field: str,
    default: Any,
    reasons_in_rows: bool = False,
    truncation_hint: str | None = None,
    empty_on_no_systems: bool = False,
) -> None:
    """Acquire, invoke ``tool``, surface diagnostics, and print ``payload[field]``.

    The shaping counterpart to :func:`call_and_echo`: emits a single payload
    field (e.g. a ``list`` verb's array) on stdout while warning on stderr if the
    result is incomplete (:func:`_warn_if_incomplete`, which recognizes both
    ``partial_result`` and ``is_complete: false``) — so shaping never silently
    drops partial-result or truncation information.

    Args:
        runtime (Runtime): The active CLI runtime.
        tool (str): The MCP tool name to invoke.
        retry_command (str): The command rendered into the corrupt-registry
            recovery hint.
        arguments (dict[str, Any]): The tool arguments.
        field (str): The payload key to emit on stdout.
        default (Any): The value emitted when ``field`` is absent, and the value
            emitted by the ``empty_on_no_systems`` short-circuit.
        reasons_in_rows (bool, optional): Forwarded to
            :func:`_warn_if_incomplete`. Set ``True`` when the emitted rows
            already carry per-system error reasons, so a ``"completed"``
            warning doesn't restate them.
        truncation_hint (str | None, optional): Forwarded to
            :func:`_warn_if_incomplete`. Sentence appended to the
            ``is_complete: false`` warning naming the verb's recovery flags.
        empty_on_no_systems (bool, optional): For discovery verbs whose result
            is provably empty on a zero-system tree (``system list``,
            ``session list``). When ``True`` and the loaded tree declares no
            servable system, emit ``default`` (an empty list) on stdout with a
            ``no_systems_configured`` guidance warning on stderr and return —
            without acquiring the daemon. Friendlier than the ``acquire``
            guard's hard error for these enumeration entry points; every other
            verb keeps the guard's error.

    Raises:
        CliError: When the daemon cannot be acquired, the MCP transport
            fails, or the tool reports ``success=False`` (exit 3).
    """
    if empty_on_no_systems and not runtime.config.has_usable_system():
        render_warning(
            no_systems_configured_message(runtime.config_dir),
            output=runtime.config.cli.output.format,
        )
        echo_payload(runtime, default)
        return
    payload = await call_for_payload(
        runtime, tool, retry_command=retry_command, arguments=arguments
    )
    _warn_if_incomplete(
        runtime,
        payload,
        reasons_in_rows=reasons_in_rows,
        truncation_hint=truncation_hint,
    )
    echo_payload(runtime, payload.get(field, default))
