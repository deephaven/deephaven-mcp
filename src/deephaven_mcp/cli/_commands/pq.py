"""``dh-mcp pq`` noun group: manage Enterprise (Core+) Persistent Queries.

Verbs: ``list``, ``details``, ``name-to-id``, ``create``, ``modify``,
``delete``, ``start``, ``stop``, ``restart``.

Enterprise (Core+) only. Persistent Queries are addressed by their fully
qualified id ``enterprise:<system>:<serial>`` — the same id the session
verbs use; use ``name-to-id`` to resolve a human name within a system.
"""

from __future__ import annotations

__all__ = ["pq"]

from collections.abc import Callable
from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
    read_local_script,
    wrapper_error_codes,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpfulGroup,
    OutputField,
    OutputSpec,
    build_help,
)
from deephaven_mcp.cli._runtime import Runtime

_OUTPUT_OBJECT = OutputSpec(
    "object", (), note="The tool's result envelope (PQ fields / batch results)."
)


@click.group(cls=HelpfulGroup)
def pq() -> None:
    """Manage Enterprise (Core+) Persistent Queries.

    Enterprise (Core+) only. Inspect with 'list', 'details', and
    'name-to-id'; manage with 'create', 'modify', 'delete'; control the
    lifecycle with 'start', 'stop', 'restart'. PQs are addressed by their
    fully qualified id 'enterprise:system:serial' — the same id the
    session verbs use. These commands auto-start the daemon unless
    --no-auto-start is set.
    """


# ---------------------------------------------------------------------------
# list / details / name-to-id
# ---------------------------------------------------------------------------


_OUTPUT_LIST = OutputSpec(
    "list",
    (
        OutputField("id", "string", "Fully qualified id 'enterprise:system:serial'."),
        OutputField("serial", "integer", "PQ serial number."),
        OutputField("name", "string", "Human-readable PQ name."),
        OutputField("status", "string", "PQ state (e.g. 'RUNNING', 'STOPPED')."),
        OutputField(
            "status_category",
            "string",
            "'ACTIVE', 'TRANSITIONAL', 'TERMINAL', or 'INVALID'.",
        ),
        OutputField("owner", "string", "Owning user."),
        OutputField("enabled", "boolean", "Whether the PQ is enabled."),
    ),
    note="Array of PQ summaries (extra per-PQ fields may be present).",
)


@pq.command(
    "list",
    output_spec=_OUTPUT_LIST,
    wraps_tool="pq_list",
    help=build_help(
        summary="List Persistent Queries on a system.",
        description=(
            "Enterprise (Core+) only. Lists the PQs configured on SYSTEM. Use "
            "a returned id verbatim with the other pq and session verbs."
        ),
        arguments=(HelpEntry("SYSTEM", "Enterprise system name. Run 'system list'."),),
        output=_OUTPUT_LIST,
        examples=(
            "$ dh-mcp pq list prod",
            "$ dh-mcp pq list prod | jq '.[].id'",
        ),
        see_also=("dh-mcp pq details ID", "dh-mcp pq name-to-id SYSTEM NAME"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("system")
@click.pass_obj
@run_async
async def pq_list(runtime: Runtime, system: str) -> None:
    """List Persistent Queries on a system."""
    await call_and_echo_field(
        runtime,
        "pq_list",
        retry_command="dh-mcp pq list",
        arguments={"system": system},
        field="pqs",
        default=[],
    )


@pq.command(
    "details",
    output_spec=_OUTPUT_OBJECT,
    wraps_tool="pq_details",
    help=build_help(
        summary="Show details for one Persistent Query.",
        description="Enterprise (Core+) only. Reports configuration and status for ID.",
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified PQ id 'enterprise:system:serial'. "
                "Run 'pq list' or 'pq name-to-id'.",
            ),
        ),
        output=_OUTPUT_OBJECT,
        examples=("$ dh-mcp pq details enterprise:prod:1234567890",),
        see_also=("dh-mcp pq list SYSTEM",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.pass_obj
@run_async
async def pq_details(runtime: Runtime, id: str) -> None:
    """Show details for one Persistent Query."""
    await call_and_echo(
        runtime,
        "pq_details",
        retry_command="dh-mcp pq details",
        arguments={"id": id},
    )


@pq.command(
    "name-to-id",
    output_spec=_OUTPUT_OBJECT,
    wraps_tool="pq_name_to_id",
    help=build_help(
        summary="Resolve a Persistent Query name to its fully qualified id.",
        description="Enterprise (Core+) only. Looks up PQ_NAME within SYSTEM.",
        arguments=(
            HelpEntry("SYSTEM", "Enterprise system name."),
            HelpEntry("PQ_NAME", "Human-readable PQ name."),
        ),
        output=_OUTPUT_OBJECT,
        examples=("$ dh-mcp pq name-to-id prod nightly-report",),
        see_also=("dh-mcp pq list SYSTEM",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("system")
@click.argument("pq_name")
@click.pass_obj
@run_async
async def pq_name_to_id(runtime: Runtime, system: str, pq_name: str) -> None:
    """Resolve a Persistent Query name to its serial id."""
    await call_and_echo(
        runtime,
        "pq_name_to_id",
        retry_command="dh-mcp pq name-to-id",
        arguments={"system": system, "pq_name": pq_name},
    )


# ---------------------------------------------------------------------------
# create / modify
# ---------------------------------------------------------------------------


def _norm(value: Any) -> Any:
    """Normalize a click value for an MCP argument (tuple → list)."""
    return list(value) if isinstance(value, tuple) else value


def _provided(value: Any) -> bool:
    """Whether a click option value was set (not ``None`` and not an empty tuple)."""
    return value is not None and value != ()


def _create_modify_args(params: dict[str, Any]) -> dict[str, Any]:
    """Build the MCP argument dict from a create/modify command's params.

    Drops unset options (``None`` / empty tuple) so the tool's own
    defaults apply, and converts repeatable-option tuples to lists. Every
    parameter maps directly to a tool argument except ``script_body_path``,
    which is materialized client-side: the CLI reads the local file (or
    stdin for ``'-'``) and forwards its contents as ``script_body``.
    """
    args = {name: _norm(value) for name, value in params.items() if _provided(value)}
    script_body_path = args.pop("script_body_path", None)
    if script_body_path is not None:
        args["script_body"] = read_local_script(script_body_path)
    return args


# Option pairs the controller rejects when combined, as
# ``(flag_a, param_a, flag_b, param_b)``.
_MUTUALLY_EXCLUSIVE: tuple[tuple[str, str, str, str], ...] = (
    ("--script-body", "script_body", "--git-script-path", "script_path"),
    ("--script-body", "script_body", "--script-body-path", "script_body_path"),
    ("--script-body-path", "script_body_path", "--git-script-path", "script_path"),
    ("--auto-delete-timeout", "auto_delete_timeout", "--schedule", "schedule"),
)


def _check_mutually_exclusive(params: dict[str, Any]) -> None:
    """Reject create/modify options the controller forbids in combination.

    Raises:
        CliError: With :attr:`ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS` when both
            members of a mutually exclusive pair are supplied.
    """
    for flag_a, key_a, flag_b, key_b in _MUTUALLY_EXCLUSIVE:
        if _provided(params.get(key_a)) and _provided(params.get(key_b)):
            raise CliError(
                f"{flag_a} and {flag_b} cannot be combined; supply at most one.",
                code=ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
            )


def _create_modify_options(f: Callable[..., Any]) -> Callable[..., Any]:
    """Apply the options shared by ``pq create`` and ``pq modify``."""
    options = (
        click.option(
            "--script-body",
            "script_body",
            default=None,
            help="Inline PQ script source, stored in the PQ definition.",
        ),
        click.option(
            "--script-body-path",
            "script_body_path",
            default=None,
            help=(
                "Local script file read by the CLI and stored as the PQ's "
                "inline body, or '-' to read stdin."
            ),
        ),
        click.option(
            "--git-script-path",
            "script_path",
            default=None,
            help=(
                "Path to a script in the Enterprise controller's Git-backed "
                "script repository, resolved on the server; not a local file."
            ),
        ),
        click.option(
            "--language",
            "programming_language",
            type=click.Choice(["Python", "Groovy"], case_sensitive=False),
            default=None,
            help="Script language.",
        ),
        click.option(
            "--configuration-type",
            "configuration_type",
            type=click.Choice(["Script", "RunAndDone"]),
            default=None,
            help="PQ configuration type.",
        ),
        click.option(
            "--schedule",
            "schedule",
            multiple=True,
            help=(
                "Schedule entry (repeatable). Omitting it leaves the schedule "
                "unchanged; clearing a schedule is not supported via the CLI."
            ),
        ),
        click.option("--server", "server", default=None, help="Server pool name."),
        click.option("--engine", "engine", default=None, help="Worker engine."),
        click.option(
            "--jvm-profile", "jvm_profile", default=None, help="JVM profile name."
        ),
        click.option(
            "--jvm-arg",
            "extra_jvm_args",
            multiple=True,
            help="Extra JVM arg (repeatable).",
        ),
        click.option(
            "--class-path",
            "extra_class_path",
            multiple=True,
            help="Extra class-path entry on the Enterprise server (repeatable).",
        ),
        click.option(
            "--python-venv",
            "python_virtual_environment",
            default=None,
            help="Name of a Python virtualenv configured on the Enterprise server.",
        ),
        click.option(
            "--env",
            "extra_environment_vars",
            multiple=True,
            metavar="KEY=VALUE",
            help="Worker environment variable as KEY=VALUE (repeatable).",
        ),
        click.option(
            "--init-timeout-nanos",
            "init_timeout_nanos",
            type=int,
            default=None,
            help="Init timeout (ns).",
        ),
        click.option(
            "--auto-delete-timeout",
            "auto_delete_timeout",
            type=int,
            default=None,
            help="Idle seconds before auto-delete.",
        ),
        click.option(
            "--admin-group",
            "admin_groups",
            multiple=True,
            help="Admin group (repeatable).",
        ),
        click.option(
            "--viewer-group",
            "viewer_groups",
            multiple=True,
            help="Viewer group (repeatable).",
        ),
        click.option(
            "--restart-users",
            "restart_users",
            default=None,
            help="Who may restart the PQ.",
        ),
        click.option(
            "--owner",
            "owner",
            default=None,
            help="PQ owner (defaults to the authenticated user).",
        ),
    )
    for option in reversed(options):
        f = option(f)
    return f


@pq.command(
    "create",
    output_spec=_OUTPUT_OBJECT,
    wraps_tool="pq_create",
    client_only_params=frozenset({"script_body_path"}),
    help=build_help(
        summary="Create a Persistent Query.",
        description=(
            "Enterprise (Core+) only. Creates a PQ named PQ_NAME on --system with "
            "--heap-size-gb of heap. Provide the script inline with --script-body, "
            "from a local file or stdin with --script-body-path (read by the CLI "
            "and stored as the inline body), or from the controller's Git script "
            "repository with --git-script-path (at most one source). "
            "--auto-delete-timeout and --schedule are mutually exclusive. Unset "
            "options use the controller's defaults."
        ),
        arguments=(HelpEntry("PQ_NAME", "Name for the new PQ."),),
        output=_OUTPUT_OBJECT,
        examples=(
            "$ dh-mcp pq create nightly --system prod --heap-size-gb 4 "
            "--script-body-path ./nightly.py",
            "$ dh-mcp pq create nightly --system prod --heap-size-gb 4 "
            "--git-script-path IrisQueries/py/nightly.py",
        ),
        see_also=("dh-mcp pq modify ID", "dh-mcp pq start ID"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
            ErrorCode.FILE_READ_FAILED,
            ErrorCode.MISSING_ARGUMENT,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("pq_name")
@click.option(
    "--system",
    "system",
    required=True,
    help="Enterprise system name to create the PQ on (see 'dh-mcp system list').",
)
@click.option(
    "--heap-size-gb",
    "heap_size_gb",
    type=float,
    required=True,
    help="JVM heap size (GB).",
)
@click.option(
    "--enabled/--disabled", "enabled", default=True, help="Whether the PQ is enabled."
)
@_create_modify_options
@click.pass_context
@run_async
async def pq_create(ctx: click.Context, **_options: Any) -> None:
    """Create a Persistent Query.

    Reads the create fields from ``ctx.params``; ``_options`` absorbs
    click's per-option keyword arguments.
    """
    runtime: Runtime = ctx.obj
    _check_mutually_exclusive(ctx.params)
    await call_and_echo(
        runtime,
        "pq_create",
        retry_command="dh-mcp pq create",
        arguments=_create_modify_args(ctx.params),
    )


@pq.command(
    "modify",
    output_spec=_OUTPUT_OBJECT,
    wraps_tool="pq_modify",
    client_only_params=frozenset({"script_body_path"}),
    help=build_help(
        summary="Modify an existing Persistent Query.",
        description=(
            "Enterprise (Core+) only. Updates only the fields you pass on ID; "
            "everything else is left unchanged. The script sources "
            "--script-body/--script-body-path/--git-script-path (and separately "
            "--auto-delete-timeout/--schedule) are mutually exclusive. Pass "
            "--restart to restart the PQ after applying the change."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified PQ id 'enterprise:system:serial'. "
                "Run 'pq list' or 'pq name-to-id'.",
            ),
        ),
        output=_OUTPUT_OBJECT,
        examples=(
            "$ dh-mcp pq modify enterprise:prod:1234567890 --heap-size-gb 8 --restart",
            "$ dh-mcp pq modify enterprise:prod:1234567890 --disabled",
        ),
        see_also=("dh-mcp pq details ID", "dh-mcp pq restart ID"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
            ErrorCode.FILE_READ_FAILED,
            ErrorCode.MISSING_ARGUMENT,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("id")
@click.option(
    "--restart",
    "restart",
    is_flag=True,
    default=False,
    help="Restart the PQ after modifying.",
)
@click.option("--pq-name", "pq_name", default=None, help="New PQ name.")
@click.option(
    "--heap-size-gb",
    "heap_size_gb",
    type=float,
    default=None,
    help="New JVM heap size (GB).",
)
@click.option(
    "--enabled/--disabled",
    "enabled",
    default=None,
    help="Enable or disable the PQ (unchanged if omitted).",
)
@_create_modify_options
@click.pass_context
@run_async
async def pq_modify(ctx: click.Context, **_options: Any) -> None:
    """Modify an existing Persistent Query.

    Reads the modify fields from ``ctx.params``; ``_options`` absorbs
    click's per-option keyword arguments.
    """
    runtime: Runtime = ctx.obj
    _check_mutually_exclusive(ctx.params)
    await call_and_echo(
        runtime,
        "pq_modify",
        retry_command="dh-mcp pq modify",
        arguments=_create_modify_args(ctx.params),
    )


# ---------------------------------------------------------------------------
# delete / start / stop / restart (batch lifecycle)
# ---------------------------------------------------------------------------


def _ids(id: tuple[str, ...]) -> list[str]:
    """Validate and return the PQ id list from a variadic argument."""
    if not id:
        raise CliError("At least one ID is required.", code=ErrorCode.MISSING_ARGUMENT)
    return list(id)


@pq.command(
    "delete",
    output_spec=_OUTPUT_OBJECT,
    wraps_tool="pq_delete",
    help=build_help(
        summary="Delete one or more Persistent Queries.",
        description=(
            "Enterprise (Core+) only. Deletes every ID given. --max-concurrent "
            "caps how many deletions run in parallel. Best-effort: exit 0 means the "
            "batch ran, not that every id succeeded — check the summary and per-item "
            "results for failures."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "One or more fully qualified PQ ids ('enterprise:system:serial').",
            ),
        ),
        output=_OUTPUT_OBJECT,
        examples=(
            "$ dh-mcp pq delete enterprise:prod:1234567890 enterprise:prod:1234567891",
        ),
        see_also=("dh-mcp pq list SYSTEM",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.MISSING_ARGUMENT, *wrapper_error_codes()),
    ),
)
@click.argument("id", nargs=-1)
@click.option(
    "--max-concurrent",
    "max_concurrent",
    type=int,
    default=None,
    help="Parallel-operation cap.",
)
@click.pass_obj
@run_async
async def pq_delete(
    runtime: Runtime, id: tuple[str, ...], max_concurrent: int | None
) -> None:
    """Delete one or more Persistent Queries."""
    arguments: dict[str, Any] = {"id": _ids(id)}
    if max_concurrent is not None:
        arguments["max_concurrent"] = max_concurrent
    await call_and_echo(
        runtime, "pq_delete", retry_command="dh-mcp pq delete", arguments=arguments
    )


def _lifecycle_command(name: str, summary: str, verb: str) -> Callable[..., Any]:
    """Build a start/stop/restart command (identical shape, distinct tool)."""
    tool = f"pq_{verb}"

    @pq.command(
        name,
        output_spec=_OUTPUT_OBJECT,
        wraps_tool=tool,
        help=build_help(
            summary=summary,
            description=(
                f"Enterprise (Core+) only. {summary} --no-wait returns without "
                "waiting for the state change; --max-concurrent caps parallelism "
                "across multiple ids. Best-effort: exit 0 means the batch ran, not "
                "that every id succeeded — check the per-item results for failures."
            ),
            arguments=(
                HelpEntry(
                    "ID",
                    "One or more fully qualified PQ ids "
                    "('enterprise:system:serial').",
                ),
            ),
            output=_OUTPUT_OBJECT,
            examples=(f"$ dh-mcp pq {name} enterprise:prod:1234567890",),
            see_also=("dh-mcp pq details ID",),
            exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
            error_codes=(ErrorCode.MISSING_ARGUMENT, *wrapper_error_codes()),
        ),
    )
    @click.argument("id", nargs=-1)
    @click.option(
        "--wait/--no-wait", "wait", default=True, help="Wait for the state change."
    )
    @click.option(
        "--max-concurrent",
        "max_concurrent",
        type=int,
        default=None,
        help="Parallel-operation cap.",
    )
    @click.pass_obj
    @run_async
    async def _cmd(
        runtime: Runtime, id: tuple[str, ...], wait: bool, max_concurrent: int | None
    ) -> None:
        arguments: dict[str, Any] = {"id": _ids(id), "wait": wait}
        if max_concurrent is not None:
            arguments["max_concurrent"] = max_concurrent
        await call_and_echo(
            runtime, tool, retry_command=f"dh-mcp pq {name}", arguments=arguments
        )

    return _cmd


pq_start = _lifecycle_command("start", "Start one or more Persistent Queries.", "start")
pq_stop = _lifecycle_command("stop", "Stop one or more Persistent Queries.", "stop")
pq_restart = _lifecycle_command(
    "restart", "Restart one or more Persistent Queries.", "restart"
)
