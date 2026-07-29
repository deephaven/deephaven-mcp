"""``dhcli pq`` noun group: manage Enterprise (Core+) Persistent Queries.

Verbs: ``list``, ``details``, ``name-to-id``, ``create``, ``modify``,
``delete``, ``start``, ``stop``, ``restart``.

Enterprise (Core+) only. Persistent Queries are addressed by their fully
qualified id ``enterprise:<system>:<serial>`` — the same id the session
verbs use; use ``name-to-id`` to resolve a human name within a system.
"""

from __future__ import annotations

__all__ = ["pq"]

from collections.abc import Callable, Mapping
from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
    call_for_payload,
    read_local_script,
    wrapper_error_codes,
    yes_option,
)
from deephaven_mcp.cli._context import (
    CONTEXT_HINT,
    CONTEXT_RISK_DESTRUCTIVE,
    CONTEXT_RISK_STATEFUL,
    ContextKey,
    clear_matching,
    require_context_target,
    require_context_value,
)
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
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
    note=(
        "Array of PQ summaries. Full configuration and state (heap size, "
        "worker kind, groups, scheduling, failure counts, ...) come from "
        "'pq details'."
    ),
)


@pq.command(
    "list",
    wraps_tool="pq_list",
    help_spec=HelpSpec(
        summary="List Persistent Queries on a system.",
        description=(
            "Enterprise (Core+) only. Lists the PQs configured on SYSTEM. Use "
            "a returned id verbatim with the other pq and session verbs."
        ),
        arguments=(
            HelpEntry(
                "SYSTEM",
                "Enterprise system name. Run 'system list'. Defaults to the "
                f"sticky context system if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dhcli pq list prod",
            "$ dhcli pq list prod | jq '.[].id'",
        ),
        see_also=(
            "dhcli pq details ID",
            "dhcli pq name-to-id SYSTEM NAME",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("system", required=False)
@click.pass_obj
@run_async
async def pq_list(runtime: Runtime, system: str | None) -> None:
    """List Persistent Queries on a system."""
    system = require_context_value(runtime, ContextKey.SYSTEM, system)
    await call_and_echo_field(
        runtime,
        "pq_list",
        retry_command="dhcli pq list",
        arguments={"system": system},
        field="pqs",
        default=[],
    )


@pq.command(
    "details",
    wraps_tool="pq_details",
    help_spec=HelpSpec(
        summary="Show details for one Persistent Query.",
        description="Enterprise (Core+) only. Reports configuration and status for ID.",
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified PQ id 'enterprise:system:serial'. "
                "Run 'pq list' or 'pq name-to-id'. Defaults to the sticky "
                f"context pq if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_OBJECT,
        examples=("$ dhcli pq details enterprise:prod:1234567890",),
        see_also=("dhcli pq list SYSTEM", "dhcli context show"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.pass_obj
@run_async
async def pq_details(runtime: Runtime, id: str | None) -> None:
    """Show details for one Persistent Query."""
    id = require_context_value(runtime, ContextKey.PQ, id)
    await call_and_echo(
        runtime,
        "pq_details",
        retry_command="dhcli pq details",
        arguments={"id": id},
    )


@pq.command(
    "name-to-id",
    wraps_tool="pq_name_to_id",
    help_spec=HelpSpec(
        summary="Resolve a Persistent Query name to its fully qualified id.",
        description="Enterprise (Core+) only. Looks up PQ_NAME within SYSTEM.",
        arguments=(
            HelpEntry("SYSTEM", "Enterprise system name."),
            HelpEntry("PQ_NAME", "Human-readable PQ name."),
        ),
        output=_OUTPUT_OBJECT,
        examples=("$ dhcli pq name-to-id prod nightly-report",),
        see_also=("dhcli pq list SYSTEM",),
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
        retry_command="dhcli pq name-to-id",
        arguments={"system": system, "pq_name": pq_name},
    )


# ---------------------------------------------------------------------------
# create / modify
# ---------------------------------------------------------------------------


def _as_json_value(value: Any) -> Any:
    """Normalize a click value for an MCP argument (tuple → list)."""
    return list(value) if isinstance(value, tuple) else value


def _provided(value: Any) -> bool:
    """Whether a click option value was set (not ``None`` and not an empty tuple)."""
    return value is not None and value != ()


def _create_modify_args(params: Mapping[str, Any]) -> dict[str, Any]:
    """Build the MCP argument dict from a create/modify command's params.

    Drops unset options (``None`` / empty tuple) so the tool's own
    defaults apply, and converts repeatable-option tuples to lists.

    ``script_body_path`` is the one client-only field that can reach here:
    it is consumed rather than forwarded, so this function both reads the
    local file (or stdin for ``'-'``) into ``script_body`` and removes the
    original. The command's other ``client_only_params`` (``no_set_context``,
    ``yes``) are named parameters of the callback, so they never enter its
    ``**options`` and cannot reach this dict at all.

    Args:
        params (Mapping[str, Any]): The command's pass-through parameter
            values, with any client-side resolution (sticky-context id,
            system) already applied by the caller.

    Returns:
        dict[str, Any]: The arguments to send to ``pq_create`` /
            ``pq_modify``.
    """
    args = {
        name: _as_json_value(value)
        for name, value in params.items()
        if _provided(value)
    }
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
    wraps_tool="pq_create",
    client_only_params=frozenset({"script_body_path", "no_set_context"}),
    help_spec=HelpSpec(
        summary="Create a Persistent Query.",
        description=(
            "Enterprise (Core+) only. Creates a PQ named PQ_NAME on --system with "
            "--heap-size-gb of heap. Provide the script inline with --script-body, "
            "from a local file or stdin with --script-body-path (read by the CLI "
            "and stored as the inline body), or from the controller's Git script "
            "repository with --git-script-path (at most one source). "
            "--auto-delete-timeout and --schedule are mutually exclusive. Unset "
            "options use the controller's defaults. " + CONTEXT_RISK_STATEFUL
        ),
        arguments=(HelpEntry("PQ_NAME", "Name for the new PQ."),),
        output=OutputSpec(
            _OUTPUT_OBJECT.mode,
            (
                OutputField(
                    "context",
                    "object",
                    "Present when the sticky context was updated: the keys "
                    "set and their new values.",
                ),
            ),
            note=_OUTPUT_OBJECT.note,
        ),
        examples=(
            "$ dhcli pq create nightly --system prod --heap-size-gb 4 "
            "--script-body-path ./nightly.py",
            "$ dhcli pq create nightly --system prod --heap-size-gb 4 "
            "--git-script-path IrisQueries/py/nightly.py",
        ),
        see_also=(
            "dhcli pq modify ID",
            "dhcli pq start ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
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
    default=None,
    help=(
        "Enterprise system name to create the PQ on (see 'dhcli system "
        f"list'). Defaults to the sticky context system if set. {CONTEXT_HINT}"
    ),
)
@click.option(
    "--no-set-context",
    "no_set_context",
    is_flag=True,
    default=False,
    help=(
        "Do not update the sticky context on success. Without this flag a "
        "successful create sets both the 'pq' and 'system' keys. This "
        "governs only the write; --no-context governs whether an omitted "
        "id reads from the sticky context."
    ),
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
async def pq_create(
    ctx: click.Context,
    *,
    system: str | None,
    no_set_context: bool,
    **options: Any,
) -> None:
    """Create a Persistent Query.

    ``system`` and ``no_set_context`` are named because this body reasons
    about them; ``options`` carries the remaining create fields, which
    pass through to the tool unread.
    """
    runtime: Runtime = ctx.obj
    if system is None:
        system = require_context_value(runtime, ContextKey.SYSTEM, None)
    _check_mutually_exclusive(options)
    payload = await call_for_payload(
        runtime,
        "pq_create",
        retry_command="dhcli pq create",
        arguments=_create_modify_args({**options, "system": system}),
    )
    new_id = payload.get("id")
    if not no_set_context and new_id:
        updates = {
            ContextKey.PQ: new_id,
            ContextKey.SYSTEM: system,
        }
        runtime.context_store.set_many(updates)
        payload = {**payload, "context": {k.value: v for k, v in updates.items()}}
    echo_payload(runtime, payload)


@pq.command(
    "modify",
    wraps_tool="pq_modify",
    client_only_params=frozenset({"script_body_path", "yes"}),
    help_spec=HelpSpec(
        summary="Modify an existing Persistent Query.",
        description=(
            "Enterprise (Core+) only. Updates only the fields you pass on ID; "
            "everything else is left unchanged. The script sources "
            "--script-body/--script-body-path/--git-script-path (and separately "
            "--auto-delete-timeout/--schedule) are mutually exclusive. Pass "
            "--restart to restart the PQ after applying the change. "
            + CONTEXT_RISK_DESTRUCTIVE
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified PQ id 'enterprise:system:serial'. "
                "Run 'pq list' or 'pq name-to-id'. Defaults to the sticky "
                f"context pq if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_OBJECT,
        examples=(
            "$ dhcli pq modify enterprise:prod:1234567890 --heap-size-gb 8 --restart",
            "$ dhcli pq modify enterprise:prod:1234567890 --disabled",
        ),
        see_also=(
            "dhcli pq details ID",
            "dhcli pq restart ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.OPERATION_CANCELED,
            ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
            ErrorCode.FILE_READ_FAILED,
            ErrorCode.MISSING_ARGUMENT,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("id", required=False)
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
@yes_option
@click.pass_context
@run_async
async def pq_modify(
    ctx: click.Context,
    *,
    id: str | None,
    yes: bool,
    **options: Any,
) -> None:
    """Modify an existing Persistent Query.

    Named/pass-through split as in :func:`pq_create`.
    """
    runtime: Runtime = ctx.obj
    # Validate the flag combination before asking anything: confirming a
    # destructive action that then fails on a bad flag pair wastes the
    # user's decision. Safe to reorder because the check reads only
    # option keys, never 'id'.
    _check_mutually_exclusive(options)
    resolved_id = require_context_target(
        runtime,
        ContextKey.PQ,
        id,
        action="Modify",
        yes=yes,
    )
    await call_and_echo(
        runtime,
        "pq_modify",
        retry_command="dhcli pq modify",
        arguments=_create_modify_args({**options, "id": resolved_id}),
    )


# ---------------------------------------------------------------------------
# delete / start / stop / restart (batch lifecycle)
# ---------------------------------------------------------------------------


def _ids(runtime: Runtime, id: tuple[str, ...]) -> list[str]:
    """Validate and return the PQ id list from a variadic argument.

    Falls back to the sticky context pq (as a single-element list) when
    ``id`` is empty. For a verb that destroys or disrupts, use
    :func:`_ids_confirmed` so the fallback can be confirmed.

    Raises:
        CliError: With :attr:`ErrorCode.CONTEXT_NOT_SET` when ``id`` is
            empty and no context pq is available.
    """
    if id:
        return list(id)
    return [require_context_value(runtime, ContextKey.PQ, None)]


def _ids_confirmed(
    runtime: Runtime, id: tuple[str, ...], *, action: str, yes: bool
) -> list[str]:
    """Return the PQ id list, confirming a sticky-context fallback.

    The :func:`_ids` counterpart for a verb that destroys or disrupts:
    an explicitly listed id is used as given, while a value taken from
    the sticky context goes through
    :func:`~deephaven_mcp.cli._context.require_context_target`.

    Args:
        runtime (Runtime): The active CLI runtime.
        id (tuple[str, ...]): The variadic ids as parsed by click.
        action (str): Imperative phrase naming the operation, used to
            build the confirmation prompt.
        yes (bool): The verb's ``--yes`` flag; skips the confirmation.

    Raises:
        CliError: With :attr:`ErrorCode.CONTEXT_NOT_SET` when ``id`` is
            empty and no context pq is available, or with
            :attr:`ErrorCode.OPERATION_CANCELED` when the confirmation
            is declined.
    """
    if id:
        return list(id)
    return [
        require_context_target(runtime, ContextKey.PQ, None, action=action, yes=yes)
    ]


def _deleted_ids(payload: dict[str, Any]) -> frozenset[str]:
    """Return the ids ``pq_delete`` reports as having actually been deleted.

    ``pq_delete`` is best-effort: ``success: True`` means the batch
    *ran*, not that every id succeeded, and the per-item ``results``
    list records which did. Clearing the sticky context for every
    *requested* id would therefore discard a pointer to a PQ that still
    exists whenever a delete failed.

    Args:
        payload (dict[str, Any]): The ``pq_delete`` success payload.

    Returns:
        frozenset[str]: The ids whose per-item result reports success.
            Empty when ``results`` is absent or not a list, so an
            unrecognized payload shape clears nothing rather than
            guessing.
    """
    results = payload.get("results")
    if not isinstance(results, list):
        return frozenset()
    return frozenset(
        item["id"]
        for item in results
        if isinstance(item, dict)
        and item.get("success")
        and isinstance(item.get("id"), str)
    )


@pq.command(
    "delete",
    wraps_tool="pq_delete",
    client_only_params=frozenset({"yes"}),
    help_spec=HelpSpec(
        summary="Delete one or more Persistent Queries.",
        description=(
            "Enterprise (Core+) only. Deletes every ID given. --max-concurrent "
            "caps how many deletions run in parallel. Best-effort: exit 0 means the "
            "batch ran, not that every id succeeded — check the summary and per-item "
            "results for failures. The sticky pq and session keys are cleared only "
            "for ids actually reported deleted, so a failed delete leaves the "
            "context pointing at the PQ that still exists. " + CONTEXT_RISK_DESTRUCTIVE
        ),
        arguments=(
            HelpEntry(
                "ID",
                "One or more fully qualified PQ ids ('enterprise:system:serial'). "
                "Defaults to the sticky context pq (as a single id) if omitted. "
                + CONTEXT_HINT,
            ),
        ),
        output=_OUTPUT_OBJECT,
        examples=(
            "$ dhcli pq delete enterprise:prod:1234567890 enterprise:prod:1234567891",
        ),
        see_also=("dhcli pq list SYSTEM", "dhcli context show"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.OPERATION_CANCELED,
            *wrapper_error_codes(),
        ),
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
@yes_option
@click.pass_obj
@run_async
async def pq_delete(
    runtime: Runtime, id: tuple[str, ...], max_concurrent: int | None, yes: bool
) -> None:
    """Delete one or more Persistent Queries."""
    ids = _ids_confirmed(runtime, id, action="Delete", yes=yes)
    arguments: dict[str, Any] = {"id": ids}
    if max_concurrent is not None:
        arguments["max_concurrent"] = max_concurrent
    payload = await call_for_payload(
        runtime, "pq_delete", retry_command="dhcli pq delete", arguments=arguments
    )
    clear_matching(
        runtime.context_store,
        _deleted_ids(payload),
        (ContextKey.PQ, ContextKey.SESSION),
    )
    echo_payload(runtime, payload)


def _lifecycle_command(name: str, summary: str, *, disruptive: bool) -> click.Command:
    """Build a start/stop/restart command (identical shape, distinct tool).

    Args:
        name (str): The verb name as it appears in the command tree,
            which is also the ``pq_<name>`` tool-name suffix.
        summary (str): One-line help summary.
        disruptive (bool): Whether acting on the wrong target disrupts a
            running service. ``True`` for ``stop``/``restart``, which
            therefore warn about an unintended context and confirm a
            context-supplied id (``--yes`` to skip); ``False`` for
            ``start``, which only leaves state behind and so warns
            without asking.
    """
    tool = f"pq_{name}"
    risk = CONTEXT_RISK_DESTRUCTIVE if disruptive else CONTEXT_RISK_STATEFUL

    @pq.command(
        name,
        wraps_tool=tool,
        client_only_params=frozenset({"yes"}) if disruptive else frozenset(),
        help_spec=HelpSpec(
            summary=summary,
            description=(
                f"Enterprise (Core+) only. {summary} --no-wait returns without "
                "waiting for the state change; --max-concurrent caps parallelism "
                "across multiple ids. Best-effort: exit 0 means the batch ran, not "
                "that every id succeeded — check the per-item results for failures. "
                + risk
            ),
            arguments=(
                HelpEntry(
                    "ID",
                    "One or more fully qualified PQ ids "
                    "('enterprise:system:serial'). Defaults to the sticky "
                    f"context pq if omitted. {CONTEXT_HINT}",
                ),
            ),
            output=_OUTPUT_OBJECT,
            examples=(f"$ dhcli pq {name} enterprise:prod:1234567890",),
            see_also=("dhcli pq details ID", "dhcli context show"),
            exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
            error_codes=(
                ErrorCode.CONTEXT_NOT_SET,
                *((ErrorCode.OPERATION_CANCELED,) if disruptive else ()),
                *wrapper_error_codes(),
            ),
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
        runtime: Runtime,
        id: tuple[str, ...],
        wait: bool,
        max_concurrent: int | None,
        yes: bool = False,
    ) -> None:
        """Run the lifecycle tool for the resolved ids.

        ``yes`` is only ever supplied by click in the ``disruptive``
        case, where ``--yes`` is attached; ``start`` runs with the
        default.
        """
        ids = (
            _ids_confirmed(runtime, id, action=name.capitalize(), yes=yes)
            if disruptive
            else _ids(runtime, id)
        )
        arguments: dict[str, Any] = {"id": ids, "wait": wait}
        if max_concurrent is not None:
            arguments["max_concurrent"] = max_concurrent
        await call_and_echo(
            runtime, tool, retry_command=f"dhcli pq {name}", arguments=arguments
        )

    return yes_option(_cmd) if disruptive else _cmd


# Registration happens inside ``_lifecycle_command`` via the ``@pq.command``
# decorator, so these names exist for symmetry with the other verbs in this
# module rather than to be read. Assigning them also keeps the built command
# reachable for tests.
pq_start = _lifecycle_command(
    "start", "Start one or more Persistent Queries.", disruptive=False
)
pq_stop = _lifecycle_command(
    "stop", "Stop one or more Persistent Queries.", disruptive=True
)
pq_restart = _lifecycle_command(
    "restart", "Restart one or more Persistent Queries.", disruptive=True
)
