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
    TARGET_SELECTION_GUIDANCE,
    TARGET_SELECTION_HINT,
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

_PQ_IDENTITY_FIELDS: tuple[OutputField, ...] = (
    OutputField("id", "string", "Fully qualified id 'enterprise:system:serial'."),
    OutputField("serial", "integer", "PQ serial number — the id's last component."),
    OutputField("name", "string", "Human-readable PQ name."),
)
"""Identity keys every single-PQ tool echoes back."""

_STATE_CATEGORY_VALUES = (
    "'ACTIVE' (serving), 'TRANSITIONAL' (still coming up or going down), "
    "'TERMINAL', or 'INVALID'"
)
"""The status_category vocabulary, worded once for the verbs that emit it."""

_BATCH_NOTE = (
    "Best-effort batch: exit 0 means the batch ran, not that every id "
    "succeeded. Branch on each results[] entry's own 'success' field, never "
    "on the exit code alone."
)
"""Shared warning for the four batch verbs, whose exit code hides per-id failure."""

_MAX_CONCURRENT_HELP = (
    "Cap on how many ids are operated on in parallel; must be greater than "
    "0. Omitted: the operator-configured default — "
    "enterprise.settings.pq_tools.default_max_concurrent, itself 20 unless "
    "set."
)
"""Shared help for the batch verbs' concurrency cap, whose default is server-side."""

_SUMMARY_FIELD = OutputField(
    "summary", "object", "Counts across the batch: total, succeeded, failed."
)
_MESSAGE_FIELD = OutputField(
    "message",
    "string",
    "One-line summary of the batch, e.g. 'Stopped 1 of 2 PQ(s), 1 failed'.",
)


def _batch_output(item_fields: str) -> OutputSpec:
    """Return the output spec for one batch lifecycle verb.

    The four batch verbs share an envelope but not their per-item value
    fields (only ``start``/``restart`` report ``state_category``, and
    ``delete`` reports no state at all), so the envelope is described
    once here and each verb supplies the rest.

    Args:
        item_fields (str): The verb's own per-item value keys, as a
            comma-joined phrase appended to the common ones.

    Returns:
        OutputSpec: The spec to hand to the verb's ``HelpSpec``.
    """
    return OutputSpec(
        "object",
        (
            OutputField(
                "results",
                "array",
                "One entry per requested id, in request order: id, serial, "
                f"success, error (null on success), {item_fields}. On a failed "
                "entry the value keys are null and 'error' carries the reason.",
            ),
            _SUMMARY_FIELD,
            _MESSAGE_FIELD,
        ),
        note=_BATCH_NOTE,
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
            "Enterprise (Core+) only. Lists every PQ configured on SYSTEM — "
            "every user's, production included — without connecting to any of "
            "them. A returned id works verbatim with the other pq verbs, and "
            "with the session, table, and catalog verbs while that PQ is "
            "running. " + TARGET_SELECTION_GUIDANCE
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
            "$ dhcli pq list prod | jq '.[] | select(.status_category==\"ACTIVE\") | .id'",
        ),
        see_also=(
            "dhcli pq details ID",
            "dhcli pq name-to-id SYSTEM NAME",
            "dhcli pq create PQ_NAME",
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


_OUTPUT_DETAILS = OutputSpec(
    "object",
    (
        *_PQ_IDENTITY_FIELDS,
        OutputField(
            "state",
            "string",
            "Current PQ state, e.g. 'RUNNING', 'STOPPED', 'FAILED'.",
        ),
        OutputField(
            "config",
            "object",
            "The stored definition: heap_size_gb, script_code / script_path, "
            "script_language, configuration_type, scheduling, server_name, "
            "worker_kind, admin_groups, viewer_groups, restart_users, owner, "
            "enabled, and more.",
        ),
        OutputField(
            "state_details",
            "object",
            "Live state: status, num_failures, initialization timestamps, "
            "dispatcher_host, and connection_details — null unless the PQ is "
            "running, otherwise carrying processor_host and protocols[].port.",
        ),
        OutputField(
            "replicas",
            "array",
            "Per-replica state for a load-balanced PQ; empty when unreplicated.",
        ),
        OutputField(
            "spares",
            "array",
            "Per-spare state for standby instances; empty when none.",
        ),
    ),
    note=(
        "This is the authority on what a PQ is really doing — read it after a "
        "lifecycle verb reports success, and read config.scheduling from it "
        "before passing --schedule to 'pq modify'."
    ),
)


@pq.command(
    "details",
    wraps_tool="pq_details",
    help_spec=HelpSpec(
        summary="Show details for one Persistent Query.",
        description=(
            "Enterprise (Core+) only. Reports the full stored definition "
            "(config) and the live state (state_details) of ID, including the "
            "running worker's connection details. This is how to confirm what "
            "a PQ is actually doing: 'pq start' / 'pq restart' report that the "
            "request was accepted, not that the worker is serving."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified PQ id 'enterprise:system:serial'. "
                "Run 'pq list' or 'pq name-to-id'. Defaults to the sticky "
                f"context pq if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_DETAILS,
        examples=(
            "$ dhcli pq details enterprise:prod:1234567890",
            "$ dhcli pq details enterprise:prod:1234567890 | jq .state",
            "$ dhcli pq details enterprise:prod:1234567890 | jq '.config.scheduling'",
        ),
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


_OUTPUT_NAME_TO_ID = OutputSpec(
    "object",
    (
        *_PQ_IDENTITY_FIELDS,
        OutputField("system", "string", "The Enterprise system that was searched."),
    ),
    note=(
        "Feed 'id' straight into the other pq verbs, and into the session, "
        "table, and catalog verbs while the PQ is running."
    ),
)


@pq.command(
    "name-to-id",
    wraps_tool="pq_name_to_id",
    help_spec=HelpSpec(
        summary="Resolve a Persistent Query name to its fully qualified id.",
        description=(
            "Enterprise (Core+) only. Looks up PQ_NAME within SYSTEM and "
            "returns the id the other verbs take. Use it when you were given "
            "a PQ by name rather than by id; a name that matches nothing "
            "exits 3."
        ),
        arguments=(
            HelpEntry(
                "SYSTEM",
                "Enterprise system name. Run 'system list'. Required here — "
                "with PQ_NAME following it, it cannot fall back to the "
                f"sticky context. {CONTEXT_HINT}",
            ),
            HelpEntry(
                "PQ_NAME",
                "Human-readable PQ name, as shown in the 'name' field of " "'pq list'.",
            ),
        ),
        output=_OUTPUT_NAME_TO_ID,
        examples=(
            "$ dhcli pq name-to-id prod nightly-report",
            "$ dhcli pq name-to-id prod nightly-report | jq -r .id",
        ),
        see_also=("dhcli pq list SYSTEM", "dhcli pq details ID"),
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


_RESTART_USERS_CHOICES = ("RU_ADMIN", "RU_ADMIN_AND_VIEWERS", "RU_VIEWERS_WHEN_DOWN")
"""The controller's restart-permission vocabulary, as a validated closed set."""


def _create_modify_options(f: Callable[..., Any]) -> Callable[..., Any]:
    """Apply the options shared by ``pq create`` and ``pq modify``.

    Only options whose help reads correctly for *both* verbs belong here.
    The test: does omitting it mean the same thing to each? Where it does
    not -- the controller picks a default on ``create`` versus the stored
    value is kept on ``modify`` -- the option is declared on each command
    separately so each help string can say what actually happens. Read
    the two commands for which options those currently are; a list here
    would rot the moment one moved.

    Splitting an option costs a second declaration that can drift in type
    as well as wording, so
    ``test_pq_create_and_modify_agree_on_option_machinery``
    (``tests/cli/test_help_contract.py``) pins every same-named option on
    the two verbs to one parsing behavior while leaving ``help`` and
    ``required`` free to differ.

    List-replacement is a ``modify``-only consequence (a ``create`` has
    no prior list to replace), so it is stated in ``pq modify``'s
    description rather than on each repeatable option.
    """
    options = (
        click.option(
            "--script-body",
            "script_body",
            default=None,
            help=(
                "Inline PQ script source, stored verbatim in the PQ "
                "definition. One of --script-body / --script-body-path / "
                "--git-script-path at most."
            ),
        ),
        click.option(
            "--script-body-path",
            "script_body_path",
            default=None,
            help=(
                "Path to a local script file, read by the CLI (so a relative "
                "path resolves against your working directory, and '~' is "
                "expanded) and stored as the PQ's inline body; '-' reads "
                "stdin. Not kept as a reference — later edits to the file do "
                "not reach the PQ."
            ),
        ),
        click.option(
            "--git-script-path",
            "script_path",
            default=None,
            help=(
                "Path to a script in the Enterprise controller's Git-backed "
                "script repository, e.g. IrisQueries/py/nightly.py. Resolved "
                "on the server, not on this machine, and stored as a "
                "reference rather than a copy."
            ),
        ),
        click.option(
            "--language",
            "programming_language",
            type=click.Choice(["Python", "Groovy"], case_sensitive=False),
            default=None,
            help="Language the script is written in.",
        ),
        click.option(
            "--configuration-type",
            "configuration_type",
            type=click.Choice(["Script", "RunAndDone"]),
            default=None,
            help=(
                "'Script' for a long-running interactive worker that stays up "
                "and can be connected to, or 'RunAndDone' for a batch job "
                "that exits when the script finishes."
            ),
        ),
        click.option(
            "--jvm-arg",
            "extra_jvm_args",
            multiple=True,
            help="One extra JVM argument, e.g. -Xmx4g (repeatable).",
        ),
        click.option(
            "--class-path",
            "extra_class_path",
            multiple=True,
            help=(
                "One extra class-path entry, resolved on the Enterprise "
                "server rather than on this machine (repeatable)."
            ),
        ),
        click.option(
            "--python-venv",
            "python_virtual_environment",
            default=None,
            help=(
                "Name of a Python virtualenv configured on the Enterprise "
                "server — a name, not a path on this machine."
            ),
        ),
        click.option(
            "--env",
            "extra_environment_vars",
            multiple=True,
            metavar="KEY=VALUE",
            help=(
                "Worker environment variable as KEY=VALUE (repeatable). The "
                "value is sent verbatim, never JSON-decoded."
            ),
        ),
        click.option(
            "--admin-group",
            "admin_groups",
            multiple=True,
            help="One group granted admin access to the PQ (repeatable).",
        ),
        click.option(
            "--viewer-group",
            "viewer_groups",
            multiple=True,
            help="One group granted viewer access to the PQ (repeatable).",
        ),
        click.option(
            "--restart-users",
            "restart_users",
            type=click.Choice(_RESTART_USERS_CHOICES),
            default=None,
            help=(
                "Who may restart the PQ: RU_ADMIN (admins only), "
                "RU_ADMIN_AND_VIEWERS, or RU_VIEWERS_WHEN_DOWN (viewers may "
                "restart it only while it is down)."
            ),
        ),
    )
    for option in reversed(options):
        f = option(f)
    return f


_OUTPUT_CREATE = OutputSpec(
    "object",
    (
        *_PQ_IDENTITY_FIELDS,
        OutputField(
            "state",
            "string",
            "Always the placeholder 'UNINITIALIZED' — the state at the instant "
            "the controller accepted the definition, NOT the live state. A "
            "permanent PQ usually starts acquiring a worker immediately, so "
            "run 'dhcli pq details' to see what it is actually doing.",
        ),
        OutputField("message", "string", "Human-readable confirmation."),
        OutputField(
            "context",
            "object",
            "Present when the sticky context was updated: the keys set and "
            "their new values.",
        ),
    ),
    note="Use 'id' as the target for the pq, session, table, and catalog verbs.",
)


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
            "options use the controller's defaults. This is the right way to get "
            "a PQ to work with: create your own rather than acting on one from "
            "'pq list'. Creating a PQ does not wait for it to come up — the "
            "returned 'state' is a fixed placeholder, so poll 'pq details' for "
            "the live state. " + CONTEXT_RISK_STATEFUL
        ),
        arguments=(
            HelpEntry(
                "PQ_NAME",
                "Name for the new PQ. Must not collide with an existing PQ on "
                "the target system; 'pq list' shows the names in use.",
            ),
        ),
        output=_OUTPUT_CREATE,
        examples=(
            "$ dhcli pq create nightly --system prod --heap-size-gb 4 "
            "--script-body-path ./nightly.py",
            "$ dhcli pq create nightly --system prod --heap-size-gb 4 "
            "--git-script-path IrisQueries/py/nightly.py",
            "$ dhcli pq create nightly --system prod --heap-size-gb 4 "
            "--script-body 'print(1)' | jq -r .id",
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
    help="JVM heap size in GB, e.g. 4 or 8.5. Required.",
)
@click.option(
    "--enabled/--disabled",
    "enabled",
    default=True,
    show_default=True,
    help=(
        "Whether the PQ may run. --disabled stores the definition without "
        "letting the controller start it."
    ),
)
@click.option(
    "--server",
    "server",
    default=None,
    help=(
        "Name of the Enterprise server pool to run the worker on "
        "(deployment-specific). Omitted: the controller chooses."
    ),
)
@click.option(
    "--owner",
    "owner",
    default=None,
    help=(
        "Username to own the new PQ. Omitted: the authenticated user. "
        "Naming another user may require server-side permission."
    ),
)
@click.option(
    "--engine",
    "engine",
    default=None,
    help=(
        "Worker engine name (deployment-specific; e.g. DeephavenCommunity, "
        "DeephavenEnterprise). Omitted: the controller's default, typically "
        "DeephavenCommunity."
    ),
)
@click.option(
    "--jvm-profile",
    "jvm_profile",
    default=None,
    help=(
        "Name of a JVM profile configured on the Enterprise controller. "
        "Omitted: the controller's default JVM settings."
    ),
)
@click.option(
    "--init-timeout-nanos",
    "init_timeout_nanos",
    type=int,
    default=None,
    help=(
        "Worker initialization timeout in NANOseconds (one second is "
        "1000000000). Omitted: the controller's default."
    ),
)
@click.option(
    "--schedule",
    "schedule",
    multiple=True,
    metavar="KEY=VALUE",
    help=(
        "Scheduler entry as KEY=VALUE (repeatable), e.g. SchedulerType=..., "
        "StartTime=08:00:00, TimeZone=America/New_York. Omitted: the "
        "controller's default scheduling. Mutually exclusive with "
        "--auto-delete-timeout."
    ),
)
@click.option(
    "--auto-delete-timeout",
    "auto_delete_timeout",
    type=int,
    default=None,
    help=(
        "Seconds of idleness after which the controller deletes the PQ; 0 "
        "installs the continuous scheduler and makes it permanent. Omitted: "
        "the controller's default scheduling. Mutually exclusive with "
        "--schedule."
    ),
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


_OUTPUT_MODIFY = OutputSpec(
    "object",
    (
        *_PQ_IDENTITY_FIELDS,
        OutputField("restarted", "boolean", "Whether --restart was applied to the PQ."),
        OutputField("message", "string", "Human-readable confirmation."),
        OutputField(
            "warning",
            "string",
            "Present only when the PQ is running and a runtime setting — "
            "script, heap, JVM args, class path, venv, language — changed "
            "without --restart: the definition is stored but the live worker "
            "still runs the old one. Check for this key and run 'dhcli pq "
            "restart' to apply.",
        ),
    ),
)


@pq.command(
    "modify",
    wraps_tool="pq_modify",
    client_only_params=frozenset({"script_body_path", "yes"}),
    help_spec=HelpSpec(
        summary="Modify an existing Persistent Query.",
        description=(
            "Enterprise (Core+) only. Updates only the fields you pass on ID; "
            "everything else is left unchanged. A repeatable option REPLACES "
            "the PQ's existing list wholesale rather than appending to it "
            "(--jvm-arg, --class-path, --env, --admin-group, --viewer-group, "
            "--schedule), so restate every entry you want to keep — read the "
            "current values from 'dhcli pq details' first. The script sources "
            "--script-body/--script-body-path/--git-script-path (and separately "
            "--auto-delete-timeout/--schedule) are mutually exclusive. Without "
            "--restart a running worker keeps serving its previous "
            "configuration: the response then carries a 'warning' field, and "
            "the change takes effect only after 'pq restart'. "
            + CONTEXT_RISK_DESTRUCTIVE
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified PQ id 'enterprise:system:serial'. "
                "Run 'pq list' or 'pq name-to-id'. Defaults to the sticky "
                f"context pq if omitted. {TARGET_SELECTION_HINT} "
                f"{CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_MODIFY,
        examples=(
            "$ dhcli pq modify enterprise:prod:1234567890 --heap-size-gb 8 --restart",
            "$ dhcli pq modify enterprise:prod:1234567890 --disabled",
            "$ dhcli pq modify enterprise:prod:1234567890 --heap-size-gb 8 "
            "| jq -r '.warning // \"applied\"'",
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
    help=(
        "Restart the PQ once the change is stored, so runtime settings "
        "(script, heap, JVM args) take effect immediately. Disrupts whoever "
        "is using the worker. Without it the change is saved but the running "
        "worker keeps its previous configuration."
    ),
)
@click.option(
    "--pq-name",
    "pq_name",
    default=None,
    help="Rename the PQ. The serial and the id do not change.",
)
@click.option(
    "--heap-size-gb",
    "heap_size_gb",
    type=float,
    default=None,
    help=(
        "New JVM heap size in GB, e.g. 8 or 16.5. Takes effect on the next " "restart."
    ),
)
@click.option(
    "--enabled/--disabled",
    "enabled",
    default=None,
    help="Enable or disable the PQ (unchanged if omitted).",
)
@click.option(
    "--server",
    "server",
    default=None,
    help=(
        "Move the PQ to this Enterprise server pool (deployment-specific; "
        "the current value shows under config.server_name in 'dhcli pq "
        "details'). Omitted: unchanged."
    ),
)
@click.option(
    "--owner",
    "owner",
    default=None,
    help=(
        "Reassign the PQ to this owner. Omitted: unchanged. Reassigning "
        "ownership may require server-side permission."
    ),
)
@click.option(
    "--engine",
    "engine",
    default=None,
    help=(
        "New worker engine name (deployment-specific; e.g. "
        "DeephavenCommunity, DeephavenEnterprise). Omitted: unchanged."
    ),
)
@click.option(
    "--jvm-profile",
    "jvm_profile",
    default=None,
    help=(
        "Name of a JVM profile configured on the Enterprise controller. "
        "Omitted: unchanged."
    ),
)
@click.option(
    "--init-timeout-nanos",
    "init_timeout_nanos",
    type=int,
    default=None,
    help=(
        "New worker initialization timeout in NANOseconds (one second is "
        "1000000000). Omitted: unchanged."
    ),
)
@click.option(
    "--schedule",
    "schedule",
    multiple=True,
    metavar="KEY=VALUE",
    help=(
        "Scheduler entry as KEY=VALUE (repeatable), e.g. SchedulerType=..., "
        "StartTime=08:00:00, TimeZone=America/New_York. A non-empty "
        "--schedule REPLACES the PQ's whole scheduling block, so restate "
        "every entry you want to keep — read the current list from 'dhcli pq "
        "details' (config.scheduling) first. Omitted: scheduling is "
        "unchanged; clearing a schedule is not supported via the CLI. "
        "Mutually exclusive with --auto-delete-timeout."
    ),
)
@click.option(
    "--auto-delete-timeout",
    "auto_delete_timeout",
    type=int,
    default=None,
    help=(
        "Seconds of idleness after which the controller deletes the PQ; 0 "
        "installs the continuous scheduler and makes it permanent. Omitted: "
        "scheduling and auto-delete are left unchanged. Mutually exclusive "
        "with --schedule."
    ),
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
            "Enterprise (Core+) only. Permanently deletes every ID given, "
            "stopping any that are running first; this cannot be undone. "
            "--max-concurrent caps how many deletions run in parallel. The "
            "sticky pq and session keys are cleared only for ids actually "
            "reported deleted, so a failed delete leaves the context pointing "
            "at the PQ that still exists. " + CONTEXT_RISK_DESTRUCTIVE
        ),
        arguments=(
            HelpEntry(
                "ID",
                "One or more fully qualified PQ ids ('enterprise:system:serial'), "
                "all from the same Enterprise system. Defaults to the sticky "
                "context pq (as a single id) if omitted. "
                f"{TARGET_SELECTION_HINT} {CONTEXT_HINT}",
            ),
        ),
        output=_batch_output("plus name (the deleted PQ's name)"),
        examples=(
            "$ dhcli pq delete enterprise:prod:1234567890 enterprise:prod:1234567891",
            "$ dhcli pq delete enterprise:prod:1234567890 | jq '.summary'",
        ),
        see_also=(
            "dhcli pq list SYSTEM",
            "dhcli pq create PQ_NAME",
            "dhcli context show",
        ),
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
    help=_MAX_CONCURRENT_HELP,
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


_WAIT_HELP = (
    "Wait for the state change before returning (default), for the "
    "operator-configured duration — enterprise.settings.timeouts.client."
    "pq_state_change_timeout_seconds, itself 120 seconds unless set. "
    "--no-wait submits the request and returns immediately, leaving the PQ "
    "mid-transition."
)
"""Shared help for the lifecycle wait flag, whose duration is server-side."""

_TIMEOUT_SEMANTICS = (
    "A --wait that runs out is reported as a per-item failure even though the "
    "controller keeps going in the background, so treat a timeout as unknown "
    "rather than failed: re-read 'dhcli pq details' instead of retrying "
    "blindly."
)
"""Shared warning: a lifecycle timeout does not mean the operation stopped."""

_READINESS_SEMANTICS = (
    "Success is acceptance, not readiness: a per-item success means the "
    "request was taken and the PQ was last seen in a non-failed state, not "
    "that a worker is serving. Branch on results[].state_category — "
    "'TRANSITIONAL' (state CONNECTING or INITIALIZING) is a normal outcome "
    "with --no-wait or a short wait — and only once it is 'ACTIVE' does the "
    "id work with the session, table, and catalog verbs. Confirm with 'dhcli "
    "pq details'."
)
"""Shared warning for start/restart, whose success does not imply RUNNING."""


def _lifecycle_command(
    name: str,
    summary: str,
    *,
    disruptive: bool,
    result_fields: str,
    semantics: str,
) -> click.Command:
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
        result_fields (str): The verb's per-item value keys, forwarded to
            :func:`_batch_output`. The three verbs differ: only
            ``start``/``restart`` report ``state_category``.
        semantics (str): Verb-specific prose on what a success means and
            what a timeout does, appended to the description.
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
                f"Enterprise (Core+) only. {summary} Every id must belong to "
                f"the same Enterprise system. {semantics} {risk}"
            ),
            arguments=(
                HelpEntry(
                    "ID",
                    "One or more fully qualified PQ ids "
                    "('enterprise:system:serial'). Defaults to the sticky "
                    f"context pq if omitted. {TARGET_SELECTION_HINT} "
                    f"{CONTEXT_HINT}",
                ),
            ),
            output=_batch_output(result_fields),
            examples=(
                f"$ dhcli pq {name} enterprise:prod:1234567890",
                f"$ dhcli pq {name} enterprise:prod:1234567890 "
                "| jq '.results[] | {id, success, error}'",
            ),
            see_also=(
                "dhcli pq details ID",
                "dhcli pq list SYSTEM",
                "dhcli context show",
            ),
            exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
            error_codes=(
                ErrorCode.CONTEXT_NOT_SET,
                *((ErrorCode.OPERATION_CANCELED,) if disruptive else ()),
                *wrapper_error_codes(),
            ),
        ),
    )
    @click.argument("id", nargs=-1)
    @click.option("--wait/--no-wait", "wait", default=True, help=_WAIT_HELP)
    @click.option(
        "--max-concurrent",
        "max_concurrent",
        type=int,
        default=None,
        help=_MAX_CONCURRENT_HELP,
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


_STATE_FIELDS = "plus name and state (e.g. 'STOPPED')"
"""Per-item value keys of ``pq stop``, which reports no state category."""

_STATE_CATEGORY_FIELDS = (
    "plus name, state (e.g. 'RUNNING', 'CONNECTING'), and state_category "
    f"({_STATE_CATEGORY_VALUES})"
)
"""Per-item value keys of ``pq start`` / ``pq restart``, which categorize the state."""

# Registration happens inside ``_lifecycle_command`` via the ``@pq.command``
# decorator, so these names exist for symmetry with the other verbs in this
# module rather than to be read. Assigning them also keeps the built command
# reachable for tests.
pq_start = _lifecycle_command(
    "start",
    "Start one or more Persistent Queries.",
    disruptive=False,
    result_fields=_STATE_CATEGORY_FIELDS,
    semantics=f"{_READINESS_SEMANTICS} {_TIMEOUT_SEMANTICS}",
)
pq_stop = _lifecycle_command(
    "stop",
    "Stop one or more Persistent Queries.",
    disruptive=True,
    result_fields=_STATE_FIELDS,
    semantics=(
        "Stopping is graceful and preserves the PQ definition, so 'pq start' "
        f"can run it again. {_TIMEOUT_SEMANTICS}"
    ),
)
pq_restart = _lifecycle_command(
    "restart",
    "Restart one or more Persistent Queries.",
    disruptive=True,
    result_fields=_STATE_CATEGORY_FIELDS,
    semantics=(
        "Reuses the stored definition and keeps the serial and id, so it is "
        "the way to apply a 'pq modify' that reported a warning. "
        f"{_READINESS_SEMANTICS} {_TIMEOUT_SEMANTICS}"
    ),
)
