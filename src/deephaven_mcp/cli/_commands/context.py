"""``dhcli context`` noun group: inspect and manage the sticky CLI context.

Verbs: ``show``, ``set``, ``unset``.

The sticky context holds a default ``session`` id, ``system`` name, and
``pq`` id, persisted in ``<runtime_dir>/context.json``. Commands across
the ``session``, ``system``, ``table``, ``catalog``, and ``pq`` groups
fall back to it when their id argument is omitted, so a session or PQ
you just created becomes the default target for the commands that
follow — see the ``_context`` module and ``docs/CLI.md``'s Context
section for the full resolution order.
"""

from __future__ import annotations

__all__ = ["context"]

from typing import Any, assert_never

import click

from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    call_for_payload,
    wrapper_error_codes,
)
from deephaven_mcp.cli._context import ContextKey, ContextProvenance
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode, render_warning
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._runtime import Runtime

_KEY_CHOICE = click.Choice([k.value for k in ContextKey])
"""Shared ``click.Choice`` for the ``KEY`` argument, single-sourced from
:class:`~deephaven_mcp.cli._context.ContextKey`."""


@click.group(cls=HelpfulGroup)
def context() -> None:
    """Inspect and manage the sticky CLI context (default session/system/PQ).

    Three keys are tracked: 'session', 'system', and 'pq'. Commands that
    take a session, system, or PQ id fall back to the matching key when
    the argument is omitted, resolved in order: the explicit argument,
    then context.json. Setting one key with 'context set' never affects
    another, but 'session create' and 'pq create' set every key that
    describes the new resource, and the delete verbs clear every key
    that pointed at the deleted one.

    The context is the one input a command's own command line does not
    show, so check it with 'context show' before running anything
    consequential: acting on an unintended context executes or
    destroys in the wrong worker or system. Set cli.json's
    context.confirm_destructive to be asked to confirm first (see
    'dhcli config show'), disable the fallback for one invocation with
    --no-context, or turn it off entirely via cli.json's
    context.enabled.
    """


def _context_payload(runtime: Runtime) -> dict[str, Any]:
    """Return the stored context for every key, with provenance.

    Reads ``context.json`` directly rather than going through
    :func:`~deephaven_mcp.cli._context.resolve_for_runtime`, because
    this is a *state inspector*, not a resolver: it must report what is
    stored even when the fallback is switched off. Resolving would
    short-circuit to ``unset`` before reading the file, so
    ``dhcli --no-context context set system prod`` would write ``prod``
    and then report nothing stored — leaving ``set`` and ``unset``
    unverifiable.

    Reads fresh on every call, so it reflects the current on-disk state
    whether called from ``show`` or after ``set``/``unset`` have just
    mutated it.

    Returns:
        dict[str, Any]: ``{key: {"value", "source"}}`` for every
            :class:`~deephaven_mcp.cli._context.ContextKey`. ``value``
            is always the stored value (possibly ``None``). ``source``
            is ``'disabled'`` for every key when the fallback is off,
            else ``'file'`` when that key holds a value and ``'unset'``
            when it does not — so "the fallback is off" is never
            confused with "nothing is stored".
    """
    stored = runtime.context_store.read()
    enabled = runtime.config.cli.context.enabled
    payload: dict[str, Any] = {}
    for key in ContextKey:
        value = stored.get(key)
        if not enabled:
            provenance = ContextProvenance.DISABLED
        elif value:
            provenance = ContextProvenance.FILE
        else:
            provenance = ContextProvenance.UNSET
        payload[key.value] = {"value": value, "source": provenance.value}
    return payload


def _warn_if_disabled(runtime: Runtime) -> None:
    """Warn that a just-written context value will not be consulted.

    ``set``/``unset`` still write when the fallback is off — staging a
    context for later is legitimate — but succeeding silently invites
    the conclusion that the value is in effect. The payload says
    ``source: 'disabled'``; this adds the remedy in prose for a human
    reading ``-o human``. Written to stderr, so structured stdout stays
    machine-parseable.
    """
    if runtime.config.cli.context.enabled:
        return
    render_warning(
        "Context fallback is disabled, so this value will not be used until "
        "it is re-enabled: drop --no-context, or set context.enabled to true "
        "in cli.json.",
        output=runtime.config.cli.output.format,
    )


_OUTPUT_CONTEXT = OutputSpec(
    "object",
    (
        OutputField("session", "object", "{value, source} for the sticky session id."),
        OutputField("system", "object", "{value, source} for the sticky system name."),
        OutputField("pq", "object", "{value, source} for the sticky PQ id."),
    ),
    note=(
        "'value' is always what is stored in context.json, and 'source' "
        "says whether it is in effect: 'file' (stored and active), "
        "'unset' (nothing stored), or 'disabled' (fallback off via "
        "--no-context or cli.json's context.enabled, so nothing is "
        "consulted regardless of what is stored)."
    ),
)


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@context.command(
    "show",
    help_spec=HelpSpec(
        summary="Show the effective sticky context, with provenance.",
        description=(
            "Reports the value dhcli would fall back to for each of "
            "'session', 'system', and 'pq' if a command omitted its id, and "
            "whether it is in effect: 'file' when stored and active, "
            "'unset' when nothing is stored, or 'disabled' when the "
            "fallback is switched off (--no-context, or cli.json's "
            "context.enabled) — in which case the stored value is still "
            "reported, but nothing will consult it. Run this before a "
            "destructive command whose id you intend to omit — the sticky "
            "target is not shown on the command line, and acting on an "
            "unintended context executes or destroys in the wrong worker or "
            "system. Never contacts the daemon."
        ),
        output=_OUTPUT_CONTEXT,
        examples=(
            "$ dhcli context show",
            "$ dhcli -o human context show",
            "$ dhcli context show | jq -r '.session // \"unset\"'",
        ),
        see_also=("dhcli context set KEY VALUE", "dhcli context unset"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
    ),
)
@click.pass_obj
@run_async
async def context_show(runtime: Runtime) -> None:
    """Show the effective sticky context, with provenance."""
    echo_payload(runtime, _context_payload(runtime))


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------


def _validate_system(runtime: Runtime, value: str) -> None:
    """Confirm ``value`` names 'community' or a configured Enterprise system.

    Raises:
        CliError: With :attr:`ErrorCode.SYSTEM_NOT_FOUND` when ``value``
            is neither.
    """
    if value == SystemType.COMMUNITY.value:
        return
    if value not in runtime.config.enterprise_systems:
        raise CliError(
            f"No system named {value!r} is configured (expected 'community' "
            "or a configured Enterprise system name; run 'dhcli system "
            "list').",
            code=ErrorCode.SYSTEM_NOT_FOUND,
        )


@context.command(
    "set",
    wraps_tools=("session_details", "pq_details"),
    client_only_params=frozenset({"key", "value"}),
    intentionally_unsupported=frozenset({"id"}),
    help_spec=HelpSpec(
        summary="Set one sticky context key.",
        description=(
            "Validates VALUE, then persists it as the sticky default for "
            "KEY: 'session' and 'pq' are confirmed to exist via the daemon "
            "(auto-starting it unless --no-auto-start is set); 'system' is "
            "checked against 'community' and the configured Enterprise "
            "systems, with no daemon contact. Overwrites any previous value "
            "for KEY; the other two keys are untouched."
        ),
        arguments=(
            HelpEntry("KEY", "One of 'session', 'system', 'pq'."),
            HelpEntry(
                "VALUE",
                "Fully qualified id for 'session'/'pq'; a system name for 'system'.",
            ),
        ),
        output=_OUTPUT_CONTEXT,
        examples=(
            "$ dhcli context set session community:community:dev",
            "$ dhcli context set system prod",
            "$ dhcli context set pq enterprise:prod:1234567890",
            "$ dhcli context set system prod | jq -r .system",
        ),
        see_also=("dhcli context show", "dhcli context unset"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.SYSTEM_NOT_FOUND, *wrapper_error_codes()),
    ),
)
@click.argument("key", type=_KEY_CHOICE)
@click.argument("value")
@click.pass_obj
@run_async
async def context_set(runtime: Runtime, key: str, value: str) -> None:
    """Validate VALUE, then set it as the sticky default for KEY."""
    context_key = ContextKey.from_value(key)
    # VALUE is what reaches each details tool as its ``id`` argument, which
    # is why ``id`` is declared ``intentionally_unsupported``: the value is
    # supplied, just not under a flag of that name.
    match context_key:
        case ContextKey.SESSION:
            await call_for_payload(
                runtime,
                "session_details",
                retry_command="dhcli context set session",
                arguments={"id": value},
            )
        case ContextKey.PQ:
            await call_for_payload(
                runtime,
                "pq_details",
                retry_command="dhcli context set pq",
                arguments={"id": value},
            )
        case ContextKey.SYSTEM:
            _validate_system(runtime, value)
        case _ as unexpected:
            assert_never(unexpected)
    runtime.context_store.set(context_key, value)
    _warn_if_disabled(runtime)
    echo_payload(runtime, _context_payload(runtime))


# ---------------------------------------------------------------------------
# unset
# ---------------------------------------------------------------------------


@context.command(
    "unset",
    help_spec=HelpSpec(
        summary="Clear one or more sticky context keys.",
        description=(
            "Removes KEY(s) from the sticky context; --all clears every "
            "key. Pass one or the other, never both — naming a key and "
            "also asking for every key is rejected rather than silently "
            "resolved. Idempotent — clearing an already-unset key is not "
            "an error. Never contacts the daemon."
        ),
        arguments=(
            HelpEntry(
                "KEY...",
                "One or more of 'session', 'system', 'pq'. Omit and pass "
                "--all to clear every key.",
            ),
        ),
        output=_OUTPUT_CONTEXT,
        examples=(
            "$ dhcli context unset session",
            "$ dhcli context unset session pq",
            "$ dhcli context unset --all",
            "$ dhcli context unset --all | jq 'keys'",
        ),
        see_also=("dhcli context show", "dhcli context set KEY VALUE"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.MISSING_ARGUMENT,
            ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
        ),
    ),
)
@click.argument("key", nargs=-1, type=_KEY_CHOICE)
@click.option(
    "--all",
    "unset_all",
    is_flag=True,
    default=False,
    help="Clear every sticky context key. Cannot be combined with KEY.",
)
@click.pass_obj
@run_async
async def context_unset(
    runtime: Runtime, key: tuple[str, ...], unset_all: bool
) -> None:
    """Clear one or more sticky context keys, or every key with --all.

    Raises:
        CliError: With :attr:`ErrorCode.MISSING_ARGUMENT` when neither a
            KEY nor ``--all`` is given, or
            :attr:`ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS` when both are.
    """
    if not key and not unset_all:
        raise CliError(
            "Provide at least one KEY to clear, or --all.",
            code=ErrorCode.MISSING_ARGUMENT,
        )
    if key and unset_all:
        raise CliError(
            "Pass either KEY(s) or --all, not both: "
            f"--all already clears {', '.join(k.value for k in ContextKey)}.",
            code=ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
        )
    target = None if unset_all else tuple(ContextKey.from_value(k) for k in key)
    runtime.context_store.unset(target)
    _warn_if_disabled(runtime)
    echo_payload(runtime, _context_payload(runtime))
