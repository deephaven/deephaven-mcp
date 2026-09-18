"""``dhcli catalog`` noun group: query an Enterprise (Core+) data catalog.

Verbs: ``tables``, ``namespaces``.

Enterprise (Core+) only. Both verbs name a SYSTEM and read the listing through
that system's shared ``WebClientData`` persistent query, scoped to the
Enterprise principal the server is configured with.
"""

from __future__ import annotations

__all__ = ["catalog"]

from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo_field,
    wrapper_error_codes,
)
from deephaven_mcp.cli._context import (
    CONTEXT_HINT,
    ContextKey,
    require_context_value,
)
from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._runtime import Runtime


@click.group(cls=HelpfulGroup)
def catalog() -> None:
    """Query an Enterprise (Core+) data catalog (database).

    Enterprise (Core+) only. 'tables' and 'namespaces' enumerate the
    catalog. Both auto-start the daemon unless --no-auto-start is set, and
    neither changes anything.

    Both take a SYSTEM and need no worker of your own: they read through the
    system's shared 'WebClientData' persistent query. That listing reflects
    the Enterprise principal the server is configured with for that system,
    not your own identity — every caller sees the same set. When SYSTEM is
    omitted they fall back to the sticky context system (run 'context show'
    to see the current defaults).
    """


def _filter_option(f: Any) -> Any:
    """Attach the shared repeatable ``--filter`` option to a command.

    On both verbs a filter narrows the catalog listing.
    """
    return click.option(
        "--filter",
        "filters",
        multiple=True,
        metavar="EXPR",
        help=(
            "Deephaven filter expression, in the query language's "
            "where-clause syntax (repeatable; expressions are ANDed). "
            "Quote it for the shell."
        ),
    )(f)


# ---------------------------------------------------------------------------
# tables
# ---------------------------------------------------------------------------

_OUTPUT_TABLES = OutputSpec(
    "list",
    (
        OutputField("namespace", "string", "The catalog namespace."),
        OutputField("table_name", "string", "The table name."),
    ),
    note=(
        "Array of {namespace, table_name} entries, one per catalog table. A "
        "listing is a candidate set: an entry can still fail to load when a "
        "session reaches for it. When the list is "
        "truncated by --max-rows, a warning is written to stderr — stdout "
        "alone cannot be told apart from a complete result."
    ),
)


@catalog.command(
    "tables",
    wraps_tool="catalog_tables_list",
    help_spec=HelpSpec(
        summary="List tables in the Enterprise catalog.",
        description=(
            "Enterprise (Core+) only. Prints one {namespace, table_name} entry "
            "per catalog table. Narrow with repeatable --filter expressions and "
            "cap rows with --max-rows."
        ),
        arguments=(
            HelpEntry(
                "SYSTEM",
                "Enterprise system name. Run 'system list'. Defaults to the "
                f"sticky context system if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_TABLES,
        examples=(
            "$ dhcli catalog tables prod",
            "$ dhcli catalog tables prod --max-rows 100",
            "$ dhcli catalog tables prod | jq -r '.[].table_name'",
        ),
        see_also=(
            "dhcli catalog namespaces SYSTEM",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("system", required=False)
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help=(
        "Maximum number of catalog entries to return. Omitted: 10000, the "
        "tool's own cap. A truncated result warns on stderr; the printed "
        "array is the truncated one."
    ),
)
@_filter_option
@click.pass_obj
@run_async
async def catalog_tables(
    runtime: Runtime,
    system: str | None,
    max_rows: int | None,
    filters: tuple[str, ...],
) -> None:
    """List tables in the Enterprise catalog."""
    system = require_context_value(runtime, ContextKey.SYSTEM, system)
    arguments: dict[str, Any] = {"system": system}
    if max_rows is not None:
        arguments["max_rows"] = max_rows
    if filters:
        arguments["filters"] = list(filters)
    await call_and_echo_field(
        runtime,
        "catalog_tables_list",
        retry_command="dhcli catalog tables",
        arguments=arguments,
        field="tables",
        default=[],
        truncation_hint="Raise --max-rows or narrow with --filter.",
    )


# ---------------------------------------------------------------------------
# namespaces
# ---------------------------------------------------------------------------

_OUTPUT_NAMESPACES = OutputSpec(
    "list",
    (),
    note=(
        "Array of namespace-name strings in the catalog. When the list is "
        "truncated by --max-rows, a warning is written to stderr — stdout "
        "alone cannot be told apart from a complete result."
    ),
)


@catalog.command(
    "namespaces",
    wraps_tool="catalog_namespaces_list",
    help_spec=HelpSpec(
        summary="List namespaces in the Enterprise catalog.",
        description=(
            "Enterprise (Core+) only. Prints the catalog's distinct namespace "
            "names. Narrow with repeatable --filter expressions and cap the "
            "count with --max-rows."
        ),
        arguments=(
            HelpEntry(
                "SYSTEM",
                "Enterprise system name. Run 'system list'. Defaults to the "
                f"sticky context system if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_NAMESPACES,
        examples=(
            "$ dhcli catalog namespaces prod",
            "$ dhcli catalog namespaces prod | jq -r '.[]'",
        ),
        see_also=("dhcli catalog tables SYSTEM", "dhcli context show"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("system", required=False)
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help=(
        "Maximum number of namespaces to return. Omitted: 1000, the tool's "
        "own cap. A truncated result warns on stderr; the printed array is "
        "the truncated one."
    ),
)
@_filter_option
@click.pass_obj
@run_async
async def catalog_namespaces(
    runtime: Runtime,
    system: str | None,
    max_rows: int | None,
    filters: tuple[str, ...],
) -> None:
    """List namespaces in the Enterprise catalog."""
    system = require_context_value(runtime, ContextKey.SYSTEM, system)
    arguments: dict[str, Any] = {"system": system}
    if max_rows is not None:
        arguments["max_rows"] = max_rows
    if filters:
        arguments["filters"] = list(filters)
    await call_and_echo_field(
        runtime,
        "catalog_namespaces_list",
        retry_command="dhcli catalog namespaces",
        arguments=arguments,
        field="namespaces",
        default=[],
        truncation_hint="Raise --max-rows or narrow with --filter.",
    )
