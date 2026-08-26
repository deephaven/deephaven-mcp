"""``dhcli catalog`` noun group: query an Enterprise (Core+) data catalog.

Verbs: ``tables``, ``namespaces``, ``schema``, ``sample``.

Enterprise (Core+) only — these operate on an enterprise system's
catalog (database). The SYSTEM must name a configured enterprise system.
"""

from __future__ import annotations

__all__ = ["catalog"]

from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    TABULAR_OUTPUT_BODY_FIELDS,
    TABULAR_OUTPUT_NOTE,
    call_and_echo,
    call_and_echo_field,
    call_and_echo_table,
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
    catalog; 'schema' returns column definitions; 'sample' returns a few
    rows of a catalog table. All take an enterprise system name and
    auto-start the daemon unless --no-auto-start is set. The catalog is
    the system's stored data, shared by every user — these verbs read it
    but do not change it.

    These verbs need no worker of your own: they read through the
    system's shared 'WebClientData' persistent query, so the result never
    depends on which PQ happens to be running.

    'tables' and 'namespaces' fall back to the sticky context system
    when their system is omitted; 'schema' and 'sample' cannot, because a
    namespace and table name follow it — pass their system explicitly
    (run 'context show' to see the current default).
    """


def _filter_option(f: Any) -> Any:
    """Attach the shared repeatable ``--filter`` option to a command.

    The listing verbs and ``sample`` give the option different meanings,
    so the help states the part they share and each verb's description
    covers its own: on ``tables`` / ``namespaces`` a filter narrows the
    catalog listing, while on ``sample`` it filters the table's rows and
    omitting it triggers the tool's partition auto-detection.
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
        "listing is a candidate set: an entry can still fail to load when "
        "'catalog schema' or 'catalog sample' reaches for it. When the list is "
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
            "cap rows with --max-rows. Follow up with 'catalog schema' or "
            "'catalog sample' for a specific table."
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
            "dhcli catalog schema SYSTEM NAMESPACE TABLE",
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


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------

_OUTPUT_SCHEMA = OutputSpec(
    "object",
    (
        OutputField("system", "string", "The enterprise system name, echoed back."),
        OutputField("namespace", "string", "The catalog namespace."),
        OutputField("table_name", "string", "The table name."),
        OutputField(
            "schema",
            "array",
            "One entry per column: name and type (Deephaven type name), plus "
            "sparse column_type ('Partitioning' or 'Grouping'; omitted for "
            "Normal columns).",
        ),
        OutputField("column_count", "integer", "Number of columns."),
    ),
    note="Schema for the one named catalog table.",
)


@catalog.command(
    "schema",
    wraps_tool="catalog_table_schema",
    help_spec=HelpSpec(
        summary="Show column definitions for one catalog table.",
        description=(
            "Enterprise (Core+) only. Returns the schema (column names and "
            "types) for a single catalog table. Discover namespace/table pairs "
            "with 'catalog tables' first."
        ),
        arguments=(
            HelpEntry(
                "SYSTEM",
                "Enterprise system name. Run 'system list'. Required — with "
                "NAMESPACE and TABLE_NAME following it, it cannot fall back "
                f"to the sticky context. {CONTEXT_HINT}",
            ),
            HelpEntry(
                "NAMESPACE",
                "The catalog namespace, as named by 'catalog namespaces' or "
                "the namespace field of 'catalog tables'.",
            ),
            HelpEntry(
                "TABLE_NAME",
                "The catalog table whose schema to show, as named by "
                "'catalog tables'.",
            ),
        ),
        output=_OUTPUT_SCHEMA,
        examples=(
            "$ dhcli catalog schema prod Market Trades",
            "$ dhcli catalog schema prod Market Trades "
            "| jq -r '.schema[] | select(.column_type) | .name'",
        ),
        see_also=(
            "dhcli catalog tables SYSTEM",
            "dhcli catalog sample SYSTEM NAMESPACE TABLE",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("system")
@click.argument("namespace")
@click.argument("table_name")
@click.pass_obj
@run_async
async def catalog_schema(
    runtime: Runtime,
    system: str,
    namespace: str,
    table_name: str,
) -> None:
    """Show column definitions for one catalog table."""
    arguments: dict[str, Any] = {
        "system": system,
        "namespace": namespace,
        "table_name": table_name,
    }
    await call_and_echo(
        runtime,
        "catalog_table_schema",
        retry_command="dhcli catalog schema",
        arguments=arguments,
    )


# ---------------------------------------------------------------------------
# sample
# ---------------------------------------------------------------------------

_OUTPUT_TABULAR = OutputSpec(
    "object",
    (
        OutputField("system", "string", "The enterprise system name, echoed back."),
        OutputField("namespace", "string", "The catalog namespace, echoed back."),
        *TABULAR_OUTPUT_BODY_FIELDS,
    ),
    note=TABULAR_OUTPUT_NOTE,
)


@catalog.command(
    "sample",
    wraps_tool="catalog_table_sample",
    help_spec=HelpSpec(
        summary="Sample rows from a catalog table.",
        description=(
            "Enterprise (Core+) only. Returns up to --max-rows rows from the "
            "head (default) or, with --tail, the tail of NAMESPACE.TABLE_NAME. "
            "A preview, not a query: the row cap is small and a truncated "
            "result reports is_complete false. Partitioned tables (DbInternal, "
            "System, and others) would return nothing without a partition "
            "filter, so with no --filter the tool detects the table's "
            "partition columns and samples the most recent partition holding "
            "data; passing --filter replaces that with your own expressions. "
            "'catalog schema' marks partition columns with column_type "
            "'Partitioning'."
        ),
        arguments=(
            HelpEntry(
                "SYSTEM",
                "Enterprise system name. Run 'system list'. Required — with "
                "NAMESPACE and TABLE_NAME following it, it cannot fall back "
                f"to the sticky context. {CONTEXT_HINT}",
            ),
            HelpEntry(
                "NAMESPACE",
                "The catalog namespace, as named by 'catalog namespaces'.",
            ),
            HelpEntry("TABLE_NAME", "The catalog table, as named by 'catalog tables'."),
        ),
        output=_OUTPUT_TABULAR,
        examples=(
            "$ dhcli catalog sample prod Market Trades",
            "$ dhcli catalog sample prod Market Trades --max-rows 20 --tail",
            "$ dhcli catalog sample prod Market Trades "
            "| jq '.row_count, .is_complete'",
        ),
        see_also=(
            "dhcli catalog tables SYSTEM",
            "dhcli catalog schema SYSTEM NAMESPACE TABLE",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("system")
@click.argument("namespace")
@click.argument("table_name")
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help=(
        "Maximum number of rows to sample. Omitted: 100, the tool's own cap. "
        "The server refuses a response over its size limit."
    ),
)
@click.option(
    "--head/--tail",
    "head",
    default=True,
    help=(
        "Take rows from the start of the table (default) or, with --tail, "
        "from the end — the most recent rows for a time-series table."
    ),
)
@_filter_option
@click.pass_obj
@run_async
async def catalog_sample(
    runtime: Runtime,
    system: str,
    namespace: str,
    table_name: str,
    max_rows: int | None,
    head: bool,
    filters: tuple[str, ...],
) -> None:
    """Sample rows from a catalog table."""
    arguments: dict[str, Any] = {
        "system": system,
        "namespace": namespace,
        "table_name": table_name,
        "head": head,
        "format": "json-row",
    }
    if max_rows is not None:
        arguments["max_rows"] = max_rows
    if filters:
        arguments["filters"] = list(filters)
    await call_and_echo_table(
        runtime,
        "catalog_table_sample",
        retry_command="dhcli catalog sample",
        arguments=arguments,
    )
