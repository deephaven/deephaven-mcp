"""``dhcli catalog`` noun group: query an Enterprise (Core+) data catalog.

Verbs: ``tables``, ``namespaces``, ``schema``, ``sample``.

Enterprise (Core+) only — these operate on an enterprise session's
catalog (database). The ID must name an enterprise session.
"""

from __future__ import annotations

__all__ = ["catalog"]

from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
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
    rows of a catalog table. All take an enterprise session id and
    auto-start the daemon unless --no-auto-start is set.
    """


def _filter_option(f: Any) -> Any:
    """Attach the shared repeatable ``--filter`` option to a command."""
    return click.option(
        "--filter",
        "filters",
        multiple=True,
        help="Deephaven filter expression (repeatable).",
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
        "Array of {namespace, table_name} entries, one per catalog table. When "
        "the list is truncated by --max-rows, a warning is written to stderr."
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
                "ID",
                "Enterprise session id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_TABLES,
        examples=("$ dhcli catalog tables enterprise:prod:rpt",),
        see_also=(
            "dhcli catalog namespaces ID",
            "dhcli catalog schema ID NAMESPACE TABLE",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help="Row cap (default: 10000).",
)
@_filter_option
@click.pass_obj
@run_async
async def catalog_tables(
    runtime: Runtime,
    id: str | None,
    max_rows: int | None,
    filters: tuple[str, ...],
) -> None:
    """List tables in the Enterprise catalog."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    arguments: dict[str, Any] = {"id": id}
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
        "truncated by --max-rows, a warning is written to stderr."
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
                "ID",
                "Enterprise session id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_NAMESPACES,
        examples=("$ dhcli catalog namespaces enterprise:prod:rpt",),
        see_also=("dhcli catalog tables ID", "dhcli context show"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help="Namespace cap (default: 1000).",
)
@_filter_option
@click.pass_obj
@run_async
async def catalog_namespaces(
    runtime: Runtime,
    id: str | None,
    max_rows: int | None,
    filters: tuple[str, ...],
) -> None:
    """List namespaces in the Enterprise catalog."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    arguments: dict[str, Any] = {"id": id}
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
        OutputField("id", "string", "The session id, echoed back."),
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
            HelpEntry("ID", "Enterprise session id. Run 'session list'."),
            HelpEntry("NAMESPACE", "The catalog namespace."),
            HelpEntry("TABLE_NAME", "The catalog table whose schema to show."),
        ),
        output=_OUTPUT_SCHEMA,
        examples=("$ dhcli catalog schema enterprise:prod:rpt Market Trades",),
        see_also=("dhcli catalog tables ID",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.argument("namespace")
@click.argument("table_name")
@click.pass_obj
@run_async
async def catalog_schema(
    runtime: Runtime,
    id: str,
    namespace: str,
    table_name: str,
) -> None:
    """Show column definitions for one catalog table."""
    arguments: dict[str, Any] = {
        "id": id,
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
    (),
    note=(
        "Tabular result envelope from the tool (columns + rows, including the "
        "session id echoed back). In -o human, the format and columns fields "
        "are omitted as noise; -o json / -o yaml keep the full envelope."
    ),
)


@catalog.command(
    "sample",
    wraps_tool="catalog_table_sample",
    help_spec=HelpSpec(
        summary="Sample rows from a catalog table.",
        description=(
            "Enterprise (Core+) only. Returns up to --max-rows rows from the "
            "head (default) or, with --tail, the tail of NAMESPACE.TABLE_NAME."
        ),
        arguments=(
            HelpEntry("ID", "Enterprise session id. Run 'session list'."),
            HelpEntry("NAMESPACE", "The catalog namespace."),
            HelpEntry("TABLE_NAME", "The catalog table."),
        ),
        output=_OUTPUT_TABULAR,
        examples=(
            "$ dhcli catalog sample enterprise:prod:rpt Market Trades",
            "$ dhcli catalog sample enterprise:prod:rpt Market Trades --max-rows 20 --tail",
        ),
        see_also=(
            "dhcli catalog tables ID",
            "dhcli catalog schema ID NAMESPACE TABLE",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.argument("namespace")
@click.argument("table_name")
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help="Row cap (default: 100).",
)
@click.option(
    "--head/--tail", "head", default=True, help="Take rows from the head or the tail."
)
@_filter_option
@click.pass_obj
@run_async
async def catalog_sample(
    runtime: Runtime,
    id: str,
    namespace: str,
    table_name: str,
    max_rows: int | None,
    head: bool,
    filters: tuple[str, ...],
) -> None:
    """Sample rows from a catalog table."""
    arguments: dict[str, Any] = {
        "id": id,
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
