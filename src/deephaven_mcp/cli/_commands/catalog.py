"""``dh-mcp catalog`` noun group: query an Enterprise (Core+) data catalog.

Verbs: ``tables``, ``namespaces``, ``schema``, ``sample``.

Enterprise (Core+) only — these operate on an enterprise session's
catalog (database). The ID must name an enterprise session.
"""

from __future__ import annotations

__all__ = ["catalog"]

from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
    call_and_echo_table,
    wrapper_error_codes,
)
from deephaven_mcp.cli._errors import ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpfulGroup,
    OutputField,
    OutputSpec,
    build_help,
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

_OUTPUT_TABULAR = OutputSpec(
    "object",
    (),
    note=(
        "Tabular result envelope from the tool (columns + rows). In -o human, "
        "the format and columns fields are omitted as noise; -o json / -o yaml "
        "keep the full envelope."
    ),
)


@catalog.command(
    "tables",
    output_spec=_OUTPUT_TABULAR,
    wraps_tool="catalog_tables_list",
    help=build_help(
        summary="List tables in the Enterprise catalog.",
        description=(
            "Enterprise (Core+) only. Returns catalog table metadata as tabular "
            "data. Narrow with repeatable --filter expressions and cap rows with "
            "--max-rows."
        ),
        arguments=(HelpEntry("ID", "Enterprise session id. Run 'session list'."),),
        output=_OUTPUT_TABULAR,
        examples=("$ dh-mcp catalog tables enterprise:prod:rpt",),
        see_also=("dh-mcp catalog namespaces ID", "dh-mcp catalog schema ID"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
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
    id: str,
    max_rows: int | None,
    filters: tuple[str, ...],
) -> None:
    """List tables in the Enterprise catalog."""
    arguments: dict[str, Any] = {"id": id, "format": "json-row"}
    if max_rows is not None:
        arguments["max_rows"] = max_rows
    if filters:
        arguments["filters"] = list(filters)
    await call_and_echo_table(
        runtime,
        "catalog_tables_list",
        retry_command="dh-mcp catalog tables",
        arguments=arguments,
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
    output_spec=_OUTPUT_NAMESPACES,
    wraps_tool="catalog_namespaces_list",
    help=build_help(
        summary="List namespaces in the Enterprise catalog.",
        description=(
            "Enterprise (Core+) only. Prints the catalog's distinct namespace "
            "names. Narrow with repeatable --filter expressions and cap the "
            "count with --max-rows."
        ),
        arguments=(HelpEntry("ID", "Enterprise session id. Run 'session list'."),),
        output=_OUTPUT_NAMESPACES,
        examples=("$ dh-mcp catalog namespaces enterprise:prod:rpt",),
        see_also=("dh-mcp catalog tables ID",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
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
    id: str,
    max_rows: int | None,
    filters: tuple[str, ...],
) -> None:
    """List namespaces in the Enterprise catalog."""
    arguments: dict[str, Any] = {"id": id}
    if max_rows is not None:
        arguments["max_rows"] = max_rows
    if filters:
        arguments["filters"] = list(filters)
    await call_and_echo_field(
        runtime,
        "catalog_namespaces_list",
        retry_command="dh-mcp catalog namespaces",
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
    (OutputField("schemas", "array", "Per-table column definitions."),),
    note="Schemas for the requested catalog tables.",
)


@catalog.command(
    "schema",
    output_spec=_OUTPUT_SCHEMA,
    wraps_tool="catalog_tables_schema",
    help=build_help(
        summary="Show column definitions for catalog tables.",
        description=(
            "Enterprise (Core+) only. Returns schemas for the named catalog "
            "tables, optionally scoped to a --namespace and capped at "
            "--max-tables."
        ),
        arguments=(
            HelpEntry("ID", "Enterprise session id. Run 'session list'."),
            HelpEntry("TABLE_NAMES", "Zero or more catalog table names."),
        ),
        output=_OUTPUT_SCHEMA,
        examples=(
            "$ dh-mcp catalog schema enterprise:prod:rpt --namespace Market",
            "$ dh-mcp catalog schema enterprise:prod:rpt Trades Quotes",
        ),
        see_also=("dh-mcp catalog tables ID",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.argument("table_names", nargs=-1)
@click.option("--namespace", "namespace", default=None, help="Limit to one namespace.")
@_filter_option
@click.option(
    "--max-tables",
    "max_tables",
    type=int,
    default=None,
    help="Table cap (default: 100).",
)
@click.pass_obj
@run_async
async def catalog_schema(
    runtime: Runtime,
    id: str,
    table_names: tuple[str, ...],
    namespace: str | None,
    filters: tuple[str, ...],
    max_tables: int | None,
) -> None:
    """Show column definitions for catalog tables."""
    arguments: dict[str, Any] = {"id": id}
    if table_names:
        arguments["table_names"] = list(table_names)
    if namespace is not None:
        arguments["namespace"] = namespace
    if filters:
        arguments["filters"] = list(filters)
    if max_tables is not None:
        arguments["max_tables"] = max_tables
    await call_and_echo(
        runtime,
        "catalog_tables_schema",
        retry_command="dh-mcp catalog schema",
        arguments=arguments,
    )


# ---------------------------------------------------------------------------
# sample
# ---------------------------------------------------------------------------


@catalog.command(
    "sample",
    output_spec=_OUTPUT_TABULAR,
    wraps_tool="catalog_table_sample",
    help=build_help(
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
            "$ dh-mcp catalog sample enterprise:prod:rpt Market Trades",
            "$ dh-mcp catalog sample enterprise:prod:rpt Market Trades --max-rows 20 --tail",
        ),
        see_also=("dh-mcp catalog tables ID", "dh-mcp catalog schema ID"),
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
        retry_command="dh-mcp catalog sample",
        arguments=arguments,
    )
