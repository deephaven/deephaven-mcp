"""``dhcli table`` noun group: inspect tables in a Deephaven session.

Verbs: ``list``, ``schema``, ``data``.
"""

from __future__ import annotations

__all__ = ["table"]

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
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._runtime import Runtime


@click.group(cls=HelpfulGroup)
def table() -> None:
    """Inspect tables in a Deephaven session.

    'list' enumerates table names; 'schema' returns column definitions;
    'data' returns row data. All take a fully qualified session id and
    auto-start the daemon unless --no-auto-start is set.
    """


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_OUTPUT_LIST = OutputSpec(
    "list", (), note="Array of table-name strings in the session."
)


@table.command(
    "list",
    wraps_tool="session_tables_list",
    help_spec=HelpSpec(
        summary="List the table names in a session.",
        description=(
            "Lightweight discovery of table names without schemas. Follow up "
            "with 'table schema' for column definitions or 'table data' for rows."
        ),
        arguments=(HelpEntry("ID", "Fully qualified id. Run 'session list'."),),
        output=_OUTPUT_LIST,
        examples=("$ dhcli table list community:community:dev",),
        see_also=("dhcli table schema ID TABLE", "dhcli table data ID TABLE"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.pass_obj
@run_async
async def table_list(runtime: Runtime, id: str) -> None:
    """List the table names in a session."""
    await call_and_echo_field(
        runtime,
        "session_tables_list",
        retry_command="dhcli table list",
        arguments={"id": id},
        field="table_names",
        default=[],
    )


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------

_OUTPUT_SCHEMA = OutputSpec(
    "object",
    (
        OutputField("id", "string", "The session id, echoed back."),
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
    note="Schema for the one named table.",
)


@table.command(
    "schema",
    wraps_tool="session_table_schema",
    help_spec=HelpSpec(
        summary="Show column definitions for one table in a session.",
        description=(
            "Returns the schema (column names and types) for a single table. "
            "Discover table names with 'table list' first."
        ),
        arguments=(
            HelpEntry("ID", "Fully qualified id. Run 'session list'."),
            HelpEntry("TABLE_NAME", "The table whose schema to show."),
        ),
        output=_OUTPUT_SCHEMA,
        examples=("$ dhcli table schema community:community:dev trades",),
        see_also=("dhcli table list ID",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.argument("table_name")
@click.pass_obj
@run_async
async def table_schema(runtime: Runtime, id: str, table_name: str) -> None:
    """Show column definitions for one table in a session."""
    arguments: dict[str, Any] = {"id": id, "table_name": table_name}
    await call_and_echo(
        runtime,
        "session_table_schema",
        retry_command="dhcli table schema",
        arguments=arguments,
    )


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------

_OUTPUT_DATA = OutputSpec(
    "object",
    (),
    note="Tabular row data for the table (columns + rows envelope from the "
    "tool, including the session id echoed back).",
)


@table.command(
    "data",
    wraps_tool="session_table_data",
    help_spec=HelpSpec(
        summary="Fetch row data from a table in a session.",
        description=(
            "Returns up to --max-rows rows from the head (default) or, with "
            "--tail, the tail of the table."
        ),
        arguments=(
            HelpEntry("ID", "Fully qualified id. Run 'session list'."),
            HelpEntry("TABLE_NAME", "The table to read."),
        ),
        output=_OUTPUT_DATA,
        examples=(
            "$ dhcli table data community:community:dev trades",
            "$ dhcli table data community:community:dev trades --max-rows 50 --tail",
        ),
        see_also=("dhcli table list ID", "dhcli table schema ID TABLE"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.argument("table_name")
@click.option(
    "--max-rows",
    "max_rows",
    type=int,
    default=None,
    help="Maximum rows to return (default: 1000).",
)
@click.option(
    "--head/--tail",
    "head",
    default=True,
    help="Take rows from the head (default) or the tail.",
)
@click.pass_obj
@run_async
async def table_data(
    runtime: Runtime,
    id: str,
    table_name: str,
    max_rows: int | None,
    head: bool,
) -> None:
    """Fetch row data from a table in a session."""
    arguments: dict[str, Any] = {
        "id": id,
        "table_name": table_name,
        "head": head,
        "format": "json-row",
    }
    if max_rows is not None:
        arguments["max_rows"] = max_rows
    await call_and_echo_table(
        runtime,
        "session_table_data",
        retry_command="dhcli table data",
        arguments=arguments,
    )
