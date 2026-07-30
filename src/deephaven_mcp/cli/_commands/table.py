"""``dhcli table`` noun group: inspect tables in a Deephaven session.

Verbs: ``list``, ``schema``, ``data``.
"""

from __future__ import annotations

__all__ = ["table"]

from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    TABULAR_OUTPUT_FIELDS,
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
def table() -> None:
    """Inspect tables in a Deephaven session.

    'list' enumerates table names; 'schema' returns column definitions;
    'data' returns row data. All take a fully qualified session id and
    auto-start the daemon unless --no-auto-start is set. The usual order
    is 'list' to find a table, 'schema' to learn its columns and types,
    then 'data' to read rows.

    'list' falls back to the sticky context session when its id is
    omitted; 'schema' and 'data' cannot, because a table name follows
    the id and a single argument would be ambiguous — pass their id
    explicitly (run 'context show' to see the current default).
    """


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_OUTPUT_LIST = OutputSpec(
    "list",
    (),
    note=(
        "Array of table-name strings currently in the session's scope. Only "
        "names — no columns, types, or row counts. An empty array means the "
        "session holds no tables, which is normal for a session in which "
        "nothing has run yet."
    ),
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
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dhcli table list community:community:dev",
            "$ dhcli table list community:community:dev | jq -r '.[]'",
        ),
        see_also=(
            "dhcli table schema ID TABLE",
            "dhcli table data ID TABLE",
            "dhcli session exec ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.pass_obj
@run_async
async def table_list(runtime: Runtime, id: str | None) -> None:
    """List the table names in a session."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
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
            HelpEntry(
                "ID",
                "Fully qualified session id. Run 'session list'. Required — "
                "with TABLE_NAME following it, it cannot fall back to the "
                f"sticky context. {CONTEXT_HINT}",
            ),
            HelpEntry(
                "TABLE_NAME",
                "The table whose schema to show, as named by 'table list'.",
            ),
        ),
        output=_OUTPUT_SCHEMA,
        examples=(
            "$ dhcli table schema community:community:dev trades",
            "$ dhcli table schema community:community:dev trades "
            "| jq -r '.schema[] | \"\\(.name) \\(.type)\"'",
        ),
        see_also=(
            "dhcli table list ID",
            "dhcli table data ID TABLE",
            "dhcli context show",
        ),
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

_OUTPUT_DATA = OutputSpec("object", TABULAR_OUTPUT_FIELDS, note=TABULAR_OUTPUT_NOTE)


@table.command(
    "data",
    wraps_tool="session_table_data",
    help_spec=HelpSpec(
        summary="Fetch row data from a table in a session.",
        description=(
            "Returns up to --max-rows rows from the head (default) or, with "
            "--tail, the tail of the table. The rows arrive as JSON objects "
            "keyed by column name. A table larger than the cap is truncated "
            "silently on stdout — is_complete reports false and no warning is "
            "printed, so check that field before drawing any conclusion about "
            "the table as a whole. Read 'table schema' first if you need "
            "column types."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified session id. Run 'session list'. Required — "
                "with TABLE_NAME following it, it cannot fall back to the "
                f"sticky context. {CONTEXT_HINT}",
            ),
            HelpEntry("TABLE_NAME", "The table to read, as named by 'table list'."),
        ),
        output=_OUTPUT_DATA,
        examples=(
            "$ dhcli table data community:community:dev trades",
            "$ dhcli table data community:community:dev trades --max-rows 50 --tail",
            "$ dhcli table data community:community:dev trades | jq '.row_count, .is_complete'",
        ),
        see_also=(
            "dhcli table list ID",
            "dhcli table schema ID TABLE",
            "dhcli context show",
        ),
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
    help=(
        "Maximum number of rows to return. Omitted: 1000, the tool's own "
        "cap. Raising it can produce a very large payload; the server "
        "refuses a response over its size limit."
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
