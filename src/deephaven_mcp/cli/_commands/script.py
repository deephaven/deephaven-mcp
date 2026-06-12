"""``dh-mcp script`` noun group: run code and inspect packages in a session.

Verbs: ``run``, ``pip-list``.
"""

from __future__ import annotations

__all__ = ["script"]

from typing import Any

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
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
def script() -> None:
    """Run code and inspect the package environment in a Deephaven session.

    'run' executes a Python/Groovy script in a session; 'pip-list'
    reports the session's installed pip packages. Both take a fully
    qualified session id and auto-start the daemon unless --no-auto-start
    is set.
    """


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

_OUTPUT_RUN = OutputSpec(
    "object",
    (),
    note="Empty object on success; on failure the command exits 3 with the error.",
)


@script.command(
    "run",
    output_spec=_OUTPUT_RUN,
    wraps_tool="session_script_run",
    help=build_help(
        summary="Run a script in a session.",
        description=(
            "Executes a script in the session's worker. Provide the code inline "
            "with --script or from a file readable by the daemon with "
            "--script-path (supply exactly one). A missing or duplicate source, "
            "or a script error, exits 3."
        ),
        arguments=(HelpEntry("SESSION_ID", "Fully qualified id. Run 'session list'."),),
        output=_OUTPUT_RUN,
        examples=(
            "$ dh-mcp script run community:community:dev --script 'print(1+1)'",
            "$ dh-mcp script run community:community:dev --script-path /tmp/job.py",
        ),
        see_also=("dh-mcp script pip-list ID",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("session_id")
@click.option("--script", "script", default=None, help="Inline script source.")
@click.option(
    "--script-path",
    "script_path",
    default=None,
    help="Path to a script file readable by the daemon.",
)
@click.pass_obj
@run_async
async def script_run(
    runtime: Runtime, session_id: str, script: str | None, script_path: str | None
) -> None:
    """Run a script in a session."""
    arguments: dict[str, Any] = {"session_id": session_id}
    if script is not None:
        arguments["script"] = script
    if script_path is not None:
        arguments["script_path"] = script_path
    await call_and_echo(
        runtime,
        "session_script_run",
        retry_command="dh-mcp script run",
        arguments=arguments,
    )


# ---------------------------------------------------------------------------
# pip-list
# ---------------------------------------------------------------------------

_OUTPUT_PIP_LIST = OutputSpec(
    "list",
    (
        OutputField("package", "string", "Package name."),
        OutputField("version", "string", "Installed version."),
    ),
    note="Array of installed pip packages.",
)


@script.command(
    "pip-list",
    output_spec=_OUTPUT_PIP_LIST,
    wraps_tool="session_pip_list",
    help=build_help(
        summary="List a session's installed pip packages.",
        description=(
            "Returns the Python packages available in the session's environment "
            "as a list of {package, version}. Use it to confirm a library is "
            "present before running a script that imports it."
        ),
        arguments=(HelpEntry("SESSION_ID", "Fully qualified id. Run 'session list'."),),
        output=_OUTPUT_PIP_LIST,
        examples=(
            "$ dh-mcp script pip-list community:community:dev",
            "$ dh-mcp -o json script pip-list community:community:dev | jq '.[].package'",
        ),
        see_also=("dh-mcp script run ID",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("session_id")
@click.pass_obj
@run_async
async def script_pip_list(runtime: Runtime, session_id: str) -> None:
    """List a session's installed pip packages."""
    await call_and_echo_field(
        runtime,
        "session_pip_list",
        retry_command="dh-mcp script pip-list",
        arguments={"session_id": session_id},
        field="result",
        default=[],
    )
