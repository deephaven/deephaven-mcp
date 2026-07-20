"""``dh-mcp config`` noun group: inspect and validate the resolved configuration tree."""

from __future__ import annotations

__all__ = ["config"]

import logging

import click

from deephaven_mcp._pydantic import dump_redacted
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._format import format_output
from deephaven_mcp.cli._help import (
    HelpfulGroup,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._runtime import Runtime

_LOGGER = logging.getLogger(__name__)


@click.group(cls=HelpfulGroup)
def config() -> None:
    """Inspect and validate the resolved configuration tree.

    Configuration is loaded and validated up front on every
    invocation. These commands view the merged result with secrets
    redacted ('show') or confirm the directory is valid, which is
    useful in CI ('validate').
    """


_OUTPUT_SHOW = OutputSpec(
    "object",
    (
        OutputField(
            "config_dir", "string", "Directory the configuration was loaded from."
        ),
        OutputField("cli", "object", "dh-mcp CLI defaults (output, daemon, request)."),
        OutputField("server", "object", "Parsed server.json; omitted when absent."),
        OutputField("community", "object", "Community config; omitted when absent."),
        OutputField("enterprise", "object", "Enterprise config; omitted when absent."),
    ),
    note="Post-merge configuration; secret-bearing fields are redacted to ***.",
)
_OUTPUT_VALIDATE = OutputSpec(
    "object",
    (
        OutputField(
            "valid", "boolean", "Always true; a failure exits 2 before this prints."
        ),
        OutputField(
            "config_dir", "string", "Absolute path of the validated directory."
        ),
        OutputField("message", "string", "Human-readable confirmation."),
    ),
)


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@config.command(
    "show",
    help_spec=HelpSpec(
        summary="Print the resolved configuration with secrets redacted.",
        description=(
            "Shows the post-merge view used at runtime. Secret-bearing "
            "fields (passwords, API keys) are replaced with *** via "
            "the schema's redaction hooks."
        ),
        output=_OUTPUT_SHOW,
        examples=(
            "$ dh-mcp config show",
            "$ dh-mcp -o json config show | jq .community",
        ),
        see_also=("dh-mcp config validate",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.CONFIG_INVALID,),
    ),
)
@click.pass_obj
@run_async
async def config_show(runtime: Runtime) -> None:
    """Print the resolved configuration with secrets redacted."""
    payload = dump_redacted(runtime.config, exclude_none=True)
    click.echo(format_output(payload, output=runtime.config.cli.output.format))


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@config.command(
    "validate",
    help_spec=HelpSpec(
        summary="Confirm the configuration is valid (exit 0 / 2).",
        description=(
            "Validation runs eagerly on every dh-mcp invocation, so a "
            "malformed file exits 2 with config_invalid before this command "
            "prints. This verb performs no extra work: when the eager load "
            "succeeds it emits a CI-friendly 'valid: true' payload. Use it "
            "as the explicit config check in CI pipelines."
        ),
        output=_OUTPUT_VALIDATE,
        examples=("$ dh-mcp config validate",),
        see_also=("dh-mcp config show",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.CONFIG_INVALID,),
    ),
)
@click.pass_obj
@run_async
async def config_validate(runtime: Runtime) -> None:
    """Confirm the configuration directory is valid.

    Validation already ran during runtime construction in
    :mod:`deephaven_mcp.cli._main`; if it had failed the CLI would
    have exited with :attr:`ErrorCode.CONFIG_INVALID` before
    dispatching to this handler. The verb's job is therefore to
    surface the success state in the active output mode (the
    structured ``valid: true`` payload is the value to operators
    and CI alike).
    """
    payload = {
        "valid": True,
        "config_dir": str(runtime.config_dir),
        "message": "Configuration validated successfully.",
    }
    click.echo(format_output(payload, output=runtime.config.cli.output.format))
