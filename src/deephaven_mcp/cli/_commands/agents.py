"""``dhcli agents``: noun-verb group for machine-readable CLI metadata.

The agents surface is the CLI's ``--help`` for AI agents: structured
metadata for every command, option, argument, and error code so agents
can reason over the CLI without scraping help text. Like every command
it defaults to ``json`` (the CLI is machine-first); pass ``-o human``
for terminal-friendly output.

Two complementary access paths render the same metadata:

- The universal ``--agents`` flag (wired in
  :mod:`deephaven_mcp.cli._help`) describes the command it is
  appended to — the machine-readable twin of ``--help``.
- This ``agents`` group exposes whole-system / cross-cutting
  views: ``tree`` (the summary tree, or the complete manifest with
  ``--full``), ``command`` (one command's self-contained node), and
  ``errors`` (the error-code registry).

The manifest builders themselves (:func:`~deephaven_mcp.cli._help.build_manifest`
and friends) live in :mod:`deephaven_mcp.cli._help`, next to
``HelpfulCommand``, so the ``--agents`` flag can reach them without a
circular import; this module only wires them into click commands.
"""

from __future__ import annotations

__all__ = ["agents"]

import click

from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpfulGroup,
    HelpSpec,
    OutputField,
    OutputSpec,
    build_manifest,
    build_summary_tree,
    describe_command,
    emit_payload,
    error_code_registry,
    resolve_command,
)


@click.group(cls=HelpfulGroup)
def agents() -> None:
    """Emit machine-readable CLI metadata for AI-agent self-discovery.

    The --help for AI agents; prefer these over scraping help text.
    The 'tree' verb prints a compact summary of every command (add
    --full for the complete manifest: every option, argument,
    environment variable, exit code, and the stable error_code
    registry); the 'command' verb prints one command's full node;
    'errors' prints the error_code registry. To describe a single
    command in place, append the universal --agents flag to it instead
    (the machine-readable twin of --help), e.g. 'dhcli daemon start
    --agents'.

    Every verb honors the root -o/--output flag and DHCLI_OUTPUT
    (human, json, json-pretty, or yaml) and defaults to json like the
    rest of the CLI — compact single-line json; pass '-o json-pretty'
    for indented json or '-o human' for terminal-friendly output. The
    group runs without a valid configuration tree, so it works even
    when 'config validate' fails — which is also why it cannot read
    cli.json's output.format (use -o/DHCLI_OUTPUT instead).
    """


_OUTPUT_TREE = OutputSpec(
    "object",
    (
        OutputField("version", "string", "Installed deephaven-mcp package version."),
        OutputField("prog", "string", "Program name (dhcli)."),
        OutputField("summary", "string", "Root command one-line summary."),
        OutputField(
            "hint", "string", "How to drill down to full command nodes (default only)."
        ),
        OutputField(
            "commands",
            "object",
            "Nested {name: {summary, commands?}} map down to the leaves; "
            "full command nodes instead with --full.",
        ),
        OutputField("description", "string", "Root command description (--full only)."),
        OutputField("examples", "array", "Root command examples (--full only)."),
        OutputField(
            "global_options",
            "array",
            "Root-level option descriptions (--full only).",
        ),
        OutputField(
            "universal_options",
            "array",
            "Options available on every command: --help, --agents (--full only).",
        ),
        OutputField(
            "default_environment",
            "array",
            "Project-wide environment variables ({name, help}) (--full only).",
        ),
        OutputField(
            "default_exit_codes",
            "array",
            "Project-wide exit codes ({code, help}) (--full only).",
        ),
        OutputField(
            "error_codes",
            "array",
            "Stable error_code registry ({code, help}) (--full only).",
        ),
    ),
    note=(
        "Superset of both variants: fields marked (default only) appear "
        "only in the summary tree, fields marked (--full only) only in "
        "the complete manifest. Sorted for stable diffs; absent keys "
        "mean false/empty/default throughout."
    ),
)


@agents.command(
    "tree",
    # Must work without a valid configuration tree — agents learn the
    # surface before any config exists.
    needs_runtime=False,
    help_spec=HelpSpec(
        summary="Print the command tree (summary by default, --full for all).",
        description=(
            "The agent orientation surface. By default prints the compact "
            "summary tree: every command path with its one-line summary, "
            "plus a hint for drilling down — small enough to keep in "
            "context (equivalent to 'dhcli --agents'). With --full, "
            "prints the complete manifest instead: every command's full "
            "node (options, arguments, output schema, examples, error "
            "codes) plus the project-wide metadata (global_options, "
            "universal_options, default_environment, default_exit_codes, "
            "and the stable error_code registry returned in error "
            "payloads). For one command's node, use 'agents command' or "
            "append --agents to that command."
        ),
        output=_OUTPUT_TREE,
        examples=(
            "$ dhcli agents tree",
            "$ dhcli agents tree | jq '.commands | keys'",
            "$ dhcli agents tree --full | jq '.error_codes'",
        ),
        exit_codes=(ExitCode.SUCCESS,),
        see_also=("dhcli --agents", "dhcli agents command"),
    ),
)
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Print the complete manifest instead of the summary tree.",
)
@click.pass_context
def agents_tree(ctx: click.Context, full: bool) -> None:
    """Emit the summary tree, or the complete manifest with ``--full``."""
    root = ctx.find_root().command
    emit_payload(ctx, build_manifest(root) if full else build_summary_tree(root))


_OUTPUT_COMMAND = OutputSpec(
    "object",
    (
        OutputField("name", "string", "The command's invocation name."),
        OutputField("summary", "string", "One-line summary."),
        OutputField(
            "description", "string", "What the command does and when to use it."
        ),
        OutputField(
            "params",
            "array",
            "Per-parameter descriptions (options + positional arguments).",
        ),
        OutputField("output", "object", "Structured output shape."),
        OutputField("examples", "array", "Shell snippets."),
        OutputField("see_also", "array", "Related commands."),
        OutputField(
            "error_codes",
            "array",
            "Error codes the command can emit ({code, help} each).",
        ),
        OutputField(
            "exit_codes", "array", "Exit codes the command can return ({code, help})."
        ),
        OutputField(
            "environment", "array", "Environment variables honored ({name, help})."
        ),
        OutputField("wraps", "object", "Wrapped-MCP-tool binding."),
        OutputField(
            "subcommands",
            "object",
            "Groups only: {name: summary} one level down (full nodes with --full).",
        ),
    ),
    note=(
        "Absent keys mean false/empty/default. Identical to " "'<path...> --agents'."
    ),
)


@agents.command(
    "command",
    needs_runtime=False,
    help_spec=HelpSpec(
        summary="Print one command's full node from the manifest.",
        description=(
            "Resolves PATH (one or more command-name tokens) against the "
            "live command tree and emits that command's self-contained "
            "node — identical to appending --agents to the command. A "
            "group's node lists its subcommands as one-line summaries; "
            "pass --full to expand them into full nested nodes. Use "
            "'agents tree --full' for the whole manifest with "
            "project-wide metadata (version, error_codes, ...)."
        ),
        arguments=(
            HelpEntry(
                "PATH...",
                "One or more command-name tokens (e.g. 'daemon start').",
            ),
        ),
        output=_OUTPUT_COMMAND,
        examples=(
            "$ dhcli agents command daemon",
            "$ dhcli agents command daemon start",
            "$ dhcli agents command tool call | jq '.params'",
            "$ dhcli agents command session --full",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.COMMAND_NOT_FOUND,),
        see_also=("dhcli agents tree",),
    ),
)
@click.argument("path", nargs=-1, required=True)
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Expand a group's subcommands into full nested nodes.",
)
@click.pass_context
def agents_command(ctx: click.Context, path: tuple[str, ...], full: bool) -> None:
    """Emit one command's manifest node, resolved from PATH.

    Resolves ``path`` against the live command tree and emits that
    command's node (see :func:`~deephaven_mcp.cli._help.describe_command`), raising
    :attr:`ErrorCode.COMMAND_NOT_FOUND` when the path does not resolve.
    PATH is required (at least one token). ``--full`` recurses a
    group's subcommands into full nested nodes.
    """
    cmd = resolve_command(ctx.find_root().command, path)
    emit_payload(ctx, describe_command(cmd, recurse=full))


_OUTPUT_ERRORS = OutputSpec(
    "list",
    (
        OutputField("code", "string", "Stable error_code string."),
        OutputField("help", "string", "What the code means."),
    ),
    note="The stable error_code registry returned in structured error payloads.",
)


@agents.command(
    "errors",
    needs_runtime=False,
    help_spec=HelpSpec(
        summary="Print the stable error-code registry.",
        description=(
            "Every error_code string the CLI can return in a structured "
            "error payload, with its meaning. Agents use this to map an "
            "error_code field back to what went wrong. Also available as "
            "the 'error_codes' key of 'agents tree --full'."
        ),
        output=_OUTPUT_ERRORS,
        examples=("$ dhcli agents errors | jq '.[].code'",),
        exit_codes=(ExitCode.SUCCESS,),
        see_also=("dhcli agents tree --full",),
    ),
)
@click.pass_context
def agents_errors(ctx: click.Context) -> None:
    """Emit the stable error-code registry as a list of ``{code, help}``."""
    emit_payload(ctx, error_code_registry())
