"""``dh-mcp introspect``: noun-verb group for machine-readable CLI metadata.

The introspection manifest is the canonical agent self-discovery
surface. It encodes structured metadata for every command, option,
argument, and error code so agents can reason over the CLI surface
without scraping ``--help`` text. Like every command it defaults to
``json`` (the CLI is machine-first); pass ``-o human`` for
terminal-friendly output.

Two complementary access paths render the same metadata:

- The universal ``--introspect`` flag (wired in
  :mod:`deephaven_mcp.cli._help`) describes the command it is
  appended to — the machine-readable twin of ``--help``.
- This ``introspect`` group exposes whole-system / cross-cutting
  views: ``tree`` (the full manifest), ``command`` (one command's
  node), and ``errors`` (the error-code registry).

The manifest builders themselves (:func:`~deephaven_mcp.cli._help.build_manifest`
and friends) live in :mod:`deephaven_mcp.cli._help`, next to
``HelpfulCommand``, so the ``--introspect`` flag can reach them without a
circular import; this module only wires them into click commands.
"""

from __future__ import annotations

__all__ = ["introspect"]

import click

from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpfulGroup,
    OutputField,
    OutputSpec,
    _describe_command,
    _emit,
    _error_code_registry,
    _resolve_command,
    build_help,
    build_manifest,
)


@click.group(cls=HelpfulGroup)
def introspect() -> None:
    """Emit machine-readable CLI metadata for AI-agent self-discovery.

    Prefer these over scraping --help. The 'tree' verb prints the
    whole manifest (every command, option, argument, environment
    variable, exit code, and the stable error_code registry); the
    'command' verb prints one command's node; 'errors' prints the
    error_code registry. To describe a single command in place, append
    the universal --introspect flag to it instead (the machine-readable
    twin of --help), e.g. 'dh-mcp daemon start --introspect'.

    Every verb honors the root -o/--output flag and DH_MCP_OUTPUT
    (human, json, or yaml) and defaults to json like the rest of the
    CLI; pass '-o human' for terminal-friendly output. The group runs
    without a valid configuration tree, so it works even when 'config
    validate' fails — which is also why it cannot read cli.json's
    output.format (use -o/DH_MCP_OUTPUT instead).
    """


_OUTPUT_TREE = OutputSpec(
    "object",
    (
        OutputField("version", "string", "Installed deephaven-mcp package version."),
        OutputField("prog", "string", "Program name (dh-mcp)."),
        OutputField("help", "string", "Root command help text."),
        OutputField("global_options", "array", "Root-level options (one object each)."),
        OutputField(
            "universal_options",
            "array",
            "Options available on every command (--help, --introspect).",
        ),
        OutputField(
            "commands", "object", "Command tree; each entry recurses into subcommands."
        ),
        OutputField(
            "default_environment", "array", "Project-wide environment variables."
        ),
        OutputField("default_exit_codes", "array", "Project-wide exit-code contract."),
        OutputField(
            "error_codes", "array", "Stable error_code registry (code + help)."
        ),
    ),
    note="Sorted for stable diffs. The whole CLI in one object.",
)


@introspect.command(
    "tree",
    output_spec=_OUTPUT_TREE,
    help=build_help(
        summary="Print the whole command tree as a structured manifest.",
        description=(
            "The full manifest: every command, option, argument, "
            "environment variable, exit code, and the stable error_code "
            "registry returned in error payloads. This is the canonical "
            "agent self-discovery surface. Equivalent to 'dh-mcp "
            "--introspect'; for one command's node use 'introspect "
            "command' or append --introspect to that command."
        ),
        output=_OUTPUT_TREE,
        examples=(
            "$ dh-mcp introspect tree | jq '.commands | keys'",
            "$ dh-mcp introspect tree | jq '.error_codes'",
        ),
        exit_codes=(ExitCode.SUCCESS,),
        see_also=("dh-mcp --introspect", "dh-mcp introspect command"),
    ),
)
@click.pass_context
def introspect_tree(ctx: click.Context) -> None:
    """Emit the full introspection manifest for the root command."""
    _emit(ctx, build_manifest(ctx.find_root().command))


_OUTPUT_COMMAND = OutputSpec(
    "object",
    (
        OutputField("name", "string", "The command's invocation name."),
        OutputField("help", "string", "Full help text as rendered by --help."),
        OutputField("short_help", "string", "One-line summary."),
        OutputField(
            "params", "array", "Per-parameter descriptions (options + arguments)."
        ),
        OutputField(
            "subcommands", "object", "Map of subcommand name to node; empty for a leaf."
        ),
        OutputField("output", "object", "Structured output shape, or null."),
        OutputField("wraps", "object", "Wrapped-MCP-tool binding, or null."),
    ),
    note=(
        "The same object found at .commands.<path...> in 'introspect "
        "tree', and identical to '<path...> --introspect'."
    ),
)


@introspect.command(
    "command",
    output_spec=_OUTPUT_COMMAND,
    help=build_help(
        summary="Print one command's node from the manifest.",
        description=(
            "Resolves PATH (one or more command-name tokens) against the "
            "live command tree and emits just that command's node — the "
            "same object found at .commands.<path...> in 'introspect "
            "tree', and identical to appending --introspect to that "
            "command. Use 'introspect tree' for the full manifest with "
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
            "$ dh-mcp introspect command daemon",
            "$ dh-mcp introspect command daemon start",
            "$ dh-mcp introspect command tool call | jq '.params'",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.COMMAND_NOT_FOUND,),
        see_also=("dh-mcp introspect tree",),
    ),
)
@click.argument("path", nargs=-1, required=True)
@click.pass_context
def introspect_command(ctx: click.Context, path: tuple[str, ...]) -> None:
    """Emit one command's manifest node, resolved from PATH.

    Resolves ``path`` against the live command tree and emits that
    command's node (see :func:`_describe_command`), raising
    :attr:`ErrorCode.COMMAND_NOT_FOUND` when the path does not resolve.
    PATH is required (at least one token).
    """
    _emit(ctx, _describe_command(_resolve_command(ctx.find_root().command, path)))


_OUTPUT_ERRORS = OutputSpec(
    "list",
    (
        OutputField("code", "string", "Stable error_code string."),
        OutputField("help", "string", "What the code means."),
    ),
    note="The stable error_code registry returned in structured error payloads.",
)


@introspect.command(
    "errors",
    output_spec=_OUTPUT_ERRORS,
    help=build_help(
        summary="Print the stable error-code registry.",
        description=(
            "Every error_code string the CLI can return in a structured "
            "error payload, with its meaning. Agents use this to map an "
            "error_code field back to what went wrong. Also available as "
            "the 'error_codes' key of 'introspect tree'."
        ),
        output=_OUTPUT_ERRORS,
        examples=("$ dh-mcp introspect errors | jq '.[].code'",),
        exit_codes=(ExitCode.SUCCESS,),
        see_also=("dh-mcp introspect tree",),
    ),
)
@click.pass_context
def introspect_errors(ctx: click.Context) -> None:
    """Emit the stable error-code registry as a list of ``{code, help}``."""
    _emit(ctx, _error_code_registry())
