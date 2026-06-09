"""``dh-mcp introspect``: emit the full command tree as a structured manifest.

The manifest is the canonical agent self-discovery surface,
emitted in JSON by default and switchable to YAML / human via
``-o``. It encodes structured metadata for every command, option,
argument, and error code so agents can reason over the CLI surface
without scraping ``--help`` text.
"""

from __future__ import annotations

__all__ = ["build_manifest", "introspect"]

import json
from importlib import metadata
from typing import Any

import click

from deephaven_mcp.cli._errors import ErrorCode
from deephaven_mcp.cli._format import OutputMode, format_output
from deephaven_mcp.cli._help import (
    DEFAULT_ENVIRONMENT_LINES,
    DEFAULT_EXIT_CODE_LINES,
    HelpfulCommand,
    OutputField,
    OutputSpec,
    build_help,
)


def _clean_help(text: str | None) -> str:
    r"""Return help text with click's no-rewrap marker removed.

    build_help prefixes pre-formatted sections with a backspace
    marker (``\b\n``) so the terminal renderer preserves their
    layout; the manifest is a machine surface, so the control
    character is stripped before serialization.

    Args:
        text (str | None): Raw ``help`` / ``short_help`` text, or
            ``None`` when the command sets neither.

    Returns:
        str: The text with markers removed and surrounding
            whitespace stripped; ``""`` when ``text`` is ``None``.
    """
    return (text or "").replace("\b\n", "").strip()


def _describe_output(spec: OutputSpec | None) -> dict[str, Any] | None:
    """Return a JSON-safe description of a command's output shape.

    Args:
        spec (OutputSpec | None): The command's output spec, or
            ``None`` for groups and commands that declare none.

    Returns:
        dict[str, Any] | None: ``{mode, fields, note}`` where each
            field is ``{name, type, help}``; ``None`` when ``spec`` is
            ``None``.
    """
    if spec is None:
        return None
    return {
        "mode": spec.mode,
        "fields": [
            {"name": f.name, "type": f.type, "help": f.help} for f in spec.fields
        ],
        "note": spec.note,
    }


def _describe_param(param: click.Parameter) -> dict[str, Any]:
    """Return a JSON-safe description of a click parameter.

    Both :class:`click.Option` and :class:`click.Argument` produce the
    same key shape. ``type`` is always present (e.g. ``"choice"``,
    ``"text"``, ``"integer"``); ``choices`` is present when
    ``type == "choice"``; ``nargs`` is present for every parameter.
    Option-only keys (``opts``, ``secondary_opts``, ``is_flag``,
    ``multiple``, ``envvar``, ``default``) are present only for
    :class:`click.Option`.
    """
    info: dict[str, Any] = {
        "name": param.name,
    }
    # ``help`` is only set on :class:`click.Option`; :class:`click.Argument`
    # does not expose the attribute, so direct access would AttributeError.
    help_text = getattr(param, "help", None)
    if help_text:
        info["help"] = help_text
    # Stable lowercase string rather than ``param.__class__.__name__`` so the
    # manifest contract does not leak click's internal class names.
    info["kind"] = "option" if isinstance(param, click.Option) else "argument"
    info["type"] = param.type.name
    info["required"] = bool(param.required)
    info["nargs"] = param.nargs
    if isinstance(param.type, click.Choice):
        info["choices"] = list(param.type.choices)
    if isinstance(param, click.Option):
        info["opts"] = list(param.opts)
        info["secondary_opts"] = list(param.secondary_opts)
        info["is_flag"] = bool(param.is_flag)
        info["multiple"] = bool(param.multiple)
        if param.envvar is not None:
            envvar = param.envvar
            info["envvar"] = (
                list(envvar) if isinstance(envvar, list | tuple) else envvar
            )
        # Skip ``callable`` defaults (e.g. ``default=lambda: Path.home()``);
        # invoking them at introspect time could touch the filesystem or
        # environment, and the resulting value would not reflect the
        # state at the time the operator actually runs the command.
        if param.default is not None and not callable(param.default):
            try:
                json.dumps(param.default)
                info["default"] = param.default
            except TypeError:
                info["default"] = repr(param.default)
    return info


def _describe_subcommands(cmd: click.Command) -> dict[str, dict[str, Any]]:
    """Return ``{name: description}`` for a group; ``{}`` for a leaf command.

    Entries are ordered by sorted key so the manifest is reproducible
    across invocations.
    """
    if not isinstance(cmd, click.Group):
        return {}
    return {
        name: _describe_command(cmd.commands[name]) for name in sorted(cmd.commands)
    }


def _describe_command(cmd: click.Command) -> dict[str, Any]:
    """Recurse over a click command tree, returning a JSON-safe dict.

    Each returned dict has the keys:

    - ``name`` (str): The command's invocation name.
    - ``help`` (str): Full help text as rendered by ``--help``.
    - ``short_help`` (str): One-line summary derived by click from
      the first line of ``help``.
    - ``params`` (list[dict]): Per-parameter descriptions produced
      by :func:`_describe_param` (options and positional arguments).
    - ``subcommands`` (dict[str, dict]): Map of subcommand name to
      a nested description with the same shape, produced by
      :func:`_describe_subcommands`. **Empty when the command is a
      plain :class:`click.Command`** (leaf verb).
    - ``output`` (dict | None): Structured output shape produced by
      :func:`_describe_output` from the command's ``output_spec``
      (set on :class:`~deephaven_mcp.cli._help.HelpfulCommand`).
      Always present; ``None`` for groups and commands that declare
      no spec.
    """
    return {
        "name": cmd.name,
        "help": _clean_help(cmd.help),
        "short_help": _clean_help(cmd.short_help),
        "params": [_describe_param(p) for p in cmd.params],
        "subcommands": _describe_subcommands(cmd),
        "output": _describe_output(getattr(cmd, "output_spec", None)),
    }


def build_manifest(root: click.Command) -> dict[str, Any]:
    """Construct the introspection manifest for the root command tree.

    The manifest is the canonical agent-discoverable description of
    the entire CLI. All collections within it are sorted (commands
    by name, error codes by value) so repeated invocations produce
    byte-identical output suitable for snapshot tests and diffs.

    Manifest shape:

    - ``version`` (str): Installed ``deephaven-mcp`` package version,
      or ``"unknown"`` if package metadata is unavailable.
    - ``prog`` (str): Program invocation name (``"dh-mcp"``).
    - ``help`` (str): Root command's full help text.
    - ``global_options`` (list[dict]): Root-level options
      (``-o``, ``--timeout``, etc.) as produced by
      :func:`_describe_param`.
    - ``commands`` (dict[str, dict]): Top-level command tree
      (``daemon``, ``tool``, ``config``, ``introspect``); each
      entry follows :func:`_describe_command`'s shape and recurses
      through any subcommand groups. Always present; empty when
      ``root`` is a plain :class:`click.Command`.
    - ``default_environment`` (list[dict]): Project-wide
      environment variables (name + help) honored by every verb.
    - ``default_exit_codes`` (list[dict]): Project-wide exit-code
      contract (code + help).
    - ``error_codes`` (list[dict]): Stable :class:`ErrorCode`
      registry (code + help) returned in structured error
      responses. Agents use this to map ``error_code`` fields back
      to their meaning.

    Args:
        root (click.Command): The root command. In production this
            is the ``dh-mcp`` :class:`click.Group`; a plain
            :class:`click.Command` is also accepted, in which case
            ``commands`` is empty.

    Returns:
        dict[str, Any]: JSON / YAML serializable manifest.
    """
    # Fall back to ``"unknown"`` (rather than a fake ``"0.0.0"``) when
    # this module is imported outside an installed-package context.
    try:
        version = metadata.version("deephaven-mcp")
    except metadata.PackageNotFoundError:
        version = "unknown"
    return {
        "version": version,
        "prog": root.name or "dh-mcp",
        "help": _clean_help(root.help),
        "global_options": [_describe_param(p) for p in root.params],
        "commands": _describe_subcommands(root),
        "default_environment": [
            {"name": name, "help": help_} for name, help_ in DEFAULT_ENVIRONMENT_LINES
        ],
        "default_exit_codes": [
            {"code": code, "help": help_} for code, help_ in DEFAULT_EXIT_CODE_LINES
        ],
        "error_codes": [
            {"code": ec.value, "help": ec.help_text}
            for ec in sorted(ErrorCode, key=lambda e: e.value)
        ],
    }


_OUTPUT_INTROSPECT = OutputSpec(
    "object",
    (
        OutputField("version", "string", "Installed deephaven-mcp package version."),
        OutputField("prog", "string", "Program name (dh-mcp)."),
        OutputField("help", "string", "Root command help text."),
        OutputField("global_options", "array", "Root-level options (one object each)."),
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
    note="Always JSON unless -o overrides; sorted for stable diffs.",
)


@click.command(
    "introspect",
    cls=HelpfulCommand,
    output_spec=_OUTPUT_INTROSPECT,
    help=build_help(
        summary="Print the full command tree as a structured manifest.",
        description=(
            "Designed for AI-agent self-discovery: prefer this over "
            "scraping --help. The manifest describes every command, "
            "option, argument, environment variable, exit code, and "
            "the stable error_code registry returned in error "
            "payloads. Defaults to JSON (so 'dh-mcp introspect | jq .' "
            "works without -o); honors the root -o/--output flag and "
            "DH_MCP_OUTPUT (json, yaml, or human). Runs without a valid "
            "configuration tree, so it works even when 'config validate' "
            "fails."
        ),
        output=_OUTPUT_INTROSPECT,
        examples=(
            "$ dh-mcp introspect | jq '.commands | keys'",
            "$ dh-mcp introspect | jq '.commands.tool.subcommands.call.params'",
            "$ dh-mcp introspect | jq '.commands.tool.subcommands.call.output'",
            "$ dh-mcp introspect | jq '.error_codes'",
        ),
        exit_codes=((0, "success"), (2, "user-facing failure")),
        see_also=("dh-mcp --help",),
    ),
)
@click.pass_context
def introspect(ctx: click.Context) -> None:
    """Emit the introspection manifest for the root command.

    Output mode is resolved from the root ``-o/--output`` flag
    (or ``DH_MCP_OUTPUT``); defaults to ``"json"`` when neither is
    set so machine consumers can run ``dh-mcp introspect | jq``
    without prefixing the flag. The command bypasses
    :func:`deephaven_mcp.cli._runtime.load_runtime` (``ctx.obj`` is
    ``None``) so it remains usable even when the configuration
    directory is malformed — an agent diagnosing the failure can
    still discover the surface.
    """
    root_ctx = ctx.find_root()
    manifest = build_manifest(root_ctx.command)
    output: OutputMode = root_ctx.params.get("output") or "json"
    click.echo(format_output(manifest, output=output))
