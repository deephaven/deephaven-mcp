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

from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._format import OutputMode, format_output
from deephaven_mcp.cli._help import (
    COMMON_ENV_VARS,
    HelpEntry,
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


def _describe_wraps(cmd: click.Command) -> dict[str, Any] | None:
    """Return the wrapped-MCP-tool binding for a command, or ``None``.

    Reads the wrapper metadata set on
    :class:`~deephaven_mcp.cli._help.HelpfulCommand`. Returns ``None``
    for any command that is not a :class:`HelpfulCommand` or that wraps
    no MCP tool (groups and non-wrapping verbs such as ``daemon`` and
    ``config``), so the manifest only carries the binding where it is
    meaningful. The schema-drift test and ``review-changes`` consume
    this so they need not import Python.

    Args:
        cmd (click.Command): The command to inspect.

    Returns:
        dict[str, Any] | None: ``{tools, intentionally_unsupported,
            router_params, client_only_params}`` (``tools`` is the union
            of ``wraps_tool`` and ``wraps_tools``, sorted) when the
            command wraps at least one tool; ``None`` otherwise.
    """
    if not isinstance(cmd, HelpfulCommand):
        return None
    tools = sorted({*cmd.wraps_tools, *([cmd.wraps_tool] if cmd.wraps_tool else [])})
    if not tools:
        return None
    return {
        "tools": tools,
        "intentionally_unsupported": sorted(cmd.intentionally_unsupported),
        "router_params": sorted(cmd.router_params),
        "client_only_params": sorted(cmd.client_only_params),
    }


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
    - ``wraps`` (dict | None): Wrapped-MCP-tool binding produced by
      :func:`_describe_wraps`. Always present; ``None`` for commands
      that wrap no MCP tool.
    """
    return {
        "name": cmd.name,
        "help": _clean_help(cmd.help),
        "short_help": _clean_help(cmd.short_help),
        "params": [_describe_param(p) for p in cmd.params],
        "subcommands": _describe_subcommands(cmd),
        "output": _describe_output(getattr(cmd, "output_spec", None)),
        "wraps": _describe_wraps(cmd),
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
    - ``commands`` (dict[str, dict]): Top-level command tree, one
      entry per registered noun group and meta command; each entry
      follows :func:`_describe_command`'s shape and recurses through
      any subcommand groups. Always present; empty when ``root`` is a
      plain :class:`click.Command`.
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
            {"name": e.name, "help": e.help} for e in COMMON_ENV_VARS
        ],
        "default_exit_codes": [
            {"code": ec.value, "help": ec.help_text} for ec in ExitCode
        ],
        "error_codes": [
            {"code": ec.value, "help": ec.help_text}
            for ec in sorted(ErrorCode, key=lambda e: e.value)
        ],
    }


def _resolve_command(root: click.Command, path: tuple[str, ...]) -> click.Command:
    """Resolve a command path against the live click command tree.

    Walks ``path`` token by token, descending through each
    :class:`click.Group`'s ``commands`` mapping.

    Args:
        root (click.Command): The command to start the walk from
            (the ``dh-mcp`` root group in production).
        path (tuple[str, ...]): Command-name tokens to descend,
            e.g. ``("daemon", "start")``.

    Returns:
        click.Command: The command reached by following ``path``.

    Raises:
        CliError: With :attr:`ErrorCode.COMMAND_NOT_FOUND` when a
            token names no subcommand of the current command, or when
            a token asks a non-group (leaf command) to descend.
    """
    current = root
    for index, token in enumerate(path):
        if not isinstance(current, click.Group) or token not in current.commands:
            resolved = " ".join(path[:index]) or (root.name or "dh-mcp")
            raise CliError(
                f"Unknown command path: {' '.join(path)!r} "
                f"(no command {token!r} under {resolved!r}).",
                code=ErrorCode.COMMAND_NOT_FOUND,
            )
        current = current.commands[token]
    return current


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
    note=(
        "Always JSON unless -o overrides; sorted for stable diffs. With a "
        "PATH argument, emits just that command's node — byte-identical to "
        "the object found at .commands.<path...> in the unscoped manifest, "
        "so the top-level manifest keys (version, error_codes, etc.) are "
        "absent."
    ),
)


@click.command(
    "introspect",
    cls=HelpfulCommand,
    output_spec=_OUTPUT_INTROSPECT,
    help=build_help(
        summary="Print the command tree as a structured manifest.",
        description=(
            "Designed for AI-agent self-discovery: prefer this over "
            "scraping --help. The manifest describes every command, "
            "option, argument, environment variable, exit code, and "
            "the stable error_code registry returned in error "
            "payloads. Pass a PATH (one or more command-name tokens, e.g. "
            "'daemon start') to emit just that command's node instead of "
            "the whole tree — the same object found at .commands.<path...> "
            "in the unscoped manifest. Defaults to JSON (so 'dh-mcp "
            "introspect | jq .' works without -o); honors the root "
            "-o/--output flag and DH_MCP_OUTPUT (json, yaml, or human). "
            "Runs without a valid configuration tree, so it works even "
            "when 'config validate' fails."
        ),
        arguments=(
            HelpEntry(
                "[PATH]...",
                "Optional command-name tokens (e.g. 'daemon start') "
                "scoping output to that command's node; omit for the "
                "full manifest.",
            ),
        ),
        output=_OUTPUT_INTROSPECT,
        examples=(
            "$ dh-mcp introspect | jq '.commands | keys'",
            "$ dh-mcp introspect daemon",
            "$ dh-mcp introspect daemon start",
            "$ dh-mcp introspect tool call | jq '.params'",
            "$ dh-mcp introspect | jq '.error_codes'",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.COMMAND_NOT_FOUND,),
        see_also=("dh-mcp --help",),
    ),
)
@click.argument("path", nargs=-1)
@click.pass_context
def introspect(ctx: click.Context, path: tuple[str, ...]) -> None:
    """Emit the introspection manifest for the root command, or one node.

    With no ``path``, emits the full manifest for the root command.
    With one or more ``path`` tokens, resolves them against the live
    command tree and emits just that command's node (see
    :func:`_describe_command`), raising
    :attr:`ErrorCode.COMMAND_NOT_FOUND` when the path does not resolve.

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
    if path:
        payload = _describe_command(_resolve_command(root_ctx.command, path))
    else:
        payload = build_manifest(root_ctx.command)
    output: OutputMode = root_ctx.params.get("output") or "json"
    click.echo(format_output(payload, output=output))
