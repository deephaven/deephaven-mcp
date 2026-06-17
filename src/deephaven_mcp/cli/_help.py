"""Help-text templating and the introspection manifest for the dh-mcp CLI.

This module describes every command twice over: to humans as ``--help``
text, and to machines as the introspection manifest.

Help side: every leaf command's help follows the same layout — a
one-line summary, an optional description paragraph, and a fixed
sequence of trailing sections (Arguments, Output, Examples, See also,
Environment, Exit codes, Error codes). ``build_help`` assembles them in
that order, defaulting the Environment and Exit codes sections from
shared constants. Help text is plain text, not reStructuredText: click
renders it verbatim in the terminal and the manifest surfaces it
verbatim. The pre-formatted trailing sections (aligned columns, one
example per line) are prefixed with click's no-rewrap marker so their
layout survives click's paragraph rewrapping.

Manifest side: ``build_manifest`` walks the live click tree into a
JSON-safe dict, and ``_describe_command`` renders one node; the
``introspect`` group and the universal ``--introspect`` flag both render
these. A command's structured output is described once as an
``OutputSpec``, which ``build_help`` renders into the Output section and
``HelpfulCommand`` carries for the manifest. The builders live here, next
to ``HelpfulCommand``, so the ``--introspect`` flag can reach them without
a circular import. Apply the ``_cli-help-standards`` skill when authoring
or reviewing command help.
"""

from __future__ import annotations

__all__ = [
    "COMMON_ENV_VARS",
    "HelpEntry",
    "HelpfulCommand",
    "HelpfulGroup",
    "OutputField",
    "OutputSpec",
    "build_help",
    "build_manifest",
]

import json
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import metadata
from typing import Any, Literal, assert_never

import click

from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._format import (
    DEFAULT_OUTPUT_MODE,
    OUTPUT_ENV_VAR,
    OutputMode,
    format_output,
)
from deephaven_mcp.config import DATA_DIR_ENV_VAR

# Click rewraps each help paragraph unless it begins with a backspace
# (\b) on its own line; prefixing the pre-formatted sections with this
# marker preserves their column alignment and one-example-per-line
# layout. introspect strips the marker so the manifest stays clean.
_NO_WRAP = "\b\n"


@dataclass(frozen=True)
class HelpEntry:
    """A name and its one-line help, rendered as an aligned two-column row.

    Used for the ``Arguments:`` and ``Environment:`` help sections.
    """

    name: str
    """The label — an argument metavar or an environment-variable name."""
    help: str
    """One-line description shown beside the name."""


COMMON_ENV_VARS: tuple[HelpEntry, ...] = (
    HelpEntry(
        DATA_DIR_ENV_VAR,
        "User-data root holding the config and runtime "
        "subdirectories (overrides the platform default).",
    ),
    HelpEntry(OUTPUT_ENV_VAR, "Output mode: human, json, or yaml."),
)
"""Standard env-var disclosures shared by every leaf command."""

OutputShape = Literal["object", "list", "text"]
"""Top-level shape of a command's structured (``-o json``) output."""


@dataclass(frozen=True)
class OutputField:
    """One field in a command's structured output."""

    name: str
    """JSON field name emitted under ``-o json``."""
    type: str
    """JSON type, e.g. ``string``, ``integer``, ``boolean``, ``object``, ``array``."""
    help: str
    """One-line description of the field."""


@dataclass(frozen=True)
class OutputSpec:
    """Structured description of a command's output.

    Rendered into the human-readable ``Output:`` section and the
    introspect manifest.
    """

    mode: OutputShape
    """Top-level shape: ``object``, ``list``, or ``text``."""
    fields: tuple[OutputField, ...] = ()
    """Per-field descriptions; empty for ``text`` mode."""
    note: str | None = None
    """Optional one-line note shown above the field list."""


def _introspect_callback(
    ctx: click.Context, param: click.Parameter, value: bool
) -> None:
    """Emit ``ctx.command``'s introspection manifest, then exit.

    The eager callback behind the universal ``--introspect`` flag, the
    machine-readable twin of ``--help``: emits the whole-tree manifest
    when invoked on the root group and a single command node otherwise,
    in the root ``-o/--output`` mode (or ``DH_MCP_OUTPUT``), defaulting
    to :data:`~deephaven_mcp.cli._format.DEFAULT_OUTPUT_MODE` (``json``)
    like every command. Pass ``-o human`` for terminal-friendly output.
    """
    if not value or ctx.resilient_parsing:
        return
    payload = (
        build_manifest(ctx.command)
        if ctx.command is ctx.find_root().command
        else _describe_command(ctx.command)
    )
    _emit(ctx, payload)
    ctx.exit()


def _build_introspect_option() -> click.Option:
    """Return a fresh eager ``--introspect`` option bound to its callback."""
    return click.Option(
        ["--introspect"],
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=_introspect_callback,
        help=(
            "Emit this command's manifest node and exit (machine twin of "
            "--help; rendered in the -o/--output mode, json by default)."
        ),
    )


def _params_with_introspect(params: list[click.Parameter]) -> list[click.Parameter]:
    """Append the lazy ``--introspect`` option to a command's parameter list."""
    params.append(_build_introspect_option())
    return params


class HelpfulCommand(click.Command):
    """Click command carrying structured output and MCP-tool-wrapping metadata."""

    def __init__(
        self,
        *args: Any,
        output_spec: OutputSpec | None = None,
        wraps_tool: str | None = None,
        wraps_tools: tuple[str, ...] = (),
        intentionally_unsupported: frozenset[str] = frozenset(),
        router_params: frozenset[str] = frozenset(),
        client_only_params: frozenset[str] = frozenset(),
        **kwargs: Any,
    ) -> None:
        """Store the wrapper metadata; defer the rest to :class:`click.Command`.

        Args:
            output_spec (OutputSpec | None): Structured description of the
                command's output, surfaced by ``dh-mcp introspect``.
            wraps_tool (str | None): Name of the single MCP tool the command
                wraps, or ``None`` when it wraps none or many.
            wraps_tools (tuple[str, ...]): Names of the MCP tools the command
                wraps when it fronts more than one.
            intentionally_unsupported (frozenset[str]): Wrapped-tool parameter
                names the command deliberately does not surface as a flag.
            router_params (frozenset[str]): Flag names not forwarded verbatim
                to one tool but that *are* a parameter of some wrapped tool
                (e.g. ``"system"`` on ``session create``, which selects the
                community/enterprise backend).
            client_only_params (frozenset[str]): Flag names that are not a
                parameter of any wrapped tool (e.g. ``"print_only"`` on
                ``session open``, which only controls local browser launch).
            *args (Any): Positional arguments forwarded to
                :class:`click.Command`.
            **kwargs (Any): Keyword arguments forwarded to
                :class:`click.Command`.
        """
        # ``*args`` / ``**kwargs`` is the standard click-subclass
        # pass-through to click.Command's broad constructor signature.
        super().__init__(*args, **kwargs)
        self.output_spec: OutputSpec | None = output_spec
        self.wraps_tool: str | None = wraps_tool
        self.wraps_tools: tuple[str, ...] = wraps_tools
        self.intentionally_unsupported: frozenset[str] = intentionally_unsupported
        self.router_params: frozenset[str] = router_params
        self.client_only_params: frozenset[str] = client_only_params

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Append the lazy ``--introspect`` option, like click's ``--help``.

        Injecting it here rather than in ``self.params`` keeps it out of
        the option-lifter and the introspect manifest (both read
        ``params``), exactly as click's own ``--help`` is invisible to
        them.
        """
        return _params_with_introspect(super().get_params(ctx))


class HelpfulGroup(click.Group):
    """Click group whose leaf commands default to :class:`HelpfulCommand`."""

    command_class = HelpfulCommand

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Append the lazy ``--introspect`` option; see :meth:`HelpfulCommand.get_params`.

        Also the seam that makes the root ``dh-mcp`` group expose
        ``--introspect``: ``cli`` in :mod:`deephaven_mcp.cli._main` is
        registered with ``cls=HelpfulGroup`` so the universal flag is
        available at every depth.
        """
        return _params_with_introspect(super().get_params(ctx))


def _aligned(pairs: Sequence[tuple[object, str]]) -> str:
    """Render ``(key, help)`` pairs as a left-aligned two-column block."""
    width = max(len(str(key)) for key, _ in pairs)
    return "\n".join(f"  {key!s:<{width}}  {help_}" for key, help_ in pairs)


def _output_lead(mode: OutputShape) -> str | None:
    """Return the one-line lead describing ``mode``; ``None`` for text output."""
    match mode:
        case "object":
            return "JSON object with fields:"
        case "list":
            return "JSON array; each element is an object with fields:"
        case "text":
            return None
    assert_never(mode)


def _render_output(spec: OutputSpec) -> str:
    """Render an :class:`OutputSpec` as the body of the ``Output:`` section."""
    lines: list[str] = []
    if spec.note:
        lines.append(f"  {spec.note}")
    lead = _output_lead(spec.mode)
    if lead and spec.fields:
        lines.append(f"  {lead}")
    if spec.fields:
        name_w = max(len(f.name) for f in spec.fields)
        type_w = max(len(f.type) for f in spec.fields)
        lines.extend(
            f"    {f.name:<{name_w}}  {f.type:<{type_w}}  {f.help}" for f in spec.fields
        )
    return "\n".join(lines)


def _section(label: str, pairs: Sequence[tuple[object, str]]) -> str:
    """Render ``(key, help)`` pairs as a labeled, no-rewrap, aligned block.

    Args:
        label (str): Section heading (e.g. ``"Arguments"``, ``"Exit codes"``).
        pairs (Sequence[tuple[object, str]]): ``(key, help)`` rows; callers
            extract these from :class:`HelpEntry` (``name``/``help``) or the
            code enums (``value``/``help_text``).
    """
    return f"{_NO_WRAP}{label}:\n{_aligned(pairs)}"


def build_help(
    *,
    summary: str,
    description: str | None = None,
    arguments: Sequence[HelpEntry] | None = None,
    output: OutputSpec | None = None,
    examples: Sequence[str] | None = None,
    see_also: Sequence[str] | None = None,
    environment: Sequence[HelpEntry] | None = None,
    exit_codes: Sequence[ExitCode] | None = None,
    error_codes: Sequence[ErrorCode] | None = None,
) -> str:
    """Compose a structured help string for a click command.

    Sections render in a fixed order: summary, description,
    ``Arguments``, ``Output``, ``Examples``, ``See also``,
    ``Environment``, ``Exit codes``, ``Error codes``. Pre-formatted
    sections are prefixed with click's no-rewrap marker so their
    column alignment survives click's paragraph rewrapping.

    Args:
        summary (str): Single-line summary, rendered verbatim as the
            command's first line (also used by ``click`` as the brief
            description in the parent group's listing).
        description (str | None): Paragraph covering what the command
            does and when to use it.
        arguments (Sequence[HelpEntry] | None): Positional arguments shown
            under an ``Arguments:`` heading. Click renders no help for
            positional arguments, so this is the only place they are
            documented.
        output (OutputSpec | None): Structured description of the
            command's output, rendered under an ``Output:`` heading.
        examples (Sequence[str] | None): Shell snippets shown under an
            ``Examples:`` heading. Each entry is prefixed with ``$ ``.
        see_also (Sequence[str] | None): Related commands shown under a
            ``See also:`` heading, one per line.
        environment (Sequence[HelpEntry] | None): Environment variables
            shown under an ``Environment:`` heading. Defaults to
            :data:`COMMON_ENV_VARS`.
        exit_codes (Sequence[ExitCode] | None): The :class:`ExitCode`
            members the command can return, rendered under an
            ``Exit codes:`` heading as ``(value, help_text)``. Defaults
            to every :class:`ExitCode` (0, 2, 3).
        error_codes (Sequence[ErrorCode] | None): The :class:`ErrorCode`
            members the command can emit, rendered under an ``Error codes:``
            heading as ``(value, help_text)``. Both code sections render
            from enum metadata so their text stays single-sourced. No
            default; omitted when ``None``.

    Returns:
        str: A multi-line help string with all sections joined by a
            single blank line.
    """
    sections: list[str] = [summary]
    if description:
        sections.append(description)
    if arguments:
        sections.append(_section("Arguments", [(e.name, e.help) for e in arguments]))
    if output is not None:
        sections.append(f"{_NO_WRAP}Output:\n{_render_output(output)}")
    if examples:
        body = "\n".join(f"  {line}" for line in examples)
        sections.append(f"{_NO_WRAP}Examples:\n{body}")
    if see_also:
        body = "\n".join(f"  {line}" for line in see_also)
        sections.append(f"{_NO_WRAP}See also:\n{body}")
    env = environment if environment is not None else COMMON_ENV_VARS
    if env:
        sections.append(_section("Environment", [(e.name, e.help) for e in env]))
    codes = exit_codes if exit_codes is not None else tuple(ExitCode)
    if codes:
        sections.append(_section("Exit codes", [(c.value, c.help_text) for c in codes]))
    if error_codes:
        sections.append(
            _section("Error codes", [(c.value, c.help_text) for c in error_codes])
        )
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Introspection manifest
#
# These builders walk the live click tree into a JSON-safe manifest. They
# live here, alongside ``HelpfulCommand``, so the ``--introspect`` flag
# callback above can call them without importing the ``introspect`` command
# module (which imports this one) — a circular import.
# ---------------------------------------------------------------------------


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

    Reads the wrapper metadata set on :class:`HelpfulCommand`. Returns
    ``None`` for any command that is not a :class:`HelpfulCommand` or
    that wraps no MCP tool (groups and non-wrapping verbs such as
    ``daemon`` and ``config``), so the manifest only carries the binding
    where it is meaningful. The schema-drift test and ``review-changes``
    consume this so they need not import Python.

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
      (set on :class:`HelpfulCommand`). Always present; ``None`` for
      groups and commands that declare no spec.
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


def _error_code_registry() -> list[dict[str, Any]]:
    """Return the stable error-code registry as sorted ``{code, help}`` entries.

    Agents use this to map an ``error_code`` field in a structured
    error response back to its meaning. Shared by :func:`build_manifest`
    (the ``error_codes`` key) and the ``introspect errors`` verb.
    """
    return [
        {"code": ec.value, "help": ec.help_text}
        for ec in sorted(ErrorCode, key=lambda e: e.value)
    ]


def _help_option() -> click.Option:
    """Return a representative ``--help`` option for manifest disclosure.

    Click synthesizes the real ``--help`` per command via
    ``help_option_names`` and owns its canonical wording; this stand-in
    mirrors it for :func:`build_manifest`'s ``universal_options`` so the
    manifest can advertise the flag without a live click context.
    """
    return click.Option(
        ["--help"],
        is_flag=True,
        expose_value=False,
        help="Show this message and exit.",
    )


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
    - ``universal_options`` (list[dict]): Options available on *every*
      command (``--help``, ``--introspect``). Injected via
      ``get_params`` rather than ``params``, so they never appear under
      a command's ``params``; this key is where an agent discovers them.
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
        "universal_options": [
            _describe_param(o) for o in (_help_option(), _build_introspect_option())
        ],
        "commands": _describe_subcommands(root),
        "default_environment": [
            {"name": e.name, "help": e.help} for e in COMMON_ENV_VARS
        ],
        "default_exit_codes": [
            {"code": ec.value, "help": ec.help_text} for ec in ExitCode
        ],
        "error_codes": _error_code_registry(),
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


def _emit(ctx: click.Context, payload: Any) -> None:
    """Render ``payload`` in the root ``-o/--output`` mode and print it.

    Output mode is resolved from the root ``-o/--output`` flag or
    ``DH_MCP_OUTPUT``, falling back to :data:`DEFAULT_OUTPUT_MODE` (``json``).
    The introspection surfaces run without the validated config, so they
    cannot consult ``cli.json``'s ``output.format``; use ``-o`` (or set
    ``DH_MCP_OUTPUT``) to opt into ``human``/``yaml`` output.
    """
    output: OutputMode = ctx.find_root().params.get("output") or DEFAULT_OUTPUT_MODE
    click.echo(format_output(payload, output=output))
