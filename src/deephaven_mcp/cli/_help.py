"""Help-text templating and the agents manifest for the dh-mcp CLI.

This module describes every command twice over from one source: a
command's ``HelpSpec`` renders to humans as ``--help`` text and to
machines as its agents-manifest node, so the two surfaces cannot
drift.

Help side: every leaf command declares a ``HelpSpec`` — a one-line
summary, an optional description paragraph, and the structured content
of the trailing sections (Arguments, Output, Examples, See also,
Environment, Exit codes, Error codes). ``build_help`` renders the
sections in that fixed order, defaulting the Environment and Exit
codes sections from shared constants. Help text is plain text, not
reStructuredText: click renders it verbatim in the terminal. The
pre-formatted trailing sections (aligned columns, one example per
line) are prefixed with click's no-rewrap marker so their layout
survives click's paragraph rewrapping.

Manifest side: ``build_summary_tree`` renders the compact orientation
view (every command path with its one-line summary),
``build_manifest`` walks the live click tree into the complete
JSON-safe manifest, and ``describe_command`` renders one
self-contained node from the command's ``HelpSpec``; the ``agents``
group and the universal ``--agents`` flag both render these. Node keys
are sparse — an absent key means false, empty, or the default. A
command's structured output is described once as an ``OutputSpec``
inside its ``HelpSpec``. The builders live here, next to
``HelpfulCommand``, so the ``--agents`` flag can reach them without
a circular import. Apply the ``_cli-help-standards`` skill when authoring
or reviewing command help.
"""

from __future__ import annotations

__all__ = [
    "COMMON_ENV_VARS",
    "HelpEntry",
    "HelpSpec",
    "HelpfulCommand",
    "HelpfulGroup",
    "OutputField",
    "OutputShape",
    "OutputSpec",
    "build_help",
    "build_manifest",
    "build_summary_tree",
    "describe_command",
    "emit_payload",
    "error_code_registry",
    "resolve_command",
]

import inspect
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
# layout. the manifest builders strip the marker so the manifest stays clean.
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
    HelpEntry(OUTPUT_ENV_VAR, "Output mode: human, json, json-pretty, or yaml."),
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
    agents manifest.
    """

    mode: OutputShape
    """Top-level shape: ``object``, ``list``, or ``text``."""
    fields: tuple[OutputField, ...] = ()
    """Per-field descriptions; empty for ``text`` mode."""
    note: str | None = None
    """Optional one-line note shown above the field list."""


@dataclass(frozen=True)
class HelpSpec:
    """Structured help content for one command.

    The single source for a command's help: :func:`build_help` renders
    it as the ``--help`` text, and the agents manifest emits its
    fields directly. Field semantics and defaults match the
    corresponding :func:`build_help` parameters.
    """

    summary: str
    """Single-line summary, rendered verbatim as the command's first line."""
    description: str | None = None
    """Paragraph covering what the command does and when to use it."""
    arguments: Sequence[HelpEntry] | None = None
    """Positional arguments shown under the ``Arguments:`` heading."""
    output: OutputSpec | None = None
    """Structured output description; also the command's ``output_spec``."""
    examples: Sequence[str] | None = None
    """Shell snippets shown under the ``Examples:`` heading."""
    see_also: Sequence[str] | None = None
    """Related commands shown under the ``See also:`` heading."""
    environment: Sequence[HelpEntry] | None = None
    """Environment variables; ``None`` means :data:`COMMON_ENV_VARS`."""
    exit_codes: Sequence[ExitCode] | None = None
    """Exit codes the command can return; ``None`` means every :class:`ExitCode`."""
    error_codes: Sequence[ErrorCode] | None = None
    """Error codes the command can emit; ``None`` renders no section."""


def _render_help_spec(spec: HelpSpec) -> str:
    """Render ``spec`` as the command's ``--help`` text via :func:`build_help`."""
    return build_help(
        summary=spec.summary,
        description=spec.description,
        arguments=spec.arguments,
        output=spec.output,
        examples=spec.examples,
        see_also=spec.see_also,
        environment=spec.environment,
        exit_codes=spec.exit_codes,
        error_codes=spec.error_codes,
    )


def _agents_callback(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    """Emit ``ctx.command``'s agents node, then exit.

    The eager callback behind the universal ``--agents`` flag, the
    machine-readable twin of ``--help``: emits the summary tree when
    invoked on the root group and a single self-contained command node
    otherwise, in the root ``-o/--output`` mode (or ``DH_MCP_OUTPUT``),
    defaulting to
    :data:`~deephaven_mcp.cli._format.DEFAULT_OUTPUT_MODE` (``json``)
    like every command. Pass ``-o human`` for terminal-friendly output.

    Root-level ordering caveat: both ``--agents`` and the root ``-o``
    are eager, and click processes eager options in command-line
    order, so a root-level ``-o`` *after* ``--agents`` is honored only
    because ``main()``'s ``_lift_root_options`` hoists root options to
    the front first. A direct ``cli.main()`` / ``CliRunner`` call with
    ``["--agents", "-o", "human"]`` (no lifter) falls back to the
    default mode.
    """
    if not value or ctx.resilient_parsing:
        return
    payload = (
        build_summary_tree(ctx.command)
        if ctx.command is ctx.find_root().command
        else describe_command(ctx.command)
    )
    emit_payload(ctx, payload)
    ctx.exit()


def _build_agents_option() -> click.Option:
    """Return a fresh eager ``--agents`` option bound to its callback."""
    return click.Option(
        ["--agents"],
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=_agents_callback,
        help=(
            "Emit this command's manifest node and exit (machine twin of "
            "--help; rendered in the -o/--output mode — compact json by "
            "default, -o json-pretty for indented)."
        ),
    )


def _params_with_agents(params: list[click.Parameter]) -> list[click.Parameter]:
    """Return ``params`` plus the lazy ``--agents`` option, as a new list."""
    # A new list, never an in-place append: click's ``get_params`` returns
    # ``self.params`` itself when a command disables its help option, and
    # appending to that would permanently grow the command's parameter
    # list on every call.
    return [*params, _build_agents_option()]


class HelpfulCommand(click.Command):
    """Click command carrying structured output and MCP-tool-wrapping metadata."""

    def __init__(
        self,
        *args: Any,
        help_spec: HelpSpec | None = None,
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
            help_spec (HelpSpec | None): Structured help content. When set,
                the command's ``help`` text is rendered from it (overriding
                any ``help`` keyword) and ``output_spec`` defaults to its
                ``output`` field.
            output_spec (OutputSpec | None): Structured description of the
                command's output, surfaced by ``dh-mcp agents``.
                Defaults to ``help_spec.output`` when a spec is given.
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
        if help_spec is not None:
            kwargs["help"] = _render_help_spec(help_spec)
            if output_spec is None:
                output_spec = help_spec.output
        # ``*args`` / ``**kwargs`` is the standard click-subclass
        # pass-through to click.Command's broad constructor signature.
        super().__init__(*args, **kwargs)
        self.help_spec: HelpSpec | None = help_spec
        self.output_spec: OutputSpec | None = output_spec
        self.wraps_tool: str | None = wraps_tool
        self.wraps_tools: tuple[str, ...] = wraps_tools
        self.intentionally_unsupported: frozenset[str] = intentionally_unsupported
        self.router_params: frozenset[str] = router_params
        self.client_only_params: frozenset[str] = client_only_params

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Append the lazy ``--agents`` option, like click's ``--help``.

        Injecting it here rather than in ``self.params`` keeps it out of
        the option-lifter and the agents manifest (both read
        ``params``), exactly as click's own ``--help`` is invisible to
        them.
        """
        return _params_with_agents(super().get_params(ctx))


class HelpfulGroup(click.Group):
    """Click group whose leaf commands default to :class:`HelpfulCommand`."""

    command_class = HelpfulCommand

    def __init__(
        self,
        *args: Any,
        help_spec: HelpSpec | None = None,
        **kwargs: Any,
    ) -> None:
        """Render ``help`` from ``help_spec`` when given; defer to :class:`click.Group`.

        Args:
            help_spec (HelpSpec | None): Structured help content. When set,
                the group's ``help`` text is rendered from it (overriding
                any ``help`` keyword).
            *args (Any): Positional arguments forwarded to
                :class:`click.Group`.
            **kwargs (Any): Keyword arguments forwarded to
                :class:`click.Group`.
        """
        if help_spec is not None:
            kwargs["help"] = _render_help_spec(help_spec)
        # ``*args`` / ``**kwargs`` is the standard click-subclass
        # pass-through to click.Group's broad constructor signature.
        super().__init__(*args, **kwargs)
        self.help_spec: HelpSpec | None = help_spec

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Append the lazy ``--agents`` option; see :meth:`HelpfulCommand.get_params`.

        Also the seam that makes the root ``dh-mcp`` group expose
        ``--agents``: ``cli`` in :mod:`deephaven_mcp.cli._main` is
        registered with ``cls=HelpfulGroup`` so the universal flag is
        available at every depth.
        """
        return _params_with_agents(super().get_params(ctx))


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
# Agents manifest
#
# These builders walk the live click tree into a JSON-safe manifest. They
# live here, alongside ``HelpfulCommand``, so the ``--agents`` flag
# callback above can call them without importing the ``agents`` command
# module (which imports this one) — a circular import.
# ---------------------------------------------------------------------------


def _split_help_text(text: str | None) -> tuple[str, str | None]:
    r"""Split raw help text into a one-line summary and a description.

    Used for commands that carry no :class:`HelpSpec` (docstring-help
    groups and plain :class:`click.Command` instances). The text is
    dedented with :func:`inspect.cleandoc` (docstring help keeps its
    source indentation until click renders it) and stripped of click's
    no-rewrap markers (``\b\n``).

    Args:
        text (str | None): Raw ``help`` text, or ``None`` when the
            command sets none.

    Returns:
        tuple[str, str | None]: ``(summary, description)`` where
            ``summary`` is the first paragraph collapsed to one line
            (``""`` when ``text`` is ``None``) and ``description`` is
            the remaining text, or ``None`` when there is none.
    """
    cleaned = inspect.cleandoc((text or "").replace("\b\n", ""))
    first, _, rest = cleaned.partition("\n\n")
    summary = " ".join(first.split())
    description = rest.strip() or None
    return summary, description


def _help_spec_of(cmd: click.Command) -> HelpSpec | None:
    """Return the command's :class:`HelpSpec`, or ``None`` when it has none."""
    if isinstance(cmd, HelpfulCommand | HelpfulGroup):
        return cmd.help_spec
    return None


def _summary_and_description(cmd: click.Command) -> tuple[str, str | None]:
    """Return ``(summary, description)`` for ``cmd``.

    Reads the :class:`HelpSpec` when the command carries one, falling
    back to splitting its raw help text with :func:`_split_help_text`.
    """
    spec = _help_spec_of(cmd)
    if spec is not None:
        return spec.summary, spec.description
    return _split_help_text(cmd.help)


def _summary_of(cmd: click.Command) -> str:
    """Return the one-line summary for ``cmd``.

    Reads the :class:`HelpSpec` summary when the command carries one,
    falling back to the first paragraph of its raw help text.
    """
    return _summary_and_description(cmd)[0]


def _describe_output(spec: OutputSpec) -> dict[str, Any]:
    """Return a JSON-safe description of a command's output shape.

    Args:
        spec (OutputSpec): The command's output spec.

    Returns:
        dict[str, Any]: ``{mode}`` plus ``fields`` (each
            ``{name, type, help}``) when the spec declares fields and
            ``note`` when it carries one — sparse keys, like the rest
            of the node.
    """
    info: dict[str, Any] = {"mode": spec.mode}
    if spec.fields:
        info["fields"] = [
            {"name": f.name, "type": f.type, "help": f.help} for f in spec.fields
        ]
    if spec.note is not None:
        info["note"] = spec.note
    return info


def _argument_help_map(spec: HelpSpec | None) -> dict[str, str]:
    """Map click parameter names to the help of the spec's ``Arguments`` entries.

    An entry name is its metavar (``"PATH..."``, ``"PQ_NAME"``); the
    corresponding click parameter name is that metavar lowercased with
    any trailing ``...`` removed (``"path"``, ``"pq_name"``).
    """
    if spec is None or not spec.arguments:
        return {}
    return {e.name.rstrip(".").lower(): e.help for e in spec.arguments}


def _describe_param(
    param: click.Parameter, argument_help: dict[str, str] | None = None
) -> dict[str, Any]:
    """Return a JSON-safe, sparse description of a click parameter.

    Keys are sparse: a key absent from the dict means false, empty, or
    the default (``nargs`` 1). Always present: ``name``, ``kind``
    (``"option"`` or ``"argument"``), and ``type`` (e.g. ``"choice"``,
    ``"text"``, ``"integer"``). Present when applicable: ``help``,
    ``required`` (only ``true``), ``nargs`` (only when not 1),
    ``choices`` (when ``type == "choice"``), and the option-only keys
    ``opts``, ``secondary_opts``, ``is_flag`` (only ``true``),
    ``multiple`` (only ``true``), ``envvar``, and ``default``.

    Args:
        param (click.Parameter): The parameter to describe.
        argument_help (dict[str, str] | None): Help text for positional
            arguments keyed by parameter name, from
            :func:`_argument_help_map`; click itself carries no help
            for :class:`click.Argument`.
    """
    info: dict[str, Any] = {
        "name": param.name,
    }
    if isinstance(param, click.Option):
        if param.help:
            info["help"] = param.help
    elif argument_help:
        entry_help = argument_help.get(param.name or "")
        if entry_help:
            info["help"] = entry_help
    # Stable lowercase string rather than ``param.__class__.__name__`` so the
    # manifest contract does not leak click's internal class names.
    info["kind"] = "option" if isinstance(param, click.Option) else "argument"
    info["type"] = param.type.name
    if param.required:
        info["required"] = True
    if param.nargs != 1:
        info["nargs"] = param.nargs
    if isinstance(param.type, click.Choice):
        info["choices"] = list(param.type.choices)
    if isinstance(param, click.Option):
        _describe_option_extras(param, info)
    return info


def _describe_option_extras(param: click.Option, info: dict[str, Any]) -> None:
    """Add the option-only sparse keys to a parameter description in place.

    Adds ``opts`` always, and ``secondary_opts``, ``is_flag`` (only
    ``true``), ``multiple`` (only ``true``), ``envvar``, and ``default``
    when applicable.

    Args:
        param (click.Option): The option being described.
        info (dict[str, Any]): The description dict from
            :func:`_describe_param`, mutated in place.
    """
    info["opts"] = list(param.opts)
    if param.secondary_opts:
        info["secondary_opts"] = list(param.secondary_opts)
    if param.is_flag:
        info["is_flag"] = True
    if param.multiple:
        info["multiple"] = True
    if param.envvar is not None:
        envvar = param.envvar
        info["envvar"] = list(envvar) if isinstance(envvar, list | tuple) else envvar
    # Skip ``callable`` defaults (e.g. ``default=lambda: Path.home()``);
    # invoking them at manifest-build time could touch the filesystem or
    # environment, and the resulting value would not reflect the
    # state at the time the operator actually runs the command. A
    # flag's ``False`` default is implied by the sparse-key contract.
    skip_default = param.default is None or (param.is_flag and param.default is False)
    if not skip_default and not callable(param.default):
        try:
            json.dumps(param.default)
            info["default"] = param.default
        except TypeError:
            # Open-set fallback: a default json cannot encode is recorded
            # as its ``repr`` so the manifest stays JSON-safe.
            info["default"] = repr(param.default)


def _describe_wraps(cmd: click.Command) -> dict[str, Any] | None:
    """Return the wrapped-MCP-tool binding for a command, or ``None``.

    Reads the wrapper metadata set on :class:`HelpfulCommand`. Returns
    ``None`` for any command that is not a :class:`HelpfulCommand` or
    that wraps no MCP tool (groups and non-wrapping verbs such as
    ``daemon`` and ``config``), so the manifest only carries the binding
    where it is meaningful.

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


def _describe_subcommands(
    cmd: click.Group, *, include_defaults: bool, recurse: bool
) -> dict[str, Any]:
    """Return the ``subcommands`` map for a group node.

    Entries are ordered by sorted key so the manifest is reproducible
    across invocations.

    Args:
        cmd (click.Group): The group whose subcommands are described.
        include_defaults (bool): Forwarded to :func:`describe_command`
            when recursing.
        recurse (bool): When ``True``, each entry is a full nested node
            from :func:`describe_command`; when ``False``, each entry
            is the subcommand's one-line summary string (the bounded
            view a standalone group node carries).
    """
    if recurse:
        return {
            name: describe_command(
                cmd.commands[name],
                include_defaults=include_defaults,
                recurse=True,
            )
            for name in sorted(cmd.commands)
        }
    return {name: _summary_of(cmd.commands[name]) for name in sorted(cmd.commands)}


def _describe_spec_extras(
    node: dict[str, Any], spec: HelpSpec, *, include_defaults: bool
) -> None:
    """Add the spec-sourced sparse keys to a command node in place.

    Adds ``examples``, ``see_also``, ``error_codes``, ``exit_codes``,
    and ``environment`` when applicable, per the node contract in
    :func:`describe_command`.

    Args:
        node (dict[str, Any]): The node dict from
            :func:`describe_command`, mutated in place.
        spec (HelpSpec): The command's help spec.
        include_defaults (bool): When ``True`` (standalone nodes), the
            resolved ``environment`` and ``exit_codes`` are always
            inlined and code entries carry their meanings; when
            ``False`` (whole-tree manifest), default entries are
            omitted and code entries are bare values.
    """
    if spec.examples:
        node["examples"] = list(spec.examples)
    if spec.see_also:
        node["see_also"] = list(spec.see_also)
    if spec.error_codes:
        node["error_codes"] = [
            ({"code": c.value, "help": c.help_text} if include_defaults else c.value)
            for c in spec.error_codes
        ]
    exit_codes = spec.exit_codes if spec.exit_codes is not None else tuple(ExitCode)
    if exit_codes and (include_defaults or spec.exit_codes is not None):
        node["exit_codes"] = [
            ({"code": c.value, "help": c.help_text} if include_defaults else c.value)
            for c in exit_codes
        ]
    environment = spec.environment if spec.environment is not None else COMMON_ENV_VARS
    if environment and (include_defaults or spec.environment is not None):
        node["environment"] = [{"name": e.name, "help": e.help} for e in environment]


def describe_command(
    cmd: click.Command, *, include_defaults: bool = True, recurse: bool = False
) -> dict[str, Any]:
    """Describe one command as a JSON-safe, sparse node dict.

    Keys are sparse: a key absent from the node means the command has
    none of that content. Always present: ``name`` and ``summary``.
    Present when applicable:

    - ``description`` (str): What the command does and when to use it.
    - ``params`` (list[dict]): Options and positional arguments from
      :func:`_describe_param`; positional arguments carry the help of
      the spec's ``Arguments`` entries.
    - ``output`` (dict): Structured output shape from
      :func:`_describe_output`.
    - ``examples`` (list[str]): Shell snippets.
    - ``see_also`` (list[str]): Related commands.
    - ``error_codes`` (list): The codes the command can emit —
      ``{code, help}`` entries in a standalone node (decodable without
      the root registry), bare code strings inside the whole-tree
      manifest (the root ``error_codes`` registry carries the
      meanings).
    - ``exit_codes`` (list): The codes the command can return —
      ``{code, help}`` entries in a standalone node, bare integers
      inside the whole-tree manifest (see ``default_exit_codes``).
    - ``environment`` (list[dict]): ``{name, help}`` entries.
    - ``wraps`` (dict): Wrapped-MCP-tool binding from
      :func:`_describe_wraps`.
    - ``subcommands`` (dict): Groups only — subcommand summaries, or
      full nested nodes when ``recurse`` is set (see
      :func:`_describe_subcommands`).

    Args:
        cmd (click.Command): The command to describe.
        include_defaults (bool): When ``True`` (standalone nodes), the
            resolved ``environment`` and ``exit_codes`` are always
            inlined and code entries carry their meanings, so the node
            is self-contained. When ``False`` (nodes inside the
            whole-tree manifest), code meanings and entries the spec
            leaves unset (the project defaults) are hoisted to the
            root's ``error_codes`` / ``default_exit_codes`` /
            ``default_environment`` keys, which carry them once.
        recurse (bool): Forwarded to :func:`_describe_subcommands` for
            groups.
    """
    spec = _help_spec_of(cmd)
    summary, description = _summary_and_description(cmd)
    node: dict[str, Any] = {"name": cmd.name, "summary": summary}
    if description:
        node["description"] = description
    argument_help = _argument_help_map(spec)
    if cmd.params:
        node["params"] = [_describe_param(p, argument_help) for p in cmd.params]
    if isinstance(cmd, HelpfulCommand) and cmd.output_spec is not None:
        node["output"] = _describe_output(cmd.output_spec)
    if spec is not None:
        _describe_spec_extras(node, spec, include_defaults=include_defaults)
    wraps = _describe_wraps(cmd)
    if wraps is not None:
        node["wraps"] = wraps
    if isinstance(cmd, click.Group):
        node["subcommands"] = _describe_subcommands(
            cmd, include_defaults=include_defaults, recurse=recurse
        )
    return node


def error_code_registry() -> list[dict[str, Any]]:
    """Return the stable error-code registry as sorted ``{code, help}`` entries.

    Agents use this to map an ``error_code`` field in a structured
    error response back to its meaning.
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


def _package_version() -> str:
    """Return the installed ``deephaven-mcp`` version, or ``"unknown"``."""
    # Fall back to ``"unknown"`` (rather than a fake ``"0.0.0"``) when
    # this module is imported outside an installed-package context.
    try:
        return metadata.version("deephaven-mcp")
    except metadata.PackageNotFoundError:
        return "unknown"


_SUMMARY_TREE_HINT = (
    "Summary view. Run 'dh-mcp agents command <path...>' (or append "
    "--agents to any command) for one command's full node, 'dh-mcp "
    "agents tree --full' for the complete manifest, and 'dh-mcp "
    "agents errors' for the error-code registry."
)
"""Drill-down pointer carried by the summary tree so it self-describes."""


def _summary_commands(group: click.Group) -> dict[str, Any]:
    """Return the nested ``{name: {summary, commands?}}`` summary map.

    Each entry carries the command's one-line ``summary``; group
    entries additionally carry ``commands``, recursing to the leaves.
    Entries are ordered by sorted key.
    """
    out: dict[str, Any] = {}
    for name in sorted(group.commands):
        cmd = group.commands[name]
        entry: dict[str, Any] = {"summary": _summary_of(cmd)}
        if isinstance(cmd, click.Group):
            entry["commands"] = _summary_commands(cmd)
        out[name] = entry
    return out


def build_summary_tree(root: click.Command) -> dict[str, Any]:
    """Construct the compact orientation view of the command tree.

    The default output of ``dh-mcp agents tree`` and the root
    ``--agents`` flag: every command path with its one-line summary,
    small enough to sit in an agent's context. Shape:

    - ``version`` (str): Installed ``deephaven-mcp`` package version.
    - ``prog`` (str): Program invocation name (``"dh-mcp"``).
    - ``summary`` (str): The root command's one-line summary.
    - ``hint`` (str): How to drill down to full nodes and the complete
      manifest.
    - ``commands`` (dict): Nested ``{name: {summary, commands?}}`` map
      from :func:`_summary_commands`; empty when ``root`` is a plain
      :class:`click.Command`.

    Args:
        root (click.Command): The root command. In production this is
            the ``dh-mcp`` :class:`click.Group`.

    Returns:
        dict[str, Any]: JSON / YAML serializable summary tree.
    """
    return {
        "version": _package_version(),
        "prog": root.name or "dh-mcp",
        "summary": _summary_of(root),
        "hint": _SUMMARY_TREE_HINT,
        "commands": (_summary_commands(root) if isinstance(root, click.Group) else {}),
    }


def build_manifest(root: click.Command) -> dict[str, Any]:
    """Construct the complete agents manifest for the root command tree.

    The manifest is the canonical agent-discoverable description of
    the entire CLI, emitted by ``dh-mcp agents tree --full``. All
    collections within it are sorted (commands by name, error codes
    by value) so repeated invocations produce byte-identical output
    suitable for snapshot tests and diffs.

    Manifest shape:

    - ``version`` (str): Installed ``deephaven-mcp`` package version,
      or ``"unknown"`` if package metadata is unavailable.
    - ``prog`` (str): Program invocation name (``"dh-mcp"``).
    - ``summary`` (str): The root command's one-line summary.
    - ``description`` (str): The root command's description, when set.
    - ``examples`` (list[str]): The root command's examples, when set.
    - ``global_options`` (list[dict]): Root-level options
      (``-o``, ``--timeout``, etc.) as produced by
      :func:`_describe_param`.
    - ``universal_options`` (list[dict]): Options available on *every*
      command (``--help``, ``--agents``). Injected via
      ``get_params`` rather than ``params``, so they never appear under
      a command's ``params``; this key is where an agent discovers them.
    - ``commands`` (dict[str, dict]): Top-level command tree, one
      entry per registered noun group and meta command; each entry
      follows :func:`describe_command`'s shape and recurses through
      any subcommand groups. Nodes omit ``environment`` and
      ``exit_codes`` their spec leaves unset (the project defaults) —
      the ``default_environment`` / ``default_exit_codes`` keys below
      carry them once. Always present; empty when ``root`` is a
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
    spec = _help_spec_of(root)
    summary, description = _summary_and_description(root)
    examples = list(spec.examples) if spec is not None and spec.examples else []
    manifest: dict[str, Any] = {
        "version": _package_version(),
        "prog": root.name or "dh-mcp",
        "summary": summary,
    }
    if description:
        manifest["description"] = description
    if examples:
        manifest["examples"] = examples
    manifest.update(
        {
            "global_options": [_describe_param(p) for p in root.params],
            "universal_options": [
                _describe_param(o) for o in (_help_option(), _build_agents_option())
            ],
            "commands": (
                _describe_subcommands(root, include_defaults=False, recurse=True)
                if isinstance(root, click.Group)
                else {}
            ),
            "default_environment": [
                {"name": e.name, "help": e.help} for e in COMMON_ENV_VARS
            ],
            "default_exit_codes": [
                {"code": ec.value, "help": ec.help_text} for ec in ExitCode
            ],
            "error_codes": error_code_registry(),
        }
    )
    return manifest


def resolve_command(root: click.Command, path: tuple[str, ...]) -> click.Command:
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


# ``Any``: ``payload`` is any JSON-safe agents-surface value (manifest,
# summary tree, command node, registry); ``format_output`` dispatches
# on type when rendering.
def emit_payload(ctx: click.Context, payload: Any) -> None:
    """Render ``payload`` in the root ``-o/--output`` mode and print it.

    Output mode is resolved from the root ``-o/--output`` flag or
    ``DH_MCP_OUTPUT``, falling back to :data:`DEFAULT_OUTPUT_MODE`
    (``json``, compact). The agents surfaces run without the validated
    config, so they cannot consult ``cli.json``'s ``output.format``;
    use ``-o`` (or set ``DH_MCP_OUTPUT``) to opt into
    ``json-pretty``/``human``/``yaml`` output.

    Args:
        ctx (click.Context): The invoking command's context; the root
            context supplies the output mode.
        payload (Any): The JSON-safe value to render.
    """
    output: OutputMode = ctx.find_root().params.get("output") or DEFAULT_OUTPUT_MODE
    click.echo(format_output(payload, output=output))
