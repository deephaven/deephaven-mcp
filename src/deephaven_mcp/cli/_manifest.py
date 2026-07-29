"""The agents manifest: the machine-readable twin of ``--help``.

Every ``dhcli`` command is described twice from one source. ``_help``
renders a command's :class:`~deephaven_mcp.cli._help.HelpSpec` to humans
as ``--help`` text; this module renders the same spec to machines, so the
two surfaces cannot drift.

- :func:`build_summary_tree` renders the compact orientation view: every
  command path with its one-line summary.
- :func:`build_manifest` walks the live click tree into the complete
  JSON-safe manifest.
- :func:`describe_command` renders one self-contained node.

The ``dhcli agents`` verbs and the universal ``--agents`` flag both emit
these. Node keys are sparse: an absent key means false, empty, or the
default. A command's structured output is described once as an
``OutputSpec`` inside its ``HelpSpec``.

Every node also carries the two facts a reader cannot reconstruct from a
node read in isolation: ``path``, the full invocation path (a node's
``name`` alone is ``"stop"``, which is not runnable), and ``usage``, the
argument order click itself would print.

:func:`_meta_of` is the single boundary where this module crosses from
click's loosely-typed containers (``Group.commands`` is
``dict[str, click.Command]``) to the project's own
:class:`~deephaven_mcp.cli._help.HelpfulMeta` base, off which every node's
metadata is read. Printing is delegated to ``_echo``, which the
``--agents`` callback uses in its no-runtime form since it fires before
any configuration load.

Apply the ``ref-cli-help-standards`` skill when authoring or reviewing
command help.
"""

from __future__ import annotations

__all__ = [
    "AGENT_CONVENTIONS",
    "NodeStyle",
    "agents_option",
    "build_error_code_registry",
    "build_manifest",
    "build_summary_tree",
    "describe_command",
    "resolve_command",
]

import inspect
import json
from enum import Enum
from importlib import metadata
from typing import Any

import click

from deephaven_mcp.cli._context import TARGET_SELECTION_GUIDANCE
from deephaven_mcp.cli._echo import echo_payload_no_runtime
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    COMMON_ENV_VARS,
    HelpfulMeta,
    HelpSpec,
    OutputSpec,
)


class NodeStyle(Enum):
    """Whether a command node must stand on its own or leans on the manifest root.

    Selects how much a node repeats. The distinction only affects the
    ``error_codes`` / ``exit_codes`` / ``environment`` keys, whose
    meanings and project-wide defaults are either inlined per node or
    carried once at the root.
    """

    STANDALONE = ("standalone", True)
    """Self-contained, for a node emitted on its own.

    Code entries carry their meanings as ``{code, help}`` and the
    resolved defaults are inlined, so a reader with only this node can
    decode every field. Used by ``--agents`` and ``dhcli agents
    command``.
    """

    EMBEDDED = ("embedded", False)
    """Terse, for a node nested inside the whole-tree manifest.

    Code entries are bare values and entries the spec leaves unset are
    omitted, because the root's ``error_codes`` / ``default_exit_codes``
    / ``default_environment`` keys carry them once for the whole tree.
    Used by :func:`build_manifest`.
    """

    standalone: bool
    """Whether a node in this style must decode without the manifest root.

    ``True`` inlines code meanings as ``{code, help}`` and the resolved
    project-wide defaults; ``False`` emits bare code values and omits
    entries the spec leaves unset, because the root states them once.
    """

    def __new__(cls, value: str, standalone: bool) -> NodeStyle:
        """Bind the wire value and the self-containment decision together.

        Carrying ``standalone`` per member, rather than deriving it from
        an identity test against one member, means a style added later
        cannot be constructed without stating whether it is
        self-contained -- where a ``style is STANDALONE`` test would have
        silently treated it as :attr:`EMBEDDED`.
        """
        member = object.__new__(cls)
        member._value_ = value
        member.standalone = standalone
        return member


def _parents_of(ctx: click.Context) -> tuple[str, ...]:
    """Return the ancestor path tokens of ``ctx``'s command, program name first.

    Read from click's own ``command_path`` (e.g. ``"dhcli pq stop"``) with
    the command's own token dropped, so the node's ``path`` matches what
    the user actually typed -- including a program renamed at the entry
    point.
    """
    return tuple(ctx.command_path.split()[:-1])


def _agents_callback(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    """Emit ``ctx.command``'s agents node, then exit.

    The eager callback behind the universal ``--agents`` flag, the
    machine-readable twin of ``--help``: emits the summary tree when
    invoked on the root group and a single self-contained command node
    otherwise, in the root ``-o/--output`` mode (or ``DHCLI_OUTPUT``),
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
        else describe_command(ctx.command, parents=_parents_of(ctx))
    )
    echo_payload_no_runtime(ctx, payload)
    ctx.exit()


def agents_option() -> click.Option:
    """Build the eager ``--agents`` option, bound to its callback.

    A fresh instance per call, mirroring how click builds each command's
    ``--help`` in ``get_help_option``, so no two commands share one
    parameter object.

    Returns:
        click.Option: The ``--agents`` flag, ready to append to a
            command's parameters.
    """
    return click.Option(
        ["--agents"],
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=_agents_callback,
        help=(
            "Print this command's machine-readable description, tuned for "
            "AI agents, and exit; the machine twin of --help. Honors "
            "-o/--output: compact json by default, -o json-pretty for "
            "indented. For the whole command tree, use 'dhcli agents "
            "tree'."
        ),
    )


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


def _meta_of(cmd: click.Command) -> HelpfulMeta:
    """Narrow a command from click's containers to :class:`HelpfulMeta`.

    The single place the manifest crosses from click's loosely-typed
    ``Group.commands`` (``dict[str, click.Command]``) to this project's
    own command base. Every command in the ``dhcli`` tree is a
    :class:`HelpfulCommand` or :class:`HelpfulGroup`, so anything else is
    a wiring bug: it would silently describe a command with no summary,
    no output schema, and no tool binding. Raising here surfaces it at
    the first ``dhcli agents`` call instead.

    Args:
        cmd (click.Command): A command read from a click container.

    Returns:
        HelpfulMeta: ``cmd`` itself, narrowed.

    Raises:
        TypeError: When ``cmd`` was registered without
            ``cls=HelpfulCommand`` / ``cls=HelpfulGroup``.
    """
    if not isinstance(cmd, HelpfulMeta):
        raise TypeError(
            f"command {cmd.name!r} is a plain {type(cmd).__name__}; every dhcli "
            "command must be a HelpfulCommand or HelpfulGroup so its help and "
            "manifest metadata exist"
        )
    return cmd


def _summary_and_description(cmd: click.Command) -> tuple[str, str | None]:
    """Return ``(summary, description)`` for ``cmd``.

    Reads the :class:`HelpSpec` when the command carries one, falling
    back to splitting its raw help text with :func:`_split_help_text` —
    the case for a noun group, which is described by its docstring.
    """
    spec = _meta_of(cmd).help_spec
    if spec is not None:
        return spec.summary, spec.description
    return _split_help_text(cmd.help)


def _summary_of(cmd: click.Command) -> str:
    """Return the one-line summary for ``cmd``.

    Reads the :class:`HelpSpec` summary when the command carries one,
    falling back to the first paragraph of its raw help text.
    """
    return _summary_and_description(cmd)[0]


def _invocation(cmd: click.Command, parents: tuple[str, ...]) -> str:
    """Return the full invocation path of ``cmd`` under ``parents``.

    Args:
        cmd (click.Command): The command being described.
        parents (tuple[str, ...]): Ancestor path tokens, program name
            first; empty for a root or for a node described without a
            known path.

    Returns:
        str: The space-joined path, e.g. ``"dhcli pq stop"``. Falls back
            to the command's own name when ``parents`` is empty.
    """
    return " ".join((*parents, cmd.name or "")).strip()


def _usage_of(cmd: click.Command, invocation: str) -> str:
    """Return the one-line usage string for ``cmd``.

    Built from click's own ``collect_usage_pieces`` -- the same source as
    the ``Usage:`` line in ``--help`` -- so the manifest cannot describe a
    different argument order than the parser accepts. Assembled here
    rather than taken from ``get_usage`` because that wraps to the
    terminal width and prefixes the label.

    A throwaway :class:`click.Context` is required by
    ``collect_usage_pieces`` (it renders each parameter's metavar); it is
    never entered or invoked, so no callback runs and no configuration is
    read.

    Args:
        cmd (click.Command): The command being described.
        invocation (str): The full invocation path from
            :func:`_invocation`, used as the usage line's prefix.

    Returns:
        str: e.g. ``"dhcli pq stop [OPTIONS] [ID]..."``.
    """
    ctx = click.Context(cmd, info_name=cmd.name)
    return " ".join([invocation, *cmd.collect_usage_pieces(ctx)])


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

    ``None`` means exactly one thing: the command wraps no MCP tool.
    That covers every group (a group declares no tool, so ``wraps_tool``
    is ``None`` by default) and non-wrapping verbs such as ``daemon`` and
    ``config``, so the manifest carries the binding only where it is
    meaningful.

    Args:
        cmd (click.Command): The command to inspect.

    Returns:
        dict[str, Any] | None: ``{tools, intentionally_unsupported,
            router_params, client_only_params}`` (``tools`` is the union
            of ``wraps_tool`` and ``wraps_tools``, sorted) when the
            command wraps at least one tool; ``None`` otherwise.
    """
    meta = _meta_of(cmd)
    tools = sorted({*meta.wraps_tools, *([meta.wraps_tool] if meta.wraps_tool else [])})
    if not tools:
        return None
    return {
        "tools": tools,
        "intentionally_unsupported": sorted(meta.intentionally_unsupported),
        "router_params": sorted(meta.router_params),
        "client_only_params": sorted(meta.client_only_params),
    }


def _describe_subcommands(
    cmd: click.Group, *, style: NodeStyle, recurse: bool, parents: tuple[str, ...] = ()
) -> dict[str, Any]:
    """Return the ``subcommands`` map for a group node.

    Entries are ordered by sorted key so the manifest is reproducible
    across invocations.

    Args:
        cmd (click.Group): The group whose subcommands are described.
        style (NodeStyle): Forwarded to :func:`describe_command` when
            recursing.
        recurse (bool): When ``True``, each entry is a full nested node
            from :func:`describe_command`; when ``False``, each entry
            is the subcommand's one-line summary string (the bounded
            view a standalone group node carries).
        parents (tuple[str, ...]): Ancestor path tokens of ``cmd``, so a
            nested node's ``path`` names the whole invocation rather than
            just its own leaf name.
    """
    if recurse:
        child_parents = (*parents, cmd.name or "")
        return {
            name: describe_command(
                cmd.commands[name],
                style=style,
                recurse=True,
                parents=child_parents,
            )
            for name in sorted(cmd.commands)
        }
    return {name: _summary_of(cmd.commands[name]) for name in sorted(cmd.commands)}


def _describe_spec_extras(spec: HelpSpec, *, style: NodeStyle) -> dict[str, Any]:
    """Return the spec-sourced sparse keys for a command node.

    Yields ``examples``, ``see_also``, ``error_codes``, ``exit_codes``,
    and ``environment`` when applicable, per the node contract in
    :func:`describe_command`. The caller merges the result into its node;
    keys are produced in node order.

    Args:
        spec (HelpSpec): The command's help spec.
        style (NodeStyle): Whether to inline code meanings and resolved
            defaults (:attr:`NodeStyle.STANDALONE`) or emit bare values
            and omit spec-unset entries (:attr:`NodeStyle.EMBEDDED`).

    Returns:
        dict[str, Any]: The applicable keys, empty when the spec carries
            none of this content.
    """
    standalone = style.standalone
    extras: dict[str, Any] = {}
    if spec.examples:
        extras["examples"] = list(spec.examples)
    if spec.see_also:
        extras["see_also"] = list(spec.see_also)
    if spec.error_codes:
        extras["error_codes"] = [
            ({"code": c.value, "help": c.help_text} if standalone else c.value)
            for c in spec.error_codes
        ]
    exit_codes = spec.exit_codes if spec.exit_codes is not None else tuple(ExitCode)
    if exit_codes and (standalone or spec.exit_codes is not None):
        extras["exit_codes"] = [
            ({"code": c.value, "help": c.help_text} if standalone else c.value)
            for c in exit_codes
        ]
    environment = spec.environment if spec.environment is not None else COMMON_ENV_VARS
    if environment and (standalone or spec.environment is not None):
        extras["environment"] = [{"name": e.name, "help": e.help} for e in environment]
    return extras


def describe_command(
    cmd: click.Command,
    *,
    style: NodeStyle = NodeStyle.STANDALONE,
    recurse: bool = False,
    parents: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Describe one command as a JSON-safe, sparse node dict.

    Keys are sparse: a key absent from the node means the command has
    none of that content. Always present: ``name`` and ``summary``.
    Present when applicable:

    - ``path`` (str), ``usage`` (str): The full invocation path and the
      argument order, on a standalone node only — ``name`` alone
      (``"stop"``) is not something a reader can run. Omitted under
      :attr:`NodeStyle.EMBEDDED`, where the node's position in the
      nested map gives the path and ``params`` gives the argument order.
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
        style (NodeStyle): Whether the node must be self-contained
            (:attr:`NodeStyle.STANDALONE`, the default) or is nested
            inside the whole-tree manifest
            (:attr:`NodeStyle.EMBEDDED`), which carries code meanings
            and project defaults once at its root.
        recurse (bool): Forwarded to :func:`_describe_subcommands` for
            groups.
        parents (tuple[str, ...]): Ancestor path tokens, program name
            first (e.g. ``("dhcli", "pq")``), used to build ``path`` and
            ``usage``. Empty falls back to the command's own name, which
            is correct for a root and honest for a detached command.
            Unused under :attr:`NodeStyle.EMBEDDED`.

    Returns:
        dict[str, Any]: The JSON-safe node.
    """
    meta = _meta_of(cmd)
    spec = meta.help_spec
    summary, description = _summary_and_description(cmd)
    node: dict[str, Any] = {"name": cmd.name}
    if style is NodeStyle.STANDALONE:
        invocation = _invocation(cmd, parents)
        node["path"] = invocation
        node["usage"] = _usage_of(cmd, invocation)
    node["summary"] = summary
    if description:
        node["description"] = description
    argument_help = _argument_help_map(spec)
    if cmd.params:
        node["params"] = [_describe_param(p, argument_help) for p in cmd.params]
    if meta.output_spec is not None:
        node["output"] = _describe_output(meta.output_spec)
    if spec is not None:
        node.update(_describe_spec_extras(spec, style=style))
    wraps = _describe_wraps(cmd)
    if wraps is not None:
        node["wraps"] = wraps
    if isinstance(cmd, click.Group):
        node["subcommands"] = _describe_subcommands(
            cmd, style=style, recurse=recurse, parents=parents
        )
    return node


def build_error_code_registry() -> list[dict[str, Any]]:
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
    "Summary view. Run 'dhcli agents command <path...>' (or append "
    "--agents to any command) for one command's full node, 'dhcli "
    "agents tree --full' for the complete manifest, and 'dhcli "
    "agents errors' for the error-code registry."
)
"""Drill-down pointer carried by the summary tree so it self-describes."""

AGENT_CONVENTIONS: tuple[str, ...] = (
    "Output is compact single-line json unless a command's output mode is "
    "'text'. Pass -o json-pretty for indented json, -o yaml, or -o human; "
    "DHCLI_OUTPUT sets it for the session. Exit codes: 0 success, 2 "
    "user-facing failure, 3 the invoked MCP tool returned isError=true. A "
    "failure prints {error, error_code, exit_code, command} in the "
    "structured modes; branch on the stable error_code, not on the "
    "message ('dhcli agents errors' lists them).",
    "A session, system, or PQ id omitted from a verb falls back to the "
    "sticky context in context.json, which the command line does not show "
    "— an omitted id means 'whatever the context holds', not 'no target'. "
    "Run 'dhcli context show' before any consequential verb whose id you "
    "intend to omit, or pass the id explicitly.",
    TARGET_SELECTION_GUIDANCE,
)
"""The three rules an agent needs before its first consequential command.

Carried by the summary tree -- the surface ``dhcli agents tree`` and the
root ``--agents`` flag emit, and so the first thing an agent reads.
Deliberately short: the tree is the cheap orientation rung of the
progressive-disclosure ladder, so only a rule that holds tree-wide *and*
cannot be read off a single node belongs here. Per-command hazards
(a truncating row cap, a confirmation prompt's non-interactive behavior)
live on the command that has them, where they can be stated accurately;
stated here they would have to be hedged into uselessness, since they
hold for some verbs and not others.

:data:`~deephaven_mcp.cli._context.TARGET_SELECTION_GUIDANCE` is shared
verbatim with the two listing verbs whose output creates the hazard, so
the rule has one wording wherever it appears.
"""


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

    The default output of ``dhcli agents tree`` and the root
    ``--agents`` flag: every command path with its one-line summary,
    small enough to sit in an agent's context. Shape:

    - ``version`` (str): Installed ``deephaven-mcp`` package version.
    - ``prog`` (str): Program invocation name (``"dhcli"``).
    - ``summary`` (str): The root command's one-line summary.
    - ``description`` (str): The root command's description — what the
      tool is and how to get started — when it declares one.
    - ``conventions`` (list[str]): :data:`AGENT_CONVENTIONS`, the
      project-wide rules that hold for every command.
    - ``hint`` (str): How to drill down to full nodes and the complete
      manifest.
    - ``commands`` (dict): Nested ``{name: {summary, commands?}}`` map
      from :func:`_summary_commands`; empty when ``root`` is a plain
      :class:`click.Command`.

    Args:
        root (click.Command): The root command. In production this is
            the ``dhcli`` :class:`click.Group`.

    Returns:
        dict[str, Any]: JSON / YAML serializable summary tree.
    """
    summary, description = _summary_and_description(root)
    tree: dict[str, Any] = {
        "version": _package_version(),
        "prog": root.name or "dhcli",
        "summary": summary,
    }
    if description:
        tree["description"] = description
    tree.update(
        {
            "conventions": list(AGENT_CONVENTIONS),
            "hint": _SUMMARY_TREE_HINT,
            "commands": (
                _summary_commands(root) if isinstance(root, click.Group) else {}
            ),
        }
    )
    return tree


def build_manifest(root: click.Command) -> dict[str, Any]:
    """Construct the complete agents manifest for the root command tree.

    The manifest is the canonical agent-discoverable description of
    the entire CLI, emitted by ``dhcli agents tree --full``. All
    collections within it are sorted (commands by name, error codes
    by value) so repeated invocations produce byte-identical output
    suitable for snapshot tests and diffs.

    Manifest shape:

    - ``version`` (str): Installed ``deephaven-mcp`` package version,
      or ``"unknown"`` if package metadata is unavailable.
    - ``prog`` (str): Program invocation name (``"dhcli"``).
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
        root (click.Command): The root command. In production this is
            the ``dhcli`` :class:`HelpfulGroup`; a :class:`HelpfulCommand`
            is also accepted, in which case ``commands`` is empty.

    Returns:
        dict[str, Any]: JSON / YAML serializable manifest.

    Raises:
        TypeError: When ``root`` is a plain :class:`click.Command`,
            carrying none of the metadata a manifest describes. See
            :func:`_meta_of`.
    """
    spec = _meta_of(root).help_spec
    summary, description = _summary_and_description(root)
    examples = list(spec.examples) if spec is not None and spec.examples else []
    manifest: dict[str, Any] = {
        "version": _package_version(),
        "prog": root.name or "dhcli",
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
                _describe_param(o) for o in (_help_option(), agents_option())
            ],
            "commands": (
                _describe_subcommands(root, style=NodeStyle.EMBEDDED, recurse=True)
                if isinstance(root, click.Group)
                else {}
            ),
            "default_environment": [
                {"name": e.name, "help": e.help} for e in COMMON_ENV_VARS
            ],
            "default_exit_codes": [
                {"code": ec.value, "help": ec.help_text} for ec in ExitCode
            ],
            "error_codes": build_error_code_registry(),
        }
    )
    return manifest


def resolve_command(root: click.Command, path: tuple[str, ...]) -> click.Command:
    """Resolve a command path against the live click command tree.

    Walks ``path`` token by token, descending through each
    :class:`click.Group`'s ``commands`` mapping.

    Args:
        root (click.Command): The command to start the walk from
            (the ``dhcli`` root group in production).
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
            resolved = " ".join(path[:index]) or (root.name or "dhcli")
            raise CliError(
                f"Unknown command path: {' '.join(path)!r} "
                f"(no command {token!r} under {resolved!r}).",
                code=ErrorCode.COMMAND_NOT_FOUND,
            )
        current = current.commands[token]
    return current
