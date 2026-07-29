"""Help-text vocabulary and rendering for the dhcli CLI.

A command is described once, as a :class:`HelpSpec`, and that one
description feeds both surfaces: :func:`build_help` renders it to humans
as ``--help`` text, and ``_manifest`` renders the same spec to machines
as the command's agents-manifest node, so the two cannot drift.

This module owns the description vocabulary and the human half:

- :class:`HelpSpec` — a one-line summary, an optional description
  paragraph, and the structured content of the trailing sections
  (Arguments, Output, Examples, See also, Environment, Exit codes, Error
  codes), with :class:`HelpEntry`, :class:`OutputField`, and
  :class:`OutputSpec` as its parts.
- :func:`build_help` — renders those sections in a fixed order,
  defaulting Environment and Exit codes from shared constants. Help text
  is plain text, not reStructuredText: click renders it verbatim in the
  terminal, and the pre-formatted sections (aligned columns, one example
  per line) carry click's no-rewrap marker so their layout survives
  click's paragraph rewrapping.
- :class:`HelpfulMeta` — the declared metadata every command and group
  carries, defined here because it is part of that vocabulary and
  because keeping it upstream of ``_manifest`` is what lets the
  dependency run one way: ``_command`` -> ``_manifest`` -> ``_help`` ->
  ``_params``.

Apply the ``ref-cli-help-standards`` skill when authoring or reviewing
command help.
"""

from __future__ import annotations

__all__ = [
    "COMMON_ENV_VARS",
    "HelpEntry",
    "HelpSpec",
    "HelpfulMeta",
    "OutputField",
    "OutputShape",
    "OutputSpec",
    "build_help",
]

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, assert_never

import click

from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._format import (
    OUTPUT_ENV_VAR,
)
from deephaven_mcp.config import DATA_DIR_ENV_VAR

# Click rewraps each help paragraph unless it begins with a backspace
# (\b) on its own line; prefixing the pre-formatted sections with this
# marker preserves their column alignment and one-example-per-line
# layout. The manifest builders strip the marker so the manifest stays clean.
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


class HelpfulMeta(click.Command):
    """The declared help and manifest metadata of a command or group.

    Both :class:`HelpfulCommand` and :class:`HelpfulGroup` inherit from
    this, so every command in the tree exposes the same attribute
    surface and the manifest builders can read ``help_spec`` /
    ``output_spec`` / ``wraps_tool`` off any node without narrowing to a
    specific subclass. A group simply reports the truthful defaults: it
    wraps no tool, so ``wraps_tool`` is ``None``.

    Unrecognized keyword arguments are forwarded up the ``__init__``
    chain to :class:`click.Command`.
    """

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
        needs_runtime: bool = True,
        **kwargs: Any,
    ) -> None:
        """Store the wrapper metadata; defer the rest to :class:`click.Command`.

        Args:
            help_spec (HelpSpec | None): Structured help content. When set,
                the command's ``help`` text is rendered from it (overriding
                any ``help`` keyword) and ``output_spec`` is always its
                ``output`` field.
            output_spec (OutputSpec | None): Structured description of the
                command's output, surfaced by ``dhcli agents``. Only for
                commands without a ``help_spec``; a command with a spec
                declares its output as ``help_spec.output``.
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
            needs_runtime (bool): Whether :meth:`invoke` materializes the
                :class:`~deephaven_mcp.cli._runtime.Runtime` (loading and
                validating the configuration tree) before the command body
                runs. Default True — almost every verb reads config.
                Declare False only for commands that must work without a
                valid configuration tree (the ``agents`` verbs).
            *args (Any): Positional arguments forwarded to
                :class:`click.Command`.
            **kwargs (Any): Keyword arguments forwarded to
                :class:`click.Command`.

        Raises:
            ValueError: When both ``help_spec`` and ``output_spec`` are
                given. The spec is the single source for both surfaces,
                so a separate ``output_spec`` could render one output in
                ``--help`` and another in ``--agents``.
        """
        if help_spec is not None:
            if output_spec is not None:
                raise ValueError(
                    "output_spec must not be passed alongside help_spec; "
                    "declare the command's output as help_spec.output"
                )
            kwargs["help"] = _render_help_spec(help_spec)
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
        self.needs_runtime: bool = needs_runtime


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
        case _ as unexpected:
            assert_never(unexpected)


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
