"""Help-text templating for the dh-mcp click commands.

Every leaf command's help follows the same layout: a one-line
summary, an optional description paragraph, and a fixed sequence of
trailing sections — Arguments, Output, Examples, See also,
Environment, Exit codes, and Error codes. ``build_help`` assembles
them in that order, defaulting the Environment and Exit codes
sections from shared constants.

Help text is plain text, not reStructuredText: click renders it
verbatim in the terminal and the introspect manifest surfaces it
verbatim. The pre-formatted trailing sections (aligned columns, one
example per line) are prefixed with click's no-rewrap marker so their
layout survives click's paragraph rewrapping.

A command's structured output is described once as an ``OutputSpec``,
which ``build_help`` renders into the Output section and
``HelpfulCommand`` carries for the introspect manifest. Apply the
``_cli-help-standards`` skill when authoring or reviewing command help.
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
]

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, assert_never

import click

from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._format import OUTPUT_ENV_VAR
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


class HelpfulGroup(click.Group):
    """Click group whose leaf commands default to :class:`HelpfulCommand`."""

    command_class = HelpfulCommand


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
