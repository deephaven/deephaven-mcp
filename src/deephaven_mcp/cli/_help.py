"""Help-text templating for the dh-mcp click commands.

Every leaf command's help follows the same layout: a one-line
summary, an optional description paragraph, and a fixed sequence of
trailing sections — Arguments, Output, Examples, See also,
Environment, Exit codes, and Error codes — that AI agents and
operators rely on for progressive disclosure. The build_help helper
assembles them in that fixed order, defaulting the Environment and
Exit codes sections from shared constants so authors can't
accidentally drop them.

Help text is plain text, not reStructuredText: it is rendered
verbatim in the terminal by click and surfaced verbatim in the
introspect manifest, so no inline markup is used. The trailing
sections are pre-formatted (aligned columns, one example per line);
build_help prefixes each with click's no-rewrap marker so the
layout survives click's paragraph rewrapping.

A command's structured output is described once as an OutputSpec
and consumed twice: build_help renders the human-readable Output
section from it, and HelpfulCommand carries it so the introspect
manifest emits the same shape. Apply the _cli-help-standards skill
when authoring or reviewing command help.
"""

from __future__ import annotations

__all__ = [
    "DEFAULT_ENVIRONMENT_LINES",
    "DEFAULT_EXIT_CODE_LINES",
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

# Click rewraps each help paragraph unless it begins with a backspace
# (\b) on its own line; prefixing the pre-formatted sections with this
# marker preserves their column alignment and one-example-per-line
# layout. introspect strips the marker so the manifest stays clean.
_NO_WRAP = "\b\n"

DEFAULT_ENVIRONMENT_LINES: tuple[tuple[str, str], ...] = (
    (
        "DH_MCP_DATA_DIR",
        (
            "User-data root holding the config and runtime "
            "subdirectories (overrides the platform default)."
        ),
    ),
    ("DH_MCP_OUTPUT", "Output mode: human, json, or yaml."),
)
"""Standard env-var disclosures shared by every leaf command."""

DEFAULT_EXIT_CODE_LINES: tuple[tuple[int, str], ...] = (
    (0, "success"),
    (2, "user-facing failure (config, daemon, MCP request)"),
    (3, "the invoked MCP tool returned isError=True"),
)
"""Standard exit-code table shared by every leaf command."""

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

    Referenced twice per command: passed to :func:`build_help` as
    ``output=`` to render the human-readable ``Output:`` section, and
    attached to the command via :class:`HelpfulCommand` (``output_spec``)
    so :mod:`deephaven_mcp.cli._commands.introspect` emits the same
    shape in the manifest.
    """

    mode: OutputShape
    """Top-level shape: ``object``, ``list``, or ``text``."""
    fields: tuple[OutputField, ...] = ()
    """Per-field descriptions; empty for ``text`` mode."""
    note: str | None = None
    """Optional one-line note shown above the field list."""


class HelpfulCommand(click.Command):
    """Click command that carries a structured :class:`OutputSpec`.

    The spec is surfaced by ``dh-mcp introspect`` so agents discover a
    command's output shape without parsing prose.
    """

    def __init__(
        self, *args: Any, output_spec: OutputSpec | None = None, **kwargs: Any
    ) -> None:
        """Store ``output_spec``; defer the rest to :class:`click.Command`."""
        # ``*args`` / ``**kwargs`` is the standard click-subclass
        # pass-through to click.Command's broad constructor signature.
        super().__init__(*args, **kwargs)
        self.output_spec: OutputSpec | None = output_spec


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


def build_help(
    *,
    summary: str,
    description: str | None = None,
    arguments: Sequence[tuple[str, str]] | None = None,
    output: OutputSpec | None = None,
    examples: Sequence[str] | None = None,
    see_also: Sequence[str] | None = None,
    environment: Sequence[tuple[str, str]] | None = None,
    exit_codes: Sequence[tuple[int, str]] | None = None,
    error_codes: Sequence[tuple[str, str]] | None = None,
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
        arguments (Sequence[tuple[str, str]] | None): ``(name, help)``
            pairs for positional arguments, shown under an
            ``Arguments:`` heading. Click renders no help for
            positional arguments, so this is the only place they are
            documented.
        output (OutputSpec | None): Structured description of the
            command's output, rendered under an ``Output:`` heading.
        examples (Sequence[str] | None): Shell snippets shown under an
            ``Examples:`` heading. Each entry is prefixed with ``$ ``.
        see_also (Sequence[str] | None): Related commands shown under a
            ``See also:`` heading, one per line.
        environment (Sequence[tuple[str, str]] | None): ``(name, help)``
            pairs under an ``Environment:`` heading. Defaults to
            :data:`DEFAULT_ENVIRONMENT_LINES`.
        exit_codes (Sequence[tuple[int, str]] | None): ``(code, help)``
            pairs under an ``Exit codes:`` heading. Defaults to
            :data:`DEFAULT_EXIT_CODE_LINES`.
        error_codes (Sequence[tuple[str, str]] | None): ``(code, help)``
            pairs under an ``Error codes:`` heading listing the stable
            ``error_code`` strings the command can emit. No default;
            omitted when ``None``.

    Returns:
        str: A multi-line help string with all sections joined by a
            single blank line.
    """
    sections: list[str] = [summary]
    if description:
        sections.append(description)
    if arguments:
        sections.append(f"{_NO_WRAP}Arguments:\n{_aligned(arguments)}")
    if output is not None:
        sections.append(f"{_NO_WRAP}Output:\n{_render_output(output)}")
    if examples:
        body = "\n".join(f"  {line}" for line in examples)
        sections.append(f"{_NO_WRAP}Examples:\n{body}")
    if see_also:
        body = "\n".join(f"  {line}" for line in see_also)
        sections.append(f"{_NO_WRAP}See also:\n{body}")
    env = environment if environment is not None else DEFAULT_ENVIRONMENT_LINES
    if env:
        sections.append(f"{_NO_WRAP}Environment:\n{_aligned(env)}")
    codes = exit_codes if exit_codes is not None else DEFAULT_EXIT_CODE_LINES
    if codes:
        sections.append(f"{_NO_WRAP}Exit codes:\n{_aligned(codes)}")
    if error_codes:
        sections.append(f"{_NO_WRAP}Error codes:\n{_aligned(error_codes)}")
    return "\n\n".join(sections)
