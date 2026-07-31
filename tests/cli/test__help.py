"""Tests for ``deephaven_mcp.cli._help``."""

from __future__ import annotations

import pytest

from deephaven_mcp.cli import _help
from deephaven_mcp.cli._errors import ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    COMMON_ENV_VARS,
    HelpEntry,
    HelpfulMeta,
    HelpSpec,
    OutputField,
    OutputSpec,
    build_help,
)


def test_build_help_minimal_summary_only() -> None:
    text = build_help(summary="Do a thing.", environment=(), exit_codes=())
    assert text == "Do a thing."


def test_build_help_includes_examples() -> None:
    text = build_help(
        summary="Do a thing.",
        examples=("$ dhcli do thing",),
        environment=(),
        exit_codes=(),
    )
    assert "Examples:" in text
    assert "$ dhcli do thing" in text


def test_build_help_default_sections() -> None:
    text = build_help(summary="Do.")
    assert "Environment:" in text
    assert "Exit codes:" in text
    for entry in COMMON_ENV_VARS:
        assert entry.name in text
    for code in (0, 2, 3):
        assert f"{code}" in text


def test_build_help_custom_environment() -> None:
    text = build_help(
        summary="Do.",
        environment=(HelpEntry("FOO", "do foo"),),
        exit_codes=(),
    )
    assert "FOO" in text
    assert "do foo" in text
    # The default-environment lines (which include DH_AI_DATA_DIR)
    # must not leak into a help block that supplied a custom
    # ``environment`` argument.
    assert "DH_AI_DATA_DIR" not in text


def test_build_help_custom_exit_codes() -> None:
    text = build_help(
        summary="Do.",
        environment=(),
        exit_codes=(ExitCode.TOOL_ERROR,),
    )
    assert f"{ExitCode.TOOL_ERROR.value}" in text
    assert ExitCode.TOOL_ERROR.help_text in text


def test_build_help_with_description() -> None:
    text = build_help(
        summary="Do.", description="Long form here.", environment=(), exit_codes=()
    )
    assert "Long form here." in text


def test_build_help_section_order_is_stable() -> None:
    """Sections must render in the documented order so AI agents can rely on it.

    Order: summary, description, examples, environment, exit codes.
    """
    text = build_help(
        summary="SUM",
        description="DESC",
        examples=("$ ex",),
        environment=(HelpEntry("ENV_VAR", "an env var"),),
        exit_codes=(ExitCode.TOOL_ERROR,),
    )
    # Locate the index of each section marker in the rendered text.
    indices = {
        "summary": text.index("SUM"),
        "description": text.index("DESC"),
        "examples": text.index("Examples:"),
        "environment": text.index("Environment:"),
        "exit_codes": text.index("Exit codes:"),
    }
    ordered = sorted(indices, key=lambda k: indices[k])
    assert ordered == [
        "summary",
        "description",
        "examples",
        "environment",
        "exit_codes",
    ]


def test_build_help_separates_sections_with_blank_line() -> None:
    """Each pair of populated sections is joined by exactly one blank line."""
    text = build_help(
        summary="SUM",
        description="DESC",
        environment=(),
        exit_codes=(),
    )
    # Exactly one ``\n\n`` between the summary and description; no
    # trailing blank line at the end.
    assert text == "SUM\n\nDESC"


def test_build_help_empty_examples_omits_section() -> None:
    """An empty ``examples`` sequence does not emit an ``Examples:`` header."""
    text = build_help(
        summary="SUM",
        examples=(),
        environment=(),
        exit_codes=(),
    )
    assert "Examples:" not in text


def test_build_help_environment_column_alignment() -> None:
    """Environment table aligns names to the widest name in the block."""
    text = build_help(
        summary="SUM",
        environment=(HelpEntry("SHORT", "s"), HelpEntry("MUCH_LONGER_NAME", "l")),
        exit_codes=(),
    )
    # The shorter name is padded so the help columns line up; the
    # column width tracks the longest name.
    assert "  SHORT             s" in text
    assert "  MUCH_LONGER_NAME  l" in text


def test_build_help_marks_preformatted_sections_no_wrap() -> None:
    """Examples, Environment, and Exit codes carry click's no-rewrap marker.

    The leading ``\\b`` keeps click from collapsing each
    column-aligned block into a single rewrapped paragraph in
    terminal --help output.
    """
    text = build_help(
        summary="SUM",
        examples=("$ ex",),
        environment=(HelpEntry("ENV", "e"),),
        exit_codes=(ExitCode.SUCCESS,),
    )
    assert "\b\nExamples:" in text
    assert "\b\nEnvironment:" in text
    assert "\b\nExit codes:" in text


def test_build_help_emits_plain_text_not_rst() -> None:
    """Help text is plain text; the shared disclosures carry no backticks.

    Help is rendered verbatim in the terminal and surfaced verbatim
    in the agents manifest, so backtick markup would leak as literal
    noise. Rejecting any backtick catches both double-backtick
    literals and single-backtick RST roles; single quotes are the
    emphasis convention.
    """
    for entry in COMMON_ENV_VARS:
        assert "`" not in entry.name
        assert "`" not in entry.help
    for ec in ExitCode:
        assert "`" not in ec.help_text


def test_build_help_renders_arguments() -> None:
    text = build_help(
        summary="S",
        arguments=(HelpEntry("NAME", "the tool name"),),
        environment=(),
        exit_codes=(),
    )
    assert "\b\nArguments:" in text
    assert "  NAME  the tool name" in text


def test_build_help_renders_see_also() -> None:
    text = build_help(
        summary="S",
        see_also=("dhcli tool list",),
        environment=(),
        exit_codes=(),
    )
    assert "\b\nSee also:" in text
    assert "  dhcli tool list" in text


def test_build_help_renders_error_codes() -> None:
    text = build_help(
        summary="S",
        error_codes=(ErrorCode.TOOL_NOT_FOUND,),
        environment=(),
        exit_codes=(),
    )
    assert "\b\nError codes:" in text
    # Rendered straight from the enum, so the help text is single-sourced.
    assert ErrorCode.TOOL_NOT_FOUND.value in text
    assert ErrorCode.TOOL_NOT_FOUND.help_text in text


def test_build_help_renders_output_object() -> None:
    spec = OutputSpec(
        "object", (OutputField("pid", "integer", "process id"),), note="the entry"
    )
    text = build_help(summary="S", output=spec, environment=(), exit_codes=())
    assert "\b\nOutput:" in text
    assert "the entry" in text
    assert "JSON object with fields:" in text
    assert "pid" in text
    assert "integer" in text
    assert "process id" in text


def test_build_help_renders_output_list() -> None:
    spec = OutputSpec("list", (OutputField("name", "string", "tool name"),))
    text = build_help(summary="S", output=spec, environment=(), exit_codes=())
    assert "JSON array; each element is an object with fields:" in text


def test_output_lead_unknown_mode_hits_assert_never() -> None:
    """An out-of-band output shape trips the runtime safety net.

    Statically unreachable thanks to the ``OutputShape`` ``Literal``;
    we deliberately bypass type checking to confirm the runtime
    ``assert_never`` branch is covered.
    """
    with pytest.raises(AssertionError):
        _help._output_lead("bogus")  # type: ignore[arg-type]
    # Suppression justified: deliberately feeding an off-``Literal``
    # value so the runtime safety net is exercised.


def test_build_help_renders_output_text() -> None:
    """Text-mode output renders the note and emits no field lead."""
    spec = OutputSpec("text", note="raw log lines")
    text = build_help(summary="S", output=spec, environment=(), exit_codes=())
    assert "\b\nOutput:" in text
    assert "raw log lines" in text
    assert "JSON object" not in text
    assert "JSON array" not in text


def test_build_help_object_with_no_fields_suppresses_lead() -> None:
    """An ``object`` spec with no fields renders the note but no field lead.

    The ``lead and spec.fields`` guard means the "JSON object with
    fields:" line is emitted only when there are fields to list.
    """
    spec = OutputSpec("object", (), note="opaque object")
    text = build_help(summary="S", output=spec, environment=(), exit_codes=())
    assert "\b\nOutput:" in text
    assert "opaque object" in text
    assert "JSON object with fields:" not in text


def test_exit_codes_and_error_codes_share_alignment() -> None:
    """Exit codes render through the same aligned two-column block as error codes."""
    xc = ExitCode.SUCCESS
    ec = ErrorCode.TOOL_NOT_FOUND
    text = build_help(
        summary="S",
        exit_codes=(xc,),
        error_codes=(ec,),
        environment=(),
    )
    assert f"Exit codes:\n  {xc.value}  {xc.help_text}" in text
    assert f"Error codes:\n  {ec.value}  {ec.help_text}" in text


def test_build_help_default_exit_codes_render_every_exitcode() -> None:
    """With no exit_codes, the section renders every ExitCode from the enum."""
    text = build_help(summary="S", environment=())
    for ec in ExitCode:
        assert f"  {ec.value}  {ec.help_text}" in text


def test_build_help_full_section_order() -> None:
    """All sections render in the documented order."""
    text = build_help(
        summary="SUM",
        description="DESC",
        arguments=(HelpEntry("NAME", "arg"),),
        output=OutputSpec("object", (OutputField("f", "string", "field"),)),
        examples=("$ ex",),
        see_also=("dhcli x",),
        environment=(HelpEntry("ENV", "env"),),
        exit_codes=(ExitCode.TOOL_ERROR,),
        error_codes=(ErrorCode.TOOL_NOT_FOUND,),
    )
    order = [
        text.index("SUM"),
        text.index("DESC"),
        text.index("Arguments:"),
        text.index("Output:"),
        text.index("Examples:"),
        text.index("See also:"),
        text.index("Environment:"),
        text.index("Exit codes:"),
        text.index("Error codes:"),
    ]
    assert order == sorted(order)


# ---------------------------------------------------------------------------
# HelpfulMeta — the declared metadata every command and group carries
# ---------------------------------------------------------------------------


def test_helpful_meta_defaults_are_empty() -> None:
    """An undeclared command carries empty metadata, never ``None`` sets.

    The manifest reads these attributes off any node without narrowing,
    so each must exist with a truthful "nothing declared" value.
    """
    meta = HelpfulMeta("c")
    assert meta.help_spec is None
    assert meta.output_spec is None
    assert meta.wraps_tool is None
    assert meta.wraps_tools == ()
    assert meta.intentionally_unsupported == frozenset()
    assert meta.router_params == frozenset()
    assert meta.client_only_params == frozenset()
    assert meta.needs_runtime is True


def test_helpful_meta_renders_help_from_spec_and_adopts_its_output() -> None:
    """A spec drives the rendered help text and supplies ``output_spec``."""
    spec = HelpSpec(
        summary="Do a thing.",
        output=OutputSpec("object", (OutputField("f", "string", "field"),)),
    )
    meta = HelpfulMeta("c", help_spec=spec)
    assert meta.help_spec is spec
    assert meta.output_spec is spec.output
    assert meta.help is not None
    assert "Do a thing." in meta.help


def test_helpful_meta_rejects_output_spec_alongside_help_spec() -> None:
    """One source for output, so the two surfaces cannot disagree.

    Accepting both would let ``--help`` render one output shape and
    ``--agents`` another.
    """
    with pytest.raises(ValueError, match="must not be passed alongside help_spec"):
        HelpfulMeta(
            "c",
            help_spec=HelpSpec(summary="s"),
            output_spec=OutputSpec("object", ()),
        )
