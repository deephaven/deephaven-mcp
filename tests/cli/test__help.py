"""Tests for ``deephaven_mcp.cli._help``."""

from __future__ import annotations

import pytest

from deephaven_mcp.cli import _help
from deephaven_mcp.cli._help import (
    DEFAULT_ENVIRONMENT_LINES,
    DEFAULT_EXIT_CODE_LINES,
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
        examples=("$ dh-mcp do thing",),
        environment=(),
        exit_codes=(),
    )
    assert "Examples:" in text
    assert "$ dh-mcp do thing" in text


def test_build_help_default_sections() -> None:
    text = build_help(summary="Do.")
    assert "Environment:" in text
    assert "Exit codes:" in text
    for name, _ in DEFAULT_ENVIRONMENT_LINES:
        assert name in text
    for code, _ in DEFAULT_EXIT_CODE_LINES:
        assert f"{code}" in text


def test_build_help_custom_environment() -> None:
    text = build_help(
        summary="Do.",
        environment=(("FOO", "do foo"),),
        exit_codes=(),
    )
    assert "FOO" in text
    assert "do foo" in text
    # The default-environment lines (which include DH_MCP_DATA_DIR)
    # must not leak into a help block that supplied a custom
    # ``environment`` argument.
    assert "DH_MCP_DATA_DIR" not in text


def test_build_help_custom_exit_codes() -> None:
    text = build_help(
        summary="Do.",
        environment=(),
        exit_codes=((42, "the answer"),),
    )
    assert "42" in text
    assert "the answer" in text


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
        environment=(("ENV_VAR", "an env var"),),
        exit_codes=((99, "boom"),),
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
        environment=(("SHORT", "s"), ("MUCH_LONGER_NAME", "l")),
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
        environment=(("ENV", "e"),),
        exit_codes=((7, "x"),),
    )
    assert "\b\nExamples:" in text
    assert "\b\nEnvironment:" in text
    assert "\b\nExit codes:" in text


def test_build_help_emits_plain_text_not_rst() -> None:
    """Help text is plain text; the shared disclosures carry no RST markup.

    Help is rendered verbatim in the terminal and surfaced verbatim
    in the introspect manifest, so reStructuredText backticks would
    leak as literal noise.
    """
    for name, help_ in DEFAULT_ENVIRONMENT_LINES:
        assert "``" not in name
        assert "``" not in help_
    for _code, help_ in DEFAULT_EXIT_CODE_LINES:
        assert "``" not in help_


def test_build_help_renders_arguments() -> None:
    text = build_help(
        summary="S",
        arguments=(("NAME", "the tool name"),),
        environment=(),
        exit_codes=(),
    )
    assert "\b\nArguments:" in text
    assert "  NAME  the tool name" in text


def test_build_help_renders_see_also() -> None:
    text = build_help(
        summary="S",
        see_also=("dh-mcp tool list",),
        environment=(),
        exit_codes=(),
    )
    assert "\b\nSee also:" in text
    assert "  dh-mcp tool list" in text


def test_build_help_renders_error_codes() -> None:
    text = build_help(
        summary="S",
        error_codes=(("tool_not_found", "unknown tool"),),
        environment=(),
        exit_codes=(),
    )
    assert "\b\nError codes:" in text
    assert "tool_not_found" in text
    assert "unknown tool" in text


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


def test_exit_codes_and_error_codes_share_alignment() -> None:
    """Exit codes render through the same aligned two-column block as error codes."""
    text = build_help(
        summary="S",
        exit_codes=((0, "ok"),),
        error_codes=(("x", "bad"),),
        environment=(),
    )
    assert "Exit codes:\n  0  ok" in text
    assert "Error codes:\n  x  bad" in text


def test_build_help_full_section_order() -> None:
    """All sections render in the documented order."""
    text = build_help(
        summary="SUM",
        description="DESC",
        arguments=(("NAME", "arg"),),
        output=OutputSpec("object", (OutputField("f", "string", "field"),)),
        examples=("$ ex",),
        see_also=("dh-mcp x",),
        environment=(("ENV", "env"),),
        exit_codes=((9, "boom"),),
        error_codes=(("err", "an error"),),
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
