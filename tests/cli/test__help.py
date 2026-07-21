"""Tests for ``deephaven_mcp.cli._help``."""

from __future__ import annotations

import json
from unittest.mock import patch

import click
import pytest
import yaml
from click.testing import CliRunner

from deephaven_mcp.cli import _help
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    COMMON_ENV_VARS,
    HelpEntry,
    HelpfulCommand,
    HelpfulGroup,
    HelpSpec,
    OutputField,
    OutputSpec,
    _describe_wraps,
    _split_help_text,
    build_help,
    build_manifest,
    build_summary_tree,
    describe_command,
    resolve_command,
)
from deephaven_mcp.cli._main import cli


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
# HelpfulCommand / HelpfulGroup — the output-spec + wrapper-binding metadata
# ---------------------------------------------------------------------------


def test_helpful_command_defaults_are_empty() -> None:
    """A bare command carries no output spec and no wrapper binding."""
    cmd = HelpfulCommand("c")
    assert cmd.output_spec is None
    assert cmd.wraps_tool is None
    assert cmd.wraps_tools == ()
    assert cmd.intentionally_unsupported == frozenset()
    assert cmd.router_params == frozenset()
    assert cmd.client_only_params == frozenset()
    assert cmd.needs_runtime is True


def test_helpful_command_needs_runtime_opt_out_is_stored() -> None:
    """``needs_runtime=False`` (the agents verbs' declaration) is stored."""
    assert HelpfulCommand("c", needs_runtime=False).needs_runtime is False


def test_invoke_materializes_runtime_from_spec() -> None:
    """``invoke`` swaps a ``RuntimeSpec`` obj for the resolved ``Runtime``."""
    from deephaven_mcp.cli._runtime import RuntimeSpec

    sentinel = object()
    seen: list[object] = []

    @click.command(cls=HelpfulCommand)
    @click.pass_obj
    def c(obj: object) -> None:
        seen.append(obj)

    spec = RuntimeSpec()
    with patch.object(RuntimeSpec, "resolve", return_value=sentinel):
        result = CliRunner().invoke(c, [], obj=spec, standalone_mode=False)
    assert result.exit_code == 0
    assert seen == [sentinel]


def test_invoke_skips_load_when_needs_runtime_false() -> None:
    """A ``needs_runtime=False`` command receives the spec untouched."""
    from deephaven_mcp.cli._runtime import RuntimeSpec

    seen: list[object] = []

    @click.command(cls=HelpfulCommand, needs_runtime=False)
    @click.pass_obj
    def c(obj: object) -> None:
        seen.append(obj)

    spec = RuntimeSpec()
    with patch.object(
        RuntimeSpec,
        "resolve",
        side_effect=AssertionError("resolve must not be called"),
    ):
        result = CliRunner().invoke(c, [], obj=spec, standalone_mode=False)
    assert result.exit_code == 0
    assert seen == [spec]


def test_invoke_leaves_prebuilt_runtime_untouched() -> None:
    """A non-spec obj (e.g. a prebuilt ``Runtime`` in tests) passes through."""
    prebuilt = object()
    seen: list[object] = []

    @click.command(cls=HelpfulCommand)
    @click.pass_obj
    def c(obj: object) -> None:
        seen.append(obj)

    result = CliRunner().invoke(c, [], obj=prebuilt, standalone_mode=False)
    assert result.exit_code == 0
    assert seen == [prebuilt]


def test_helpful_command_stores_output_spec_and_wrapper_binding() -> None:
    """Every metadata field is stored verbatim for ``agents`` to read."""
    spec = OutputSpec("object", (OutputField("f", "string", "field"),))
    cmd = HelpfulCommand(
        "c",
        output_spec=spec,
        wraps_tool="t",
        wraps_tools=("a", "b"),
        intentionally_unsupported=frozenset({"x"}),
        router_params=frozenset({"system"}),
        client_only_params=frozenset({"print_only"}),
    )
    assert cmd.output_spec is spec
    assert cmd.wraps_tool == "t"
    assert cmd.wraps_tools == ("a", "b")
    assert cmd.intentionally_unsupported == frozenset({"x"})
    assert cmd.router_params == frozenset({"system"})
    assert cmd.client_only_params == frozenset({"print_only"})


def test_helpful_command_derives_output_spec_from_help_spec() -> None:
    """A command with a help_spec always exposes the spec's output."""
    output = OutputSpec("object", (OutputField("f", "string", "field"),))
    cmd = HelpfulCommand("c", help_spec=HelpSpec(summary="S.", output=output))
    assert cmd.output_spec is output


def test_helpful_command_rejects_output_spec_alongside_help_spec() -> None:
    """help_spec is the single source; a separate output_spec is rejected."""
    with pytest.raises(ValueError, match="help_spec.output"):
        HelpfulCommand(
            "c",
            help_spec=HelpSpec(summary="S."),
            output_spec=OutputSpec("text"),
        )


def test_helpful_group_leaves_default_to_helpful_command() -> None:
    """A ``HelpfulGroup``'s verbs are ``HelpfulCommand`` so they carry metadata."""
    assert HelpfulGroup.command_class is HelpfulCommand
    group = HelpfulGroup("g")

    @group.command("v", wraps_tool="some_tool")
    def _verb() -> None: ...

    leaf = group.commands["v"]
    assert isinstance(leaf, HelpfulCommand)
    assert leaf.wraps_tool == "some_tool"


def test_helpful_group_stores_help_spec() -> None:
    """HelpfulGroup renders help from a spec and stores it for the manifest."""
    group = HelpfulGroup("g", help_spec=HelpSpec(summary="Group summary."))
    assert group.help_spec is not None
    assert group.help_spec.summary == "Group summary."
    assert group.help is not None and "Group summary." in group.help


# ---------------------------------------------------------------------------
# _split_help_text — summary/description extraction for docstring help
# ---------------------------------------------------------------------------


def test_split_help_text_strips_no_wrap_marker() -> None:
    """build_help's \\b no-rewrap marker never reaches the manifest."""
    summary, description = _split_help_text("Summary.\n\n\b\nExamples:\n  $ x")
    assert "\b" not in summary
    assert description is not None and "\b" not in description


def test_split_help_text_handles_none() -> None:
    """A command with no help yields an empty summary and no description."""
    assert _split_help_text(None) == ("", None)


def test_split_help_text_dedents_docstring_indentation() -> None:
    """Raw docstring indentation (the group-help case) is removed."""
    text = "Do the thing.\n\n    Indented continuation\n    lines here."
    summary, description = _split_help_text(text)
    assert summary == "Do the thing."
    assert description == "Indented continuation\nlines here."


def test_split_help_text_collapses_wrapped_summary() -> None:
    """A summary paragraph wrapped over source lines becomes one line."""
    summary, description = _split_help_text("Line one\nline two.\n\nRest.")
    assert summary == "Line one line two."
    assert description == "Rest."


# ---------------------------------------------------------------------------
# manifest cleanliness
# ---------------------------------------------------------------------------


def _all_strings(obj: object):
    """Yield every string value reachable in a nested dict/list structure."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for value in obj.values():
            yield from _all_strings(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _all_strings(value)


@pytest.mark.parametrize(
    "payload_builder",
    [
        lambda: build_manifest(cli),
        lambda: build_summary_tree(cli),
        lambda: describe_command(cli.commands["daemon"].commands["start"]),
    ],
    ids=["manifest", "summary_tree", "standalone_node"],
)
def test_agents_surfaces_contain_no_markup_or_markers(payload_builder) -> None:
    """Every string on every agents surface is clean, agent-ready text.

    Locks the agent-facing contract: no summary, description, option
    help, error_code help, output-field help, or note carries click's
    \\b no-rewrap marker or backtick markup, so an agent can render
    the payload verbatim. Rejecting any backtick catches both
    double-backtick literals and single-backtick RST roles, matching
    the command-help contract in test_help_contract.py.
    """
    for text in _all_strings(payload_builder()):
        assert "\b" not in text
        assert "`" not in text


# ---------------------------------------------------------------------------
# build_manifest — the complete (--full) manifest
# ---------------------------------------------------------------------------


def test_manifest_includes_output_schema_for_leaf() -> None:
    """A leaf command exposes its structured output shape."""
    manifest = build_manifest(cli)
    call = manifest["commands"]["tool"]["subcommands"]["call"]
    out = call["output"]
    assert out["mode"] == "object"
    names = {field["name"] for field in out["fields"]}
    assert {"content", "isError"} <= names


def test_manifest_output_absent_for_group() -> None:
    """A group declares no output spec, so its node carries no output key."""
    manifest = build_manifest(cli)
    assert "output" not in manifest["commands"]["tool"]


def test_build_manifest_root_has_expected_top_level_keys() -> None:
    manifest = build_manifest(cli)
    assert manifest["prog"] == "dhcli"
    assert "version" in manifest
    # Structured summary/description replace the old rendered ``help``.
    assert "help" not in manifest
    assert manifest["summary"]
    assert "description" in manifest
    assert "examples" in manifest
    assert "global_options" in manifest
    assert "commands" in manifest
    assert "error_codes" in manifest
    assert "default_environment" in manifest
    assert "default_exit_codes" in manifest


def test_build_manifest_covers_every_noun_group() -> None:
    manifest = build_manifest(cli)
    for noun in ("daemon", "tool", "config", "agents"):
        assert noun in manifest["commands"]


def test_build_manifest_recurses_into_subcommands() -> None:
    """Group nodes carry a ``subcommands`` map with full nested nodes."""
    manifest = build_manifest(cli)
    daemon = manifest["commands"]["daemon"]
    assert "subcommands" in daemon
    status = daemon["subcommands"]["status"]
    assert status["name"] == "status"
    assert status["summary"]
    assert "help" not in status
    assert "short_help" not in status
    assert "params" not in status or status["params"]
    # Leaves carry no ``subcommands`` key at all (sparse contract).
    assert "subcommands" not in status


def test_manifest_group_summary_is_dedented() -> None:
    """Group docstring help loses its source indentation in the manifest."""
    manifest = build_manifest(cli)
    catalog = manifest["commands"]["catalog"]
    assert "\n    " not in catalog["summary"]
    assert "\n    " not in catalog.get("description", "")


def test_manifest_nodes_hoist_default_environment_and_code_meanings() -> None:
    """Inside the whole-tree manifest, defaults live once at the root.

    A node whose environment is the project default omits the key; its
    error codes are bare strings (meanings in the root registry) and
    its exit codes bare integers (meanings in default_exit_codes).
    """
    manifest = build_manifest(cli)
    start = manifest["commands"]["daemon"]["subcommands"]["start"]
    assert "environment" not in start
    assert all(isinstance(c, str) for c in start["error_codes"])
    assert all(isinstance(c, int) for c in start["exit_codes"])
    registry = {entry["code"] for entry in manifest["error_codes"]}
    assert set(start["error_codes"]) <= registry


def test_build_manifest_uses_stable_lowercase_kind() -> None:
    """``kind`` is ``"option"`` / ``"argument"``, not click's class name."""
    manifest = build_manifest(cli)
    kinds = {opt["kind"] for opt in manifest["global_options"]}
    assert kinds == {"option"}
    show = manifest["commands"]["tool"]["subcommands"]["show"]
    arg_kinds = {p["kind"] for p in show["params"] if p["name"] == "name"}
    assert arg_kinds == {"argument"}


def test_build_manifest_describes_options_with_envvars() -> None:
    manifest = build_manifest(cli)
    global_opts = {opt["name"]: opt for opt in manifest["global_options"]}
    assert "output" in global_opts
    assert global_opts["output"]["envvar"] == "DHCLI_OUTPUT"
    assert global_opts["output"]["type"] == "choice"
    assert "choices" in global_opts["output"]


def test_manifest_params_use_sparse_keys() -> None:
    """Param keys absent when false/empty/default; present when meaningful."""
    manifest = build_manifest(cli)
    global_opts = {opt["name"]: opt for opt in manifest["global_options"]}
    # ``-o/--output`` is not a flag, not multiple, not required, nargs 1.
    output = global_opts["output"]
    assert "is_flag" not in output
    assert "multiple" not in output
    assert "required" not in output
    assert "nargs" not in output
    assert "secondary_opts" not in output
    # ``--no-auto-start`` is a flag; its False default is implied.
    no_auto = global_opts["no_auto_start"]
    assert no_auto["is_flag"] is True
    assert "default" not in no_auto
    # ``agents command PATH...`` is variadic and required.
    node = manifest["commands"]["agents"]["subcommands"]["command"]
    path = next(p for p in node["params"] if p["name"] == "path")
    assert path["nargs"] == -1
    assert path["required"] is True


def test_manifest_positional_arguments_carry_help() -> None:
    """Positional args get their help from the spec's Arguments entries."""
    manifest = build_manifest(cli)
    show = manifest["commands"]["tool"]["subcommands"]["show"]
    name_arg = next(p for p in show["params"] if p["name"] == "name")
    assert "tool list" in name_arg["help"]


def test_build_manifest_lists_every_error_code() -> None:
    manifest = build_manifest(cli)
    codes = {entry["code"] for entry in manifest["error_codes"]}
    assert codes == {ec.value for ec in ErrorCode}


def test_manifest_advertises_universal_flags() -> None:
    """``universal_options`` discloses the every-command flags (--help, --agents).

    These are injected via ``get_params`` (not ``params``), so they appear
    in no command's ``params``; ``universal_options`` is the one place an
    agent can discover them from structured data.
    """
    manifest = build_manifest(cli)
    opts = {opt for entry in manifest["universal_options"] for opt in entry["opts"]}
    assert "--help" in opts
    assert "--agents" in opts


def test_build_manifest_accepts_plain_command_with_empty_commands() -> None:
    """When passed a :class:`click.Command` (not a Group), ``commands`` is empty."""

    @click.command()
    def standalone() -> None:
        """A standalone command, not a group."""

    manifest = build_manifest(standalone)
    assert manifest["prog"] == "standalone"
    assert manifest["commands"] == {}
    assert manifest["summary"] == "A standalone command, not a group."


def test_build_manifest_falls_back_to_unknown_version() -> None:
    """When the package metadata lookup fails, the manifest reports ``"unknown"``."""
    from importlib import metadata as importlib_metadata

    with patch.object(
        _help.metadata,
        "version",
        side_effect=importlib_metadata.PackageNotFoundError(),
    ):
        manifest = build_manifest(cli)
    assert manifest["version"] == "unknown"


# ---------------------------------------------------------------------------
# build_summary_tree — the compact orientation view
# ---------------------------------------------------------------------------


def test_summary_tree_shape() -> None:
    tree = build_summary_tree(cli)
    assert tree["prog"] == "dhcli"
    assert "version" in tree
    assert tree["summary"]
    assert "agents command" in tree["hint"]
    assert "--full" in tree["hint"]
    daemon = tree["commands"]["daemon"]
    assert daemon["summary"]
    # Group entries recurse; their children are leaves with summary only.
    start = daemon["commands"]["start"]
    assert set(start) == {"summary"}


def test_summary_tree_covers_every_leaf() -> None:
    """Every leaf command in the click tree appears in the summary tree."""

    def _leaf_paths(group: click.Group, prefix=()):
        for name, cmd in group.commands.items():
            path = (*prefix, name)
            if isinstance(cmd, click.Group):
                yield from _leaf_paths(cmd, path)
            else:
                yield path

    tree = build_summary_tree(cli)
    for path in _leaf_paths(cli):
        node = {"commands": tree["commands"]}
        for token in path:
            node = node["commands"][token]
        assert node["summary"]


def test_summary_tree_for_plain_command_has_empty_commands() -> None:
    @click.command()
    def standalone() -> None:
        """A standalone command."""

    tree = build_summary_tree(standalone)
    assert tree["commands"] == {}


# ---------------------------------------------------------------------------
# describe_command — standalone (self-contained) nodes
# ---------------------------------------------------------------------------


def test_standalone_leaf_node_is_self_contained() -> None:
    """A standalone node inlines env vars and code meanings.

    The node is the agent's --help: everything the rendered help text
    conveys must be present without fetching the root registry.
    """
    node = describe_command(cli.commands["daemon"].commands["start"])
    env_names = {e["name"] for e in node["environment"]}
    assert {e.name for e in COMMON_ENV_VARS} <= env_names
    assert all(
        isinstance(c, dict) and c["code"] and c["help"] for c in node["error_codes"]
    )
    assert all(
        isinstance(c, dict) and isinstance(c["code"], int) and c["help"]
        for c in node["exit_codes"]
    )


def test_standalone_group_node_lists_subcommand_summaries() -> None:
    """A group node is bounded: one level of name -> summary strings."""
    node = describe_command(cli.commands["session"])
    assert all(isinstance(v, str) and v for v in node["subcommands"].values())


def test_describe_command_recurse_expands_full_nodes() -> None:
    """``recurse=True`` nests full self-contained child nodes."""
    node = describe_command(cli.commands["daemon"], recurse=True)
    start = node["subcommands"]["start"]
    assert start["summary"]
    assert "environment" in start


def test_describe_command_handles_argument_without_help() -> None:
    """A plain command's Argument (no spec) yields no help key."""

    @click.command()
    @click.argument("name")
    def sample(name: str) -> None:
        """Sample."""

    info = describe_command(sample)
    arg = next(p for p in info["params"] if p["name"] == "name")
    assert "help" not in arg
    assert arg["kind"] == "argument"
    assert "type" in arg


# ---------------------------------------------------------------------------
# content preservation — every HelpSpec fact surfaces in the node
# ---------------------------------------------------------------------------


def _leaf_commands(group: click.Group, prefix=()):
    """Return ``(path, command)`` for every leaf command under ``group``."""
    leaves = []
    for name, cmd in group.commands.items():
        path = (*prefix, name)
        if isinstance(cmd, click.Group):
            leaves.extend(_leaf_commands(cmd, path))
        else:
            leaves.append((" ".join(path), cmd))
    return leaves


_LEAVES = _leaf_commands(cli)
_LEAF_IDS = [path for path, _ in _LEAVES]


@pytest.mark.parametrize(("path", "cmd"), _LEAVES, ids=_LEAF_IDS)
def test_standalone_node_preserves_every_help_spec_fact(
    path: str, cmd: click.Command
) -> None:
    """The standalone node carries every fact the command's --help renders.

    The content-preservation contract: an agent reading ``<cmd>
    --agents`` must lose nothing relative to a human reading ``<cmd>
    --help``.
    """
    assert isinstance(cmd, HelpfulCommand), path
    spec = cmd.help_spec
    assert spec is not None, path
    node = describe_command(cmd)
    assert node["summary"] == spec.summary
    if spec.description:
        assert node["description"] == spec.description
    for entry in spec.arguments or ():
        matches = [
            p
            for p in node["params"]
            if p["kind"] == "argument" and p.get("help") == entry.help
        ]
        assert matches, f"{path}: argument entry {entry.name!r} not surfaced"
    if spec.output is not None:
        assert node["output"]["mode"] == spec.output.mode
        assert [f.name for f in spec.output.fields] == [
            f["name"] for f in node["output"].get("fields", [])
        ]
    if spec.examples:
        assert node["examples"] == list(spec.examples)
    if spec.see_also:
        assert node["see_also"] == list(spec.see_also)
    if spec.error_codes:
        assert node["error_codes"] == [
            {"code": c.value, "help": c.help_text} for c in spec.error_codes
        ]
    expected_exits = spec.exit_codes if spec.exit_codes is not None else tuple(ExitCode)
    assert node["exit_codes"] == [
        {"code": c.value, "help": c.help_text} for c in expected_exits
    ]
    expected_env = spec.environment if spec.environment is not None else COMMON_ENV_VARS
    if expected_env:
        assert node["environment"] == [
            {"name": e.name, "help": e.help} for e in expected_env
        ]
    else:
        # Sparse keys: an explicitly empty disclosure omits the key.
        assert "environment" not in node
    for option in cmd.params:
        if isinstance(option, click.Option) and option.help:
            match = next(p for p in node["params"] if p["name"] == option.name)
            assert match["help"] == option.help


# ---------------------------------------------------------------------------
# _describe_wraps — the MCP-tool binding emitted for wrapper commands
# ---------------------------------------------------------------------------


def test_describe_wraps_none_for_group() -> None:
    """A group is not a HelpfulCommand, so it carries no wraps binding."""
    assert _describe_wraps(click.Group("g")) is None


def test_describe_wraps_none_for_plain_command() -> None:
    """A non-HelpfulCommand has no wrapper metadata."""
    assert _describe_wraps(click.Command("c")) is None


def test_describe_wraps_none_when_helpful_command_wraps_nothing() -> None:
    """A HelpfulCommand that fronts no tool (daemon/config verb) maps to None."""
    assert _describe_wraps(HelpfulCommand("c")) is None


def test_describe_wraps_single_tool_has_empty_exemptions() -> None:
    assert _describe_wraps(HelpfulCommand("c", wraps_tool="list_systems")) == {
        "tools": ["list_systems"],
        "intentionally_unsupported": [],
        "router_params": [],
        "client_only_params": [],
    }


def test_describe_wraps_merges_tools_and_sorts_every_field() -> None:
    """``tools`` is the sorted union of ``wraps_tool`` + ``wraps_tools``."""
    cmd = HelpfulCommand(
        "c",
        wraps_tool="z_tool",
        wraps_tools=("b_tool", "a_tool"),
        intentionally_unsupported=frozenset({"foo", "bar"}),
        router_params=frozenset({"system"}),
        client_only_params=frozenset({"print_only"}),
    )
    assert _describe_wraps(cmd) == {
        "tools": ["a_tool", "b_tool", "z_tool"],
        "intentionally_unsupported": ["bar", "foo"],
        "router_params": ["system"],
        "client_only_params": ["print_only"],
    }


def test_manifest_emits_wraps_for_passthrough_wrapper() -> None:
    """A wrapper verb carries its tool binding in the manifest."""
    manifest = build_manifest(cli)
    wraps = manifest["commands"]["system"]["subcommands"]["list"]["wraps"]
    assert wraps["tools"] == ["list_systems"]
    assert wraps["router_params"] == []
    assert wraps["client_only_params"] == []


def test_manifest_wraps_absent_for_non_wrapper_and_group() -> None:
    """``wraps`` is absent where no tool is wrapped (sparse contract)."""
    manifest = build_manifest(cli)
    assert "wraps" not in manifest["commands"]["daemon"]["subcommands"]["start"]
    assert "wraps" not in manifest["commands"]["session"]


def test_manifest_emits_router_and_client_params_for_session() -> None:
    """The system-router and client-side-composite bindings round-trip."""
    manifest = build_manifest(cli)
    create = manifest["commands"]["session"]["subcommands"]["create"]["wraps"]
    assert set(create["tools"]) == {
        "session_community_create",
        "session_enterprise_create",
    }
    assert create["router_params"] == ["system"]

    open_ = manifest["commands"]["session"]["subcommands"]["open"]["wraps"]
    assert open_["tools"] == ["session_community_credentials"]
    assert open_["client_only_params"] == ["print_only"]


# ---------------------------------------------------------------------------
# resolve_command — scoping the manifest to a command path
# ---------------------------------------------------------------------------


def test_resolve_command_empty_path_returns_root() -> None:
    """An empty path resolves to the root command unchanged."""
    assert resolve_command(cli, ()) is cli


def test_resolve_command_descends_to_group_and_leaf() -> None:
    """A path descends through groups to the named leaf command."""
    daemon = resolve_command(cli, ("daemon",))
    assert daemon is cli.commands["daemon"]
    start = resolve_command(cli, ("daemon", "start"))
    assert start is cli.commands["daemon"].commands["start"]


def test_resolve_command_unknown_noun_raises_command_not_found() -> None:
    """An unknown top-level token raises COMMAND_NOT_FOUND (exit 2)."""
    with pytest.raises(CliError) as excinfo:
        resolve_command(cli, ("bogus",))
    assert excinfo.value.code is ErrorCode.COMMAND_NOT_FOUND
    assert excinfo.value.exit_code == 2


def test_resolve_command_unknown_verb_raises_command_not_found() -> None:
    """An unknown verb under a real noun raises COMMAND_NOT_FOUND."""
    with pytest.raises(CliError) as excinfo:
        resolve_command(cli, ("daemon", "bogus"))
    assert excinfo.value.code is ErrorCode.COMMAND_NOT_FOUND


def test_resolve_command_descend_into_leaf_raises_command_not_found() -> None:
    """Asking a leaf (non-group) command to descend raises COMMAND_NOT_FOUND."""
    with pytest.raises(CliError) as excinfo:
        resolve_command(cli, ("daemon", "start", "deeper"))
    assert excinfo.value.code is ErrorCode.COMMAND_NOT_FOUND


# ---------------------------------------------------------------------------
# --agents flag — the universal twin of --help
# ---------------------------------------------------------------------------


def test_agents_flag_on_root_emits_summary_tree() -> None:
    """``dhcli --agents`` equals the summary tree (== ``agents tree``)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--agents"], standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output) == build_summary_tree(cli)


def test_agents_flag_on_leaf_matches_command_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``daemon start --agents`` equals the standalone ``daemon start`` node.

    The runtime-load bypass inspects ``sys.argv``; ``CliRunner.invoke``
    does not set it, so the test patches it explicitly (mirroring the
    ``--help`` path) — otherwise CI without a default config dir fails.
    """
    argv = ["daemon", "start", "--agents"]
    monkeypatch.setattr("sys.argv", ["dhcli", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == describe_command(cli.commands["daemon"].commands["start"])


def test_agents_flag_on_group_matches_group_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``daemon --agents`` equals the bounded ``daemon`` group node."""
    argv = ["daemon", "--agents"]
    monkeypatch.setattr("sys.argv", ["dhcli", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output) == describe_command(cli.commands["daemon"])


def test_agents_flag_honors_output_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag honors the root ``-o`` flag (rendered as YAML here)."""
    argv = ["-o", "yaml", "daemon", "start", "--agents"]
    monkeypatch.setattr("sys.argv", ["dhcli", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload == describe_command(cli.commands["daemon"].commands["start"])


def test_agents_flag_not_hoisted_to_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``config show --agents`` describes ``config show``, not the root.

    The flag is injected lazily (never in ``cli.params``), so the
    option-lifter never sees it and cannot hoist it to the root.
    """
    argv = ["config", "show", "--agents"]
    monkeypatch.setattr("sys.argv", ["dhcli", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["name"] == "show"
    # Not the summary tree.
    assert "version" not in payload


def test_agents_flag_bypasses_config_load(tmp_path, monkeypatch) -> None:
    """``--agents`` renders even when the config dir is empty/malformed.

    The bypass inspects ``sys.argv``; ``CliRunner.invoke`` does not set
    it, so the test patches it explicitly (mirroring the ``--help`` path).
    """
    argv = ["--config-dir", str(tmp_path / "nonexistent"), "daemon", "--agents"]
    monkeypatch.setattr("sys.argv", ["dhcli", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output)["name"] == "daemon"


def test_agents_flag_absent_from_manifest_params() -> None:
    """``--agents`` is invisible to the manifest, exactly like ``--help``.

    Lazy injection (via ``get_params``) keeps it out of ``cmd.params``,
    which is what :func:`build_manifest` reads.
    """
    manifest = build_manifest(cli)

    def _all_opts(node: dict) -> set[str]:
        opts: set[str] = set()
        for param in node.get("params", []):
            opts.update(param.get("opts", []))
        subcommands = node.get("subcommands", {})
        if isinstance(subcommands, dict):
            for sub in subcommands.values():
                if isinstance(sub, dict):
                    opts |= _all_opts(sub)
        return opts

    seen = {opt for p in manifest["global_options"] for opt in p.get("opts", [])}
    for node in manifest["commands"].values():
        seen |= _all_opts(node)
    assert "--agents" not in seen
    assert "--help" not in seen


def test_get_params_never_mutates_command_params() -> None:
    """Repeated ``get_params`` calls do not grow ``cmd.params``.

    When a command disables its help option, click's ``get_params``
    returns ``self.params`` itself; the ``--agents`` injection must
    build a new list, not append in place, or every call would add
    another ``--agents`` to the command permanently.
    """
    cmd = HelpfulCommand("c", add_help_option=False)
    ctx = click.Context(cmd)
    before = len(cmd.params)
    for _ in range(3):
        params = cmd.get_params(ctx)
        assert sum(1 for p in params if "--agents" in p.opts) == 1
    assert len(cmd.params) == before
