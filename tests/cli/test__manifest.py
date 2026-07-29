"""Tests for ``deephaven_mcp.cli._manifest``.

Covers the agents manifest: the node schema, the summary tree, the
single narrowing boundary, and the content-preservation contract that
every ``HelpSpec`` fact surfaces in a command's node.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import click
import pytest
import yaml
from click.testing import CliRunner

from deephaven_mcp.cli import _manifest
from deephaven_mcp.cli._command import HelpfulCommand, HelpfulGroup
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._help import (
    COMMON_ENV_VARS,
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._manifest import (
    _describe_wraps,
    _split_help_text,
    build_manifest,
    build_summary_tree,
    describe_command,
    resolve_command,
)

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


def test_build_manifest_accepts_a_non_group_root_with_empty_commands() -> None:
    """When the root is a leaf rather than a group, ``commands`` is empty."""

    @click.command(cls=HelpfulCommand)
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
        _manifest.metadata,
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


def test_summary_tree_for_a_non_group_root_has_empty_commands() -> None:
    @click.command(cls=HelpfulCommand)
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
    """An Argument on a command with no spec yields no help key."""

    @click.command(cls=HelpfulCommand)
    @click.argument("name")
    def sample(name: str) -> None:
        """Sample."""

    info = describe_command(sample)
    arg = next(p for p in info["params"] if p["name"] == "name")
    assert "help" not in arg
    assert arg["kind"] == "argument"
    assert "type" in arg


def test_describe_command_handles_option_without_help() -> None:
    """An Option declared with no ``help=`` yields no help key."""

    @click.command(cls=HelpfulCommand)
    @click.option("--flag", is_flag=True)
    def sample(flag: bool) -> None:
        """Sample."""

    info = describe_command(sample)
    opt = next(p for p in info["params"] if p["name"] == "flag")
    assert "help" not in opt
    assert opt["kind"] == "option"


def test_describe_command_omits_help_for_undocumented_argument() -> None:
    """An argument absent from a spec that documents others yields no help.

    The map is consulted per argument, so documenting one positional
    must not attach its text to another.
    """

    @click.command(
        cls=HelpfulCommand,
        help_spec=HelpSpec(
            summary="S.",
            arguments=(HelpEntry("FIRST", "The documented one."),),
        ),
    )
    @click.argument("first")
    @click.argument("second")
    def sample(first: str, second: str) -> None:
        """Sample."""

    params = {p["name"]: p for p in describe_command(sample)["params"]}
    assert params["first"]["help"] == "The documented one."
    assert "help" not in params["second"]


def test_embedded_node_omits_entries_the_spec_leaves_unset() -> None:
    """An embedded node drops what the manifest root already carries.

    A minimal spec has no examples or see_also, and leaves exit codes
    and environment to the project-wide defaults; an embedded node emits
    none of them, because ``build_manifest`` states them once at the
    root.
    """
    cmd = HelpfulCommand("c", help_spec=HelpSpec(summary="S."))
    node = describe_command(cmd, style=_manifest.NodeStyle.EMBEDDED)
    assert node["summary"] == "S."
    for key in ("examples", "see_also", "exit_codes", "environment"):
        assert key not in node


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
# _meta_of — the single narrowing boundary
# ---------------------------------------------------------------------------


def test_meta_of_rejects_a_plain_click_command() -> None:
    """A command registered without ``cls=Helpful*`` is a wiring bug.

    It would otherwise be described with no summary, no output schema,
    and no tool binding -- silently. Raising surfaces it on the first
    ``dhcli agents`` call. This replaces three separate tolerant
    fallbacks that each returned an empty value instead.
    """
    with pytest.raises(TypeError, match="must be a HelpfulCommand or HelpfulGroup"):
        _manifest._meta_of(click.Command("c"))


def test_meta_of_accepts_both_command_and_group() -> None:
    """Both concrete classes narrow to the shared metadata base."""
    cmd = HelpfulCommand("c")
    group = HelpfulGroup("g")
    assert _manifest._meta_of(cmd) is cmd
    assert _manifest._meta_of(group) is group


def test_group_reports_the_truthful_metadata_defaults() -> None:
    """A group inherits the metadata surface, reporting "wraps nothing".

    This is what lets the manifest read attributes off any node without
    narrowing to a specific subclass.
    """
    group = HelpfulGroup("g")
    assert group.wraps_tool is None
    assert group.wraps_tools == ()
    assert group.output_spec is None
    assert group.help_spec is None
    assert group.client_only_params == frozenset()


# ---------------------------------------------------------------------------
# _describe_wraps — the MCP-tool binding emitted for wrapper commands
# ---------------------------------------------------------------------------


def test_describe_wraps_none_for_group() -> None:
    """A group declares no tool, so it carries no wraps binding.

    ``None`` now means exactly one thing -- wraps no tool -- rather than
    doubling as "not one of our classes".
    """
    assert _describe_wraps(HelpfulGroup("g")) is None


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
