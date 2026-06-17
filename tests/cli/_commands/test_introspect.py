"""Tests for ``deephaven_mcp.cli._commands.introspect``."""

from __future__ import annotations

import json
from unittest.mock import patch

import click
import pytest
import yaml
from click.testing import CliRunner

from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._help import (
    _clean_help,
    _resolve_command,
    build_manifest,
)
from deephaven_mcp.cli._main import cli


def test_clean_help_strips_no_wrap_marker() -> None:
    """build_help's \\b no-rewrap marker is removed for the manifest."""
    assert _clean_help("\b\nExamples:\n  $ x") == "Examples:\n  $ x"


def test_clean_help_handles_none() -> None:
    """A command with neither help nor short_help yields the empty string."""
    assert _clean_help(None) == ""


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


def test_manifest_contains_no_markup_or_markers() -> None:
    """Every string anywhere in the manifest is clean, agent-ready text.

    Locks the agent-facing contract: no command help, option help,
    error_code help, output-field help, or note carries click's \\b
    no-rewrap marker or reStructuredText backticks, so an agent can
    render the manifest verbatim.
    """
    for text in _all_strings(build_manifest(cli)):
        assert "\b" not in text
        assert "``" not in text


def test_manifest_includes_output_schema_for_leaf() -> None:
    """A leaf command exposes its structured output shape."""
    manifest = build_manifest(cli)
    call = manifest["commands"]["tool"]["subcommands"]["call"]
    out = call["output"]
    assert out is not None
    assert out["mode"] == "object"
    names = {field["name"] for field in out["fields"]}
    assert {"content", "isError"} <= names


def test_manifest_output_is_none_for_group() -> None:
    """A group declares no output spec, so its manifest output is None."""
    manifest = build_manifest(cli)
    assert manifest["commands"]["tool"]["output"] is None


def test_describe_output_handles_none() -> None:
    """A command without an output spec maps to None."""
    from deephaven_mcp.cli._help import _describe_output

    assert _describe_output(None) is None


def test_build_manifest_root_has_expected_top_level_keys() -> None:
    manifest = build_manifest(cli)
    assert manifest["prog"] == "dh-mcp"
    assert "version" in manifest
    # ``help`` (not ``description``) for symmetry with ``_describe_command``.
    assert "help" in manifest
    assert "description" not in manifest
    assert "global_options" in manifest
    assert "commands" in manifest
    assert "error_codes" in manifest
    assert "default_environment" in manifest
    assert "default_exit_codes" in manifest


def test_build_manifest_covers_every_noun_group() -> None:
    manifest = build_manifest(cli)
    for noun in ("daemon", "tool", "config", "introspect"):
        assert noun in manifest["commands"]


def test_build_manifest_recurses_into_subcommands() -> None:
    """Group commands carry a ``subcommands`` map with full verb shape."""
    manifest = build_manifest(cli)
    daemon = manifest["commands"]["daemon"]
    assert "subcommands" in daemon
    status = daemon["subcommands"]["status"]
    # Each verb carries the same canonical fields.
    assert status["name"] == "status"
    assert "help" in status
    assert "params" in status
    # ``subcommands`` is always present (consistent with ``commands`` on the
    # root manifest) and empty for leaf verbs so agents can index it without
    # a presence check.
    assert status["subcommands"] == {}


def test_build_manifest_uses_stable_lowercase_kind() -> None:
    """``kind`` is ``"option"`` / ``"argument"``, not click's class name."""
    manifest = build_manifest(cli)
    # Root has options; assert at least one is tagged ``"option"`` (lowercase).
    kinds = {opt["kind"] for opt in manifest["global_options"]}
    assert kinds == {"option"}
    # ``tool show NAME`` carries one positional argument.
    show = manifest["commands"]["tool"]["subcommands"]["show"]
    arg_kinds = {p["kind"] for p in show["params"] if p["name"] == "name"}
    assert arg_kinds == {"argument"}


def test_build_manifest_describes_options_with_envvars() -> None:
    manifest = build_manifest(cli)
    global_opts = {opt["name"]: opt for opt in manifest["global_options"]}
    # ``-o/--output`` is documented and carries its envvar.
    assert "output" in global_opts
    assert global_opts["output"]["envvar"] == "DH_MCP_OUTPUT"
    # ``type`` is always present; ``choices`` is additionally present
    # because the option's type is ``click.Choice``.
    assert global_opts["output"]["type"] == "choice"
    assert "choices" in global_opts["output"]


def test_build_manifest_emits_nargs_for_every_param() -> None:
    """Every option and argument carries ``nargs`` for variadic discovery."""
    manifest = build_manifest(cli)
    for opt in manifest["global_options"]:
        assert "nargs" in opt
    # Spot-check a positional argument too: ``dh-mcp tool show NAME``.
    show = manifest["commands"]["tool"]["subcommands"]["show"]
    name_arg = next(p for p in show["params"] if p["name"] == "name")
    assert "nargs" in name_arg


def test_build_manifest_lists_every_error_code() -> None:
    manifest = build_manifest(cli)
    codes = {entry["code"] for entry in manifest["error_codes"]}
    assert codes == {ec.value for ec in ErrorCode}


def test_manifest_advertises_universal_flags() -> None:
    """``universal_options`` discloses the every-command flags (--help, --introspect).

    These are injected via ``get_params`` (not ``params``), so they appear
    in no command's ``params``; ``universal_options`` is the one place an
    agent can discover them from structured data.
    """
    manifest = build_manifest(cli)
    opts = {opt for entry in manifest["universal_options"] for opt in entry["opts"]}
    assert "--help" in opts
    assert "--introspect" in opts


def test_introspect_tree_defaults_to_json() -> None:
    """No ``-o`` and no ``DH_MCP_OUTPUT`` -> defaults to json, like every command."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect", "tree"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["prog"] == "dh-mcp"


def test_introspect_tree_honors_human_with_flag() -> None:
    """``-o human`` opts out of the json default into terminal-friendly output."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-o", "human", "introspect", "tree"], standalone_mode=False
    )
    assert result.exit_code == 0
    # Human output is not JSON; assert it doesn't parse as JSON.
    try:
        json.loads(result.output)
    except json.JSONDecodeError:
        pass
    else:  # pragma: no cover - regression guard only
        raise AssertionError("human output unexpectedly parsed as JSON")
    assert "dh-mcp" in result.output


def test_introspect_tree_honors_yaml_output_mode() -> None:
    """``-o yaml`` now produces a YAML document (previously always JSON)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "yaml", "introspect", "tree"])
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["prog"] == "dh-mcp"


def test_introspect_tree_honors_human_output_mode() -> None:
    """``-o human`` is supported even though it produces a less useful manifest."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "human", "introspect", "tree"])
    assert result.exit_code == 0
    # Human output is not JSON; assert it doesn't parse as JSON to lock the contract.
    try:
        json.loads(result.output)
    except json.JSONDecodeError:
        pass
    else:  # pragma: no cover - regression guard only
        raise AssertionError("human output unexpectedly parsed as JSON")
    # And it should still mention the program name somewhere.
    assert "dh-mcp" in result.output


def test_introspect_tree_honors_envvar_output_mode() -> None:
    """``DH_MCP_OUTPUT=yaml`` selects YAML without ``-o``."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect", "tree"], env={"DH_MCP_OUTPUT": "yaml"})
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["prog"] == "dh-mcp"


def test_describe_command_handles_argument_without_help() -> None:
    """Click ``Argument`` lacks ``.help``; introspect must not crash."""

    @click.command()
    @click.argument("name")
    def sample(name: str) -> None:
        """Sample."""

    # Direct call to ensure the param-description path tolerates Arguments.
    from deephaven_mcp.cli._help import _describe_command

    info = _describe_command(sample)
    arg = next(p for p in info["params"] if p["name"] == "name")
    assert "help" not in arg
    # And the new ``nargs`` / ``type`` keys are present on arguments too.
    assert "nargs" in arg
    assert "type" in arg


def test_build_manifest_accepts_plain_command_with_empty_commands() -> None:
    """When passed a :class:`click.Command` (not a Group), ``commands`` is empty.

    Locks the contract that :func:`build_manifest` accepts any
    :class:`click.Command` and simply omits subcommand enumeration
    when the command is not a :class:`click.Group`. This is what
    eliminated the need for a ``cast`` at the call site.
    """

    @click.command()
    def standalone() -> None:
        """A standalone command, not a group."""

    manifest = build_manifest(standalone)
    assert manifest["prog"] == "standalone"
    assert manifest["commands"] == {}


def test_build_manifest_falls_back_to_unknown_version() -> None:
    """When the package metadata lookup fails, the manifest reports ``"unknown"``."""
    from importlib import metadata as importlib_metadata

    from deephaven_mcp.cli import _help as help_mod

    with patch.object(
        help_mod.metadata,
        "version",
        side_effect=importlib_metadata.PackageNotFoundError(),
    ):
        manifest = build_manifest(cli)
    assert manifest["version"] == "unknown"


# ---------------------------------------------------------------------------
# _describe_wraps — the MCP-tool binding emitted for wrapper commands
# ---------------------------------------------------------------------------


def test_describe_wraps_none_for_group() -> None:
    """A group is not a HelpfulCommand, so it carries no wraps binding."""
    from deephaven_mcp.cli._help import _describe_wraps

    assert _describe_wraps(click.Group("g")) is None


def test_describe_wraps_none_for_plain_command() -> None:
    """A non-HelpfulCommand has no wrapper metadata."""
    from deephaven_mcp.cli._help import _describe_wraps

    assert _describe_wraps(click.Command("c")) is None


def test_describe_wraps_none_when_helpful_command_wraps_nothing() -> None:
    """A HelpfulCommand that fronts no tool (daemon/config verb) maps to None."""
    from deephaven_mcp.cli._help import HelpfulCommand, _describe_wraps

    assert _describe_wraps(HelpfulCommand("c")) is None


def test_describe_wraps_single_tool_has_empty_exemptions() -> None:
    from deephaven_mcp.cli._help import HelpfulCommand, _describe_wraps

    assert _describe_wraps(HelpfulCommand("c", wraps_tool="list_systems")) == {
        "tools": ["list_systems"],
        "intentionally_unsupported": [],
        "router_params": [],
        "client_only_params": [],
    }


def test_describe_wraps_merges_tools_and_sorts_every_field() -> None:
    """``tools`` is the sorted union of ``wraps_tool`` + ``wraps_tools``."""
    from deephaven_mcp.cli._help import HelpfulCommand, _describe_wraps

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


def test_manifest_wraps_is_none_for_non_wrapper_and_group() -> None:
    """``wraps`` is always present, and ``None`` where no tool is wrapped."""
    manifest = build_manifest(cli)
    # A daemon verb wraps no MCP tool.
    assert manifest["commands"]["daemon"]["subcommands"]["start"]["wraps"] is None
    # A group itself wraps nothing.
    assert manifest["commands"]["session"]["wraps"] is None


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
# _resolve_command — scoping introspect to a command path
# ---------------------------------------------------------------------------


def test_resolve_command_empty_path_returns_root() -> None:
    """An empty path resolves to the root command unchanged."""
    assert _resolve_command(cli, ()) is cli


def test_resolve_command_descends_to_group_and_leaf() -> None:
    """A path descends through groups to the named leaf command."""
    daemon = _resolve_command(cli, ("daemon",))
    assert daemon is cli.commands["daemon"]
    start = _resolve_command(cli, ("daemon", "start"))
    assert start is cli.commands["daemon"].commands["start"]


def test_resolve_command_unknown_noun_raises_command_not_found() -> None:
    """An unknown top-level token raises COMMAND_NOT_FOUND (exit 2)."""
    with pytest.raises(CliError) as excinfo:
        _resolve_command(cli, ("bogus",))
    assert excinfo.value.code is ErrorCode.COMMAND_NOT_FOUND
    assert excinfo.value.exit_code == 2


def test_resolve_command_unknown_verb_raises_command_not_found() -> None:
    """An unknown verb under a real noun raises COMMAND_NOT_FOUND."""
    with pytest.raises(CliError) as excinfo:
        _resolve_command(cli, ("daemon", "bogus"))
    assert excinfo.value.code is ErrorCode.COMMAND_NOT_FOUND


def test_resolve_command_descend_into_leaf_raises_command_not_found() -> None:
    """Asking a leaf (non-group) command to descend raises COMMAND_NOT_FOUND."""
    with pytest.raises(CliError) as excinfo:
        _resolve_command(cli, ("daemon", "start", "deeper"))
    assert excinfo.value.code is ErrorCode.COMMAND_NOT_FOUND


# ---------------------------------------------------------------------------
# introspect command <path> — one command's node
# ---------------------------------------------------------------------------


def test_introspect_command_to_noun_matches_manifest_node() -> None:
    """``introspect command daemon`` equals the ``.commands.daemon`` node."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["introspect", "command", "daemon"], standalone_mode=False
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == build_manifest(cli)["commands"]["daemon"]


def test_introspect_command_to_verb_matches_manifest_node() -> None:
    """``introspect command daemon start`` equals the nested ``subcommands`` node.

    Pins the documented invariant:
    ``introspect command daemon start`` == ``introspect tree -o json | jq
    '.commands.daemon.subcommands.start'``.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["introspect", "command", "daemon", "start"],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    expected = build_manifest(cli)["commands"]["daemon"]["subcommands"]["start"]
    assert payload == expected
    # The node carries no top-level manifest keys.
    assert "version" not in payload
    assert "error_codes" not in payload


def test_introspect_command_requires_path() -> None:
    """``introspect command`` with no PATH is a usage error, not a JSON dump.

    PATH is required, so an empty invocation names the missing argument
    and exits 2 — consistent with other required-argument commands.
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect", "command"])
    assert result.exit_code == 2
    assert "PATH" in result.output


def test_introspect_command_unknown_path_exits_2() -> None:
    """A path that does not resolve fails with COMMAND_NOT_FOUND, exit 2."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect", "command", "daemon", "bogus"])
    assert result.exit_code == 2


def test_introspect_command_honors_yaml_output_mode() -> None:
    """``-o yaml introspect command daemon`` renders the node as YAML."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "yaml", "introspect", "command", "daemon"])
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload == build_manifest(cli)["commands"]["daemon"]


def test_introspect_command_bypasses_config_load(tmp_path) -> None:
    """``introspect command`` works even when the config dir is empty/malformed.

    The runtime-load bypass fires on ``invoked_subcommand == "introspect"``
    (matched here), as well as on ``--help`` / ``--introspect`` at any
    depth, so an agent can learn one subtree before any valid config
    exists.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-dir",
            str(tmp_path / "nonexistent"),
            "introspect",
            "command",
            "daemon",
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["name"] == "daemon"


# ---------------------------------------------------------------------------
# introspect errors / tree equivalences and bare group
# ---------------------------------------------------------------------------


def test_introspect_errors_matches_manifest_registry() -> None:
    """``introspect errors`` equals the ``error_codes`` slice of the manifest."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect", "errors"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == build_manifest(cli)["error_codes"]
    assert {entry["code"] for entry in payload} == {ec.value for ec in ErrorCode}


def test_introspect_tree_matches_build_manifest() -> None:
    """``introspect tree`` equals :func:`build_manifest` for the root."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect", "tree"], standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output) == build_manifest(cli)


def test_bare_introspect_shows_group_help() -> None:
    """``dh-mcp introspect`` with no verb lists the verbs (like any bare noun)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect"])
    # A group with no subcommand: usage error (exit 2) plus the verb listing,
    # identical to bare ``dh-mcp daemon``.
    assert result.exit_code == 2
    for verb in ("tree", "command", "errors"):
        assert verb in result.output


# ---------------------------------------------------------------------------
# --introspect flag — the universal twin of --help
# ---------------------------------------------------------------------------


def test_introspect_flag_on_root_emits_whole_tree() -> None:
    """``dh-mcp --introspect`` equals the whole-tree manifest."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--introspect"], standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output) == build_manifest(cli)


def test_introspect_flag_on_leaf_matches_command_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``daemon start --introspect`` equals the ``daemon start`` manifest node.

    The runtime-load bypass inspects ``sys.argv``; ``CliRunner.invoke``
    does not set it, so the test patches it explicitly (mirroring the
    ``--help`` path) — otherwise CI without a default config dir fails.
    """
    argv = ["daemon", "start", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == build_manifest(cli)["commands"]["daemon"]["subcommands"]["start"]


def test_introspect_flag_on_group_matches_group_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``daemon --introspect`` equals the ``daemon`` group node."""
    argv = ["daemon", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output) == build_manifest(cli)["commands"]["daemon"]


def test_introspect_flag_equals_command_verb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag and the ``command`` verb produce byte-identical nodes."""
    runner = CliRunner()
    flag_argv = ["tool", "call", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *flag_argv])
    via_flag = runner.invoke(cli, flag_argv, standalone_mode=False)
    via_verb = runner.invoke(
        cli, ["introspect", "command", "tool", "call"], standalone_mode=False
    )
    assert via_flag.exit_code == via_verb.exit_code == 0
    assert json.loads(via_flag.output) == json.loads(via_verb.output)


def test_introspect_flag_honors_output_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag honors the root ``-o`` flag (rendered as YAML here)."""
    argv = ["-o", "yaml", "daemon", "start", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload == build_manifest(cli)["commands"]["daemon"]["subcommands"]["start"]


def test_introspect_flag_not_hoisted_to_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``config show --introspect`` describes ``config show``, not the root.

    The flag is injected lazily (never in ``cli.params``), so the
    option-lifter never sees it and cannot hoist it to the root.
    """
    argv = ["config", "show", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["name"] == "show"
    # Not the whole-tree manifest.
    assert "version" not in payload


def test_introspect_flag_bypasses_config_load(tmp_path, monkeypatch) -> None:
    """``--introspect`` renders even when the config dir is empty/malformed.

    The bypass inspects ``sys.argv``; ``CliRunner.invoke`` does not set
    it, so the test patches it explicitly (mirroring the ``--help`` path).
    """
    argv = ["--config-dir", str(tmp_path / "nonexistent"), "daemon", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    runner = CliRunner()
    result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output)["name"] == "daemon"


def test_introspect_flag_absent_from_manifest_params() -> None:
    """``--introspect`` is invisible to the manifest, exactly like ``--help``.

    Lazy injection (via ``get_params``) keeps it out of ``cmd.params``,
    which is what :func:`build_manifest` reads.
    """
    manifest = build_manifest(cli)

    def _all_opts(node: dict) -> set[str]:
        opts: set[str] = set()
        for param in node.get("params", []):
            opts.update(param.get("opts", []))
        for sub in node.get("subcommands", {}).values():
            opts |= _all_opts(sub)
        return opts

    seen = {opt for p in manifest["global_options"] for opt in p.get("opts", [])}
    for node in manifest["commands"].values():
        seen |= _all_opts(node)
    assert "--introspect" not in seen
    assert "--help" not in seen
