"""Tests for ``deephaven_mcp.cli._commands.agents``."""

from __future__ import annotations

import json

import pytest
import yaml
from click.testing import CliRunner

from deephaven_mcp.cli._errors import ErrorCode
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._manifest import (
    build_manifest,
    build_summary_tree,
    describe_command,
)

# ---------------------------------------------------------------------------
# agents tree — summary by default, --full for the complete manifest
# ---------------------------------------------------------------------------


def test_agents_tree_defaults_to_summary_json() -> None:
    """No flags -> the compact summary tree as compact JSON."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "tree"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == build_summary_tree(cli)
    # Compact JSON: a single line, no indentation whitespace.
    assert "\n" not in result.output.strip()


def test_agents_tree_json_pretty_emits_indented_json() -> None:
    """``-o json-pretty`` emits the same payload as an indented document."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-o", "json-pretty", "agents", "tree"], standalone_mode=False
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == build_summary_tree(cli)
    assert "\n" in result.output.strip()


def test_agents_tree_full_matches_build_manifest() -> None:
    """``agents tree --full`` equals :func:`build_manifest` for the root."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "tree", "--full"], standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output) == build_manifest(cli)


def test_agents_tree_honors_human_with_flag() -> None:
    """``-o human`` opts out of the json default into terminal-friendly output."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-o", "human", "agents", "tree"], standalone_mode=False
    )
    assert result.exit_code == 0
    try:
        json.loads(result.output)
    except json.JSONDecodeError:
        pass
    else:  # pragma: no cover - regression guard only
        raise AssertionError("human output unexpectedly parsed as JSON")
    assert "dhcli" in result.output


def test_agents_tree_honors_yaml_output_mode() -> None:
    """``-o yaml`` produces a YAML document."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "yaml", "agents", "tree"])
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["prog"] == "dhcli"


def test_agents_tree_honors_envvar_output_mode() -> None:
    """``DHCLI_OUTPUT=yaml`` selects YAML without ``-o``."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "tree"], env={"DHCLI_OUTPUT": "yaml"})
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["prog"] == "dhcli"


# ---------------------------------------------------------------------------
# agents command <path> — one command's node
# ---------------------------------------------------------------------------


def test_agents_command_to_noun_matches_standalone_node() -> None:
    """``agents command daemon`` equals the standalone group node."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "command", "daemon"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == describe_command(cli.commands["daemon"])
    # Bounded view: subcommand values are summary strings.
    assert all(isinstance(v, str) for v in payload["subcommands"].values())


def test_agents_command_to_verb_matches_standalone_node() -> None:
    """``agents command daemon start`` equals the standalone leaf node."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["agents", "command", "daemon", "start"],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == describe_command(cli.commands["daemon"].commands["start"])
    # The node carries no top-level manifest keys.
    assert "version" not in payload


def test_agents_command_json_pretty_emits_indented_json() -> None:
    """``-o json-pretty`` emits the same node as an indented document."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["-o", "json-pretty", "agents", "command", "daemon", "start"],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == describe_command(cli.commands["daemon"].commands["start"])
    assert "\n" in result.output.strip()


def test_agents_command_full_expands_group() -> None:
    """``agents command daemon --full`` recurses into full child nodes."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["agents", "command", "daemon", "--full"], standalone_mode=False
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == describe_command(cli.commands["daemon"], recurse=True)
    assert payload["subcommands"]["start"]["summary"]


def test_agents_command_requires_path() -> None:
    """``agents command`` with no PATH is a usage error, not a JSON dump."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "command"])
    assert result.exit_code == 2
    assert "PATH" in result.output


def test_agents_command_unknown_path_exits_2() -> None:
    """A path that does not resolve fails with COMMAND_NOT_FOUND, exit 2."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "command", "daemon", "bogus"])
    assert result.exit_code == 2


def test_agents_command_honors_yaml_output_mode() -> None:
    """``-o yaml agents command daemon`` renders the node as YAML."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "yaml", "agents", "command", "daemon"])
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload == describe_command(cli.commands["daemon"])


def test_agents_command_bypasses_config_load(tmp_path) -> None:
    """``agents command`` works even when the config dir is empty/malformed.

    The runtime-load bypass fires on ``invoked_subcommand == "agents"``
    (matched here), as well as on ``--help`` / ``--agents`` at any
    depth, so an agent can learn one subtree before any valid config
    exists.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-dir",
            str(tmp_path / "nonexistent"),
            "agents",
            "command",
            "daemon",
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["name"] == "daemon"


# ---------------------------------------------------------------------------
# agents errors / bare group
# ---------------------------------------------------------------------------


def test_agents_errors_matches_manifest_registry() -> None:
    """``agents errors`` equals the ``error_codes`` slice of the manifest."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents", "errors"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == build_manifest(cli)["error_codes"]
    assert {entry["code"] for entry in payload} == {ec.value for ec in ErrorCode}


def test_agents_errors_json_pretty_emits_indented_json() -> None:
    """``-o json-pretty`` emits the same registry as an indented document."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-o", "json-pretty", "agents", "errors"], standalone_mode=False
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == build_manifest(cli)["error_codes"]
    assert "\n" in result.output.strip()


def test_bare_agents_shows_group_help() -> None:
    """``dhcli agents`` with no verb lists the verbs (like any bare noun)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agents"])
    # A group with no subcommand: usage error (exit 2) plus the verb listing,
    # identical to bare ``dhcli daemon``.
    assert result.exit_code == 2
    for verb in ("tree", "command", "errors"):
        assert verb in result.output


# ---------------------------------------------------------------------------
# --agents flag equivalence — the verb matches the universal flag
# ---------------------------------------------------------------------------


def test_agents_flag_equals_command_verb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag and the ``command`` verb produce byte-identical nodes."""
    runner = CliRunner()
    flag_argv = ["tool", "call", "--agents"]
    monkeypatch.setattr("sys.argv", ["dhcli", *flag_argv])
    via_flag = runner.invoke(cli, flag_argv, standalone_mode=False)
    via_verb = runner.invoke(
        cli, ["agents", "command", "tool", "call"], standalone_mode=False
    )
    assert via_flag.exit_code == via_verb.exit_code == 0
    assert via_flag.output == via_verb.output
