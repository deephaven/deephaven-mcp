"""Tests for ``deephaven_mcp.cli._commands.introspect``."""

from __future__ import annotations

import json
from unittest.mock import patch

import click
import yaml
from click.testing import CliRunner

from deephaven_mcp.cli._commands.introspect import (
    _clean_help,
    build_manifest,
    introspect,
)
from deephaven_mcp.cli._errors import ErrorCode
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
    from deephaven_mcp.cli._commands.introspect import _describe_output

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


def test_introspect_command_emits_json_by_default() -> None:
    """No ``-o`` flag and no ``DH_MCP_OUTPUT`` -> defaults to JSON."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["prog"] == "dh-mcp"


def test_introspect_honors_yaml_output_mode() -> None:
    """``-o yaml`` now produces a YAML document (previously always JSON)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "yaml", "introspect"])
    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["prog"] == "dh-mcp"


def test_introspect_honors_human_output_mode() -> None:
    """``-o human`` is supported even though it produces a less useful manifest."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-o", "human", "introspect"])
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


def test_introspect_honors_envvar_output_mode() -> None:
    """``DH_MCP_OUTPUT=yaml`` selects YAML without ``-o``."""
    runner = CliRunner()
    result = runner.invoke(cli, ["introspect"], env={"DH_MCP_OUTPUT": "yaml"})
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
    from deephaven_mcp.cli._commands.introspect import _describe_command

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

    from deephaven_mcp.cli._commands import introspect as introspect_mod

    with patch.object(
        introspect_mod.metadata,
        "version",
        side_effect=importlib_metadata.PackageNotFoundError(),
    ):
        manifest = build_manifest(cli)
    assert manifest["version"] == "unknown"
