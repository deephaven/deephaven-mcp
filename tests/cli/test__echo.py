"""Tests for ``deephaven_mcp.cli._echo``."""

from __future__ import annotations

import json
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from deephaven_mcp.cli._echo import echo_payload, echo_payload_no_runtime

from ._helpers import make_runtime

# ---------------------------------------------------------------------------
# echo_payload — mode from the loaded runtime config
# ---------------------------------------------------------------------------


def test_echo_payload_renders_in_configured_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """echo_payload prints the value via format_output in the runtime's mode."""
    echo_payload(make_runtime(tmp_path, output_format="human"), {"a": 1, "b": 2})
    out = capsys.readouterr().out
    assert "a: 1" in out
    assert "b: 2" in out


def test_echo_payload_forwards_empty_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """echo_payload forwards empty_message to format_output for an empty list."""
    echo_payload(
        make_runtime(tmp_path, output_format="human"),
        [],
        empty_message="(nothing here)",
    )
    assert capsys.readouterr().out.strip() == "(nothing here)"


def test_echo_payload_forwards_sort_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """echo_payload forwards sort_keys to format_output.

    The default sorts object keys alphabetically; ``sort_keys=False`` preserves
    insertion order, which the daemon-reporting commands rely on.
    """
    rt = make_runtime(tmp_path, output_format="json")

    echo_payload(rt, {"b": 1, "a": 2})
    sorted_out = capsys.readouterr().out
    assert sorted_out.index('"a"') < sorted_out.index('"b"')

    echo_payload(rt, {"b": 1, "a": 2}, sort_keys=False)
    insertion_out = capsys.readouterr().out
    assert insertion_out.index('"b"') < insertion_out.index('"a"')


def test_echo_payload_human_exclude_drops_keys_in_human_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human_exclude drops the named dict keys in human mode."""
    echo_payload(
        make_runtime(tmp_path, output_format="human"),
        {"keep": 1, "drop": 2},
        human_exclude=("drop",),
    )
    out = capsys.readouterr().out
    assert "keep: 1" in out
    assert "drop" not in out


def test_echo_payload_human_exclude_ignored_in_json_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human_exclude is a no-op outside human mode; json keeps every key."""
    echo_payload(
        make_runtime(tmp_path, output_format="json"),
        {"keep": 1, "drop": 2},
        human_exclude=("drop",),
    )
    assert json.loads(capsys.readouterr().out) == {"keep": 1, "drop": 2}


def test_echo_payload_human_exclude_ignored_for_non_dict(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """human_exclude only filters dicts; a list value passes through unchanged."""
    echo_payload(
        make_runtime(tmp_path, output_format="human"),
        ["drop", "keep"],
        human_exclude=("drop",),
    )
    out = capsys.readouterr().out
    assert "drop" in out
    assert "keep" in out


# ---------------------------------------------------------------------------
# echo_payload_no_runtime — mode from the root -o flag, no config
# ---------------------------------------------------------------------------


def _root_group() -> click.Group:
    """Build a two-level tree whose root carries the ``-o/--output`` option.

    Mirrors the real CLI shape: the leaf reads the mode off the *root*
    context, not its own, which is what the function under test relies on.
    """

    @click.group()
    @click.option("-o", "--output", default=None)
    def root(output: str | None) -> None:
        pass

    @root.command("leaf")
    @click.pass_context
    def leaf(ctx: click.Context) -> None:
        echo_payload_no_runtime(ctx, {"b": 1, "a": 2})

    return root


def test_echo_payload_no_runtime_defaults_to_compact_json() -> None:
    """With no -o, the mode is DEFAULT_OUTPUT_MODE: compact, sorted json."""
    result = CliRunner().invoke(_root_group(), ["leaf"])
    assert result.exit_code == 0
    assert result.output.strip() == '{"a":2,"b":1}'


def test_echo_payload_no_runtime_honors_the_root_output_flag() -> None:
    """A root-level -o is read off the root context by a nested leaf."""
    result = CliRunner().invoke(_root_group(), ["-o", "human", "leaf"])
    assert result.exit_code == 0
    assert "b: 1" in result.output
    assert "a: 2" in result.output


def test_echo_payload_no_runtime_reads_no_configuration(tmp_path: Path) -> None:
    """The emitter never consults cli.json.

    The whole point of this variant: its callers run before the
    configuration is loaded, so a config file setting ``human`` must not
    change its output. Writing one and invoking with no ``-o`` still
    yields the compact-json default.
    """
    (tmp_path / "cli.json").write_text('{"output": {"format": "human"}}')
    result = CliRunner().invoke(_root_group(), ["leaf"])
    assert result.exit_code == 0
    assert result.output.strip() == '{"a":2,"b":1}'
