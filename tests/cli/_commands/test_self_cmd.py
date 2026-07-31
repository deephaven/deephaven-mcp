"""Tests for ``deephaven_mcp.cli._commands.self_cmd``."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from deephaven_mcp.cli._commands.self_cmd import (
    SUPPORTED_SHELLS,
    completion,
    self_group,
)
from deephaven_mcp.cli._main import cli

# ---------------------------------------------------------------------------
# self — the noun group
# ---------------------------------------------------------------------------


def test_self_group_name_and_verbs() -> None:
    """The group is named 'self' and exposes exactly the completion verb."""
    assert self_group.name == "self"
    assert sorted(self_group.commands) == ["completion"]


# ---------------------------------------------------------------------------
# self completion <shell> — script emission
# ---------------------------------------------------------------------------


def test_supported_shells_is_clicks_native_set() -> None:
    """The supported set is exactly click's natively maintained set."""
    assert SUPPORTED_SHELLS == ("bash", "zsh", "fish")


@pytest.mark.parametrize("shell", SUPPORTED_SHELLS)
def test_completion_emits_script_for_each_shell(shell: str) -> None:
    """Each supported shell yields a script wired to dhcli's protocol var."""
    runner = CliRunner()
    result = runner.invoke(cli, ["self", "completion", shell], standalone_mode=False)
    assert result.exit_code == 0
    assert "_DHCLI_COMPLETE" in result.output
    assert "dhcli" in result.output


def test_completion_bash_defines_completion_function() -> None:
    """The bash script defines and registers the completion function."""
    runner = CliRunner()
    result = runner.invoke(cli, ["self", "completion", "bash"], standalone_mode=False)
    assert result.exit_code == 0
    assert "_dhcli_completion()" in result.output
    assert "complete -o nosort -F _dhcli_completion dhcli" in result.output


def test_completion_zsh_declares_compdef() -> None:
    """The zsh script starts with the ``#compdef dhcli`` declaration."""
    runner = CliRunner()
    result = runner.invoke(cli, ["self", "completion", "zsh"], standalone_mode=False)
    assert result.exit_code == 0
    assert result.output.startswith("#compdef dhcli")


def test_completion_fish_registers_complete_command() -> None:
    """The fish script registers completions via fish's ``complete``."""
    runner = CliRunner()
    result = runner.invoke(cli, ["self", "completion", "fish"], standalone_mode=False)
    assert result.exit_code == 0
    assert "complete --no-files --command dhcli" in result.output


def test_completion_rejects_unsupported_shell() -> None:
    """An unsupported shell fails argument parsing with exit code 2."""
    runner = CliRunner()
    result = runner.invoke(cli, ["self", "completion", "powershell"])
    assert result.exit_code == 2
    assert "powershell" in result.output


def test_completion_requires_shell_argument() -> None:
    """A missing SHELL argument fails argument parsing with exit code 2."""
    runner = CliRunner()
    result = runner.invoke(cli, ["self", "completion"])
    assert result.exit_code == 2


def test_completion_unknown_shell_hits_assert_never() -> None:
    """An out-of-band shell hits the ``assert_never`` safety net.

    Statically unreachable thanks to the ``Shell`` ``Literal`` and
    ``click.Choice``; we feed an off-type string by bypassing both to
    confirm the runtime safety net fires rather than silently emitting
    the wrong script.
    """
    assert completion.callback is not None
    ctx = click.Context(cli, info_name="dhcli")
    with ctx, pytest.raises(AssertionError):
        completion.callback("powershell")  # type: ignore[arg-type]
        # Suppression justified: deliberately constructing a value the
        # ``Literal`` rejects so the runtime ``assert_never`` branch is
        # covered.


# ---------------------------------------------------------------------------
# click completion protocol — the machinery the emitted scripts call
# ---------------------------------------------------------------------------

_PROTOCOL_ENVS: dict[str, dict[str, str]] = {
    "bash": {
        "_DHCLI_COMPLETE": "bash_complete",
        "COMP_WORDS": "dhcli daem",
        "COMP_CWORD": "1",
    },
    "zsh": {
        "_DHCLI_COMPLETE": "zsh_complete",
        "COMP_WORDS": "dhcli daem",
        "COMP_CWORD": "1",
    },
    "fish": {
        "_DHCLI_COMPLETE": "fish_complete",
        "COMP_WORDS": "dhcli daem",
        "COMP_CWORD": "daem",
    },
}


@pytest.mark.parametrize("shell", SUPPORTED_SHELLS)
def test_completion_protocol_suggests_daemon(shell: str) -> None:
    """The per-shell completion protocol completes ``daem`` to ``daemon``."""
    runner = CliRunner()
    result = runner.invoke(cli, [], env=_PROTOCOL_ENVS[shell])
    assert "daemon" in result.output


def test_completion_protocol_suggests_completion_shells() -> None:
    """The protocol completes the SHELL choice argument of ``completion``."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [],
        env={
            "_DHCLI_COMPLETE": "bash_complete",
            "COMP_WORDS": "dhcli self completion ",
            "COMP_CWORD": "3",
        },
    )
    for shell in SUPPORTED_SHELLS:
        assert shell in result.output
