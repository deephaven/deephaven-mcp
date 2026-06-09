"""Contract tests for dh-mcp help content across the live command tree.

Enforces the _cli-help-standards contract on every registered leaf
command, parametrized over the live click tree so a newly-added
command is covered automatically.
"""

from __future__ import annotations

import click
import pytest

from deephaven_mcp.cli._main import cli


def _leaf_commands(
    group: click.Group, prefix: tuple[str, ...] = ()
) -> list[tuple[str, click.Command]]:
    """Return ``(path, command)`` for every leaf command under ``group``."""
    leaves: list[tuple[str, click.Command]] = []
    for name, cmd in group.commands.items():
        path = (*prefix, name)
        if isinstance(cmd, click.Group):
            leaves.extend(_leaf_commands(cmd, path))
        else:
            leaves.append((" ".join(path), cmd))
    return leaves


def _all_commands(
    group: click.Group, prefix: tuple[str, ...] = ()
) -> list[tuple[str, click.Command]]:
    """Return ``(path, command)`` for ``group`` and every command beneath it.

    Unlike :func:`_leaf_commands`, this includes the groups (the root
    ``dh-mcp`` group and each noun group), so checks that apply to every
    command's surfaced strings -- plain-text and option-help presence --
    also cover group-level options such as the global ``-o``/``--timeout``/
    ``-v``/``-q`` flags on the root.
    """
    path = " ".join(prefix) or "dh-mcp"
    commands: list[tuple[str, click.Command]] = [(path, group)]
    for name, cmd in group.commands.items():
        sub = (*prefix, name)
        if isinstance(cmd, click.Group):
            commands.extend(_all_commands(cmd, sub))
        else:
            commands.append((" ".join(sub), cmd))
    return commands


_LEAVES = _leaf_commands(cli)
_LEAF_IDS = [path for path, _ in _LEAVES]
_ALL = _all_commands(cli)
_ALL_IDS = [path for path, _ in _ALL]

# Pure discovery commands have no command-specific failure mode (they
# cannot raise a CliError), so they document no Error codes section.
# Every operational command must.
_NO_ERROR_CODES = {"introspect"}


@pytest.mark.parametrize("path,cmd", _LEAVES, ids=_LEAF_IDS)
def test_leaf_help_has_required_sections(path: str, cmd: click.Command) -> None:
    """Every leaf command's help carries the required contract sections."""
    help_text = cmd.help or ""
    assert help_text, f"{path}: no help text"
    for section in ("Output:", "Examples:", "See also:", "Exit codes:"):
        assert section in help_text, f"{path}: missing {section!r}"
    if path not in _NO_ERROR_CODES:
        assert "Error codes:" in help_text, f"{path}: missing 'Error codes:'"


@pytest.mark.parametrize("path,cmd", _LEAVES, ids=_LEAF_IDS)
def test_leaf_help_documents_positional_arguments(
    path: str, cmd: click.Command
) -> None:
    """A command with a positional argument documents it in an Arguments block."""
    has_argument = any(isinstance(p, click.Argument) for p in cmd.params)
    if has_argument:
        assert "Arguments:" in (cmd.help or ""), f"{path}: undocumented positional"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_help_is_plain_text(path: str, cmd: click.Command) -> None:
    """Help text carries no backtick markup (literal or RST role).

    Surfaced help is rendered verbatim by click and re-emitted verbatim
    in the introspect manifest; neither interprets markup. Rejecting any
    backtick catches both double-backtick literals and single-backtick
    RST roles (``:func:`x```). Single quotes are the emphasis convention.
    """
    assert "`" not in (cmd.help or ""), f"{path}: backtick markup in help"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_options_help_is_plain_text(path: str, cmd: click.Command) -> None:
    """Every option's help string carries no backtick markup."""
    for param in cmd.params:
        help_ = getattr(param, "help", None)
        if help_:
            assert "`" not in help_, f"{path}: backtick markup in --{param.name} help"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_options_have_help(path: str, cmd: click.Command) -> None:
    """Every option carries a non-empty help string (the Options contract)."""
    for param in cmd.params:
        if isinstance(param, click.Option):
            assert param.help, f"{path}: option --{param.name} has no help"
