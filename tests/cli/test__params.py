"""Tests for ``deephaven_mcp.cli._params``."""

from __future__ import annotations

from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from deephaven_mcp.cli import _params
from deephaven_mcp.cli._command import HelpfulCommand, HelpfulGroup
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._params import NonBlankPath

# ---------------------------------------------------------------------------
# reject_blank_values: leaf parameters
# ---------------------------------------------------------------------------


def _blank_probe() -> click.Command:
    """A command covering every parameter shape the guard must police."""

    @click.command(cls=HelpfulCommand)
    @click.argument("id", required=False)
    @click.argument("rest", nargs=-1)
    @click.option("--system", default=None)
    @click.option("--env", multiple=True)
    @click.option("--pair", nargs=2, multiple=True)
    @click.option("--count", type=int, default=None)
    @click.pass_obj
    def c(obj: object, **_kwargs: object) -> None:
        pass

    return c


def _blank_error(args: list[str]) -> CliError:
    result = CliRunner().invoke(_blank_probe(), args, obj=None, standalone_mode=False)
    assert isinstance(result.exception, CliError)
    return result.exception


def test_blank_positional_argument_is_rejected() -> None:
    """A blank id must not reach the tool, nor look like an omitted one."""
    error = _blank_error([""])
    assert error.code is ErrorCode.MISSING_ARGUMENT
    assert error.exit_code == 2
    assert str(error).startswith("ID cannot be blank")


def test_blank_message_names_the_parameter_bare_not_as_a_metavar() -> None:
    """No usage-line grammar in the message.

    click's ``get_error_hint`` would render '[ID]' / '[REST]...'; the
    brackets mean "optional" and the ellipsis "repeatable", which
    describe the *signature*, not the blank value supplied.
    """
    for args in ([""], ["ok", ""]):
        message = str(_blank_error(args))
        assert "[" not in message
        assert "..." not in message


def test_blank_string_option_is_rejected() -> None:
    """The bug that motivated the guard: --system '' silently retargeted."""
    error = _blank_error(["--system", ""])
    assert error.code is ErrorCode.MISSING_ARGUMENT
    assert "--system" in str(error)


def test_whitespace_only_value_is_rejected() -> None:
    """Whitespace is as unusable as an empty string for every parameter."""
    assert _blank_error(["--system", "   "]).code is ErrorCode.MISSING_ARGUMENT


def test_blank_element_of_variadic_argument_is_rejected() -> None:
    """One blank among several ids is still a malformed invocation."""
    error = _blank_error(["ok", "also-ok", ""])
    assert error.code is ErrorCode.MISSING_ARGUMENT
    assert str(error).startswith("REST cannot be blank")


def test_blank_element_of_repeatable_option_is_rejected() -> None:
    assert _blank_error(["--env", "A=1", "--env", ""]).code is (
        ErrorCode.MISSING_ARGUMENT
    )


def test_blank_element_of_a_nested_repeatable_option_is_rejected() -> None:
    """``multiple=True`` with ``nargs=2`` yields a tuple *of tuples*.

    A scan of only the outer tuple's elements sees inner tuples, never a
    string, and lets the blank through.
    """
    error = _blank_error(["--pair", "a", ""])
    assert error.code is ErrorCode.MISSING_ARGUMENT
    assert str(error).startswith("--pair cannot be blank")


def test_key_equals_empty_value_is_accepted() -> None:
    """``--env 'DEBUG='`` sets an empty env var; the parameter is not blank."""
    result = CliRunner().invoke(
        _blank_probe(), ["--env", "DEBUG="], obj=None, standalone_mode=False
    )
    assert result.exit_code == 0


def test_non_string_and_omitted_parameters_are_untouched() -> None:
    """Ints, unset options, and empty variadics are never 'blank'."""
    result = CliRunner().invoke(
        _blank_probe(), ["--count", "0"], obj=None, standalone_mode=False
    )
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# reject_blank_values: group parameters
# ---------------------------------------------------------------------------


def _blank_group() -> HelpfulGroup:
    """A group with its own string option and one leaf."""

    @click.group(cls=HelpfulGroup)
    @click.option("--label", "label", default=None)
    def group(label: str | None) -> None:
        pass

    @group.command("leaf")
    def leaf() -> None:
        click.echo("leaf ran")

    return group


def test_blank_group_level_option_is_rejected() -> None:
    """A group option gets the same blank check as a leaf parameter.

    A group is not a ``HelpfulCommand``, so before ``HelpfulGroup.invoke``
    shared the guard, ``--label '' leaf`` ran the subcommand with a blank
    group option -- the root's own options would have escaped the
    CLI-wide rule.
    """
    result = CliRunner().invoke(
        _blank_group(), ["--label", "", "leaf"], obj=None, standalone_mode=False
    )
    assert isinstance(result.exception, CliError)
    assert result.exception.code is ErrorCode.MISSING_ARGUMENT
    assert str(result.exception).startswith("--label cannot be blank")


def test_group_with_valid_and_omitted_options_still_dispatches() -> None:
    """The group guard rejects only blanks, never a real or absent value."""
    for argv in (["--label", "prod", "leaf"], ["leaf"]):
        result = CliRunner().invoke(
            _blank_group(), argv, obj=None, standalone_mode=False
        )
        assert result.exit_code == 0, argv
        assert "leaf ran" in result.output


# ---------------------------------------------------------------------------
# NonBlankPath
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("blank", ["", "   "])
def test_non_blank_path_rejects_a_blank_before_conversion(blank: str) -> None:
    """A blank path must not become ``Path('.')`` silently.

    The post-parse guard cannot catch this: click converts ``''`` to
    ``PosixPath('.')``, which is not a blank string.
    """
    param = click.Option(["--config-dir"], "config_dir")
    with pytest.raises(CliError) as excinfo:
        NonBlankPath(path_type=Path).convert(blank, param, None)
    assert excinfo.value.code is ErrorCode.MISSING_ARGUMENT
    assert str(excinfo.value).startswith("--config-dir cannot be blank")


def test_non_blank_path_still_accepts_an_explicit_dot() -> None:
    """``--config-dir .`` is a legitimate choice and must survive.

    This is why the check is pre-conversion: ``Path('')`` and
    ``Path('.')`` are indistinguishable afterwards.
    """
    assert NonBlankPath(path_type=Path).convert(".", None, None) == Path(".")


def test_non_blank_path_without_a_param_names_the_value_generically() -> None:
    """A type used outside a parameter still produces a usable message.

    ``click.Parameter.type_cast_value`` always passes itself, so the CLI
    never reaches this branch; click's ``convert`` signature permits
    ``None``, so the function stays total.
    """
    with pytest.raises(CliError, match="PATH cannot be blank"):
        NonBlankPath(path_type=Path).convert("", None, None)


# ---------------------------------------------------------------------------
# param_label / _has_blank
# ---------------------------------------------------------------------------


def test_param_label_prefers_the_long_flag() -> None:
    """An option with both spellings is named by its long form."""
    assert _params.param_label(click.Option(["-s", "--system"], "system")) == "--system"


def test_param_label_falls_back_to_a_short_only_flag() -> None:
    """An option with no long form is still named, not left blank."""
    assert _params.param_label(click.Option(["-o"], "o")) == "-o"


def test_has_blank_ignores_non_string_sequence_elements() -> None:
    """A tuple of non-strings (e.g. a multiple=True int option) is not blank."""
    assert _params._has_blank((1, 2)) is False
    assert _params._has_blank(None) is False
