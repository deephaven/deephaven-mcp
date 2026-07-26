"""Tests for :mod:`deephaven_mcp.cli._prompt`.

Covers:

- :func:`can_prompt` TTY / ``--no-input`` gating.
- :func:`prompt_text` / :func:`confirm` stderr prompting and their
  refusal to run when prompting is not permitted.
- :func:`prompt_optional` / :func:`prompt_optional_int` conditional
  prompting.
- :func:`require_confirmation` destructive-action confirmation.
- :func:`require_value` hybrid flag-or-prompt resolution and the
  structured ``missing_required_option`` failure.
- :func:`require_choice` closed-set resolution, its ``Literal``
  narrowing, and the ``InternalError`` wiring-bug backstop.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.cli import _prompt
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._prompt import (
    can_prompt,
    confirm,
    prompt_optional,
    prompt_optional_int,
    prompt_text,
    require_choice,
    require_confirmation,
    require_value,
)


def _allow_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force :func:`can_prompt` to report prompting is permitted."""
    monkeypatch.setattr(_prompt, "can_prompt", lambda *, no_input: True)


def _deny_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force :func:`can_prompt` to report prompting is unavailable."""
    monkeypatch.setattr(_prompt, "can_prompt", lambda *, no_input: False)


# ---------------------------------------------------------------------------
# can_prompt
# ---------------------------------------------------------------------------


def test_can_prompt_false_when_no_input(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    assert can_prompt(no_input=True) is False


def test_can_prompt_false_when_not_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert can_prompt(no_input=False) is False


def test_can_prompt_true_on_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    assert can_prompt(no_input=False) is True


# ---------------------------------------------------------------------------
# prompt_text / confirm
# ---------------------------------------------------------------------------


def test_prompt_text_reads_value(monkeypatch: pytest.MonkeyPatch) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="hello\n"):
        assert prompt_text("Value", no_input=False) == "hello"


def test_prompt_text_default_on_enter(monkeypatch: pytest.MonkeyPatch) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="\n"):
        assert prompt_text("Value", no_input=False, default="fallback") == "fallback"


def test_prompt_text_choices_reprompts_until_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="bogus\npsk\n"):
        assert (
            prompt_text("Auth", no_input=False, choices=("anonymous", "psk")) == "psk"
        )


def test_prompt_text_refuses_when_not_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    with pytest.raises(InternalError):
        prompt_text("Value", no_input=True)


def test_confirm_yes(monkeypatch: pytest.MonkeyPatch) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="y\n"):
        assert confirm("Proceed?", no_input=False) is True


def test_confirm_default_no(monkeypatch: pytest.MonkeyPatch) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="\n"):
        assert confirm("Proceed?", no_input=False) is False


def test_confirm_refuses_when_not_permitted(monkeypatch: pytest.MonkeyPatch) -> None:
    _deny_prompt(monkeypatch)
    with pytest.raises(InternalError):
        confirm("Proceed?", no_input=True)


# ---------------------------------------------------------------------------
# prompt_optional / prompt_optional_int
# ---------------------------------------------------------------------------


def test_prompt_optional_returns_supplied_value() -> None:
    assert prompt_optional("given", label="Host", no_input=True) == "given"


def test_prompt_optional_prompts_when_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="typed\n"):
        assert prompt_optional(None, label="Host", no_input=False) == "typed"


def test_prompt_optional_none_when_not_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    assert prompt_optional(None, label="Host", no_input=True) is None


def test_prompt_optional_int_returns_supplied_value() -> None:
    assert prompt_optional_int(10000, label="Port", no_input=True) == 10000


def test_prompt_optional_int_reprompts_on_non_numeric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="abc\n8080\n"):
        assert prompt_optional_int(None, label="Port", no_input=False) == 8080


def test_prompt_optional_int_none_when_not_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    assert prompt_optional_int(None, label="Port", no_input=True) is None


# ---------------------------------------------------------------------------
# require_confirmation
# ---------------------------------------------------------------------------


def test_require_confirmation_yes_skips_prompt() -> None:
    assert require_confirmation("Delete?", yes=True, no_input=True) is None


def test_require_confirmation_raises_without_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    with pytest.raises(CliError) as exc:
        require_confirmation("Delete?", yes=False, no_input=True)
    assert exc.value.code is ErrorCode.MISSING_REQUIRED_OPTION


def test_require_confirmation_proceeds_on_yes(monkeypatch: pytest.MonkeyPatch) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="y\n"):
        assert require_confirmation("Delete?", yes=False, no_input=False) is None


def test_require_confirmation_aborts_on_no(monkeypatch: pytest.MonkeyPatch) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="n\n"):
        with pytest.raises(CliError) as exc:
            require_confirmation("Delete?", yes=False, no_input=False)
    assert exc.value.code is ErrorCode.OPERATION_CANCELED
    assert exc.value.exit_code == 2


# ---------------------------------------------------------------------------
# require_value
# ---------------------------------------------------------------------------


def test_require_value_returns_supplied_flag() -> None:
    assert require_value("given", flag="--host", label="Host", no_input=True) == "given"


def test_require_value_prompts_when_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="typed\n"):
        assert (
            require_value(None, flag="--host", label="Host", no_input=False) == "typed"
        )


def test_require_value_falls_back_to_default_without_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    assert (
        require_value(
            None, flag="--host", label="Host", no_input=True, default="localhost"
        )
        == "localhost"
    )


def test_require_value_raises_structured_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    with pytest.raises(CliError) as exc:
        require_value(None, flag="--token", label="Token", no_input=True)
    assert exc.value.code is ErrorCode.MISSING_REQUIRED_OPTION
    assert "--token" in exc.value.message


# ---------------------------------------------------------------------------
# require_choice
# ---------------------------------------------------------------------------


def test_require_choice_returns_matched_member() -> None:
    assert (
        require_choice(
            "psk",
            flag="--auth",
            label="Auth",
            no_input=True,
            choices=("anonymous", "psk"),
        )
        == "psk"
    )


def test_require_choice_prompts_when_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_prompt(monkeypatch)
    runner = CliRunner()
    with runner.isolation(input="psk\n"):
        assert (
            require_choice(
                None,
                flag="--auth",
                label="Auth",
                no_input=False,
                choices=("anonymous", "psk"),
            )
            == "psk"
        )


def test_require_choice_raises_structured_error_without_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_prompt(monkeypatch)
    with pytest.raises(CliError) as exc:
        require_choice(
            None,
            flag="--auth",
            label="Auth",
            no_input=True,
            choices=("anonymous", "psk"),
        )
    assert exc.value.code is ErrorCode.MISSING_REQUIRED_OPTION
    assert "--auth" in exc.value.message


def test_require_choice_rejects_value_not_in_choices() -> None:
    # Reachable only when a closed-set flag is wired without a matching
    # click.Choice, so click never validated the flag path; require_choice
    # fails loud rather than returning an out-of-set value.
    with pytest.raises(InternalError):
        require_choice(
            "bogus",
            flag="--auth",
            label="Auth",
            no_input=True,
            choices=("anonymous", "psk"),
        )
