"""Tests for ``deephaven_mcp.cli._errors``."""

from __future__ import annotations

import io
import json

import pytest
import yaml

from deephaven_mcp.cli._errors import CliError, ErrorCode, render_error


def test_cli_error_default_exit_code() -> None:
    err = CliError("nope", code=ErrorCode.DAEMON_NOT_RUNNING)
    assert err.exit_code == 2
    assert err.code is ErrorCode.DAEMON_NOT_RUNNING
    assert err.format_message() == "nope"


def test_cli_error_derives_exit_code_from_code() -> None:
    """exit_code comes solely from the code; no independent override."""
    err = CliError("bad", code=ErrorCode.TOOL_RETURNED_ERROR)
    assert err.exit_code == 3


def test_cli_error_rejects_exit_code_argument() -> None:
    """The loose exit_code parameter is gone — it lives on the code."""
    with pytest.raises(TypeError):
        CliError("bad", code=ErrorCode.TOOL_RETURNED_ERROR, exit_code=3)  # type: ignore[call-arg]
    # Suppression justified: deliberately passing a removed kwarg to
    # confirm the API no longer accepts it.


def test_error_code_exit_codes() -> None:
    """Only TOOL_RETURNED_ERROR exits 3; every other code defaults to 2."""
    assert ErrorCode.TOOL_RETURNED_ERROR.exit_code == 3
    others = [ec for ec in ErrorCode if ec is not ErrorCode.TOOL_RETURNED_ERROR]
    assert all(ec.exit_code == 2 for ec in others)


def test_render_error_human_writes_command_prefix() -> None:
    buf = io.StringIO()
    err = CliError("boom", code=ErrorCode.MCP_REQUEST_FAILED)
    render_error(err, output="human", command="tool list", stream=buf)
    assert buf.getvalue().rstrip() == "tool list: boom"


def test_render_error_json_emits_structured_payload() -> None:
    buf = io.StringIO()
    err = CliError("boom", code=ErrorCode.MCP_REQUEST_FAILED)
    render_error(err, output="json", command="tool list", stream=buf)
    payload = json.loads(buf.getvalue())
    assert payload == {
        "error": "boom",
        "error_code": "mcp_request_failed",
        "exit_code": 2,
        "command": "tool list",
    }


def test_render_error_yaml_emits_structured_payload() -> None:
    buf = io.StringIO()
    err = CliError("boom", code=ErrorCode.CONFIG_INVALID)
    render_error(err, output="yaml", command="config validate", stream=buf)
    payload = yaml.safe_load(buf.getvalue())
    assert payload == {
        "error": "boom",
        "error_code": "config_invalid",
        "exit_code": 2,
        "command": "config validate",
    }


def test_render_error_unknown_mode_raises_assertion_error() -> None:
    """An out-of-band mode hits the ``assert_never`` safety net.

    Statically unreachable thanks to the ``OutputMode`` ``Literal``;
    we feed an off-type string by bypassing type checking to confirm
    the runtime safety net fires rather than silently defaulting to
    JSON (the previous "defensive" behavior, which masked drift).
    """
    buf = io.StringIO()
    err = CliError("boom", code=ErrorCode.ARG_PARSE_ERROR)
    with pytest.raises(AssertionError):
        render_error(err, output="rainbow", command="x", stream=buf)  # type: ignore[arg-type]
    # Suppression justified: deliberately constructing a value the
    # ``Literal`` rejects so the runtime ``assert_never`` branch is
    # covered. The bracketed error code (``arg-type``) names what is
    # being silenced; mypy still flags any *unintentional* misuse.


def test_error_code_values_are_unique() -> None:
    seen = {ec.value for ec in ErrorCode}
    assert len(seen) == len(ErrorCode)


@pytest.mark.parametrize("code", list(ErrorCode))
def test_every_error_code_has_non_empty_help_text(code: ErrorCode) -> None:
    """The introspect manifest exposes ``ec.help_text`` to agents."""
    assert code.help_text, f"{code.value} is missing help text"


def test_error_code_help_text_values_are_unique() -> None:
    """No two members share the same help string.

    Regression test for the previous design that read ``ec.__doc__``
    per member: ``Enum`` returns the *class* docstring for every
    member, so the introspect manifest silently emitted identical
    help text for every code.
    """
    seen = {ec.help_text for ec in ErrorCode}
    assert len(seen) == len(ErrorCode)


def test_error_code_help_text_is_attribute_of_member() -> None:
    """``help_text`` is a member attribute, not a class attribute.

    Each member carries its own ``help_text`` via the tuple-value
    ``__new__``. Reading it from a member must return that member's
    string, not the class-level descriptor.
    """
    assert (
        ErrorCode.DAEMON_NOT_RUNNING.help_text
        != ErrorCode.DAEMON_REGISTRY_CORRUPT.help_text
    )
    assert "auto-start" in ErrorCode.DAEMON_NOT_RUNNING.help_text
