"""Tests for ``deephaven_mcp.cli._main``."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._main import (
    _argv_command_path,
    _build_cli_overrides,
    _verbosity_to_level,
    cli,
    main,
)
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.schema import CliConfig, ServerConfig
from deephaven_mcp.config.tree import ConfigTree

from ._helpers import fake_load_runtime


def _runtime() -> Runtime:
    cli_config = CliConfig()
    return Runtime(
        config_dir=Path("/tmp/cfg"),
        runtime_dir=Path("/tmp/rt"),
        config=ConfigTree(
            config_dir=Path("/tmp/cfg"),
            cli=cli_config,
            server=ServerConfig(),
        ),
        daemon_dir=MagicMock(),
    )


# ---------------------------------------------------------------------------
# _build_cli_overrides
# ---------------------------------------------------------------------------


def test_build_cli_overrides_no_change_returns_empty_dict() -> None:
    """When no flags are supplied the helper produces no overrides."""
    out = _build_cli_overrides(
        template=CliConfig(),
        output=None,
        timeout=None,
        no_auto_start=False,
    )
    assert out == {}


def test_build_cli_overrides_sets_output_and_timeout() -> None:
    """``-o`` and ``--timeout`` map to fresh ``output`` / ``request`` sub-models."""
    out = _build_cli_overrides(
        template=CliConfig(),
        output="json",
        timeout=5,
        no_auto_start=False,
    )
    assert "output" in out
    assert "request" in out
    assert "daemon" not in out
    # Apply via model_copy so we can assert on the resulting CliConfig shape.
    cli_cfg = CliConfig().model_copy(update=out)
    assert cli_cfg.output.format == "json"
    assert cli_cfg.request.timeouts.default_seconds == 5
    assert cli_cfg.daemon.auto_start is True


def test_build_cli_overrides_no_auto_start_disables_field() -> None:
    """``--no-auto-start`` disables ``daemon.auto_start`` and nothing else."""
    out = _build_cli_overrides(
        template=CliConfig(),
        output=None,
        timeout=None,
        no_auto_start=True,
    )
    cli_cfg = CliConfig().model_copy(update=out)
    assert cli_cfg.daemon.auto_start is False
    assert cli_cfg.output.format == "human"


# ---------------------------------------------------------------------------
# _verbosity_to_level
# ---------------------------------------------------------------------------


def test_verbosity_default_is_warning() -> None:
    assert _verbosity_to_level(0, False) == logging.WARNING


def test_verbosity_v_is_info() -> None:
    assert _verbosity_to_level(1, False) == logging.INFO


def test_verbosity_vv_is_debug() -> None:
    assert _verbosity_to_level(2, False) == logging.DEBUG


def test_verbosity_quiet_overrides_verbose() -> None:
    assert _verbosity_to_level(2, True) == logging.ERROR


# ---------------------------------------------------------------------------
# _argv_command_path
# ---------------------------------------------------------------------------


def test_argv_command_path_simple_verb() -> None:
    assert _argv_command_path(["daemon", "start"]) == "daemon start"


def test_argv_command_path_skips_options_and_their_values() -> None:
    assert (
        _argv_command_path(
            ["-o", "json", "--config-dir", "/tmp", "tool", "list", "--all"]
        )
        == "tool list"
    )


def test_argv_command_path_empty_returns_root_name() -> None:
    assert _argv_command_path([]) == "dh-mcp"


# ---------------------------------------------------------------------------
# _output_from_argv
# ---------------------------------------------------------------------------


def test_output_from_argv_picks_up_explicit_o() -> None:
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["-o", "json", "daemon", "status"]) == "json"
    assert _output_from_argv(["--output", "yaml", "tool", "list"]) == "yaml"


def test_output_from_argv_unknown_value_falls_back_to_human() -> None:
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["-o", "xml"]) == "human"


def test_output_from_argv_no_o_falls_back_to_human() -> None:
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["daemon", "status"]) == "human"


def test_output_from_argv_o_at_end_with_no_value() -> None:
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["-o"]) == "human"


def test_output_from_argv_long_equals_form() -> None:
    """``--output=json`` (the ``=`` form) is recognized."""
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["--output=json", "daemon", "status"]) == "json"
    assert _output_from_argv(["--output=yaml"]) == "yaml"


def test_output_from_argv_short_equals_form() -> None:
    """``-o=json`` is recognized identically to ``-o json``."""
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["-o=json"]) == "json"


def test_output_from_argv_equals_form_with_unknown_value() -> None:
    """The ``=`` form with an unrecognized value falls back to ``"human"``."""
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["--output=xml"]) == "human"
    assert _output_from_argv(["-o=toml"]) == "human"


# ---------------------------------------------------------------------------
# _is_help_invocation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        # Bare help spellings.
        (["--help"], True),
        (["-h"], True),
        # Help after a value-taking option whose value is supplied.
        (["--config-dir", "/tmp", "--help"], True),
        # ``--help`` consumed as the value of a value-taking option.
        (["--config-dir", "--help"], False),
        # ``=``-form value-taking option does not consume the next token.
        (["--config-dir=/tmp", "--help"], True),
        # No help token anywhere.
        (["daemon", "stop"], False),
        # Empty argv.
        ([], False),
    ],
)
def test_is_help_invocation_table_driven(
    argv: list[str], expected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_is_help_invocation`` reads ``sys.argv[1:]``; monkey-patch it."""
    from deephaven_mcp.cli._main import _is_help_invocation

    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    assert _is_help_invocation() is expected


# ---------------------------------------------------------------------------
# _value_taking_options
# ---------------------------------------------------------------------------


def test_value_taking_options_excludes_arguments_and_flags() -> None:
    """A synthetic group exercises every branch of the helper."""
    import click

    from deephaven_mcp.cli._main import _value_taking_options

    @click.command()
    @click.argument("positional")
    @click.option("--flag", is_flag=True)
    @click.option("-v", "--verbose", count=True)
    @click.option("-c", "--config", type=str)
    @click.option("--name", type=str)
    def synthetic(
        positional: str, flag: bool, verbose: int, config: str, name: str
    ) -> None:
        pass

    result = _value_taking_options(synthetic.params)
    assert result == frozenset({"-c", "--config", "--name"})


# ---------------------------------------------------------------------------
# Top-level help / version / introspect smoke
# ---------------------------------------------------------------------------


def test_help_lists_top_level_nouns() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"], standalone_mode=False)
    assert result.exit_code == 0
    for noun in ("daemon", "tool", "config", "introspect"):
        assert noun in result.output


def test_version_flag() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"], standalone_mode=False)
    assert result.exit_code == 0
    assert result.output.startswith("dh-mcp ")


def test_introspect_runs_without_loading_runtime() -> None:
    """``introspect`` must work without a valid configuration tree.

    The root callback short-circuits before calling ``load_runtime``;
    we confirm by stubbing ``load_runtime`` to raise and asserting
    the verb still completes with a 0 exit code.
    """
    runner = CliRunner()
    with patch.object(
        _main,
        "load_runtime",
        AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID)),
    ):
        result = runner.invoke(cli, ["introspect"], standalone_mode=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "commands" in payload
    assert "daemon" in payload["commands"]


def test_help_runs_without_loading_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--help`` at any depth must work without a valid configuration tree.

    The root callback short-circuits on help by inspecting
    ``sys.argv``; ``CliRunner.invoke`` does not patch ``sys.argv``,
    so the test must do it explicitly.
    """
    monkeypatch.setattr("sys.argv", ["dh-mcp", "daemon", "--help"])
    runner = CliRunner()
    with patch.object(
        _main,
        "load_runtime",
        AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID)),
    ):
        result = runner.invoke(cli, ["daemon", "--help"], standalone_mode=False)
    assert result.exit_code == 0
    assert "Usage" in result.output


# ---------------------------------------------------------------------------
# main: configuration error handling
# ---------------------------------------------------------------------------


def test_main_renders_config_error_human(capsys) -> None:
    """A ``CliError(CONFIG_INVALID)`` from ``load_runtime`` reaches the renderer."""
    fail = AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID))
    with (
        patch.object(_main, "load_runtime", fail),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["daemon", "start"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "daemon start: nope" in err


def test_main_renders_config_error_json(capsys) -> None:
    """Same error path under ``-o json`` emits a structured stderr payload."""
    fail = AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID))
    with (
        patch.object(_main, "load_runtime", fail),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["-o", "json", "daemon", "start"])
    assert exc_info.value.code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error_code"] == ErrorCode.CONFIG_INVALID.value
    assert payload["command"] == "daemon start"
    assert payload["exit_code"] == 2


def test_main_returns_zero_on_success(capsys) -> None:
    """Success path: ``main`` exits ``0`` and the callback's output reaches stdout."""
    rt = _runtime()
    with (
        patch.object(_main, "load_runtime", fake_load_runtime(rt)),
        patch(
            "deephaven_mcp.cli._commands.daemon.stop_daemon",
            AsyncMock(return_value=False),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["-o", "json", "daemon", "stop"])
    assert exc_info.value.code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["stopped"] is False


# ---------------------------------------------------------------------------
# Mutual exclusivity of -v / -q + misc top-level error paths
# ---------------------------------------------------------------------------


def test_main_handles_usage_error(capsys) -> None:
    """An unknown subcommand surfaces click's ``UsageError`` → exit 2."""
    with pytest.raises(SystemExit) as exc_info:
        main(["totally-bogus"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "Usage" in err or "No such command" in err


def test_main_unexpected_exception_wraps_as_structured_error(capsys) -> None:
    """A non-CliError bubbling out of ``cli.main`` is wrapped + rendered."""
    with (
        patch.object(_main.cli, "main", side_effect=RuntimeError("boom")),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["daemon", "status"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "Unexpected error: boom" in err


def test_main_unexpected_exception_json_uses_internal_error_code(capsys) -> None:
    """Under ``-o json`` an unexpected error renders a structured payload.

    The top-level safety net honors the active output mode (not a
    hardcoded human render) and tags the failure with the dedicated
    ``internal_error`` code rather than a subsystem-specific one.
    """
    with (
        patch.object(_main.cli, "main", side_effect=RuntimeError("boom")),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["-o", "json", "daemon", "status"])
    assert exc_info.value.code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error_code"] == ErrorCode.INTERNAL_ERROR.value
    assert "Unexpected error: boom" in payload["error"]


def test_v_and_q_are_mutually_exclusive() -> None:
    """``-v`` and ``-q`` together raise a ``CliError``."""
    rt = _runtime()
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(rt)):
        result = runner.invoke(cli, ["-v", "-q", "daemon", "status"])
    assert result.exit_code == 2
