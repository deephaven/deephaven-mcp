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
    _NOISY_DEPENDENCY_LOGGERS,
    _argv_command_path,
    _build_cli_overrides,
    _quiet_dependency_loggers,
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
    assert cli_cfg.output.format == "json"


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
# _quiet_dependency_loggers
# ---------------------------------------------------------------------------


@pytest.fixture
def _restore_dependency_logger_levels() -> object:
    """Save and restore the levels of the noisy dependency loggers."""
    saved = {name: logging.getLogger(name).level for name in _NOISY_DEPENDENCY_LOGGERS}
    try:
        yield
    finally:
        for name, level in saved.items():
            logging.getLogger(name).setLevel(level)


def test_quiet_dependency_loggers_default_pins_error(
    _restore_dependency_logger_levels: object,
) -> None:
    """Without verbosity, each noisy logger is pinned to ERROR."""
    _quiet_dependency_loggers(0)
    for name in _NOISY_DEPENDENCY_LOGGERS:
        assert logging.getLogger(name).level == logging.ERROR


def test_quiet_dependency_loggers_verbose_resets_to_notset(
    _restore_dependency_logger_levels: object,
) -> None:
    """With ``-v``/``-vv`` the loggers are reset to NOTSET to follow root."""
    for name in _NOISY_DEPENDENCY_LOGGERS:
        logging.getLogger(name).setLevel(logging.ERROR)
    _quiet_dependency_loggers(1)
    for name in _NOISY_DEPENDENCY_LOGGERS:
        assert logging.getLogger(name).level == logging.NOTSET


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


def test_output_from_argv_unknown_value_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recognized ``-o`` with an invalid value falls through to env/default."""
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.delenv("DH_MCP_OUTPUT", raising=False)
    assert _output_from_argv(["-o", "xml"]) == "json"


def test_output_from_argv_no_o_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.delenv("DH_MCP_OUTPUT", raising=False)
    assert _output_from_argv(["daemon", "status"]) == "json"


def test_output_from_argv_honors_env_when_no_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no ``-o`` in argv, the error path honors ``DH_MCP_OUTPUT``."""
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.setenv("DH_MCP_OUTPUT", "json")
    assert _output_from_argv(["daemon", "status"]) == "json"


def test_output_from_argv_flag_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit ``-o`` in argv beats ``DH_MCP_OUTPUT`` on the error path."""
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.setenv("DH_MCP_OUTPUT", "json")
    assert _output_from_argv(["-o", "yaml", "daemon", "status"]) == "yaml"


def test_output_from_argv_ignores_invalid_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An invalid ``DH_MCP_OUTPUT`` is ignored, falling back to the default."""
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.setenv("DH_MCP_OUTPUT", "xml")
    assert _output_from_argv(["daemon", "status"]) == "json"


def test_output_from_argv_o_at_end_with_no_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.delenv("DH_MCP_OUTPUT", raising=False)
    assert _output_from_argv(["-o"]) == "json"


def test_output_from_argv_long_equals_form() -> None:
    """``--output=json`` (the ``=`` form) is recognized."""
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["--output=json", "daemon", "status"]) == "json"
    assert _output_from_argv(["--output=yaml"]) == "yaml"


def test_output_from_argv_short_equals_form() -> None:
    """``-o=json`` is recognized identically to ``-o json``."""
    from deephaven_mcp.cli._main import _output_from_argv

    assert _output_from_argv(["-o=json"]) == "json"


def test_output_from_argv_equals_form_with_unknown_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``=`` form with an unrecognized value falls back to the default."""
    from deephaven_mcp.cli._main import _output_from_argv

    monkeypatch.delenv("DH_MCP_OUTPUT", raising=False)
    assert _output_from_argv(["--output=xml"]) == "json"
    assert _output_from_argv(["-o=toml"]) == "json"


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


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        # Bare flag.
        (["--introspect"], True),
        # Flag appended to a command path.
        (["daemon", "start", "--introspect"], True),
        # Flag after a value-taking option whose value is supplied.
        (["--config-dir", "/tmp", "--introspect"], True),
        # ``--introspect`` consumed as the value of a value-taking option.
        (["--config-dir", "--introspect"], False),
        # ``=``-form value-taking option does not consume the next token.
        (["--config-dir=/tmp", "--introspect"], True),
        # No introspect token anywhere.
        (["daemon", "stop"], False),
        # Empty argv.
        ([], False),
    ],
)
def test_is_introspect_invocation_table_driven(
    argv: list[str], expected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_is_introspect_invocation`` reads ``sys.argv[1:]``; monkey-patch it."""
    from deephaven_mcp.cli._main import _is_introspect_invocation

    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    assert _is_introspect_invocation() is expected


# ---------------------------------------------------------------------------
# _value_taking_root_options
# ---------------------------------------------------------------------------


def test_value_taking_root_options_is_liftable_value_taking_half() -> None:
    """The value-taking accessor is single-sourced from the lifter bucketer.

    Pins that ``_value_taking_root_options`` returns exactly the
    value-taking half of ``_liftable_root_options`` — the single
    ``cli.params`` classifier — so the two can never drift apart.
    """
    from deephaven_mcp.cli._main import (
        _liftable_root_options,
        _value_taking_root_options,
    )

    assert _value_taking_root_options() == _liftable_root_options()[0]


# ---------------------------------------------------------------------------
# Top-level help / version / introspect smoke
# ---------------------------------------------------------------------------


def test_help_lists_top_level_nouns() -> None:
    """Every group registered in ``_main`` is reachable from the root help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"], standalone_mode=False)
    assert result.exit_code == 0
    for noun in (
        "daemon",
        "tool",
        "session",
        "system",
        "table",
        "script",
        "catalog",
        "pq",
        "config",
        "introspect",
    ):
        assert noun in result.output


def test_main_registers_exactly_the_expected_groups() -> None:
    """``cli.add_command`` wiring matches the documented command surface.

    Pins the registration in ``_main`` so a group added to (or dropped
    from) ``_commands`` without wiring it here fails this test rather
    than silently going missing from the CLI.
    """
    assert set(cli.commands) == {
        "daemon",
        "tool",
        "session",
        "system",
        "table",
        "script",
        "catalog",
        "pq",
        "config",
        "introspect",
    }


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
        result = runner.invoke(
            cli, ["-o", "json", "introspect", "tree"], standalone_mode=False
        )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "commands" in payload
    assert "daemon" in payload["commands"]


def test_introspect_flag_runs_without_loading_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``--introspect`` flag short-circuits ``load_runtime`` too.

    The root callback inspects ``sys.argv``; ``CliRunner.invoke`` does
    not set it, so the test patches it explicitly (mirroring the
    ``--help`` path).
    """
    argv = ["-o", "json", "daemon", "start", "--introspect"]
    monkeypatch.setattr("sys.argv", ["dh-mcp", *argv])
    runner = CliRunner()
    with patch.object(
        _main,
        "load_runtime",
        AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID)),
    ):
        result = runner.invoke(cli, argv, standalone_mode=False)
    assert result.exit_code == 0
    assert json.loads(result.output)["name"] == "start"


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


@pytest.mark.parametrize(
    "args", [["daemon", "start", "--help"], ["-o", "json", "daemon", "start", "--help"]]
)
def test_help_is_human_regardless_of_output_mode(args: list[str]) -> None:
    """``--help`` always renders human help text, even under ``-o json``.

    Help goes through click's help formatter, independent of the output-mode
    system, so the JSON default does not turn it into JSON. This locks the
    intended split: ``--help`` is human, ``--introspect`` is the machine twin.
    """
    runner = CliRunner()
    result = runner.invoke(cli, args, standalone_mode=False)
    assert result.exit_code == 0
    assert result.output.startswith("Usage:")
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.output)


# ---------------------------------------------------------------------------
# main: configuration error handling
# ---------------------------------------------------------------------------


def test_main_renders_config_error_human(capsys) -> None:
    """``-o human`` renders the error as plain text (json is now the default)."""
    fail = AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID))
    with (
        patch.object(_main, "load_runtime", fail),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["-o", "human", "daemon", "start"])
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


def test_main_renders_config_error_honors_env(
    capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no ``-o`` but ``DH_MCP_OUTPUT=json``, the error renders structured."""
    monkeypatch.setenv("DH_MCP_OUTPUT", "json")
    fail = AsyncMock(side_effect=CliError("nope", code=ErrorCode.CONFIG_INVALID))
    with (
        patch.object(_main, "load_runtime", fail),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["daemon", "start"])
    assert exc_info.value.code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error_code"] == ErrorCode.CONFIG_INVALID.value


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


def test_main_output_flag_after_introspect_subcommand(capsys) -> None:
    """``dh-mcp introspect tree -o json`` works end-to-end through ``main()``.

    Exercises the interaction this change relies on: the argv lifter hoists
    the trailing ``-o json`` to the front, the eager ``-o`` resolves before
    the introspect verb renders, and the introspect bypass means no config
    is loaded. (``CliRunner`` does not run the lifter, so this must go
    through ``main()``.)
    """
    with pytest.raises(SystemExit) as exc_info:
        main(["introspect", "tree", "-o", "json"])
    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["prog"] == "dh-mcp"


# ---------------------------------------------------------------------------
# DH_MCP_OUTPUT environment variable
# ---------------------------------------------------------------------------


def test_output_env_var_drives_a_normal_command(
    capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DH_MCP_OUTPUT (no -o flag) selects a normal command's output mode.

    Exercises the full wiring the constant single-sources: the root
    option's ``envvar=OUTPUT_ENV_VAR`` resolves the env value, the
    callback folds it into ``cli_overrides``, and the command renders
    in that mode. JSON parsing fails (and the test with it) if the env
    var is not honored — human mode prints ``stopped: false``.
    """
    monkeypatch.setenv("DH_MCP_OUTPUT", "json")
    rt = _runtime()
    with (
        patch.object(_main, "load_runtime", fake_load_runtime(rt)),
        patch(
            "deephaven_mcp.cli._commands.daemon.stop_daemon",
            AsyncMock(return_value=False),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["daemon", "stop"])
    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["stopped"] is False


def test_explicit_output_flag_overrides_env_var(
    capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit ``-o`` wins over DH_MCP_OUTPUT (the documented precedence)."""
    monkeypatch.setenv("DH_MCP_OUTPUT", "yaml")
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
    # JSON (the flag), not YAML (the env): YAML output would not parse here.
    payload = json.loads(capsys.readouterr().out)
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


# ---------------------------------------------------------------------------
# _liftable_root_options
# ---------------------------------------------------------------------------


def test_liftable_options_skips_arguments_help_and_version() -> None:
    """Synthetic group exercises every classifier branch in one pass.

    Drives :func:`_liftable_options` (the params-taking helper)
    rather than the ``cli``-bound wrapper so both the
    ``not isinstance(param, click.Option)`` skip (positional argument)
    and the ``--help`` / ``--version`` exclusion branches are
    exercised. The real ``cli.params`` only ever exposes options.
    """
    import click

    from deephaven_mcp.cli._main import _liftable_options

    @click.command()
    @click.argument("positional")
    @click.option("-h", "--help", "help_", is_flag=True)
    @click.option("--version", is_flag=True)
    @click.option("--flag", is_flag=True)
    @click.option("-v", "--verbose", count=True)
    @click.option("-c", "--config", type=str)
    def synthetic(
        positional: str,
        help_: bool,
        version: bool,
        flag: bool,
        verbose: int,
        config: str,
    ) -> None:
        pass

    value_taking, value_less = _liftable_options(synthetic.params)
    assert value_taking == frozenset({"-c", "--config"})
    assert value_less == frozenset({"--flag", "-v", "--verbose"})


def test_liftable_root_options_excludes_help_and_version() -> None:
    """``--help`` / ``-h`` / ``--version`` are never returned in either set.

    Lifting them would actively break ``dh-mcp daemon --help`` (would
    rewrite to ``dh-mcp --help daemon`` and render root help) and
    ``dh-mcp daemon --version`` semantics. Pinned here so a future
    refactor cannot silently start lifting them.
    """
    from deephaven_mcp.cli._main import _liftable_root_options

    value_taking, value_less = _liftable_root_options()
    excluded = {"--help", "-h", "--version"}
    assert not (value_taking & excluded)
    assert not (value_less & excluded)


def test_liftable_root_options_classifies_known_root_options() -> None:
    """Every other root option is bucketed by whether it takes a value.

    Drives off the live ``cli.params`` so adding a new root option
    that misclassifies (e.g. forgets ``is_flag=True``) fails this
    test, not user-facing behavior.
    """
    from deephaven_mcp.cli._main import _liftable_root_options

    value_taking, value_less = _liftable_root_options()
    assert {"--config-dir", "--runtime-dir", "-o", "--output", "--timeout"} <= (
        value_taking
    )
    assert {"-v", "--verbose", "-q", "--quiet", "--no-auto-start"} <= value_less
    # Disjoint by construction.
    assert not (value_taking & value_less)


# ---------------------------------------------------------------------------
# _lift_root_options
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        # Already at the front: no reordering.
        (["-o", "json", "config", "show"], ["-o", "json", "config", "show"]),
        (
            ["--output", "yaml", "config", "show"],
            ["--output", "yaml", "config", "show"],
        ),
        # Bare value-taking, lifted from after the subcommand.
        (["config", "show", "-o", "json"], ["-o", "json", "config", "show"]),
        (
            ["config", "show", "--output", "yaml"],
            ["--output", "yaml", "config", "show"],
        ),
        # ``=`` form. Long form: click accepts ``--opt=value`` and the
        # lifter moves it cleanly. Short form: click does *not* accept
        # ``-o=value`` (it parses ``-oVALUE`` with no separator, so
        # ``-o=json`` would fail validation at click regardless of
        # position). The lifter still recognizes the shape for symmetry
        # and leaves rejection to click — same outcome as not lifting,
        # never worse.
        (["config", "show", "--output=json"], ["--output=json", "config", "show"]),
        (["config", "show", "-o=json"], ["-o=json", "config", "show"]),
        # ``=`` form whose prefix is *not* a root option: left in place
        # (e.g. a subcommand option's ``key=value`` value). The lifter
        # must not hoist it just because it contains ``=`` and starts with
        # ``-``.
        (
            ["tool", "call", "x", "--arg=type=community"],
            ["tool", "call", "x", "--arg=type=community"],
        ),
        # Value-less flags and counters.
        (["daemon", "status", "-v"], ["-v", "daemon", "status"]),
        (["daemon", "status", "-vvv"], ["-vvv", "daemon", "status"]),
        (["daemon", "status", "--quiet"], ["--quiet", "daemon", "status"]),
        (
            ["daemon", "status", "--no-auto-start"],
            ["--no-auto-start", "daemon", "status"],
        ),
        # Multiple root options after the subcommand preserve relative order.
        (
            ["config", "show", "-o", "json", "--timeout", "5"],
            ["-o", "json", "--timeout", "5", "config", "show"],
        ),
        # Mixed: some root options before, some after.
        (
            ["-v", "config", "show", "-o", "json"],
            ["-v", "-o", "json", "config", "show"],
        ),
        # Subcommand-local options (unknown to root) are never touched.
        (
            ["tool", "list", "--all"],
            ["tool", "list", "--all"],
        ),
        # Empty argv is a no-op.
        ([], []),
        # ``--`` sentinel: tokens after it are preserved verbatim, even
        # if they look like root options.
        (
            ["config", "show", "--", "-o", "json"],
            ["config", "show", "--", "-o", "json"],
        ),
        # ``--`` rescues a subcommand-option value that collides with a
        # root spelling: without the sentinel the lexical lift would
        # hoist ``--timeout`` (and the following token) away from the
        # subcommand; after ``--`` it stays put. This is the documented
        # escape hatch for the lifter's grammar-free limitation.
        (
            ["session", "create", "--jvm-arg", "--", "--timeout"],
            ["session", "create", "--jvm-arg", "--", "--timeout"],
        ),
        # Without the sentinel, the same colliding value IS hoisted —
        # pins the limitation itself so the ``--`` workaround is the
        # difference, not luck.
        (
            ["session", "create", "--jvm-arg", "--timeout", "5"],
            ["--timeout", "5", "session", "create", "--jvm-arg"],
        ),
        # Value-taking option at end-of-argv with missing value: lifted
        # alone; Click will then surface its own usage error.
        (["config", "show", "-o"], ["-o", "config", "show"]),
    ],
)
def test_lift_root_options_table_driven(argv: list[str], expected: list[str]) -> None:
    """Pins the argv-rewrite contract across every supported shape."""
    from deephaven_mcp.cli._main import _lift_root_options

    assert _lift_root_options(argv) == expected


def test_lift_root_options_none_defaults_to_sys_argv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``argv=None`` falls back to ``sys.argv[1:]`` (skipping the program name).

    Mirrors :meth:`click.Group.main`'s convention. ``sys.argv[0]`` is
    the OS-supplied program name and must not be treated as a user
    argument; the slice is the standard Python idiom.
    """
    from deephaven_mcp.cli._main import _lift_root_options

    monkeypatch.setattr("sys.argv", ["/path/to/dh-mcp", "config", "show", "-o", "json"])
    assert _lift_root_options() == ["-o", "json", "config", "show"]
    assert _lift_root_options(None) == ["-o", "json", "config", "show"]


def test_main_accepts_output_after_subcommand(capsys) -> None:
    """End-to-end: ``-o`` after the subcommand reaches the renderer.

    Without ``_lift_root_options`` Click would fail this argv shape
    with ``No such option '--output'``; with the lifter it succeeds
    and JSON output reaches stdout.
    """
    rt = _runtime()
    with (
        patch.object(_main, "load_runtime", fake_load_runtime(rt)),
        patch(
            "deephaven_mcp.cli._commands.daemon.stop_daemon",
            AsyncMock(return_value=False),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["daemon", "stop", "-o", "json"])
    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["stopped"] is False


def test_main_help_at_depth_still_routes_to_subcommand(capsys) -> None:
    """``dh-mcp daemon --help`` must keep rendering daemon's help, not root.

    ``--help`` is intentionally excluded from the lift set; this test
    locks that exclusion against future regressions.
    """
    with pytest.raises(SystemExit) as exc_info:
        main(["daemon", "--help"])
    assert exc_info.value.code == 0
    out = capsys.readouterr().out
    # The daemon group's help lists verbs, including 'repair'; the
    # root help does not.
    assert "repair" in out
    assert "Manage the local dh-mcp daemon" in out
