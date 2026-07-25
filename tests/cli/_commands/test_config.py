"""Tests for ``dhcli config`` (:mod:`deephaven_mcp.cli._commands.config`)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import click
import pytest
from click.testing import CliRunner, Result

from deephaven_mcp._exceptions import (
    ConfigurationError,
    ConfigurationPathError,
    InternalError,
)
from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import config as config_mod
from deephaven_mcp.cli._commands.config import (
    _authoring_spec,
    _entity_status_entries,
    _map_config_error,
    _resolve_entity,
    _resolve_field_target,
    _store_from_spec,
    _warn_restart_hint,
    _warn_template_resolution,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime, RuntimeSpec
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._store import ConfigStore
from deephaven_mcp.config.schema import CliConfig, ServerConfig
from deephaven_mcp.config.tree import ConfigTree

from .._helpers import fake_load_runtime, make_runtime

# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------


def _invoke(
    args: list[str], *, input: str | None = None, standalone: bool = True
) -> Result:
    return CliRunner().invoke(cli, args, input=input, standalone_mode=standalone)


def _run(tmp_path: Path, *extra: str, standalone: bool = True) -> Result:
    # ``--no-input`` is a root option; tests invoke the group directly
    # (bypassing main()'s argv lifter), so hoist it to root position.
    tokens = list(extra)
    root = ["--no-input"] if "--no-input" in tokens else []
    tokens = [t for t in tokens if t != "--no-input"]
    args = [
        "--config-dir",
        str(tmp_path),
        "-o",
        "json",
        *root,
        "config",
        *tokens,
    ]
    return _invoke(args, standalone=standalone)


def _error_code(result: Result) -> str:
    """Return the raised CliError's stable code.

    The structured JSON error payload is rendered in ``main()``
    (``standalone_mode=False``), which ``CliRunner`` does not exercise;
    invoke with ``standalone=False`` so the ``CliError`` propagates
    into ``result.exception`` and assert its code here.
    """
    return result.exception.code.value  # type: ignore[union-attr]


def _add_session(tmp_path: Path, name: str = "local") -> None:
    _run(
        tmp_path,
        "session",
        "add",
        name,
        "--host",
        "localhost",
        "--auth",
        "anonymous",
        "--no-input",
    )


def _enable_prompts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the TTY gate open (CliRunner's stdin is never a real TTY).

    ``can_prompt`` is patched at both binding sites: the ``_prompt``
    module global (used by ``require_value``) and the name imported
    into the config command module (used by the direct gate checks).
    The ``--no-input`` flag keeps working: the patched gate still
    honors it.
    """
    gate = lambda *, no_input: not no_input  # noqa: E731 - tiny test stub
    monkeypatch.setattr("deephaven_mcp.cli._prompt.can_prompt", gate)
    monkeypatch.setattr("deephaven_mcp.cli._commands.config.can_prompt", gate)


@pytest.fixture
def _prompts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fixture form of :func:`_enable_prompts` for ``usefixtures``."""
    _enable_prompts(monkeypatch)


# ---------------------------------------------------------------------------
# show / validate
# ---------------------------------------------------------------------------


def _invoke_runtime(args: list[str], runtime: Runtime):
    runner = CliRunner()
    with patch.object(runtime_mod, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args)


# ---------------------------------------------------------------------------
# config show
# ---------------------------------------------------------------------------


def test_config_show_outputs_paths_and_models(tmp_path: Path) -> None:
    """Default tree (cli + server populated, others absent) dumps cleanly."""
    rt = make_runtime(tmp_path)
    result = _invoke_runtime(["-o", "json", "config", "show"], rt)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["config_dir"] == str(rt.config_dir)
    assert "cli" in payload
    assert "server" in payload
    assert "community" not in payload
    assert "enterprise" not in payload


def test_config_show_with_path_navigates_dumped_tree(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result = _invoke_runtime(["-o", "json", "config", "show", "cli.output.format"], rt)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == "json"


def test_config_show_with_bad_path_raises_config_path_invalid(
    tmp_path: Path,
) -> None:
    """A path that does not resolve exits 2 (config_path_invalid).

    ``_invoke_runtime`` uses click's default (standalone) exception handling,
    which prints the error and calls ``sys.exit`` internally — the
    structured JSON payload is only produced by ``main()``'s renderer,
    which this helper bypasses (matching
    ``test_config_validate_failure_propagates_load_error``).
    """
    rt = make_runtime(tmp_path)
    result = _invoke_runtime(["config", "show", "cli.does_not_exist"], rt)
    assert result.exit_code == 2
    assert "cli.does_not_exist" in result.output


def test_config_show_includes_community_and_enterprise_when_present(
    tmp_path: Path,
) -> None:
    """When the tree has all four sections, all four appear in the dump."""
    from deephaven_mcp.config.schema import (
        CommunityConfig,
        CommunitySettings,
        EnterpriseConfig,
        EnterpriseSettings,
    )

    config = ConfigTree(
        config_dir=tmp_path / "cfg",
        cli=CliConfig(),
        server=ServerConfig(),
        community=CommunityConfig(settings=CommunitySettings(), sessions={}),
        enterprise=EnterpriseConfig(settings=EnterpriseSettings(), systems={}),
    )
    rt = make_runtime(tmp_path, config=config)
    result = _invoke_runtime(["-o", "json", "config", "show"], rt)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert "community" in payload
    assert "enterprise" in payload


def test_config_show_redacts_secrets(tmp_path: Path) -> None:
    """Secret-bearing fields under any section are redacted in the dump."""
    from pydantic import SecretStr

    from deephaven_mcp.config.schema import (
        DaemonProcessConfig,
        ServerConfig,
    )

    server = ServerConfig(
        psk=SecretStr("supersecret-token"),
        daemon=DaemonProcessConfig(),
    )
    config = ConfigTree(config_dir=tmp_path / "cfg", cli=CliConfig(), server=server)
    rt = make_runtime(tmp_path, config=config)
    result = _invoke_runtime(["-o", "json", "config", "show"], rt)
    assert result.exit_code == 0, result.output
    assert "supersecret-token" not in result.output


# ---------------------------------------------------------------------------
# config validate
# ---------------------------------------------------------------------------


def test_config_validate_success(tmp_path: Path) -> None:
    """``config validate`` returns the success payload when load_runtime succeeds.

    Validation happens during runtime construction (``load_runtime``);
    ``config validate``'s body just renders the success record. We
    inject a pre-built ``Runtime`` to mimic that pre-validated state.
    """
    rt = make_runtime(tmp_path)
    result = _invoke_runtime(["-o", "json", "config", "validate"], rt)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["valid"] is True
    assert payload["config_dir"] == str(rt.config_dir)


def test_config_validate_failure_propagates_load_error(tmp_path: Path) -> None:
    """A ``CliError`` raised from ``load_runtime`` produces exit code 2.

    ``config validate``'s only job is to confirm the load succeeded.
    Any failure is detected in ``HelpfulCommand.invoke``'s runtime
    materialization, which raises before the verb body runs.
    """
    from deephaven_mcp.cli._errors import CliError, ErrorCode

    runner = CliRunner()
    with patch.object(
        runtime_mod,
        "load_runtime",
        AsyncMock(side_effect=CliError("oops", code=ErrorCode.CONFIG_INVALID)),
    ):
        result = runner.invoke(cli, ["-o", "json", "config", "validate"])
    assert result.exit_code == 2


def test_config_validate_help_describes_pre_body_validation() -> None:
    """The help reflects the pre-body validation, not a per-verb re-load.

    Regression test: the help previously claimed the verb "re-loads the
    configuration tree from disk," contradicting the handler (which only
    reports the already-validated state).
    """
    from deephaven_mcp.cli._commands.config import config as config_group

    help_text = config_group.commands["validate"].help or ""
    assert "before every command body" in help_text
    assert "re-load" not in help_text.lower()


# ---------------------------------------------------------------------------
# get / set / unset / keys: assert_never safety nets
# ---------------------------------------------------------------------------


def _ctx(tmp_path: Path) -> click.Context:
    ctx = click.Context(cli, info_name="dhcli")
    ctx.obj = RuntimeSpec(config_dir_override=tmp_path)
    return ctx


def test_config_get_hits_assert_never(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config_mod, "resolve_path", lambda path: object())
    assert config_mod.config_get.callback is not None
    with _ctx(tmp_path).scope(), pytest.raises(AssertionError):
        config_mod.config_get.callback(None)


def test_config_keys_hits_assert_never(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config_mod, "resolve_path", lambda path: object())
    assert config_mod.config_keys.callback is not None
    with _ctx(tmp_path).scope(), pytest.raises(AssertionError):
        config_mod.config_keys.callback(None)


# ---------------------------------------------------------------------------
# get / set / unset / keys
# ---------------------------------------------------------------------------


def _add_system(tmp_path: Path, name: str = "prod") -> None:
    _run(
        tmp_path,
        "system",
        "add",
        name,
        "--url",
        "https://x/iris/connection.json",
        "--auth",
        "password",
        "--username",
        "u",
        "--password",
        "p",
        "--no-input",
    )


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_get_whole_tree_empty_dir(tmp_path: Path) -> None:
    result = _run(tmp_path, "get")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {}


def test_get_whole_tree_aggregates_files(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _add_system(tmp_path)
    payload = json.loads(_run(tmp_path, "get").stdout)
    assert payload["community"]["sessions"]["local"]["host"] == "localhost"
    assert payload["enterprise"]["systems"]["prod"]["connection_json_url"]


def test_get_section_aggregates_only_that_section(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _add_system(tmp_path)
    payload = json.loads(_run(tmp_path, "get", "community").stdout)
    assert "sessions" in payload
    assert "enterprise" not in payload


def test_get_file_returns_whole_file(tmp_path: Path) -> None:
    _add_session(tmp_path)
    payload = json.loads(_run(tmp_path, "get", "community.sessions.local").stdout)
    assert payload["host"] == "localhost"


def test_get_leaf_returns_bare_scalar(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(tmp_path, "get", "community.sessions.local.host")
    assert result.exit_code == 0
    assert json.loads(result.stdout) == "localhost"


def test_get_refs_stay_unexpanded(tmp_path: Path) -> None:
    _run(
        tmp_path,
        "session",
        "add",
        "s1",
        "--auth",
        "psk",
        "--token",
        "${env:DOES_NOT_EXIST_XYZ}",
        "--no-input",
    )
    result = _run(tmp_path, "get", "community.sessions.s1.auth.credentials.token")
    assert json.loads(result.stdout) == "${env:DOES_NOT_EXIST_XYZ}"


def test_get_missing_file_not_found(tmp_path: Path) -> None:
    result = _run(tmp_path, "get", "cli", standalone=False)
    assert _error_code(result) == "not_found"


def test_get_missing_leaf_not_found(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(tmp_path, "get", "community.sessions.local.port", standalone=False)
    assert _error_code(result) == "not_found"


def test_get_bad_path_syntax(tmp_path: Path) -> None:
    result = _run(tmp_path, "get", "a..b", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_get_unknown_path(tmp_path: Path) -> None:
    result = _run(tmp_path, "get", "bogus", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_get_unparseable_file_maps_to_config_invalid(tmp_path: Path) -> None:
    # 'get' reads raw JSON5 without schema validation, so it only
    # fails on genuinely unparseable content, not on extra/invalid
    # fields (a schema question, answered by 'config files'/'show').
    (tmp_path / "cli.json").write_text("{broken")
    result = _run(tmp_path, "get", "cli", standalone=False)
    assert _error_code(result) == "config_invalid"


def test_get_aggregate_surfaces_unparseable_file(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("{broken")
    result = _run(tmp_path, "get", standalone=False)
    assert _error_code(result) == "config_invalid"


def test_get_aggregate_ignores_schema_errors(tmp_path: Path) -> None:
    # Extra/invalid fields are a schema question 'get' does not raise
    # on: it is the raw on-disk view.
    (tmp_path / "cli.json").write_text('{"unknown_key": 1}')
    result = _run(tmp_path, "get")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["cli"] == {"unknown_key": 1}


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------


def test_set_creates_unnamed_file(tmp_path: Path) -> None:
    result = _run(tmp_path, "set", "cli.output.format=human")
    assert result.exit_code == 0, result.output
    data = json.loads((tmp_path / "cli.json").read_text())
    assert data == {"output": {"format": "human"}}


def test_set_edits_existing_named_file(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _run(tmp_path, "set", "community.sessions.local.port=10001")
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert data["port"] == 10001
    assert data["host"] == "localhost"  # untouched


def test_set_multiple_assignments_across_files_atomic(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(
        tmp_path,
        "set",
        "community.sessions.local.port=10001",
        "cli.output.format=human",
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert set(payload["paths"]) == {
        "community.sessions.local.port",
        "cli.output.format",
    }
    assert len(payload["files"]) == 2


def test_set_value_parsed_as_json_number() -> None:
    from deephaven_mcp.cli._commands._wrapping import parse_key_value

    assert parse_key_value("x=42", decode_json=True) == ("x", 42)


def test_set_cannot_create_named_entity(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        "set",
        "community.sessions.new_one.port=10001",
        standalone=False,
    )
    assert _error_code(result) == "not_found"
    assert "add" in result.exception.message  # type: ignore[union-attr]


def test_set_rejects_section_path(tmp_path: Path) -> None:
    result = _run(tmp_path, "set", "community=5", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_set_bad_token_arg_parse_error(tmp_path: Path) -> None:
    result = _run(tmp_path, "set", "no-equals-sign", standalone=False)
    assert _error_code(result) == "arg_parse_error"


def test_set_invalid_value_fails_schema(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(
        tmp_path,
        "set",
        "community.sessions.local.port=not_a_number",
        standalone=False,
    )
    assert _error_code(result) == "config_invalid"
    # Nothing written.
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert "port" not in data


def test_set_replaces_whole_object_field(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _run(
        tmp_path,
        "set",
        'community.sessions.local.auth={"credentials": {"type": "anonymous"}}',
    )
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert data["auth"]["credentials"]["type"] == "anonymous"


def test_set_warns_on_unresolved_ref(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(
        tmp_path,
        "set",
        "community.sessions.local.auth.credentials.token=${env:DOES_NOT_EXIST_XYZ}",
    )
    # anonymous->psk swap isn't valid without 'type', but token alone
    # under an anonymous credentials object fails schema (extra field).
    assert result.exit_code == 2


def test_set_unresolved_ref_in_typed_field_warns_and_writes(tmp_path: Path) -> None:
    # An unresolved ref in an int-typed field is a warning, not an
    # error: the daemon's environment may resolve it at load time.
    _add_session(tmp_path)
    result = _run(
        tmp_path,
        "set",
        "community.sessions.local.port=${env:DOES_NOT_EXIST_XYZ}",
    )
    assert result.exit_code == 0, result.output
    assert "DOES_NOT_EXIST_XYZ" in result.stderr
    assert "unresolved templating ref" in result.stderr
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert data["port"] == "${env:DOES_NOT_EXIST_XYZ}"


def test_set_whole_file_replaces_contents(tmp_path: Path) -> None:
    # Assignment semantics at a whole-file path: the object replaces
    # the file's contents; nothing from the old contents survives.
    (tmp_path / "cli.json").write_text(
        '{"request": {"timeouts": {"default_seconds": 9}}}'
    )
    result = _run(
        tmp_path,
        "set",
        'cli={"output": {"format": "human"}}',
    )
    assert result.exit_code == 0, result.output
    data = json.loads((tmp_path / "cli.json").read_text())
    assert data == {"output": {"format": "human"}}


def test_set_bad_path_syntax(tmp_path: Path) -> None:
    result = _run(tmp_path, "set", "a..b=1", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_set_unparseable_existing_file(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("{broken")
    result = _run(tmp_path, "set", "cli.output.format=human", standalone=False)
    assert _error_code(result) == "config_invalid"


def test_set_two_assignments_same_file_reuse_loaded_data(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(
        tmp_path,
        "set",
        "community.sessions.local.port=10001",
        "community.sessions.local.programming_language=Groovy",
    )
    assert result.exit_code == 0, result.output
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert data["port"] == 10001
    assert data["programming_language"] == "Groovy"
    assert len(json.loads(result.stdout)["files"]) == 1


def test_set_intermediate_not_object(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(
        tmp_path,
        "set",
        "community.sessions.local.host.sub=1",
        standalone=False,
    )
    assert _error_code(result) == "config_path_invalid"


def test_set_refuses_json5_only_file(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text('{\n  // a comment\n  "output": {}\n}\n')
    result = _run(tmp_path, "set", "cli.output.format=human", standalone=False)
    assert _error_code(result) == "config_not_rewritable"


def test_set_write_failure_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deephaven_mcp._exceptions import ConfigurationError

    def _boom(*args: object, **kwargs: object) -> None:
        raise ConfigurationError("disk exploded")

    monkeypatch.setattr("deephaven_mcp.config._store.ConfigStore.write_all", _boom)
    result = _run(tmp_path, "set", "cli.output.format=human", standalone=False)
    assert _error_code(result) == "config_invalid"


# ---------------------------------------------------------------------------
# unset
# ---------------------------------------------------------------------------


def test_unset_removes_field(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _run(tmp_path, "set", "community.sessions.local.port=10001")
    result = _run(tmp_path, "unset", "community.sessions.local.port")
    assert result.exit_code == 0, result.output
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert "port" not in data


def test_unset_multiple_paths_across_files(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _run(
        tmp_path,
        "set",
        "community.sessions.local.port=10001",
        "cli.output.format=human",
    )
    result = _run(
        tmp_path,
        "unset",
        "community.sessions.local.port",
        "cli.output.format",
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert len(payload["files"]) == 2


def test_unset_whole_file_named_kind_points_to_remove(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(tmp_path, "unset", "community.sessions.local", standalone=False)
    assert _error_code(result) == "config_path_invalid"
    assert "remove" in result.exception.message  # type: ignore[union-attr]


def test_unset_whole_file_unnamed_kind_points_to_set(tmp_path: Path) -> None:
    _run(tmp_path, "set", "cli.output.format=human")
    result = _run(tmp_path, "unset", "cli", standalone=False)
    assert _error_code(result) == "config_path_invalid"
    assert "set" in result.exception.message  # type: ignore[union-attr]


def test_unset_missing_field_not_found(tmp_path: Path) -> None:
    _add_session(tmp_path)
    result = _run(tmp_path, "unset", "community.sessions.local.port", standalone=False)
    assert _error_code(result) == "not_found"


def test_unset_missing_file_not_found(tmp_path: Path) -> None:
    result = _run(tmp_path, "unset", "cli.output.format", standalone=False)
    assert _error_code(result) == "not_found"


def test_unset_bad_section_path(tmp_path: Path) -> None:
    result = _run(tmp_path, "unset", "community", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_unset_bad_path_syntax(tmp_path: Path) -> None:
    result = _run(tmp_path, "unset", "a..b", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_unset_two_paths_same_file_reuse_loaded_data(tmp_path: Path) -> None:
    _add_session(tmp_path)
    _run(
        tmp_path,
        "set",
        "community.sessions.local.port=10001",
        "community.sessions.local.programming_language=Groovy",
    )
    result = _run(
        tmp_path,
        "unset",
        "community.sessions.local.port",
        "community.sessions.local.programming_language",
    )
    assert result.exit_code == 0, result.output
    data = json.loads((tmp_path / "community" / "sessions" / "local.json").read_text())
    assert "port" not in data
    assert "programming_language" not in data
    assert len(json.loads(result.stdout)["files"]) == 1


def test_unset_refuses_json5_only_file(tmp_path: Path) -> None:
    _add_session(tmp_path)
    session_file = tmp_path / "community" / "sessions" / "local.json"
    session_file.write_text(
        session_file.read_text().rstrip() + "\n// trailing comment\n"
    )
    result = _run(
        tmp_path,
        "unset",
        "community.sessions.local.host",
        standalone=False,
    )
    assert _error_code(result) == "config_not_rewritable"


def test_unset_write_failure_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deephaven_mcp._exceptions import ConfigurationError

    _add_session(tmp_path)
    _run(tmp_path, "set", "community.sessions.local.port=10001")

    def _boom(*args: object, **kwargs: object) -> None:
        raise ConfigurationError("disk exploded")

    monkeypatch.setattr("deephaven_mcp.config._store.ConfigStore.write_all", _boom)
    result = _run(
        tmp_path,
        "unset",
        "community.sessions.local.port",
        standalone=False,
    )
    assert _error_code(result) == "config_invalid"


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------


def test_keys_root_lists_every_kind(tmp_path: Path) -> None:
    payload = json.loads(_run(tmp_path, "keys").stdout)
    paths = {k["path"] for k in payload["keys"]}
    assert any(p.startswith("cli.") for p in paths)
    assert any(p.startswith("community.sessions.<name>.") for p in paths)
    assert any(p.startswith("enterprise.systems.<name>.") for p in paths)


def test_keys_scoped_to_file_kind(tmp_path: Path) -> None:
    payload = json.loads(_run(tmp_path, "keys", "server").stdout)
    paths = {k["path"] for k in payload["keys"]}
    assert "server.psk" in paths
    assert not any(p.startswith("cli.") for p in paths)


def test_keys_scoped_to_field_prefix(tmp_path: Path) -> None:
    payload = json.loads(
        _run(tmp_path, "keys", "community.settings.session_creation").stdout
    )
    paths = {k["path"] for k in payload["keys"]}
    assert all(p.startswith("community.settings.session_creation") for p in paths)
    assert len(paths) > 1


def test_keys_secret_flag(tmp_path: Path) -> None:
    payload = json.loads(_run(tmp_path, "keys", "server").stdout)
    psk = next(k for k in payload["keys"] if k["path"] == "server.psk")
    assert psk["secret"] is True


def test_keys_present_flag_for_real_entity(tmp_path: Path) -> None:
    _add_session(tmp_path)
    payload = json.loads(_run(tmp_path, "keys", "community.sessions.local").stdout)
    by_path = {k["path"]: k for k in payload["keys"]}
    assert by_path["community.sessions.local.host"].get("present") is True
    assert "present" not in by_path["community.sessions.local.port"]


def test_keys_no_present_flag_for_template(tmp_path: Path) -> None:
    _add_session(tmp_path)
    payload = json.loads(_run(tmp_path, "keys").stdout)
    by_path = {k["path"]: k for k in payload["keys"]}
    assert "present" not in by_path["community.sessions.<name>.host"]


def test_keys_present_reflects_unreadable_file(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("{broken")
    payload = json.loads(_run(tmp_path, "keys", "cli").stdout)
    # No crash; present is simply omitted since the file can't be read.
    assert all("present" not in k for k in payload["keys"])


def test_keys_bad_path(tmp_path: Path) -> None:
    result = _run(tmp_path, "keys", "bogus", standalone=False)
    assert _error_code(result) == "config_path_invalid"


def test_keys_required_flag(tmp_path: Path) -> None:
    payload = json.loads(_run(tmp_path, "keys", "enterprise.systems").stdout)
    by_path = {k["path"]: k for k in payload["keys"]}
    assert by_path["enterprise.systems.<name>.connection_json_url"]["required"] is True


# ---------------------------------------------------------------------------
# edit
# ---------------------------------------------------------------------------


def _fake_editor(monkeypatch: pytest.MonkeyPatch, result: str | None) -> None:
    monkeypatch.setattr(
        "deephaven_mcp.cli._commands.config._open_editor",
        lambda text: result,
    )


@pytest.mark.usefixtures("_prompts")
def test_open_editor_delegates_to_click_edit(monkeypatch: pytest.MonkeyPatch) -> None:
    from deephaven_mcp.cli._commands.config import _open_editor

    monkeypatch.setattr(
        "deephaven_mcp.cli._commands.config.click.edit",
        lambda *, text, extension: f"edited:{text}:{extension}",
    )
    assert _open_editor("original") == "edited:original:.json"


# ---------------------------------------------------------------------------
# TTY gating
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_prompts")
def test_edit_no_tty_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "deephaven_mcp.cli._commands.config.can_prompt",
        lambda *, no_input: not no_input,
    )
    result = _run(tmp_path, "edit", "cli", "--no-input", standalone=False)
    assert _error_code(result) == "no_tty"


# ---------------------------------------------------------------------------
# path resolution guards
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_prompts")
def test_edit_field_path_rejected(tmp_path: Path) -> None:
    result = _run(tmp_path, "edit", "cli.output.format", standalone=False)
    assert _error_code(result) == "config_path_invalid"


@pytest.mark.usefixtures("_prompts")
def test_edit_missing_named_entity_not_found(tmp_path: Path) -> None:
    result = _run(tmp_path, "edit", "community.sessions.local", standalone=False)
    assert _error_code(result) == "not_found"


@pytest.mark.usefixtures("_prompts")
def test_edit_bad_path_syntax(tmp_path: Path) -> None:
    result = _run(tmp_path, "edit", "a..b", standalone=False)
    assert _error_code(result) == "config_path_invalid"


# ---------------------------------------------------------------------------
# editor round trip
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_prompts")
def test_edit_creates_absent_unnamed_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_editor(monkeypatch, '{\n  "output": {"format": "human"}\n}\n')
    result = _run(tmp_path, "edit", "cli")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["changed"] is True
    data = json.loads((tmp_path / "cli.json").read_text())
    assert data == {"output": {"format": "human"}}


@pytest.mark.usefixtures("_prompts")
def test_edit_no_changes_when_editor_returns_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "cli.json").write_text('{"output": {"format": "json"}}')
    _fake_editor(monkeypatch, None)
    result = _run(tmp_path, "edit", "cli")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["changed"] is False
    assert (tmp_path / "cli.json").read_text() == '{"output": {"format": "json"}}'


@pytest.mark.usefixtures("_prompts")
def test_edit_no_changes_when_editor_returns_identical_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = '{"output": {"format": "json"}}'
    (tmp_path / "cli.json").write_text(original)
    _fake_editor(monkeypatch, original)
    result = _run(tmp_path, "edit", "cli")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["changed"] is False


@pytest.mark.usefixtures("_prompts")
def test_edit_preserves_json5_comments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The literal saved text is written back verbatim, comments included."""
    (tmp_path / "cli.json").write_text('{"output": {"format": "json"}}')
    edited = '{\n  // a comment\n  "output": {"format": "human"}\n}\n'
    _fake_editor(monkeypatch, edited)
    result = _run(tmp_path, "edit", "cli")
    assert result.exit_code == 0, result.output
    assert (tmp_path / "cli.json").read_text() == edited


@pytest.mark.usefixtures("_prompts")
def test_edit_named_entity_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _add_session(tmp_path)
    session_file = tmp_path / "community" / "sessions" / "local.json"
    edited = json.dumps(
        {
            "host": "localhost",
            "port": 12345,
            "auth": {"credentials": {"type": "anonymous"}},
        }
    )
    _fake_editor(monkeypatch, edited)
    result = _run(tmp_path, "edit", "community.sessions.local")
    assert result.exit_code == 0, result.output
    assert json.loads(session_file.read_text())["port"] == 12345


# ---------------------------------------------------------------------------
# validation failures leave the file untouched
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_prompts")
def test_edit_invalid_json5_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "cli.json").write_text('{"output": {"format": "json"}}')
    _fake_editor(monkeypatch, "{broken")
    result = _run(tmp_path, "edit", "cli", standalone=False)
    assert _error_code(result) == "config_invalid"
    assert (tmp_path / "cli.json").read_text() == '{"output": {"format": "json"}}'


@pytest.mark.usefixtures("_prompts")
def test_edit_top_level_non_object_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "cli.json").write_text('{"output": {"format": "json"}}')
    _fake_editor(monkeypatch, "[1, 2, 3]")
    result = _run(tmp_path, "edit", "cli", standalone=False)
    assert _error_code(result) == "config_invalid"


@pytest.mark.usefixtures("_prompts")
def test_edit_schema_validation_failure_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "cli.json").write_text('{"output": {"format": "json"}}')
    _fake_editor(monkeypatch, '{"output": {"format": "not_a_real_format"}}')
    result = _run(tmp_path, "edit", "cli", standalone=False)
    assert _error_code(result) == "config_invalid"
    assert (tmp_path / "cli.json").read_text() == '{"output": {"format": "json"}}'


@pytest.mark.usefixtures("_prompts")
def test_edit_unreadable_file_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "cli.json").write_text('{"output": {"format": "json"}}')
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, *a, **k: (_ for _ in ()).throw(OSError("boom")),
    )
    result = _run(tmp_path, "edit", "cli", standalone=False)
    assert _error_code(result) == "config_invalid"


# ---------------------------------------------------------------------------
# files
# ---------------------------------------------------------------------------


def _files_payload(config_dir: Path) -> dict:
    result = _invoke(["--config-dir", str(config_dir), "-o", "json", "config", "files"])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


def _entry(payload: dict, path: str) -> dict:
    matches = [f for f in payload["files"] if f["path"] == path]
    assert len(matches) == 1, f"no unique entry for {path}: {payload['files']}"
    return matches[0]


def test_files_empty_directory_lists_unnamed_kinds(tmp_path: Path) -> None:
    payload = _files_payload(tmp_path)
    assert payload["config_dir"] == str(tmp_path)
    paths = [f["path"] for f in payload["files"]]
    assert paths == ["cli", "server", "community.settings", "enterprise.settings"]
    assert all(f["exists"] is False for f in payload["files"])
    assert all("valid" not in f for f in payload["files"])


def test_files_reports_valid_and_named_entries(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("{}")
    sessions = tmp_path / "community" / "sessions"
    sessions.mkdir(parents=True)
    (sessions / "dev.json").write_text(
        json.dumps(
            {"host": "localhost", "auth": {"credentials": {"type": "anonymous"}}}
        )
    )
    payload = _files_payload(tmp_path)
    assert _entry(payload, "cli")["valid"] is True
    dev = _entry(payload, "community.sessions.dev")
    assert dev["exists"] is True
    assert dev["valid"] is True
    assert dev["file"] == str(sessions / "dev.json")


def test_files_reports_invalid_file_with_error(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text('{"unknown_key": 1}')
    payload = _files_payload(tmp_path)
    entry = _entry(payload, "cli")
    assert entry["valid"] is False
    assert "cli.json" in entry["error"]


def test_files_reports_unparseable_file_with_error(tmp_path: Path) -> None:
    (tmp_path / "server.json").write_text("{broken")
    entry = _entry(_files_payload(tmp_path), "server")
    assert entry["valid"] is False
    assert "Invalid JSON/JSON5" in entry["error"]


def test_files_reports_template_warnings(tmp_path: Path) -> None:
    sessions = tmp_path / "community" / "sessions"
    sessions.mkdir(parents=True)
    (sessions / "dev.json").write_text(
        json.dumps(
            {
                "auth": {
                    "credentials": {
                        "type": "psk",
                        "token": "${env:DOES_NOT_EXIST_XYZ}",
                    }
                }
            }
        )
    )
    entry = _entry(_files_payload(tmp_path), "community.sessions.dev")
    assert entry["valid"] is True
    assert any("DOES_NOT_EXIST_XYZ" in w for w in entry["warnings"])


def test_files_works_without_valid_tree(tmp_path: Path) -> None:
    """The verb is needs_runtime=False: it lists the layout without
    performing the full tree load, so even a broken tree cannot block it."""
    result = _invoke(["--config-dir", str(tmp_path), "config", "files"])
    assert result.exit_code == 0


def test_files_human_mode(tmp_path: Path) -> None:
    result = _invoke(["--config-dir", str(tmp_path), "-o", "human", "config", "files"])
    assert result.exit_code == 0
    assert "cli.json" in result.output


# ---------------------------------------------------------------------------
# init: guided first-time setup wizard
# ---------------------------------------------------------------------------


def _run_init(tmp_path: Path, *, input: str) -> Result:
    return _invoke(
        ["--config-dir", str(tmp_path), "-o", "json", "config", "init"],
        input=input,
    )


def _run_no_tty(tmp_path: Path) -> Result:
    return _invoke(
        [
            "--no-input",
            "--config-dir",
            str(tmp_path),
            "-o",
            "json",
            "config",
            "init",
        ]
    )


@pytest.mark.usefixtures("_prompts")
def test_init_no_tty_raises(tmp_path: Path) -> None:
    result = _run_no_tty(tmp_path)
    assert result.exit_code == 2
    assert "no_tty" in result.output or result.exception is not None


@pytest.mark.usefixtures("_prompts")
def test_init_declines_both_writes_nothing(tmp_path: Path) -> None:
    result = _run_init(tmp_path, input="n\nn\n")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload == {}
    assert not (tmp_path / "community").exists()
    assert not (tmp_path / "enterprise").exists()


@pytest.mark.usefixtures("_prompts")
def test_init_creates_session_only(tmp_path: Path) -> None:
    # y (session) / name / host (default) / port (default) / auth / n (system)
    result = _run_init(tmp_path, input="y\ndev\n\n\nanonymous\nn\n")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["community_session"]["name"] == "dev"
    assert "enterprise_system" not in payload
    data = json.loads((tmp_path / "community" / "sessions" / "dev.json").read_text())
    assert data["host"] == "localhost"
    assert data["port"] == 10000
    assert data["auth"]["credentials"] == {"type": "anonymous"}
    assert not (tmp_path / "enterprise").exists()


@pytest.mark.usefixtures("_prompts")
def test_init_creates_system_only(tmp_path: Path) -> None:
    # n (session) / y (system) / name / url / auth / username / password
    result = _run_init(
        tmp_path,
        input=(
            "n\n"
            "y\n"
            "prod\n"
            "https://dhe.example.com/iris/connection.json\n"
            "password\n"
            "alice\n"
            "secret\n"
        ),
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert "community_session" not in payload
    assert payload["enterprise_system"]["name"] == "prod"
    data = json.loads((tmp_path / "enterprise" / "systems" / "prod.json").read_text())
    assert data["auth"]["credentials"]["username"] == "alice"
    assert not (tmp_path / "community").exists()


@pytest.mark.usefixtures("_prompts")
def test_init_creates_both(tmp_path: Path) -> None:
    result = _run_init(
        tmp_path,
        input=(
            "y\ndev\n\n\nanonymous\n"
            "y\nprod\nhttps://dhe.example.com/iris/connection.json\n"
            "password\nalice\nsecret\n"
        ),
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["community_session"]["name"] == "dev"
    assert payload["enterprise_system"]["name"] == "prod"
    assert (tmp_path / "community" / "sessions" / "dev.json").is_file()
    assert (tmp_path / "enterprise" / "systems" / "prod.json").is_file()


@pytest.mark.usefixtures("_prompts")
def test_init_propagates_entity_creation_errors(tmp_path: Path) -> None:
    """A name collision from a prior run surfaces the same structured error
    as 'config session add' (already_exists), not a wizard-specific one."""
    sessions = tmp_path / "community" / "sessions"
    sessions.mkdir(parents=True)
    (sessions / "dev.json").write_text(
        json.dumps({"auth": {"credentials": {"type": "anonymous"}}})
    )
    result = _invoke(
        [
            "--config-dir",
            str(tmp_path),
            "-o",
            "json",
            "config",
            "init",
        ],
        input="y\ndev\n",
    )
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# session sub-group
# ---------------------------------------------------------------------------


def _add(
    tmp_path: Path,
    *extra: str,
    input: str | None = None,
    standalone: bool = True,
) -> Result:
    # ``--no-input`` is a root option; tests invoke the group directly
    # (bypassing main()'s argv lifter), so hoist it to root position.
    tokens = list(extra)
    root = ["--no-input"] if "--no-input" in tokens else []
    tokens = [t for t in tokens if t != "--no-input"]
    args = [
        "--config-dir",
        str(tmp_path),
        "-o",
        "json",
        *root,
        "config",
        "session",
        *tokens,
    ]
    return _invoke(args, input=input, standalone=standalone)


def _session_file(tmp_path: Path, name: str) -> Path:
    return tmp_path / "community" / "sessions" / f"{name}.json"


# ---------------------------------------------------------------------------
# add — non-interactive
# ---------------------------------------------------------------------------


def test_add_anonymous(tmp_path: Path) -> None:
    result = _add(tmp_path, "add", "local", "--auth", "anonymous", "--no-input")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["name"] == "local"
    assert payload["path"] == "community.sessions.local"
    data = json.loads(_session_file(tmp_path, "local").read_text())
    assert data == {"auth": {"credentials": {"type": "anonymous"}}}


def test_add_writes_transport_fields(tmp_path: Path) -> None:
    result = _add(
        tmp_path,
        "add",
        "prod",
        "--host",
        "dh.example.com",
        "--port",
        "11000",
        "--language",
        "Groovy",
        "--auth",
        "anonymous",
        "--no-input",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_session_file(tmp_path, "prod").read_text())
    assert data["host"] == "dh.example.com"
    assert data["port"] == 11000
    assert data["programming_language"] == "Groovy"


def test_add_psk_with_ref_token_warns_unresolved(tmp_path: Path) -> None:
    result = _add(
        tmp_path,
        "add",
        "s1",
        "--auth",
        "psk",
        "--token",
        "${env:DOES_NOT_EXIST_XYZ}",
        "--no-input",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_session_file(tmp_path, "s1").read_text())
    # Refs are stored verbatim, never expanded at write time.
    assert data["auth"]["credentials"] == {
        "type": "psk",
        "token": "${env:DOES_NOT_EXIST_XYZ}",
    }
    assert "DOES_NOT_EXIST_XYZ" in result.stderr
    # A ref is not a literal — no literal-secret hint.
    assert "hint:" not in result.stderr


def test_add_psk_literal_token_hints_ref(tmp_path: Path) -> None:
    result = _add(
        tmp_path, "add", "s1", "--auth", "psk", "--token", "sekrit", "--no-input"
    )
    assert result.exit_code == 0, result.output
    assert "hint: --token" in result.stderr


def test_add_password_with_effective_user(tmp_path: Path) -> None:
    result = _add(
        tmp_path,
        "add",
        "s1",
        "--auth",
        "password",
        "--username",
        "alice",
        "--password",
        "${env:PW_XYZ:-fallback}",
        "--effective-user",
        "svc",
        "--no-input",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_session_file(tmp_path, "s1").read_text())
    assert data["auth"]["credentials"] == {
        "type": "password",
        "username": "alice",
        "password": "${env:PW_XYZ:-fallback}",
        "effective_user": "svc",
    }


def test_add_custom(tmp_path: Path) -> None:
    result = _add(
        tmp_path,
        "add",
        "s1",
        "--auth",
        "custom",
        "--auth-type",
        "com.example.MyHandler",
        "--auth-token",
        "${env:TOK:-t}",
        "--no-input",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_session_file(tmp_path, "s1").read_text())
    assert data["auth"]["credentials"]["type"] == "custom"
    assert data["auth"]["credentials"]["auth_type"] == "com.example.MyHandler"


# ---------------------------------------------------------------------------
# add — error paths
# ---------------------------------------------------------------------------


def test_add_rejects_inapplicable_flag(tmp_path: Path) -> None:
    result = _add(
        tmp_path,
        "add",
        "s1",
        "--auth",
        "anonymous",
        "--token",
        "x",
        "--no-input",
        standalone=False,
    )
    assert _error_code(result) == "option_not_applicable"
    assert "--token" in result.exception.message  # type: ignore[union-attr]
    assert not _session_file(tmp_path, "s1").exists()


def test_add_missing_auth_without_tty(tmp_path: Path) -> None:
    result = _add(tmp_path, "add", "s1", "--no-input", standalone=False)
    assert _error_code(result) == "missing_required_option"
    assert "--auth" in result.exception.message  # type: ignore[union-attr]


def test_add_missing_token_without_tty(tmp_path: Path) -> None:
    result = _add(
        tmp_path, "add", "s1", "--auth", "psk", "--no-input", standalone=False
    )
    assert _error_code(result) == "missing_required_option"
    assert "--token" in result.exception.message  # type: ignore[union-attr]


def test_session_add_duplicate_refused(tmp_path: Path) -> None:
    assert (
        _add(tmp_path, "add", "s1", "--auth", "anonymous", "--no-input").exit_code == 0
    )
    result = _add(
        tmp_path, "add", "s1", "--auth", "anonymous", "--no-input", standalone=False
    )
    assert _error_code(result) == "already_exists"


def test_add_rejects_dotted_name(tmp_path: Path) -> None:
    result = _add(
        tmp_path,
        "add",
        "bad.name",
        "--auth",
        "anonymous",
        "--no-input",
        standalone=False,
    )
    assert _error_code(result) == "config_path_invalid"
    assert "rename" in result.exception.message  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# add — interactive prompting
# ---------------------------------------------------------------------------


def test_session_add_prompts_on_tty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_prompts(monkeypatch)
    # Prompts: host (default), port (default), auth choice, token.
    result = _add(
        tmp_path,
        "add",
        "dev",
        input="\n\npsk\nmy-token\n",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_session_file(tmp_path, "dev").read_text())
    assert data["host"] == "localhost"
    assert data["port"] == 10000
    assert data["auth"]["credentials"] == {"type": "psk", "token": "my-token"}


def test_add_no_input_flag_disables_prompts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Even with the TTY gate open, --no-input suppresses prompting.
    _enable_prompts(monkeypatch)
    result = _invoke(
        [
            "--config-dir",
            str(tmp_path),
            "--no-input",
            "config",
            "session",
            "add",
            "dev",
        ],
        standalone=False,
    )
    assert _error_code(result) == "missing_required_option"


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


def test_session_remove_with_yes(tmp_path: Path) -> None:
    _add(tmp_path, "add", "s1", "--auth", "anonymous", "--no-input")
    result = _add(tmp_path, "remove", "s1", "--yes")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["name"] == "s1"
    assert not _session_file(tmp_path, "s1").exists()


def test_session_remove_requires_yes_without_tty(tmp_path: Path) -> None:
    _add(tmp_path, "add", "s1", "--auth", "anonymous", "--no-input")
    result = _add(tmp_path, "remove", "s1", standalone=False)
    assert _error_code(result) == "missing_required_option"
    assert _session_file(tmp_path, "s1").exists()


def test_remove_missing_session(tmp_path: Path) -> None:
    result = _add(tmp_path, "remove", "ghost", "--yes", standalone=False)
    assert _error_code(result) == "not_found"


def test_remove_interactive_confirm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _add(tmp_path, "add", "s1", "--auth", "anonymous", "--no-input")
    _enable_prompts(monkeypatch)
    result = _add(tmp_path, "remove", "s1", input="y\n")
    assert result.exit_code == 0, result.output
    assert not _session_file(tmp_path, "s1").exists()


def test_session_remove_interactive_decline_aborts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _add(tmp_path, "add", "s1", "--auth", "anonymous", "--no-input")
    _enable_prompts(monkeypatch)
    result = _add(tmp_path, "remove", "s1", input="n\n")
    assert result.exit_code != 0
    assert _session_file(tmp_path, "s1").exists()


# ---------------------------------------------------------------------------
# failure remaps and the assert_never guard
# ---------------------------------------------------------------------------


def test_session_add_write_failure_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deephaven_mcp._exceptions import ConfigurationError

    def _boom(*args: object, **kwargs: object) -> None:
        raise ConfigurationError("disk exploded")

    monkeypatch.setattr("deephaven_mcp.config._store.ConfigStore.write", _boom)
    result = _add(
        tmp_path, "add", "s1", "--auth", "anonymous", "--no-input", standalone=False
    )
    assert _error_code(result) == "config_invalid"


def test_session_remove_delete_failure_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deephaven_mcp._exceptions import ConfigurationError

    _add(tmp_path, "add", "s1", "--auth", "anonymous", "--no-input")

    def _boom(*args: object, **kwargs: object) -> None:
        raise ConfigurationError("cannot delete")

    monkeypatch.setattr("deephaven_mcp.config._store.ConfigStore.delete", _boom)
    result = _add(tmp_path, "remove", "s1", "--yes", standalone=False)
    assert _error_code(result) == "config_invalid"


def test_session_build_credentials_rejects_out_of_vocabulary_auth() -> None:
    from deephaven_mcp.cli._commands.config import (
        _build_session_credentials as _build_credentials,
    )

    with pytest.raises(AssertionError):
        _build_credentials(
            auth="rainbow",  # type: ignore[arg-type]
            token=None,
            username=None,
            password=None,
            effective_user=None,
            auth_type=None,
            auth_token=None,
            no_input=True,
        )
    # Suppression justified: deliberately constructing a value the
    # ``Literal`` rejects so the runtime ``assert_never`` branch is
    # covered. Bracketed ``arg-type`` names what is silenced; mypy
    # still flags any *unintentional* misuse at real call sites.


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_session_list_empty(tmp_path: Path) -> None:
    result = _add(tmp_path, "list")
    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"sessions": []}


def test_session_list_reports_validity(tmp_path: Path) -> None:
    _add(tmp_path, "add", "good", "--auth", "anonymous", "--no-input")
    bad = _session_file(tmp_path, "bad")
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text('{"unknown_key": 1}')
    result = _add(tmp_path, "list")
    payload = json.loads(result.stdout)
    by_name = {s["name"]: s for s in payload["sessions"]}
    assert by_name["good"]["valid"] is True
    assert by_name["bad"]["valid"] is False
    assert "error" in by_name["bad"]


# ---------------------------------------------------------------------------
# system sub-group
# ---------------------------------------------------------------------------


_URL = "https://dhe.example.com/iris/connection.json"


def _system(
    tmp_path: Path,
    *extra: str,
    input: str | None = None,
    standalone: bool = True,
) -> Result:
    # ``--no-input`` is a root option; tests invoke the group directly
    # (bypassing main()'s argv lifter), so hoist it to root position.
    tokens = list(extra)
    root = ["--no-input"] if "--no-input" in tokens else []
    tokens = [t for t in tokens if t != "--no-input"]
    args = [
        "--config-dir",
        str(tmp_path),
        "-o",
        "json",
        *root,
        "config",
        "system",
        *tokens,
    ]
    return _invoke(args, input=input, standalone=standalone)


def _system_file(tmp_path: Path, name: str) -> Path:
    return tmp_path / "enterprise" / "systems" / f"{name}.json"


def _add_password(
    tmp_path: Path,
    name: str = "prod",
    *extra: str,
    standalone: bool = True,
) -> Result:
    return _system(
        tmp_path,
        "add",
        name,
        "--url",
        _URL,
        "--auth",
        "password",
        "--username",
        "alice",
        "--password",
        "${env:PW_XYZ:-pw}",
        "--no-input",
        *extra,
        standalone=standalone,
    )


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------


def test_add_password(tmp_path: Path) -> None:
    result = _add_password(tmp_path)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["path"] == "enterprise.systems.prod"
    data = json.loads(_system_file(tmp_path, "prod").read_text())
    assert data == {
        "connection_json_url": _URL,
        "auth": {
            "credentials": {
                "type": "password",
                "username": "alice",
                "password": "${env:PW_XYZ:-pw}",
            }
        },
    }


def test_add_password_effective_user(tmp_path: Path) -> None:
    result = _add_password(tmp_path, "prod", "--effective-user", "svc")
    assert result.exit_code == 0, result.output
    data = json.loads(_system_file(tmp_path, "prod").read_text())
    assert data["auth"]["credentials"]["effective_user"] == "svc"


def test_add_private_key(tmp_path: Path) -> None:
    result = _system(
        tmp_path,
        "add",
        "stg",
        "--url",
        _URL,
        "--auth",
        "private_key",
        "--key",
        "${env:KEY_XYZ:-fake-pem}",
        "--no-input",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_system_file(tmp_path, "stg").read_text())
    assert data["auth"]["credentials"] == {
        "type": "private_key",
        "key_text": "${env:KEY_XYZ:-fake-pem}",
    }


def test_add_session_creation_block(tmp_path: Path) -> None:
    result = _add_password(tmp_path, "prod", "--max-sessions", "3", "--heap-gb", "8")
    assert result.exit_code == 0, result.output
    data = json.loads(_system_file(tmp_path, "prod").read_text())
    assert data["session_creation"] == {
        "max_concurrent_sessions": 3,
        "defaults": {"heap_size_gb": 8.0},
    }


def test_add_omits_session_creation_by_default(tmp_path: Path) -> None:
    _add_password(tmp_path)
    data = json.loads(_system_file(tmp_path, "prod").read_text())
    assert "session_creation" not in data


def test_add_rejects_inapplicable_key_for_password(tmp_path: Path) -> None:
    result = _system(
        tmp_path,
        "add",
        "prod",
        "--url",
        _URL,
        "--auth",
        "password",
        "--username",
        "a",
        "--password",
        "b",
        "--key",
        "pem",
        "--no-input",
        standalone=False,
    )
    assert _error_code(result) == "option_not_applicable"
    assert "--key" in result.exception.message  # type: ignore[union-attr]


def test_add_rejects_username_for_private_key(tmp_path: Path) -> None:
    result = _system(
        tmp_path,
        "add",
        "prod",
        "--url",
        _URL,
        "--auth",
        "private_key",
        "--key",
        "pem",
        "--username",
        "alice",
        "--no-input",
        standalone=False,
    )
    assert _error_code(result) == "option_not_applicable"


def test_add_reserved_name_community(tmp_path: Path) -> None:
    result = _system(
        tmp_path,
        "add",
        "community",
        "--url",
        _URL,
        "--auth",
        "password",
        "--username",
        "a",
        "--password",
        "b",
        "--no-input",
        standalone=False,
    )
    assert _error_code(result) == "config_path_invalid"
    assert "reserved" in result.exception.message  # type: ignore[union-attr]


def test_add_missing_url_without_tty(tmp_path: Path) -> None:
    result = _system(
        tmp_path, "add", "prod", "--auth", "password", "--no-input", standalone=False
    )
    assert _error_code(result) == "missing_required_option"
    assert "--url" in result.exception.message  # type: ignore[union-attr]


def test_system_add_duplicate_refused(tmp_path: Path) -> None:
    assert _add_password(tmp_path).exit_code == 0
    result = _add_password(tmp_path, standalone=False)
    assert _error_code(result) == "already_exists"


def test_system_add_prompts_on_tty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_prompts(monkeypatch)
    # Prompts: url, auth choice, username, password.
    result = _system(
        tmp_path,
        "add",
        "prod",
        input=f"{_URL}\npassword\nalice\npw\n",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(_system_file(tmp_path, "prod").read_text())
    assert data["connection_json_url"] == _URL
    assert data["auth"]["credentials"]["username"] == "alice"


# ---------------------------------------------------------------------------
# remove / list
# ---------------------------------------------------------------------------


def test_system_remove_with_yes(tmp_path: Path) -> None:
    _add_password(tmp_path)
    result = _system(tmp_path, "remove", "prod", "--yes")
    assert result.exit_code == 0, result.output
    assert not _system_file(tmp_path, "prod").exists()


def test_system_remove_requires_yes_without_tty(tmp_path: Path) -> None:
    _add_password(tmp_path)
    result = _system(tmp_path, "remove", "prod", standalone=False)
    assert _error_code(result) == "missing_required_option"


def test_remove_missing_system(tmp_path: Path) -> None:
    result = _system(tmp_path, "remove", "ghost", "--yes", standalone=False)
    assert _error_code(result) == "not_found"


def test_system_remove_interactive_decline_aborts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _add_password(tmp_path)
    _enable_prompts(monkeypatch)
    result = _system(tmp_path, "remove", "prod", input="n\n")
    assert result.exit_code != 0
    assert _system_file(tmp_path, "prod").exists()


def test_system_add_write_failure_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deephaven_mcp._exceptions import ConfigurationError

    def _boom(*args: object, **kwargs: object) -> None:
        raise ConfigurationError("disk exploded")

    monkeypatch.setattr("deephaven_mcp.config._store.ConfigStore.write", _boom)
    result = _add_password(tmp_path, "prod", standalone=False)
    assert _error_code(result) == "config_invalid"


def test_system_remove_delete_failure_maps_to_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deephaven_mcp._exceptions import ConfigurationError

    _add_password(tmp_path)

    def _boom(*args: object, **kwargs: object) -> None:
        raise ConfigurationError("cannot delete")

    monkeypatch.setattr("deephaven_mcp.config._store.ConfigStore.delete", _boom)
    result = _system(tmp_path, "remove", "prod", "--yes", standalone=False)
    assert _error_code(result) == "config_invalid"


def test_system_build_credentials_rejects_out_of_vocabulary_auth() -> None:
    from deephaven_mcp.cli._commands.config import (
        _build_system_credentials as _build_credentials,
    )

    with pytest.raises(AssertionError):
        _build_credentials(
            auth="rainbow",  # type: ignore[arg-type]
            username=None,
            password=None,
            effective_user=None,
            key=None,
            no_input=True,
        )
    # Suppression justified: deliberately constructing a value the
    # ``Literal`` rejects so the runtime ``assert_never`` branch is
    # covered. Bracketed ``arg-type`` names what is silenced; mypy
    # still flags any *unintentional* misuse at real call sites.


def test_system_list_empty(tmp_path: Path) -> None:
    result = _system(tmp_path, "list")
    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"systems": []}


def test_system_list_reports_validity(tmp_path: Path) -> None:
    _add_password(tmp_path)
    bad = _system_file(tmp_path, "bad")
    bad.write_text("{broken")
    result = _system(tmp_path, "list")
    payload = json.loads(result.stdout)
    by_name = {s["name"]: s for s in payload["systems"]}
    assert by_name["prod"]["valid"] is True
    assert by_name["bad"]["valid"] is False


# ---------------------------------------------------------------------------
# shared authoring helpers
# ---------------------------------------------------------------------------


def _ctx_with_obj(obj: object) -> click.Context:
    ctx = MagicMock(spec=click.Context)
    ctx.obj = obj
    return ctx


def test_authoring_spec_returns_runtime_spec() -> None:
    spec = RuntimeSpec(no_input=True)
    assert _authoring_spec(_ctx_with_obj(spec)) is spec


def test_authoring_spec_raises_when_obj_not_runtime_spec() -> None:
    with pytest.raises(InternalError):
        _authoring_spec(_ctx_with_obj(object()))


def test_store_from_spec_uses_override(tmp_path: Path) -> None:
    spec = RuntimeSpec(config_dir_override=tmp_path)
    store = _store_from_spec(spec)
    assert isinstance(store, ConfigStore)
    assert store.config_dir == tmp_path


def test_store_from_spec_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DH_AI_DATA_DIR", "/data/root")
    assert _store_from_spec(RuntimeSpec()).config_dir == Path("/data/root/config")


def test_map_config_error_path_error() -> None:
    err = _map_config_error(ConfigurationPathError("bad path"))
    assert err.code is ErrorCode.CONFIG_PATH_INVALID
    assert err.message == "bad path"


def test_map_config_error_generic() -> None:
    err = _map_config_error(ConfigurationError("bad file"))
    assert err.code is ErrorCode.CONFIG_INVALID


def test_warn_template_resolution(capsys: pytest.CaptureFixture[str]) -> None:
    _warn_template_resolution(["env var 'X' is not set"])
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "warning: env var 'X' is not set" in captured.err


def test_warn_template_resolution_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _warn_template_resolution([])
    assert capsys.readouterr().err == ""


def test_warn_restart_hint(capsys: pytest.CaptureFixture[str]) -> None:
    _warn_restart_hint()
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "daemon stop" in captured.err


def test_resolve_entity_rejects_section_path() -> None:
    with pytest.raises(CliError) as exc:
        _resolve_entity(FieldPath(("community", "sessions")))
    assert exc.value.code is ErrorCode.CONFIG_PATH_INVALID
    assert "does not name a configuration file" in exc.value.message


def test_resolve_entity_hits_assert_never(monkeypatch: pytest.MonkeyPatch) -> None:
    """An out-of-band ``resolve_path`` result hits the ``assert_never`` net.

    Statically unreachable thanks to ``resolve_path``'s
    ``ConfigFieldLocation | ConfigSection`` return type; we monkeypatch it
    to return an off-type value to confirm the runtime safety net fires.
    """
    monkeypatch.setattr(config_mod, "resolve_path", lambda path: object())
    with pytest.raises(AssertionError):
        _resolve_entity(FieldPath(("cli",)))


def test_resolve_field_target_rejects_section_path() -> None:
    with pytest.raises(CliError) as exc:
        _resolve_field_target("community.sessions")
    assert exc.value.code is ErrorCode.CONFIG_PATH_INVALID
    assert "does not name a configuration file or field" in exc.value.message


def test_resolve_field_target_hits_assert_never(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An out-of-band ``resolve_path`` result hits the ``assert_never`` net."""
    monkeypatch.setattr(config_mod, "resolve_path", lambda path: object())
    with pytest.raises(AssertionError):
        _resolve_field_target("cli")


def test_entity_status_entries_reports_template_warnings(tmp_path: Path) -> None:
    sessions = tmp_path / "community" / "sessions"
    sessions.mkdir(parents=True)
    (sessions / "dev.json").write_text(
        json.dumps(
            {
                "auth": {
                    "credentials": {
                        "type": "psk",
                        "token": "${env:DOES_NOT_EXIST_XYZ}",
                    }
                }
            }
        )
    )
    entries = _entity_status_entries(
        ConfigStore(tmp_path), FieldPath(("community", "sessions"))
    )
    assert len(entries) == 1
    assert entries[0]["valid"] is True
    assert any("DOES_NOT_EXIST_XYZ" in w for w in entries[0]["warnings"])
