"""Tests for ``deephaven_mcp.cli._commands.session``."""

from __future__ import annotations

import json
import webbrowser
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _browser as browser_mod
from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._commands import session as session_mod
from deephaven_mcp.cli._context import ContextKey
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime

from .._helpers import fake_load_runtime, make_entry, make_runtime

_SID = "community:community:my-session"
_EID = "enterprise:prod:rpt"


def _invoke(
    args: list[str],
    runtime: Runtime,
    *,
    standalone_mode: bool = True,
    input: str | None = None,
):
    runner = CliRunner()
    with patch.object(runtime_mod, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args, standalone_mode=standalone_mode, input=input)


def _result(payload: dict) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload,
    )


def _patch(result: CallToolResult):
    return (
        patch.object(wrapping_mod, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(wrapping_mod, "call_tool", AsyncMock(return_value=result)),
    )


def _run(
    args: list[str],
    payload: dict,
    tmp_path: Path,
    *,
    standalone_mode: bool = True,
    input: str | None = None,
    runtime: Runtime | None = None,
):
    """Invoke ``args`` with the call_tool seam returning ``payload``."""
    rt = runtime or make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(payload))
    with acquire_p, call_p as call:
        result = _invoke(args, rt, standalone_mode=standalone_mode, input=input)
    return result, call


def _error_code(result) -> str:
    """Return the raised ``CliError``'s stable code.

    The structured JSON error payload is rendered in ``main()``
    (``standalone_mode=False``), which ``CliRunner`` does not exercise;
    invoke with ``standalone_mode=False`` so the ``CliError`` propagates
    into ``result.exception`` and assert its code here.
    """
    return result.exception.code.value


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_LIST = {
    "success": True,
    "sessions": [
        {"id": _SID, "type": "community", "system": "community"},
        {"id": _EID, "type": "enterprise", "system": "prod"},
    ],
}


def test_list_success_bare_array(tmp_path: Path) -> None:
    result, call = _run(["-o", "json", "session", "list"], _LIST, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.output) == _LIST["sessions"]
    assert call.await_args.args[2] == "sessions_list"
    assert call.await_args.args[3] == {}


_LIST_WITH_PARTIAL = {
    "success": True,
    "sessions": [{"id": _SID, "type": "community", "system": "community"}],
    "partial_result": {
        "phase": "completed",
        "detail": "Some enterprise systems had connection issues during discovery.",
        "errors": {"prod": "connection refused"},
    },
}


def test_list_partial_result_keeps_stdout_clean_under_json(tmp_path: Path) -> None:
    """`partial_result` is surfaced on stderr; -o json stdout stays the array."""
    result, _ = _run(["-o", "json", "session", "list"], _LIST_WITH_PARTIAL, tmp_path)
    assert result.exit_code == 0
    # stdout is the bare sessions array; the diagnostic is not mixed into it.
    assert json.loads(result.stdout) == _LIST_WITH_PARTIAL["sessions"]
    assert "connection issues" in result.stderr


def test_list_partial_result_warns_with_detail_on_stderr(tmp_path: Path) -> None:
    # -o human so the partial-result warning renders as readable text (the
    # default json renders it as a structured stderr object instead).
    result, _ = _run(["-o", "human", "session", "list"], _LIST_WITH_PARTIAL, tmp_path)
    assert result.exit_code == 0
    assert _SID in result.stdout
    assert result.stderr.startswith("warning: ")
    assert "prod: connection refused" in result.stderr


def test_list_no_partial_result_writes_no_stderr(tmp_path: Path) -> None:
    result, _ = _run(["session", "list"], _LIST, tmp_path)
    assert result.exit_code == 0
    assert result.stderr == ""


def test_list_no_systems_short_circuits_to_empty(tmp_path: Path) -> None:
    """On a zero-system tree ``session list`` returns an empty list (exit 0)
    with a stderr hint and never acquires the daemon."""
    rt = make_runtime(tmp_path, with_system=False)
    acquire_mock = AsyncMock(return_value=make_entry())
    with patch.object(wrapping_mod, "acquire", acquire_mock):
        result = _invoke(["-o", "json", "session", "list"], rt)
    assert result.exit_code == 0
    assert json.loads(result.stdout) == []
    assert "No systems configured" in result.stderr
    assert "dhcli config init" in result.stderr
    acquire_mock.assert_not_awaited()


def test_list_forwards_filters(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "list", "--type", "community", "--origin", "dynamic"],
        _LIST,
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"type": "community", "origin": "dynamic"}


def test_list_human_and_yaml(tmp_path: Path) -> None:
    result, _ = _run(["session", "list"], _LIST, tmp_path)
    assert result.exit_code == 0 and _SID in result.output
    result, _ = _run(["-o", "yaml", "session", "list"], _LIST, tmp_path)
    assert result.exit_code == 0 and "id:" in result.output


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

_SHOW = {"success": True, "session": {"id": _SID, "liveness_status": "ONLINE"}}


def test_show_emits_session_object(tmp_path: Path) -> None:
    result, call = _run(["-o", "json", "session", "show", _SID], _SHOW, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.output) == _SHOW["session"]
    assert call.await_args.args[2] == "session_details"
    assert call.await_args.args[3] == {"id": _SID}


def test_show_connect_flag(tmp_path: Path) -> None:
    result, call = _run(["session", "show", _SID, "--connect"], _SHOW, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID, "attempt_to_connect": True}


def test_show_falls_back_when_no_session_key(tmp_path: Path) -> None:
    result, _ = _run(
        ["-o", "json", "session", "show", _SID], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == {}


def test_show_not_found_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "show", _SID],
        {"success": False, "error": "no such session"},
        tmp_path,
    )
    assert result.exit_code == 3
    assert "no such session" in result.output


def test_show_falls_back_to_context_session(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SESSION, _SID)
    result, call = _run(["session", "show"], _SHOW, tmp_path, runtime=rt)
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID}


def test_show_no_id_and_no_context_fails(tmp_path: Path) -> None:
    result, call = _run(["session", "show"], _SHOW, tmp_path)
    assert result.exit_code == 2
    assert "no sticky context session is set" in result.output
    call.assert_not_awaited()


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

_CREATED = {"success": True, "id": _SID, "session_name": "dev"}


def test_create_community_minimal(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "create", "dev", "--launch-method", "python"], _CREATED, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_community_create"
    assert call.await_args.args[3] == {
        "session_name": "dev",
        "launch_method": "python",
    }


def test_create_normalizes_flag_casing_to_canonical(tmp_path: Path) -> None:
    """Mixed-case flag input is normalized by click.Choice before it hits the wire.

    The MCP tools take exact-case closed vocabularies (``"docker"`` /
    ``"python"``; ``"Python"`` / ``"Groovy"``); the CLI stays forgiving
    but must only ever send canonical values.
    """
    result, call = _run(
        [
            "session",
            "create",
            "dev",
            "--launch-method",
            "DOCKER",
            "--language",
            "groovy",
        ],
        _CREATED,
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "session_name": "dev",
        "launch_method": "docker",
        "programming_language": "Groovy",
    }


def test_create_community_shared_opts_and_env(tmp_path: Path) -> None:
    result, call = _run(
        [
            "session",
            "create",
            "dev",
            "--heap-size-gb",
            "2",
            "--jvm-arg",
            "-Xmx2g",
            "--env",
            "LOG=DEBUG",
            "--env",
            "TZ=UTC",
        ],
        _CREATED,
        tmp_path,
    )
    assert result.exit_code == 0
    args = call.await_args.args[3]
    assert args["session_name"] == "dev"
    assert args["heap_size_gb"] == 2.0
    assert args["extra_jvm_args"] == ["-Xmx2g"]
    assert args["environment_vars"] == {"LOG": "DEBUG", "TZ": "UTC"}


def test_create_language_choice_normalizes_case(tmp_path: Path) -> None:
    """--language is a case-insensitive Choice normalized to the tool's casing."""
    result, call = _run(
        ["session", "create", "rpt", "--system", "prod", "--language", "groovy"],
        _CREATED,
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3]["programming_language"] == "Groovy"


def test_create_invalid_language_exits_2(tmp_path: Path) -> None:
    """An out-of-set --language value is rejected by the Choice (exit 2)."""
    result, _ = _run(
        ["session", "create", "dev", "--language", "java"], _CREATED, tmp_path
    )
    assert result.exit_code == 2
    assert "java" in result.output


def test_create_enterprise_auto_name_and_session_arg(tmp_path: Path) -> None:
    result, call = _run(
        [
            "session",
            "create",
            "--system",
            "prod",
            "--engine",
            "DeephavenEnterprise",
            "--session-arg",
            "maxHeap=8",
        ],
        {"success": True, "id": _EID},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_enterprise_create"
    args = call.await_args.args[3]
    assert args["system"] == "prod"
    assert args["engine"] == "DeephavenEnterprise"
    assert args["session_arguments"] == {"maxHeap": 8}
    assert "session_name" not in args


def test_create_community_expands_tilde_in_local_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """~ expands in --python-venv-path and a volume's host half only."""
    monkeypatch.setenv("HOME", "/home/tester")
    result, call = _run(
        [
            "session",
            "create",
            "dev",
            "--launch-method",
            "docker",
            "--docker-volume",
            "~/data:/data:ro",
            "--docker-volume",
            "/abs:/abs",
        ],
        _CREATED,
        tmp_path,
    )
    assert result.exit_code == 0
    args = call.await_args.args[3]
    # Host half expanded; container path and mode verbatim.
    assert args["docker_volumes"] == ["/home/tester/data:/data:ro", "/abs:/abs"]

    result, call = _run(
        [
            "session",
            "create",
            "dev",
            "--launch-method",
            "python",
            "--python-venv-path",
            "~/venvs/dh",
        ],
        _CREATED,
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3]["python_venv_path"] == "/home/tester/venvs/dh"


def test_create_community_missing_name_errors(tmp_path: Path) -> None:
    result, _ = _run(["session", "create"], _CREATED, tmp_path, standalone_mode=False)
    assert _error_code(result) == "option_not_applicable"
    assert result.exception.exit_code == 2
    assert "SESSION_NAME is required" in str(result.exception)


def test_system_origin_unhandled_provenance_hits_assert_never() -> None:
    """An out-of-band provenance trips the runtime safety net.

    Statically unreachable: the ``match`` covers all four
    ``ContextProvenance`` members, so mypy proves exhaustiveness. We
    bypass type checking to confirm the runtime net is covered rather
    than a new member silently rendering as the default.
    """
    with pytest.raises(AssertionError):
        session_mod._system_origin("prod", "bogus")  # type: ignore[arg-type]
    # Suppression justified: deliberately passing a value the parameter's
    # type rejects so the runtime ``assert_never`` branch is covered.
    # Bracketed ``arg-type`` names what is silenced; mypy still flags any
    # unintentional misuse at real call sites.


def test_type_specific_create_params_are_declared_options() -> None:
    """Every name in the type-specific tuples is a real command parameter.

    '_create_flags' looks each misused name up among the command's own
    declared params, so a name that matches none of them would raise
    KeyError instead of the intended message.
    """
    declared = {param.name for param in session_mod.session_create.params}
    assert {
        *session_mod._COMMUNITY_ONLY_CREATE,
        *session_mod._ENTERPRISE_ONLY_CREATE,
    } <= declared


def test_create_community_with_enterprise_option_errors(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "create", "dev", "--server", "pool-a"],
        _CREATED,
        tmp_path,
        standalone_mode=False,
    )
    assert _error_code(result) == "option_not_applicable"
    assert result.exception.exit_code == 2
    # The flag as typed, not the tool parameter name.
    assert "--server" in str(result.exception)
    assert "Community" in str(result.exception)


def test_create_enterprise_with_community_option_errors(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "create", "rpt", "--system", "prod", "--docker-volume", "/a:/b"],
        _CREATED,
        tmp_path,
        standalone_mode=False,
    )
    assert _error_code(result) == "option_not_applicable"
    assert result.exception.exit_code == 2
    # '--docker-volume' is singular where its tool parameter is plural, so
    # this also pins that the message is not a dash-substituted param name.
    message = str(result.exception)
    assert "--docker-volume" in message
    assert "docker_volumes" not in message
    assert "Enterprise" in message


def test_create_names_sticky_context_as_the_system_source(tmp_path: Path) -> None:
    """When --system came from the context, the error says so.

    Naming '--system' alone would describe an argument the user never
    typed, leaving them unable to see why the Enterprise branch applied.
    """
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "prod")
    result, _ = _run(
        ["session", "create", "rpt", "--docker-volume", "/a:/b"],
        _CREATED,
        tmp_path,
        runtime=rt,
        standalone_mode=False,
    )
    assert _error_code(result) == "option_not_applicable"
    message = str(result.exception)
    assert "sticky context" in message
    assert "dhcli context show" in message


def test_create_bad_env_token_exits_2(tmp_path: Path) -> None:
    result, _ = _run(["session", "create", "dev", "--env", "noeq"], _CREATED, tmp_path)
    assert result.exit_code == 2


def test_create_tool_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "create", "dev"],
        {"success": False, "error": "quota exceeded"},
        tmp_path,
    )
    assert result.exit_code == 3
    assert "quota exceeded" in result.output


def test_create_community_sets_sticky_session(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        ["-o", "json", "session", "create", "dev", "--launch-method", "python"],
        _CREATED,
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    updated = rt.context_store.read()
    assert updated.session == _SID
    assert updated.system is None
    payload = json.loads(result.output)
    assert payload["context"] == {"session": _SID}


def test_create_enterprise_sets_sticky_session_system_and_pq(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        ["-o", "json", "session", "create", "rpt", "--system", "prod"],
        {"success": True, "id": _EID},
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    updated = rt.context_store.read()
    assert updated.session == _EID
    assert updated.system == "prod"
    assert updated.pq == _EID
    payload = json.loads(result.output)
    assert payload["context"] == {"session": _EID, "system": "prod", "pq": _EID}


def test_create_no_set_context_skips_update(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        [
            "-o",
            "json",
            "session",
            "create",
            "dev",
            "--launch-method",
            "python",
            "--no-set-context",
        ],
        _CREATED,
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    assert rt.context_store.read().session is None
    payload = json.loads(result.output)
    assert "context" not in payload


def test_create_falls_back_to_context_system(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "prod")
    result, call = _run(
        ["session", "create", "rpt"],
        {"success": True, "id": _EID},
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_enterprise_create"
    assert call.await_args.args[3]["system"] == "prod"


def test_create_defaults_to_community_without_context(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "create", "dev", "--launch-method", "python"], _CREATED, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_community_create"


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_routes_community(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "delete", _SID], {"success": True, "id": _SID}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_community_delete"
    assert call.await_args.args[3] == {"id": _SID}


def test_delete_routes_enterprise(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "delete", _EID], {"success": True, "id": _EID}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_enterprise_delete"


def test_delete_tool_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "delete", _SID],
        {"success": False, "error": "not dynamic"},
        tmp_path,
    )
    assert result.exit_code == 3


def test_delete_clears_matching_sticky_context(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set_many({ContextKey.SESSION: _SID, ContextKey.PQ: _SID})
    result, _ = _run(
        ["session", "delete", _SID], {"success": True, "id": _SID}, tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    updated = rt.context_store.read()
    assert updated.session is None
    assert updated.pq is None


def test_delete_leaves_unrelated_context_untouched(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SESSION, _EID)
    result, _ = _run(
        ["session", "delete", _SID], {"success": True, "id": _SID}, tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    assert rt.context_store.read().session == _EID


def test_delete_falls_back_to_context_session(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SESSION, _SID)
    result, call = _run(
        ["session", "delete"], {"success": True, "id": _SID}, tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID}


def test_delete_no_id_and_no_context_fails(tmp_path: Path) -> None:
    result, call = _run(["session", "delete"], {"success": True}, tmp_path)
    assert result.exit_code == 2
    assert "no sticky context session is set" in result.output
    call.assert_not_awaited()


# ---------------------------------------------------------------------------
# exec
# ---------------------------------------------------------------------------


def test_exec_inline_script(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "exec", _SID, "--script", "print(1)"],
        {"success": True, "id": _SID},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_script_run"
    assert call.await_args.args[3] == {"id": _SID, "script": "print(1)"}


def test_exec_script_path_reads_file_client_side(tmp_path: Path) -> None:
    script_file = tmp_path / "j.py"
    script_file.write_text("print('from file')\n")
    result, call = _run(
        ["session", "exec", _SID, "--script-path", str(script_file)],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID, "script": "print('from file')\n"}


def test_exec_script_path_unreadable_exits_2(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.py"
    result, call = _run(
        ["session", "exec", _SID, "--script-path", str(missing)],
        {"success": True},
        tmp_path,
        standalone_mode=False,
    )
    assert _error_code(result) == "file_read_failed"
    assert result.exception.exit_code == 2
    assert "Could not read script file" in str(result.exception)
    call.assert_not_awaited()


def test_exec_stdin(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "exec", _SID, "--script-path", "-"],
        {"success": True},
        tmp_path,
        input="print('from stdin')\n",
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": _SID, "script": "print('from stdin')\n"}


def test_exec_stdin_empty_exits_2(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "exec", _SID, "--script-path", "-"],
        {"success": True},
        tmp_path,
        standalone_mode=False,
        input="",
    )
    assert _error_code(result) == "missing_argument"
    assert result.exception.exit_code == 2
    assert "Standard input was empty" in str(result.exception)
    call.assert_not_awaited()


def test_exec_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "exec", _SID, "--script", "boom()"],
        {"success": False, "error": "script blew up"},
        tmp_path,
    )
    assert result.exit_code == 3
    assert "script blew up" in result.output


def test_exec_requires_a_source(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "exec", _SID], {"success": True}, tmp_path, standalone_mode=False
    )
    assert _error_code(result) == "missing_argument"
    assert result.exception.exit_code == 2
    assert "Provide a script source" in str(result.exception)
    call.assert_not_awaited()


def test_exec_rejects_both_sources(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "exec", _SID, "--script", "print(1)", "--script-path", "/tmp/j.py"],
        {"success": True},
        tmp_path,
        standalone_mode=False,
    )
    assert _error_code(result) == "mutually_exclusive_options"
    assert result.exception.exit_code == 2
    assert "cannot be combined" in str(result.exception)
    call.assert_not_awaited()


def test_exec_checks_the_script_source_before_resolving_the_target(
    tmp_path: Path,
) -> None:
    """A missing source is rejected before the target is resolved.

    'require_context_target' can prompt to confirm a sticky-context
    session, so validating the source afterwards would make the user
    decide on a run that is then refused anyway.
    """
    with patch.object(session_mod, "require_context_target") as target:
        result, call = _run(
            ["session", "exec"], {"success": True}, tmp_path, standalone_mode=False
        )
    assert _error_code(result) == "missing_argument"
    target.assert_not_called()
    call.assert_not_awaited()


def test_exec_rejects_stdin_with_inline(tmp_path: Path) -> None:
    result, call = _run(
        ["session", "exec", _SID, "--script", "print(1)", "--script-path", "-"],
        {"success": True},
        tmp_path,
        standalone_mode=False,
        input="print(2)\n",
    )
    assert _error_code(result) == "mutually_exclusive_options"
    call.assert_not_awaited()


# ---------------------------------------------------------------------------
# pip-list
# ---------------------------------------------------------------------------


def test_pip_list_emits_array(tmp_path: Path) -> None:
    payload = {"success": True, "packages": [{"package": "numpy", "version": "1.25"}]}
    result, call = _run(["-o", "json", "session", "pip-list", _SID], payload, tmp_path)
    assert result.exit_code == 0
    assert json.loads(result.output) == payload["packages"]
    assert call.await_args.args[2] == "session_pip_list"
    assert call.await_args.args[3] == {"id": _SID}


def test_pip_list_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "pip-list", _SID],
        {"success": False, "error": "no such session"},
        tmp_path,
    )
    assert result.exit_code == 3
    assert "no such session" in result.output


# ---------------------------------------------------------------------------
# credentials / url / open
# ---------------------------------------------------------------------------

_CREDS = {
    "success": True,
    "id": _SID,
    "auth_type": "PSK",
    "auth_token": "tok-123",
    "connection_url": "http://localhost:45123",
    "connection_url_with_auth": "http://localhost:45123/?psk=tok-123",
}


def test_credentials_success_strips_success(tmp_path: Path) -> None:
    result, call = _run(
        ["-o", "json", "session", "credentials", _SID], _CREDS, tmp_path
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {k: _CREDS[k] for k in _CREDS if k != "success"}
    assert call.await_args.args[2] == "session_community_credentials"


def test_credentials_gate_disabled_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "credentials", _SID],
        {"success": False, "error": "disabled (mode='none')"},
        tmp_path,
    )
    assert result.exit_code == 3
    assert "disabled" in result.output


def test_url_prints_authed_url(tmp_path: Path) -> None:
    result, _ = _run(["session", "url", _SID], _CREDS, tmp_path)
    assert result.exit_code == 0
    assert result.output.strip() == _CREDS["connection_url_with_auth"]


def test_url_falls_back_to_connection_url(tmp_path: Path) -> None:
    creds = {"success": True, "connection_url": "http://h:1", "auth_type": "Anonymous"}
    result, _ = _run(["session", "url", _SID], creds, tmp_path)
    assert result.exit_code == 0
    assert result.output.strip() == "http://h:1"


def test_open_launches_browser(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=True) as wb,
    ):
        result = _invoke(["-o", "json", "session", "open", _SID], rt)
    assert result.exit_code == 0
    # The browser gets the authenticated URL...
    wb.assert_called_once_with(_CREDS["connection_url_with_auth"])
    payload = json.loads(result.output)
    # ...but stdout does not: it already holds the credential, so echoing
    # the token would only spread it to logs and shell history.
    assert payload == {"opened": _CREDS["connection_url"], "launched": True}
    assert _CREDS["auth_token"] not in result.output


def test_open_print_only_does_not_launch(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=True) as wb,
    ):
        result = _invoke(["-o", "json", "session", "open", _SID, "--print"], rt)
    assert result.exit_code == 0
    wb.assert_not_called()
    # --print controls launching only: it is not a disclosure opt-in, so the
    # token stays out of the output until --reveal-secrets asks for it.
    payload = json.loads(result.output)
    assert payload == {"opened": _CREDS["connection_url"], "launched": False}
    assert _CREDS["auth_token"] not in result.output


def test_open_reveal_secrets_includes_the_token(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=True) as wb,
    ):
        result = _invoke(
            ["-o", "json", "session", "open", _SID, "--print", "--reveal-secrets"], rt
        )
    assert result.exit_code == 0
    wb.assert_not_called()
    payload = json.loads(result.output)
    assert payload == {
        "opened": _CREDS["connection_url_with_auth"],
        "launched": False,
    }


def test_open_reveal_secrets_is_independent_of_print(tmp_path: Path) -> None:
    """--reveal-secrets discloses the token even when a browser was launched."""
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=True) as wb,
    ):
        result = _invoke(
            ["-o", "json", "session", "open", _SID, "--reveal-secrets"], rt
        )
    assert result.exit_code == 0
    wb.assert_called_once_with(_CREDS["connection_url_with_auth"])
    payload = json.loads(result.output)
    assert payload == {
        "opened": _CREDS["connection_url_with_auth"],
        "launched": True,
    }


def test_open_no_browser_found_exits_2(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=False) as wb,
    ):
        result = _invoke(["session", "open", _SID], rt, standalone_mode=False)
    assert _error_code(result) == "browser_launch_failed"
    assert result.exception.exit_code == 2
    assert "manually" in str(result.exception)
    # The browser still got a URL that can actually log in...
    wb.assert_called_once_with(_CREDS["connection_url_with_auth"])
    # ...but the failure message did not: an error path must not walk
    # around --reveal-secrets and put the credential on stderr.
    message = str(result.exception)
    assert _CREDS["auth_token"] not in message
    assert _CREDS["connection_url"] in message
    assert "dhcli session url" in message


def test_open_browser_error_exits_2(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(
            browser_mod.webbrowser, "open", side_effect=webbrowser.Error("nope")
        ),
    ):
        result = _invoke(["session", "open", _SID], rt, standalone_mode=False)
    assert _error_code(result) == "browser_launch_failed"
    assert result.exception.exit_code == 2
    assert "Could not launch" in str(result.exception)
    assert _CREDS["auth_token"] not in str(result.exception)


def test_open_browser_failure_on_anonymous_session_omits_the_hint(
    tmp_path: Path,
) -> None:
    """An anonymous session has no token, so its authenticated URL *is*
    its base URL. Nothing was withheld, and 'session url' would return
    the very same string -- promising a better one would be a lie."""
    creds = {
        "success": True,
        "id": _SID,
        "auth_type": "ANONYMOUS",
        "auth_token": "",
        "connection_url": "http://h:1",
        "connection_url_with_auth": "http://h:1",
    }
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(creds))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=False),
    ):
        result = _invoke(["session", "open", _SID], rt, standalone_mode=False)
    assert _error_code(result) == "browser_launch_failed"
    message = str(result.exception)
    assert "http://h:1" in message
    assert "omits the auth token" not in message
    assert "dhcli session url" not in message


def test_open_browser_failure_message_ends_with_the_url(tmp_path: Path) -> None:
    """Trailing prose after a URL gets swallowed by terminal and chat
    autolinkers, so the URL must be the last thing in the message."""
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=False),
    ):
        result = _invoke(["session", "open", _SID], rt, standalone_mode=False)
    assert str(result.exception).endswith(_CREDS["connection_url"])


def test_open_browser_failure_reveals_the_token_when_asked(tmp_path: Path) -> None:
    """--reveal-secrets governs the failure path too, so the operator who
    opted in gets a URL they can paste straight into a browser."""
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=False),
    ):
        result = _invoke(
            ["session", "open", _SID, "--reveal-secrets"], rt, standalone_mode=False
        )
    assert _error_code(result) == "browser_launch_failed"
    message = str(result.exception)
    assert _CREDS["connection_url_with_auth"] in message
    # The redirect to 'session url' would be noise: they already have it.
    assert "dhcli session url" not in message


def test_open_no_url_exits_2(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "open", _SID], {"success": True, "auth_type": "PSK"}, tmp_path
    )
    assert result.exit_code == 2
    assert "no connection URL" in result.output


def test_url_no_url_exits_2(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "url", _SID], {"success": True, "auth_type": "PSK"}, tmp_path
    )
    assert result.exit_code == 2
    assert "no connection URL" in result.output


# ---------------------------------------------------------------------------
# argument validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("verb", ["show", "delete", "credentials", "url", "open"])
def test_id_verbs_require_id(verb: str, tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result = _invoke(["session", verb], rt)
    assert result.exit_code == 2
