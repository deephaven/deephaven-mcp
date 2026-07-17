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
from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._commands import session as session_mod
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
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
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
):
    """Invoke ``args`` with the call_tool seam returning ``payload``."""
    rt = make_runtime(tmp_path)
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


def test_create_community_with_enterprise_option_errors(tmp_path: Path) -> None:
    result, _ = _run(
        ["session", "create", "dev", "--server", "pool-a"],
        _CREATED,
        tmp_path,
        standalone_mode=False,
    )
    assert _error_code(result) == "option_not_applicable"
    assert result.exception.exit_code == 2
    assert "server" in str(result.exception)
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
    assert "docker_volumes" in str(result.exception)
    assert "Enterprise" in str(result.exception)


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
    wb.assert_called_once_with(_CREDS["connection_url_with_auth"])
    payload = json.loads(result.output)
    assert payload == {"opened": _CREDS["connection_url_with_auth"], "launched": True}


def test_open_print_only_does_not_launch(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=True) as wb,
    ):
        result = _invoke(["session", "open", _SID, "--print"], rt)
    assert result.exit_code == 0
    wb.assert_not_called()


def test_open_no_browser_found_exits_2(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    acquire_p, call_p = _patch(_result(_CREDS))
    with (
        acquire_p,
        call_p,
        patch.object(browser_mod.webbrowser, "open", return_value=False),
    ):
        result = _invoke(["session", "open", _SID], rt, standalone_mode=False)
    assert _error_code(result) == "browser_launch_failed"
    assert result.exception.exit_code == 2
    assert "manually" in str(result.exception)


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
