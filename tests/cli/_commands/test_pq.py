"""Tests for ``deephaven_mcp.cli._commands.pq``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._commands.pq import _create_modify_args
from deephaven_mcp.cli._context import ContextKey
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime

from .._helpers import fake_load_runtime, make_entry, make_runtime


def _run(
    args: list[str],
    payload: dict,
    tmp_path: Path,
    input: str | None = None,
    runtime: Runtime | None = None,
):
    rt = runtime or make_runtime(tmp_path)
    result = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload,
    )
    with (
        patch.object(wrapping_mod, "acquire", AsyncMock(return_value=make_entry())),
        patch.object(wrapping_mod, "call_tool", AsyncMock(return_value=result)) as call,
        patch.object(runtime_mod, "load_runtime", fake_load_runtime(rt)),
    ):
        return CliRunner().invoke(cli, args, input=input), call


_PQS = [
    {"id": "enterprise:prod:12345", "serial": 12345, "name": "analytics"},
    {"id": "enterprise:prod:67890", "serial": 67890, "name": "reporting"},
]


def test_list(tmp_path: Path) -> None:
    result, call = _run(
        ["-o", "json", "pq", "list", "prod"],
        {"success": True, "system": "prod", "pqs": _PQS},
        tmp_path,
    )
    assert result.exit_code == 0
    # stdout is the bare pqs array, not the tool envelope.
    assert json.loads(result.output) == _PQS
    assert call.await_args.args[2] == "pq_list"
    assert call.await_args.args[3] == {"system": "prod"}


def test_list_empty(tmp_path: Path) -> None:
    result, _ = _run(
        ["-o", "json", "pq", "list", "prod"], {"success": True, "pqs": []}, tmp_path
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == []


def test_details(tmp_path: Path) -> None:
    result, call = _run(["pq", "details", "123"], {"success": True}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_details"
    assert call.await_args.args[3] == {"id": "123"}


def test_name_to_id(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "name-to-id", "prod", "nightly"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_name_to_id"
    assert call.await_args.args[3] == {"system": "prod", "pq_name": "nightly"}


def test_create_builds_args(tmp_path: Path) -> None:
    result, call = _run(
        [
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--git-script-path",
            "IrisQueries/py/n.py",
            "--jvm-arg",
            "-Xmx2g",
            "--schedule",
            "daily",
        ],
        {"success": True, "id": "999"},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_create"
    args = call.await_args.args[3]
    assert args["pq_name"] == "nightly"
    assert args["system"] == "prod"
    assert args["heap_size_gb"] == 4.0
    assert args["enabled"] is True
    assert args["script_path"] == "IrisQueries/py/n.py"
    assert args["extra_jvm_args"] == ["-Xmx2g"]
    assert args["schedule"] == ["daily"]
    # Unset options are omitted so the controller defaults apply.
    assert "server" not in args


def test_create_script_body_path_reads_file_client_side(tmp_path: Path) -> None:
    script_file = tmp_path / "n.py"
    script_file.write_text("print('from file')\n")
    result, call = _run(
        [
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--script-body-path",
            str(script_file),
        ],
        {"success": True, "id": "999"},
        tmp_path,
    )
    assert result.exit_code == 0
    args = call.await_args.args[3]
    # The local file is materialized into script_body; the client-only
    # script_body_path key is never forwarded.
    assert args["script_body"] == "print('from file')\n"
    assert "script_body_path" not in args
    assert "script_path" not in args


def test_modify_script_body_path_stdin(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "modify", "123", "--script-body-path", "-"],
        {"success": True},
        tmp_path,
        input="print('from stdin')\n",
    )
    assert result.exit_code == 0
    args = call.await_args.args[3]
    assert args["script_body"] == "print('from stdin')\n"
    assert "script_body_path" not in args


def test_create_script_body_path_unreadable_exits_2(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.py"
    result, call = _run(
        [
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--script-body-path",
            str(missing),
        ],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 2
    assert "Could not read script file" in result.output
    call.assert_not_awaited()


def test_modify_only_passes_given_fields(tmp_path: Path) -> None:
    result, call = _run(
        [
            "pq",
            "modify",
            "123",
            "--disabled",
            "--pq-name",
            "new",
            "--heap-size-gb",
            "8",
        ],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_modify"
    args = call.await_args.args[3]
    assert args["id"] == "123"
    assert args["enabled"] is False
    assert args["pq_name"] == "new"
    assert args["heap_size_gb"] == 8.0
    assert "server" not in args


def test_modify_restart_flag(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "modify", "123", "--restart"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[3]["restart"] is True


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--script-body", "print(1)", "--git-script-path", "IrisQueries/py/n.py"],
        ["--script-body", "print(1)", "--script-body-path", "/tmp/n.py"],
        ["--script-body-path", "/tmp/n.py", "--git-script-path", "IrisQueries/py/n.py"],
        ["--auto-delete-timeout", "60", "--schedule", "daily"],
    ],
)
def test_create_rejects_mutually_exclusive_options(
    extra_args: list[str], tmp_path: Path
) -> None:
    result, call = _run(
        ["pq", "create", "n", "--system", "prod", "--heap-size-gb", "4", *extra_args],
        {"success": True},
        tmp_path,
    )
    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    call.assert_not_awaited()


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--script-body", "print(1)", "--git-script-path", "IrisQueries/py/n.py"],
        ["--script-body", "print(1)", "--script-body-path", "/tmp/n.py"],
        ["--script-body-path", "/tmp/n.py", "--git-script-path", "IrisQueries/py/n.py"],
        ["--auto-delete-timeout", "60", "--schedule", "daily"],
    ],
)
def test_modify_rejects_mutually_exclusive_options(
    extra_args: list[str], tmp_path: Path
) -> None:
    result, call = _run(
        ["pq", "modify", "123", *extra_args], {"success": True}, tmp_path
    )
    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    call.assert_not_awaited()


def test_delete_multiple_with_max_concurrent(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "delete", "1", "2", "--max-concurrent", "3"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_delete"
    assert call.await_args.args[3] == {"id": ["1", "2"], "max_concurrent": 3}


def test_delete_requires_an_id(tmp_path: Path) -> None:
    result, _ = _run(["pq", "delete"], {"success": True}, tmp_path)
    assert result.exit_code == 2
    assert "no sticky context pq is set" in result.output


@pytest.mark.parametrize(
    "verb,tool", [("start", "pq_start"), ("stop", "pq_stop"), ("restart", "pq_restart")]
)
def test_lifecycle_no_wait(verb: str, tool: str, tmp_path: Path) -> None:
    result, call = _run(["pq", verb, "1", "--no-wait"], {"success": True}, tmp_path)
    assert result.exit_code == 0
    assert call.await_args.args[2] == tool
    assert call.await_args.args[3] == {"id": ["1"], "wait": False}


def test_lifecycle_default_wait_and_max_concurrent(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "start", "1", "2", "--max-concurrent", "2"], {"success": True}, tmp_path
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {
        "id": ["1", "2"],
        "wait": True,
        "max_concurrent": 2,
    }


def test_lifecycle_requires_an_id(tmp_path: Path) -> None:
    result, _ = _run(["pq", "start"], {"success": True}, tmp_path)
    assert result.exit_code == 2


def test_list_failure_exits_3(tmp_path: Path) -> None:
    result, _ = _run(
        ["pq", "list", "prod"], {"success": False, "error": "boom"}, tmp_path
    )
    assert result.exit_code == 3


# ---------------------------------------------------------------------------
# sticky context: fallback and auto-set/clear side effects
# ---------------------------------------------------------------------------


def test_list_falls_back_to_context_system(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "prod")
    result, call = _run(
        ["-o", "json", "pq", "list"],
        {"success": True, "pqs": []},
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"system": "prod"}


def test_details_falls_back_to_context_pq(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.PQ, "enterprise:prod:123")
    result, call = _run(["pq", "details"], {"success": True}, tmp_path, runtime=rt)
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": "enterprise:prod:123"}


def test_create_falls_back_to_context_system(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "prod")
    result, call = _run(
        ["pq", "create", "nightly", "--heap-size-gb", "4"],
        {"success": True, "id": "999"},
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    assert call.await_args.args[3]["system"] == "prod"


def test_create_no_system_and_no_context_fails(tmp_path: Path) -> None:
    result, call = _run(
        ["pq", "create", "nightly", "--heap-size-gb", "4"],
        {"success": True, "id": "999"},
        tmp_path,
    )
    assert result.exit_code == 2
    assert "no sticky context system is set" in result.output
    call.assert_not_awaited()


def test_create_sets_sticky_context_on_success(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        [
            "-o",
            "json",
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
        ],
        {"success": True, "id": "enterprise:prod:999"},
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    updated = rt.context_store.read()
    assert updated.pq == "enterprise:prod:999"
    assert updated.system == "prod"
    payload = json.loads(result.output)
    assert payload["context"] == {"pq": "enterprise:prod:999", "system": "prod"}


def test_create_no_set_context_skips_update(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        [
            "-o",
            "json",
            "pq",
            "create",
            "nightly",
            "--system",
            "prod",
            "--heap-size-gb",
            "4",
            "--no-set-context",
        ],
        {"success": True, "id": "enterprise:prod:999"},
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    assert rt.context_store.read() == rt.context_store.read().__class__()
    payload = json.loads(result.output)
    assert "context" not in payload


# ---------------------------------------------------------------------------
# _create_modify_args (a pure function: no click context required)
# ---------------------------------------------------------------------------


def test_create_modify_args_drops_unset_and_normalizes_tuples() -> None:
    """Unset options are omitted so the tool's own defaults apply."""
    args = _create_modify_args(
        {
            "pq_name": "nightly",
            "heap_size_gb": None,
            "schedule": (),
            "jvm_args": ("-Xmx1g", "-Xms1g"),
        }
    )
    assert args == {"pq_name": "nightly", "jvm_args": ["-Xmx1g", "-Xms1g"]}


def test_create_modify_args_consumes_script_body_path(tmp_path: Path) -> None:
    """The local file is read into ``script_body`` and the path removed.

    ``script_body_path`` is the only client-only field that can reach
    this function, and it is consumed rather than forwarded.
    """
    script = tmp_path / "n.py"
    script.write_text("print(1)\n")
    args = _create_modify_args({"script_body_path": str(script)})
    assert args == {"script_body": "print(1)\n"}


@pytest.mark.parametrize(
    "argv,forbidden",
    [
        (
            ["pq", "create", "nightly", "--system", "prod", "--heap-size-gb", "4"],
            "no_set_context",
        ),
        (["pq", "modify", "123", "--pq-name", "renamed"], "yes"),
    ],
    ids=["create/no_set_context", "modify/yes"],
)
def test_client_only_flags_never_reach_the_tool(
    argv: list[str], forbidden: str, tmp_path: Path
) -> None:
    """A client-only flag is absent from the tool arguments.

    These two are guaranteed structurally: they are named parameters of
    the callback, so click never routes them into ``**options`` and they
    cannot enter the argument dict. That structure replaced a runtime
    filter keyed on the ``client_only_params`` declaration, so pin the
    outcome here -- renaming a param back into ``**options`` would
    otherwise silently forward it.
    """
    result, call = _run(argv, {"success": True, "id": "999"}, tmp_path)
    assert result.exit_code == 0
    assert forbidden not in call.await_args.args[3]


def _delete_payload(*results: tuple[str, bool]) -> dict:
    """Build a ``pq_delete`` best-effort payload with per-item outcomes.

    ``pq_delete`` always reports ``success: True`` when the batch *ran*;
    whether each id was deleted lives in ``results``.
    """
    return {
        "success": True,
        "results": [
            {"id": id, "serial": 1, "success": ok, "name": None, "error": None}
            for id, ok in results
        ],
    }


def test_delete_clears_matching_sticky_context(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set_many(
        {ContextKey.PQ: "1", ContextKey.SESSION: "1", ContextKey.SYSTEM: "prod"}
    )
    result, _ = _run(
        ["pq", "delete", "1"], _delete_payload(("1", True)), tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    updated = rt.context_store.read()
    assert updated.pq is None
    assert updated.session is None
    # An unrelated key is left untouched.
    assert updated.system == "prod"


def test_delete_leaves_unrelated_context_untouched(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.PQ, "2")
    result, _ = _run(
        ["pq", "delete", "1"], _delete_payload(("1", True)), tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    assert rt.context_store.read().pq == "2"


def test_delete_keeps_context_for_id_whose_delete_failed(tmp_path: Path) -> None:
    """A failed delete must not clear the context pointing at that PQ.

    ``pq delete`` is best-effort and exits 0 when the batch ran, so
    clearing every *requested* id would discard a pointer to a PQ that
    still exists.
    """
    rt = make_runtime(tmp_path)
    rt.context_store.set_many({ContextKey.PQ: "2", ContextKey.SESSION: "2"})
    result, _ = _run(
        ["pq", "delete", "1", "2"],
        _delete_payload(("1", True), ("2", False)),
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    updated = rt.context_store.read()
    assert updated.pq == "2"
    assert updated.session == "2"


def test_delete_clears_nothing_when_results_missing(tmp_path: Path) -> None:
    """An unrecognized payload shape clears nothing rather than guessing."""
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.PQ, "1")
    result, _ = _run(["pq", "delete", "1"], {"success": True}, tmp_path, runtime=rt)
    assert result.exit_code == 0
    assert rt.context_store.read().pq == "1"


def test_lifecycle_falls_back_to_context_pq(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.PQ, "1")
    result, call = _run(
        ["pq", "start", "--no-wait"], {"success": True}, tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    assert call.await_args.args[3] == {"id": ["1"], "wait": False}
