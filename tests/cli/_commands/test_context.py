"""Tests for ``deephaven_mcp.cli._commands.context``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import click
import pytest
from click.testing import CliRunner
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._commands import _wrapping as wrapping_mod
from deephaven_mcp.cli._commands import context as context_mod
from deephaven_mcp.cli._commands.context import context_set
from deephaven_mcp.cli._context import ContextKey
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.schema import CliConfig, ServerConfig
from deephaven_mcp.config.schema._enterprise import EnterpriseConfig, EnterpriseSettings
from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.sessions._enterprise import EnterpriseSystemConfig

from .._helpers import fake_load_runtime, make_entry, make_runtime


def _enterprise_runtime(tmp_path: Path) -> Runtime:
    system = EnterpriseSystemConfig.model_validate(
        {
            "name": "prod",
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com:8123/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "shh",
                }
            },
        }
    )
    config = ConfigTree(
        config_dir=tmp_path / "cfg",
        cli=CliConfig(),
        server=ServerConfig(),
        enterprise=EnterpriseConfig(
            settings=EnterpriseSettings(), systems={"prod": system}
        ),
    )
    return make_runtime(tmp_path, config=config)


def _run(
    args: list[str],
    tmp_path: Path,
    *,
    payload: dict | None = None,
    runtime: Runtime | None = None,
):
    rt = runtime or make_runtime(tmp_path)
    call = None
    patches = [patch.object(runtime_mod, "load_runtime", fake_load_runtime(rt))]
    if payload is not None:
        result = CallToolResult(
            content=[TextContent(type="text", text=json.dumps(payload))],
            structuredContent=payload,
        )
        patches.append(
            patch.object(wrapping_mod, "acquire", AsyncMock(return_value=make_entry()))
        )
        call = AsyncMock(return_value=result)
        patches.append(patch.object(wrapping_mod, "call_tool", call))
    for p in patches:
        p.start()
    try:
        return CliRunner().invoke(cli, args), call
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


def test_show_all_unset(tmp_path: Path) -> None:
    result, _ = _run(["-o", "json", "context", "show"], tmp_path)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {
        "session": {"value": None, "source": "unset"},
        "system": {"value": None, "source": "unset"},
        "pq": {"value": None, "source": "unset"},
    }


def test_show_human_mode_renders_unset_without_python_none(tmp_path: Path) -> None:
    """An unset key reads as (none) on a terminal, not as Python's None.

    ``show`` always reports all three keys, so it is the command most
    likely to render a null; the JSON mode keeps a real ``null``.
    """
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "prod")
    result, _ = _run(["-o", "human", "context", "show"], tmp_path, runtime=rt)
    assert result.exit_code == 0
    assert "None" not in result.output
    assert "value: (none)" in result.output
    assert "value: prod" in result.output


def test_show_reflects_file_value(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "community")
    result, _ = _run(["-o", "json", "context", "show"], tmp_path, runtime=rt)
    payload = json.loads(result.output)
    assert payload["system"] == {"value": "community", "source": "file"}


def test_show_reports_stored_value_as_disabled_when_fallback_off(
    tmp_path: Path,
) -> None:
    """``--no-context`` still reveals what is stored, flagged 'disabled'.

    The stored value must remain visible: reporting 'unset' here would
    make ``context set``/``unset`` unverifiable under ``--no-context``,
    and would conflate "the fallback is off" with "nothing is stored".
    """
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "community")
    result, _ = _run(
        ["--no-context", "-o", "json", "context", "show"], tmp_path, runtime=rt
    )
    payload = json.loads(result.output)
    assert payload["system"] == {"value": "community", "source": "disabled"}


def test_show_reports_disabled_even_when_nothing_stored(tmp_path: Path) -> None:
    """'disabled' is reported per key regardless of whether one holds a value.

    Falling back to 'unset' for an empty key would leave an agent unable
    to tell "run context set" from "re-enable the fallback".
    """
    result, _ = _run(["--no-context", "-o", "json", "context", "show"], tmp_path)
    payload = json.loads(result.output)
    assert payload == {
        "session": {"value": None, "source": "disabled"},
        "system": {"value": None, "source": "disabled"},
        "pq": {"value": None, "source": "disabled"},
    }


def test_set_while_fallback_disabled_warns_and_reports_stored_value(
    tmp_path: Path,
) -> None:
    """A write under ``--no-context`` succeeds, warns, and echoes the value."""
    result, _ = _run(
        ["--no-context", "-o", "json", "context", "set", "system", "community"],
        tmp_path,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["system"] == {"value": "community", "source": "disabled"}
    assert "disabled" in result.stderr


def test_unset_while_fallback_disabled_warns(tmp_path: Path) -> None:
    """The same warning covers ``unset``, whose effect is equally unverifiable."""
    rt = make_runtime(tmp_path)
    rt.context_store.set(ContextKey.SYSTEM, "community")
    result, _ = _run(
        ["--no-context", "-o", "json", "context", "unset", "system"],
        tmp_path,
        runtime=rt,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["system"] == {"value": None, "source": "disabled"}
    assert "disabled" in result.stderr


# ---------------------------------------------------------------------------
# set: system (config-only, no daemon)
# ---------------------------------------------------------------------------


def test_set_system_community_succeeds(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        ["-o", "json", "context", "set", "system", "community"], tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    assert rt.context_store.read().system == "community"


def test_set_system_configured_enterprise_succeeds(tmp_path: Path) -> None:
    rt = _enterprise_runtime(tmp_path)
    result, _ = _run(
        ["-o", "json", "context", "set", "system", "prod"], tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    assert rt.context_store.read().system == "prod"


def test_set_system_unknown_fails(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        ["-o", "json", "context", "set", "system", "bogus"], tmp_path, runtime=rt
    )
    assert result.exit_code == 2
    assert "No system named 'bogus' is configured" in result.output
    assert rt.context_store.read().system is None


# ---------------------------------------------------------------------------
# set: session / pq (daemon-validated)
# ---------------------------------------------------------------------------


def test_set_session_validates_via_daemon(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, call = _run(
        ["-o", "json", "context", "set", "session", "community:community:dev"],
        tmp_path,
        payload={"success": True, "session": {"id": "community:community:dev"}},
        runtime=rt,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "session_details"
    assert call.await_args.args[3] == {"id": "community:community:dev"}
    assert rt.context_store.read().session == "community:community:dev"


def test_set_session_not_found_does_not_write_context(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, _ = _run(
        ["-o", "json", "context", "set", "session", "community:community:nope"],
        tmp_path,
        payload={"success": False, "error": "not found", "isError": True},
        runtime=rt,
    )
    assert result.exit_code == 3
    assert "not found" in result.output
    assert rt.context_store.read().session is None


def test_set_pq_validates_via_daemon(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    result, call = _run(
        ["-o", "json", "context", "set", "pq", "enterprise:prod:123"],
        tmp_path,
        payload={"success": True, "id": "enterprise:prod:123"},
        runtime=rt,
    )
    assert result.exit_code == 0
    assert call.await_args.args[2] == "pq_details"
    assert rt.context_store.read().pq == "enterprise:prod:123"


class _UnhandledKey:
    """Stand-in for ``ContextKey`` whose instances match no ``case`` pattern.

    Carries the three real members so the value patterns still resolve,
    but :meth:`from_value` -- the lookup the command calls -- returns an
    instance equal to none of them, so the subject falls through to the
    ``assert_never`` net.
    """

    SESSION = ContextKey.SESSION
    PQ = ContextKey.PQ
    SYSTEM = ContextKey.SYSTEM

    def __init__(self, value: str) -> None:
        self.value = value

    @classmethod
    def from_value(cls, value: str) -> _UnhandledKey:
        """Mirror ``ContextKey.from_value``, returning an unmatched instance."""
        return cls(value)


def test_set_unhandled_key_hits_assert_never(tmp_path: Path) -> None:
    """An unhandled context key trips the runtime safety net.

    Statically unreachable: ``ContextKey`` has exactly three members and
    the ``KEY`` argument is constrained by a ``click.Choice`` built from
    it. We swap in a stand-in enum to confirm that adding a fourth key
    without extending ``context_set`` fails loudly rather than silently
    persisting an unvalidated value.
    """
    assert context_set.callback is not None
    ctx = click.Context(cli, info_name="dhcli")
    ctx.obj = make_runtime(tmp_path)
    with (
        ctx,
        patch.object(context_mod, "ContextKey", _UnhandledKey),
        pytest.raises(AssertionError),
    ):
        context_set.callback("session", "community:community:dev")


# ---------------------------------------------------------------------------
# unset
# ---------------------------------------------------------------------------


def test_unset_one_key(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set_many({ContextKey.SESSION: "s", ContextKey.SYSTEM: "sys"})
    result, _ = _run(
        ["-o", "json", "context", "unset", "session"], tmp_path, runtime=rt
    )
    assert result.exit_code == 0
    context = rt.context_store.read()
    assert context.session is None
    assert context.system == "sys"


def test_unset_all_clears_everything(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.context_store.set_many(
        {ContextKey.SESSION: "s", ContextKey.SYSTEM: "sys", ContextKey.PQ: "p"}
    )
    result, _ = _run(["-o", "json", "context", "unset", "--all"], tmp_path, runtime=rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert all(v["value"] is None for v in payload.values())


def test_unset_without_keys_or_all_fails(tmp_path: Path) -> None:
    result, _ = _run(["-o", "json", "context", "unset"], tmp_path)
    assert result.exit_code == 2
    assert "Provide at least one KEY to clear, or --all" in result.output


def test_unset_rejects_a_key_combined_with_all(tmp_path: Path) -> None:
    """Naming a key *and* passing --all is an error, not a silent --all.

    Resolving the combination in favor of ``--all`` would clear the two
    keys the caller did not name while reporting success, so the request
    is refused and nothing is written.
    """
    rt = make_runtime(tmp_path)
    rt.context_store.set_many(
        {ContextKey.SESSION: "s", ContextKey.SYSTEM: "sys", ContextKey.PQ: "p"}
    )
    result, _ = _run(
        ["-o", "json", "context", "unset", "session", "--all"], tmp_path, runtime=rt
    )
    assert result.exit_code == 2
    assert "Pass either KEY(s) or --all, not both" in result.output
    context = rt.context_store.read()
    assert (context.session, context.system, context.pq) == ("s", "sys", "p")
