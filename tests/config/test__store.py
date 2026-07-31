"""Tests for :mod:`deephaven_mcp.config._store`.

Covers:

- :meth:`ConfigStore.read`: raw (unexpanded) parse, the strict-JSON
  flag, and every rejection path.
- :meth:`ConfigStore.validate`: schema validation, stem injection for
  named kinds, template-resolution warnings vs. syntax failures.
- :meth:`ConfigStore.write` / :meth:`ConfigStore.write_all`: atomic
  writes, directory creation and modes, all-or-nothing batches, temp
  cleanup on failure.
- :meth:`ConfigStore.write_text`: literal-text atomic write (the
  ``config edit`` path), parse/schema rejection, temp cleanup.
- :meth:`ConfigStore.delete`.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config import _store
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._logical_paths import ConfigFieldLocation
from deephaven_mcp.config._store import ConfigStore

_CLI = ConfigFieldLocation(
    kind=ConfigFileKind.CLI, name=None, field_path=FieldPath.ROOT
)
_SERVER = ConfigFieldLocation(
    kind=ConfigFileKind.SERVER, name=None, field_path=FieldPath.ROOT
)
_SESSION = ConfigFieldLocation(
    kind=ConfigFileKind.COMMUNITY_SESSION, name="local", field_path=FieldPath.ROOT
)


def _session_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "host": "localhost",
        "auth": {"credentials": {"type": "anonymous"}},
    }
    payload.update(overrides)
    return payload


# ---------------------------------------------------------------------------
# ConfigStore.read
# ---------------------------------------------------------------------------


def test_read_strict_json(tmp_path: Path) -> None:
    path = tmp_path / "cli.json"
    path.write_text('{"output": {"format": "human"}}')
    store = ConfigStore(tmp_path)
    raw = store.read(_CLI)
    assert raw.data == {"output": {"format": "human"}}
    assert raw.strict_json is True


def test_read_json5_comment_sets_strict_false(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text(
        '{\n  // a comment\n  "output": {"format": "human"},\n}'
    )
    store = ConfigStore(tmp_path)
    raw = store.read(_CLI)
    assert raw.data == {"output": {"format": "human"}}
    assert raw.strict_json is False


def test_read_leaves_placeholders_unexpanded(tmp_path: Path) -> None:
    target = ConfigFieldLocation(
        kind=ConfigFileKind.COMMUNITY_SESSION, name="s", field_path=FieldPath.ROOT
    )
    (tmp_path / "community" / "sessions").mkdir(parents=True)
    (tmp_path / "community" / "sessions" / "s.json").write_text(
        '{"token": "${env:DOES_NOT_EXIST_XYZ}"}'
    )
    store = ConfigStore(tmp_path)
    raw = store.read(target)
    assert raw.data == {"token": "${env:DOES_NOT_EXIST_XYZ}"}


def test_read_missing_file(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="Cannot read"):
        store.read(_CLI)


def test_read_unparseable(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("{not json at all")
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="Invalid JSON/JSON5"):
        store.read(_CLI)


def test_read_non_object_top_level(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("[1, 2]")
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="JSON object at the top level"):
        store.read(_CLI)


def test_path_of_returns_absolute_path(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    assert store.path_of(_SESSION) == tmp_path / "community" / "sessions" / "local.json"


def test_config_dir_property(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    assert store.config_dir == tmp_path


# ---------------------------------------------------------------------------
# ConfigStore.validate
# ---------------------------------------------------------------------------


def test_validate_valid_unnamed(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    assert store.validate(_CLI, {}) == []


def test_validate_valid_named_injects_stem(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    warnings = store.validate(_SESSION, _session_payload())
    assert warnings == []


def test_validate_named_declared_name_mismatch(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="does not match"):
        store.validate(_SESSION, _session_payload(session_name="other"))


def test_validate_named_rejects_embedded_name(tmp_path: Path) -> None:
    """A named-kind file that supplies its own 'name' key is rejected.

    Regression: 'name' is derived from the filename; unpacking file
    contents last let a raw 'name' override the injected one, so
    foo.json could validate and write as 'bar' and the loader would
    register a mismatched, silently collision-prone entity.
    """
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="derived from the filename"):
        store.validate(_SESSION, _session_payload(name="bar"))


def test_validate_schema_failure_names_file(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError) as excinfo:
        store.validate(_CLI, {"unknown_key": 1})
    assert str(tmp_path / "cli.json") in str(excinfo.value)


def test_validate_unresolved_env_ref_is_warning(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    payload = _session_payload(
        auth={"credentials": {"type": "psk", "token": "${env:DOES_NOT_EXIST_XYZ}"}}
    )
    warnings = store.validate(_SESSION, payload)
    assert len(warnings) == 1
    assert "DOES_NOT_EXIST_XYZ" in warnings[0]
    assert str(tmp_path / "community" / "sessions" / "local.json") in warnings[0]


def test_validate_unresolved_ref_in_list_is_warning(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    target = ConfigFieldLocation(
        kind=ConfigFileKind.ENTERPRISE_SYSTEM, name="prod", field_path=FieldPath.ROOT
    )
    payload = {
        "connection_json_url": "https://x/iris/connection.json",
        "auth": {"credentials": {"type": "password", "username": "u", "password": "p"}},
        "session_creation": {
            "defaults": {"extra_jvm_args": ["${env:DOES_NOT_EXIST_XYZ}"]}
        },
    }
    warnings = store.validate(target, payload)
    assert len(warnings) == 1


def test_validate_resolved_env_ref_expands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DH_TEST_PORT_XYZ", "10000")
    store = ConfigStore(tmp_path)
    payload = _session_payload(port="${env:DH_TEST_PORT_XYZ}")
    assert store.validate(_SESSION, payload) == []


def test_validate_unresolved_ref_in_typed_field_warns(tmp_path: Path) -> None:
    # An unresolved ref inside an int-typed field survives lenient
    # expansion; the resulting schema type error is downgraded to a
    # warning — the value's type is checked when the daemon resolves it.
    store = ConfigStore(tmp_path)
    payload = _session_payload(port="${env:DOES_NOT_EXIST_XYZ}")
    warnings = store.validate(_SESSION, payload)
    assert any("DOES_NOT_EXIST_XYZ" in w for w in warnings)
    assert any("unresolved templating ref" in w for w in warnings)


def test_validate_downgrade_keys_on_error_location(tmp_path: Path) -> None:
    # The downgrade applies only when the schema error's own location
    # holds the unresolved ref. An unrelated unresolved ref elsewhere
    # (token, a str field that raises no schema error) must not rescue
    # a genuine type error (wrong-literal port).
    store = ConfigStore(tmp_path)
    payload = _session_payload(
        port="not-a-port",
        auth={"credentials": {"type": "psk", "token": "${env:DOES_NOT_EXIST_XYZ}"}},
    )
    with pytest.raises(ConfigurationError, match="port"):
        store.validate(_SESSION, payload)


def test_error_at_unresolved_location_direct() -> None:
    # Direct pins for the loc walk: exact match, list index, skipping a
    # non-data segment (a discriminated-union tag), and misses.
    expanded = {"a": {"b": "${env:X}"}, "items": ["${env:Y}"]}
    unresolved = frozenset({("a", "b"), ("items", 0)})
    assert _store._error_at_unresolved_location(("a", "b"), expanded, unresolved)
    assert _store._error_at_unresolved_location(("items", 0), expanded, unresolved)
    # A union-tag segment ("psk") is not a key of the dict and is skipped.
    assert _store._error_at_unresolved_location(("a", "psk", "b"), expanded, unresolved)
    # A location that is not unresolved does not match.
    assert not _store._error_at_unresolved_location(("a",), expanded, unresolved)
    # An out-of-range or non-list index can never land on a ref.
    assert not _store._error_at_unresolved_location(("items", 5), expanded, unresolved)
    assert not _store._error_at_unresolved_location(("a", 0), expanded, unresolved)


def test_validate_wrong_literal_in_typed_field_fails_schema(tmp_path: Path) -> None:
    # A genuinely wrong literal (no templating involved) still fails.
    store = ConfigStore(tmp_path)
    payload = _session_payload(port="not-a-port")
    with pytest.raises(ConfigurationError, match="port"):
        store.validate(_SESSION, payload)


def test_validate_unknown_field_with_unresolved_ref_fails_schema(
    tmp_path: Path,
) -> None:
    # ``extra_forbidden`` is exempt from the downgrade: the field is
    # illegal regardless of what its unresolved value would resolve to.
    store = ConfigStore(tmp_path)
    payload = _session_payload()
    payload["bogus_field"] = "${env:DOES_NOT_EXIST_XYZ}"
    with pytest.raises(ConfigurationError, match="bogus_field"):
        store.validate(_SESSION, payload)


def test_validate_mixed_errors_reports_only_fatal(tmp_path: Path) -> None:
    # One downgradable error (templated port) plus one genuine error
    # (wrong-literal host type): the raise carries only the genuine one.
    store = ConfigStore(tmp_path)
    payload = _session_payload(port="${env:DOES_NOT_EXIST_XYZ}")
    payload["host"] = 12345
    with pytest.raises(ConfigurationError) as excinfo:
        store.validate(_SESSION, payload)
    assert "host" in str(excinfo.value)
    assert "port" not in str(excinfo.value)


def test_validate_placeholder_syntax_error_blocks(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    payload = _session_payload(host="${bogus:thing}")
    with pytest.raises(ConfigurationError, match="unknown placeholder kind"):
        store.validate(_SESSION, payload)


# ---------------------------------------------------------------------------
# ConfigStore.write
# ---------------------------------------------------------------------------


def test_write_creates_file_and_parents(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    warnings = store.write(_SESSION, _session_payload())
    assert warnings == []
    final = config_dir / "community" / "sessions" / "local.json"
    assert final.is_file()
    text = final.read_text()
    assert text.endswith("\n")
    assert json.loads(text) == _session_payload()
    if sys.platform != "win32":
        assert (final.stat().st_mode & 0o777) == 0o600
        assert (config_dir.stat().st_mode & 0o777) == 0o700
        assert ((config_dir / "community").stat().st_mode & 0o777) == 0o700


def test_write_creates_missing_data_root_ancestors(tmp_path: Path) -> None:
    """First write on a fresh machine: the data root itself is absent.

    Ancestors above the config dir are created but not tightened
    (mirroring ``harden_private_dir``); the config dir and below get
    the usual 0o700/0o600 modes.
    """
    config_dir = tmp_path / "missing-root" / "ai" / "config"
    store = ConfigStore(config_dir)
    warnings = store.write(_SESSION, _session_payload())
    assert warnings == []
    final = config_dir / "community" / "sessions" / "local.json"
    assert final.is_file()
    assert (tmp_path / "missing-root" / "ai").is_dir()
    if sys.platform != "win32":
        assert (final.stat().st_mode & 0o777) == 0o600
        assert (config_dir.stat().st_mode & 0o777) == 0o700
        assert ((config_dir / "community").stat().st_mode & 0o777) == 0o700


def test_write_invalid_data_writes_nothing(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    with pytest.raises(ConfigurationError):
        store.write(_CLI, {"unknown_key": 1})
    assert not (config_dir / "cli.json").exists()


def test_write_replaces_existing_atomically(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write(_CLI, {})
    store.write(_CLI, {"output": {"format": "human"}})
    data = json.loads((config_dir / "cli.json").read_text())
    assert data == {"output": {"format": "human"}}
    # No temp litter.
    assert list(config_dir.glob("*.tmp")) == []


def test_write_failure_leaves_existing_file_untouched(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write(_CLI, {"output": {"format": "human"}})
    with pytest.raises(ConfigurationError):
        store.write(_CLI, {"unknown_key": 1})
    data = json.loads((config_dir / "cli.json").read_text())
    assert data == {"output": {"format": "human"}}


def test_write_returns_template_warnings(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    payload = _session_payload(
        auth={"credentials": {"type": "psk", "token": "${env:DOES_NOT_EXIST_XYZ}"}}
    )
    warnings = store.write(_SESSION, payload)
    assert len(warnings) == 1
    # The ref is stored verbatim, never expanded at write time.
    stored = json.loads(
        (config_dir / "community" / "sessions" / "local.json").read_text()
    )
    assert stored["auth"]["credentials"]["token"] == "${env:DOES_NOT_EXIST_XYZ}"


# ---------------------------------------------------------------------------
# ConfigStore.write_all (batch)
# ---------------------------------------------------------------------------


def test_write_all_writes_all(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    warnings = store.write_all([(_CLI, {}), (_SESSION, _session_payload())])
    assert warnings == []
    assert (config_dir / "cli.json").is_file()
    assert (config_dir / "community" / "sessions" / "local.json").is_file()


def test_write_all_or_nothing_on_validation_failure(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    with pytest.raises(ConfigurationError):
        store.write_all([(_CLI, {}), (_SERVER, {"unknown_key": 1})])
    assert not (config_dir / "cli.json").exists()
    assert not (config_dir / "server.json").exists()


def test_write_all_commit_failure_cleans_temps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom(src: object, dst: object) -> None:
        raise OSError("simulated rename failure")

    monkeypatch.setattr(_store.os, "replace", _boom)
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_all([(_CLI, {}), (_SERVER, {})])
    assert not (config_dir / "cli.json").exists()
    assert not (config_dir / "server.json").exists()
    assert list(config_dir.rglob("*.tmp")) == []


def _counting_replace(
    monkeypatch: pytest.MonkeyPatch, fail_on: set[int]
) -> dict[str, int]:
    """Patch ``os.replace`` to raise on the given 1-indexed call numbers."""
    real_replace = os.replace
    calls = {"count": 0}

    def _maybe_boom(src: object, dst: object) -> None:
        calls["count"] += 1
        if calls["count"] in fail_on:
            raise OSError("simulated rename failure")
        real_replace(src, dst)

    monkeypatch.setattr(_store.os, "replace", _maybe_boom)
    return calls


def test_write_all_rollback_restores_existing_file_on_partial_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    original = {"output": {"format": "human"}}
    store.write(_CLI, original)

    # Commit order (the backup is a copy, not an os.replace, so it is
    # not counted): CLI's new-content rename (#1), then SESSION's (new
    # file) single rename (#2). Fail on #2 so CLI is already committed
    # when the batch fails; the rollback's restore of CLI's backup is
    # os.replace #3 and must succeed.
    _counting_replace(monkeypatch, fail_on={2})
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_all(
            [(_CLI, {"output": {"format": "json"}}), (_SESSION, _session_payload())]
        )

    assert json.loads((config_dir / "cli.json").read_text()) == original
    assert not (config_dir / "community" / "sessions" / "local.json").exists()
    assert list(config_dir.rglob("*.tmp")) == []
    assert list(config_dir.rglob("*.bak")) == []


def test_write_all_rollback_removes_newly_created_file_on_partial_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    # Both files are new: each commit is a single rename (#1, #2).
    # Fail on #2 so CLI (already committed) must be rolled back.
    _counting_replace(monkeypatch, fail_on={2})
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_all([(_CLI, {}), (_SESSION, _session_payload())])

    assert not (config_dir / "cli.json").exists()
    assert not (config_dir / "community" / "sessions" / "local.json").exists()
    assert list(config_dir.rglob("*.tmp")) == []
    assert list(config_dir.rglob("*.bak")) == []


def test_write_all_rollback_double_failure_names_inconsistent_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    original = {"output": {"format": "human"}}
    store.write(_CLI, original)

    # The backup is a copy, not an os.replace, so it is not counted.
    # #1 places CLI's new content. #2 fails committing SERVER (new
    # file), triggering rollback. #3 is the rollback's attempt to
    # restore CLI's backup — also fails, so CLI is left holding its
    # *new* content with the original preserved only in the (now stuck)
    # backup file.
    _counting_replace(monkeypatch, fail_on={2, 3})
    new_data = {"output": {"format": "json"}}
    with pytest.raises(
        ConfigurationError, match="could not be restored from backup"
    ) as exc:
        store.write_all([(_CLI, new_data), (_SERVER, {})])

    assert "cli.json" in str(exc.value)
    assert json.loads((config_dir / "cli.json").read_text()) == new_data
    backups = list(config_dir.rglob("*.bak"))
    assert len(backups) == 1
    assert json.loads(backups[0].read_text()) == original
    assert list(config_dir.rglob("*.tmp")) == []


def test_write_overwrite_keeps_target_present_during_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Overwriting an existing file never leaves a missing-file window.

    Regression: the backup is a copy, not a move, so the live target is
    still on disk at the instant os.replace atomically overwrites it. A
    concurrent loader therefore observes either the old content or the
    new, never a missing file.
    """
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write(_CLI, {"output": {"format": "human"}})
    final = config_dir / "cli.json"

    real_replace = _store.os.replace
    seen: dict[str, bool] = {}

    def _checking_replace(src: object, dst: object) -> None:
        if Path(dst) == final:
            seen["present_at_replace"] = final.exists()
        real_replace(src, dst)

    monkeypatch.setattr(_store.os, "replace", _checking_replace)
    store.write(_CLI, {"output": {"format": "json"}})

    assert seen["present_at_replace"] is True
    assert json.loads(final.read_text()) == {"output": {"format": "json"}}
    assert list(config_dir.rglob("*.bak")) == []


def test_write_stage_failure_cleans_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom(fd: int, *args: object, **kwargs: object) -> None:
        os.close(fd)
        raise OSError("simulated fdopen failure")

    monkeypatch.setattr(_store.os, "fdopen", _boom)
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write(_CLI, {})
    assert list(config_dir.rglob("*.tmp")) == []


def test_write_stage_failure_cleanup_also_fails_preserves_original_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom_fdopen(fd: int, *args: object, **kwargs: object) -> None:
        os.close(fd)
        raise OSError("simulated fdopen failure")

    def _boom_unlink(self: Path, missing_ok: bool = False) -> None:
        raise OSError("cannot remove temp")

    monkeypatch.setattr(_store.os, "fdopen", _boom_fdopen)
    monkeypatch.setattr(Path, "unlink", _boom_unlink)
    # The original staging failure surfaces, not the unlink failure.
    with pytest.raises(ConfigurationError, match="simulated fdopen failure"):
        store.write(_CLI, {})


def test_write_all_success_backup_cleanup_failure_does_not_fail_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write(_CLI, {"output": {"format": "human"}})

    real_unlink = Path.unlink

    def _flaky_unlink(self: Path, missing_ok: bool = False) -> None:
        if self.suffix == ".bak":
            raise OSError("cannot remove backup")
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", _flaky_unlink)
    warnings = store.write(_CLI, {"output": {"format": "json"}})
    assert warnings == []
    assert json.loads((config_dir / "cli.json").read_text()) == {
        "output": {"format": "json"}
    }
    # The leftover backup survives since its cleanup failed, but the
    # write itself is reported as a success, not an error.
    assert len(list(config_dir.rglob("*.bak"))) == 1


def test_write_all_temp_cleanup_failure_preserves_original_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom_replace(src: object, dst: object) -> None:
        raise OSError("simulated rename failure")

    def _boom_unlink(self: Path, missing_ok: bool = False) -> None:
        if self.suffix == ".tmp":
            raise OSError("cannot remove temp")

    monkeypatch.setattr(_store.os, "replace", _boom_replace)
    monkeypatch.setattr(Path, "unlink", _boom_unlink)
    # The batch-write failure surfaces, not the temp-cleanup failure.
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_all([(_CLI, {})])


# ---------------------------------------------------------------------------
# ConfigStore.write_text
# ---------------------------------------------------------------------------


def test_write_text_writes_verbatim_including_comments(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    text = '{\n  // a comment\n  "output": {"format": "human"}\n}\n'
    warnings = store.write_text(_CLI, text)
    assert warnings == []
    assert (config_dir / "cli.json").read_text() == text


def test_write_text_creates_parents(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write_text(_SESSION, json.dumps(_session_payload()))
    final = config_dir / "community" / "sessions" / "local.json"
    assert final.is_file()
    if sys.platform != "win32":
        assert (final.stat().st_mode & 0o777) == 0o600


def test_write_text_invalid_json5_writes_nothing(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    with pytest.raises(ConfigurationError, match="Invalid JSON/JSON5"):
        store.write_text(_CLI, "{not json")
    assert not (config_dir / "cli.json").exists()


def test_write_text_non_object_top_level_writes_nothing(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    with pytest.raises(ConfigurationError, match="JSON object at the top level"):
        store.write_text(_CLI, "[1, 2]")
    assert not (config_dir / "cli.json").exists()


def test_write_text_schema_failure_writes_nothing(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    with pytest.raises(ConfigurationError, match="cli.json"):
        store.write_text(_CLI, '{"unknown_key": 1}')
    assert not (config_dir / "cli.json").exists()


def test_write_text_replaces_existing_atomically(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write_text(_CLI, "{}")
    store.write_text(_CLI, '{"output": {"format": "human"}}')
    assert json.loads((config_dir / "cli.json").read_text()) == {
        "output": {"format": "human"}
    }
    assert list(config_dir.glob("*.tmp")) == []


def test_write_text_commit_failure_raises_and_cleans_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom(src: object, dst: object) -> None:
        raise OSError("simulated rename failure")

    monkeypatch.setattr(_store.os, "replace", _boom)
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_text(_CLI, "{}")
    assert not (config_dir / "cli.json").exists()
    assert list(config_dir.rglob("*.tmp")) == []


def test_write_text_stage_failure_raises_config_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A staging OSError surfaces as ConfigurationError, not raw OSError.

    Regression: directory creation and staging ran outside the
    OSError->ConfigurationError conversion, so a permission or disk
    failure there leaked a raw OSError — which 'config edit' (catching
    only ConfigurationError) would report as internal_error rather than
    the documented structured config error. No temp is left behind
    (staging never produced one).
    """
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom(final_path: Path, text: str) -> Path:
        raise OSError("simulated staging failure")

    monkeypatch.setattr(_store, "_stage_file", _boom)
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_text(_CLI, "{}")
    assert list(config_dir.rglob("*.tmp")) == []


def test_write_text_temp_cleanup_failure_preserves_original_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)

    def _boom_replace(src: object, dst: object) -> None:
        raise OSError("simulated rename failure")

    def _boom_unlink(self: Path, missing_ok: bool = False) -> None:
        raise OSError("cannot remove temp")

    monkeypatch.setattr(_store.os, "replace", _boom_replace)
    monkeypatch.setattr(Path, "unlink", _boom_unlink)
    # The commit failure surfaces, not the temp-cleanup failure.
    with pytest.raises(ConfigurationError, match="Failed to write"):
        store.write_text(_CLI, "{}")


# ---------------------------------------------------------------------------
# ConfigStore.delete
# ---------------------------------------------------------------------------


def test_delete_removes_file(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    store = ConfigStore(config_dir)
    store.write(_SESSION, _session_payload())
    deleted = store.delete(_SESSION)
    assert deleted == config_dir / "community" / "sessions" / "local.json"
    assert not deleted.exists()


def test_delete_missing_file_raises(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="does not exist"):
        store.delete(_CLI)


def test_delete_oserror_raises(tmp_path: Path) -> None:
    # Make the target path a directory: unlink then fails with an
    # OSError subtype on every platform.
    (tmp_path / "cli.json").mkdir(parents=True)
    store = ConfigStore(tmp_path)
    with pytest.raises(ConfigurationError, match="Cannot delete"):
        store.delete(_CLI)
