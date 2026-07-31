"""Tests for ``deephaven_mcp.cli._context``."""

from __future__ import annotations

import dataclasses
import os
import stat
from enum import StrEnum
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from deephaven_mcp.cli._context import (
    CliContext,
    ContextKey,
    ContextProvenance,
    ContextStore,
    clear_matching,
    require_context_target,
    require_context_value,
    resolve_context_value,
    resolve_for_runtime,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.schema._cli import ContextConfig

from ._helpers import make_runtime

# ---------------------------------------------------------------------------
# CliContext
# ---------------------------------------------------------------------------


def test_cli_context_defaults_are_all_none() -> None:
    context = CliContext()
    assert context.session is None
    assert context.system is None
    assert context.pq is None


@pytest.mark.parametrize(
    "key,attribute",
    [
        (ContextKey.SESSION, "session"),
        (ContextKey.SYSTEM, "system"),
        (ContextKey.PQ, "pq"),
    ],
)
def test_cli_context_get_reads_the_matching_field(
    key: ContextKey, attribute: str
) -> None:
    """``get`` maps every key onto its field, checked exhaustively by mypy."""
    context = CliContext(**{attribute: "value"})
    assert context.get(key) == "value"
    for other in ContextKey:
        if other is not key:
            assert context.get(other) is None


# ---------------------------------------------------------------------------
# ContextKey per-member wording
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,label,descriptor",
    [
        (ContextKey.SESSION, "session", "session id"),
        (ContextKey.SYSTEM, "system", "system name"),
        (ContextKey.PQ, "PQ", "PQ id"),
    ],
)
def test_context_key_user_facing_wording(
    key: ContextKey, label: str, descriptor: str
) -> None:
    """'PQ' is an acronym in prose, and only 'system' holds a name."""
    assert key.label == label
    assert key.descriptor == descriptor


@pytest.mark.parametrize("key", list(ContextKey))
def test_context_key_from_value_round_trips(key: ContextKey) -> None:
    """Every member is recoverable from its wire value."""
    assert ContextKey.from_value(key.value) is key


def test_context_key_from_value_rejects_an_unknown_value() -> None:
    """An unknown key fails loudly rather than returning a default."""
    with pytest.raises(KeyError):
        ContextKey.from_value("bogus")


def test_context_key_requires_wording_for_every_member() -> None:
    """A member declared without its wording fails at class construction."""
    with pytest.raises(TypeError):
        StrEnum("Incomplete", {"BARE": "bare"}, type=ContextKey)  # type: ignore[call-overload]
    # Suppression justified: deliberately constructing an enum whose
    # member omits the (label, descriptor) metadata, to prove __new__
    # rejects it. Bracketed ``call-overload`` names what is silenced;
    # mypy still flags any unintentional misuse at real call sites.


def test_cli_context_get_rejects_a_non_member() -> None:
    """The exhaustive dispatch has no silent default branch.

    Guards the enum-to-field correspondence: a ``ContextKey`` member
    added without its ``CliContext`` field is a mypy error at every
    ``get`` call site, and anything outside the closed set fails loudly
    here rather than reading as unset.
    """
    with pytest.raises(AssertionError):
        CliContext().get("bogus")  # type: ignore[arg-type]
    # Suppression justified: deliberately passing a value the parameter's
    # type rejects so the runtime ``assert_never`` branch is covered.
    # Bracketed ``arg-type`` names what is silenced; mypy still flags any
    # unintentional misuse at real call sites.


# ---------------------------------------------------------------------------
# ContextStore.read
# ---------------------------------------------------------------------------


def test_read_missing_file_returns_empty_context(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    assert store.read() == CliContext()


def test_read_corrupt_file_returns_empty_context_and_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text("not json")
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result == CliContext()
    assert "corrupt" in caplog.text.lower()


def test_read_unreadable_file_returns_empty_context_and_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.mkdir()  # a directory where a file is expected -> OSError on read
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result == CliContext()
    assert "cannot read" in caplog.text.lower()


def test_read_round_trips_written_context(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.write(CliContext(session="community:community:dev"))
    assert store.read() == CliContext(session="community:community:dev")


# ---------------------------------------------------------------------------
# ContextStore.write / set / set_many / unset
# ---------------------------------------------------------------------------


def test_write_creates_parent_directory(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "nested" / "runtime"
    store = ContextStore.for_runtime_dir(runtime_dir)
    store.write(CliContext(system="prod"))
    assert store.path.exists()
    assert store.read().system == "prod"


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission semantics only")
def test_write_produces_private_file_and_directory(tmp_path: Path) -> None:
    """The file is 0o600 inside a 0o700 directory."""
    store = ContextStore.for_runtime_dir(tmp_path / "runtime")
    store.write(CliContext(session="community:community:dev"))
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.path.parent.stat().st_mode) == 0o700


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission semantics only")
def test_write_tightens_a_preexisting_loose_directory(tmp_path: Path) -> None:
    """An already-loose parent is tightened, not left as found.

    ``mkdir(mode=...)`` applies its mode only when it creates the
    directory, so a pre-existing 0o755 runtime dir would keep those
    bits. ``harden_private_dir`` chmods unconditionally.
    """
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o755)
    ContextStore.for_runtime_dir(runtime_dir).write(CliContext(system="prod"))
    assert stat.S_IMODE(runtime_dir.stat().st_mode) == 0o700


def test_mutations_are_logged(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Every context mutation leaves a DEBUG trace naming the keys.

    The sticky context silently redirects later commands, so a change to
    it must be reconstructable from the logs.
    """
    store = ContextStore.for_runtime_dir(tmp_path)
    with caplog.at_level("DEBUG", logger="deephaven_mcp.cli._context"):
        store.set(ContextKey.SESSION, "community:community:dev")
        store.unset((ContextKey.SESSION,))
    assert "community:community:dev" in caplog.text
    assert "Clearing sticky context keys" in caplog.text


def test_set_preserves_other_keys(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "community:community:dev")
    store.set(ContextKey.SYSTEM, "prod")
    context = store.read()
    assert context.session == "community:community:dev"
    assert context.system == "prod"
    assert context.pq is None


def test_set_many_sets_several_keys_atomically(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "stale")
    updated = store.set_many(
        {ContextKey.PQ: "enterprise:prod:1", ContextKey.SYSTEM: "prod"}
    )
    assert updated.pq == "enterprise:prod:1"
    assert updated.system == "prod"
    assert updated.session == "stale"
    assert store.read() == updated


def test_unset_one_key(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set_many({ContextKey.SESSION: "s", ContextKey.SYSTEM: "sys"})
    store.unset((ContextKey.SESSION,))
    context = store.read()
    assert context.session is None
    assert context.system == "sys"


def test_unset_all_clears_every_key(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set_many(
        {ContextKey.SESSION: "s", ContextKey.SYSTEM: "sys", ContextKey.PQ: "pq"}
    )
    store.unset(None)
    assert store.read() == CliContext()


def test_set_many_rejects_a_non_string_value(tmp_path: Path) -> None:
    """A non-string value raises rather than writing an unreadable file.

    The auto-set call sites take the id straight from an MCP tool
    payload, where it arrives untyped. Validating on write matters
    because a file that fails validation is read as *empty*, so one bad
    value would otherwise discard every other key as well.
    """
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "community:community:dev")
    before = store.path.read_text()

    # Deliberately violates the annotated type: the point is what happens
    # when an untyped tool payload smuggles a non-string through.
    with pytest.raises(ValidationError):
        store.set_many({ContextKey.PQ: 12345})  # type: ignore[dict-item]

    assert store.path.read_text() == before
    assert store.read().session == "community:community:dev"


# ---------------------------------------------------------------------------
# clear_matching
# ---------------------------------------------------------------------------


def test_clear_matching_clears_only_matching_keys(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set_many({ContextKey.SESSION: "id-1", ContextKey.PQ: "id-1"})
    clear_matching(store, frozenset({"id-1"}), (ContextKey.SESSION, ContextKey.PQ))
    assert store.read() == CliContext()


def test_clear_matching_is_noop_when_value_differs(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "id-1")
    clear_matching(store, frozenset({"id-2"}), (ContextKey.SESSION,))
    assert store.read().session == "id-1"


def test_clear_matching_is_noop_when_key_unset(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    clear_matching(store, frozenset({"id-1"}), (ContextKey.SESSION, ContextKey.PQ))
    assert store.read() == CliContext()


def test_clear_matching_is_noop_for_empty_value_set(tmp_path: Path) -> None:
    """An empty set clears nothing - the every-delete-failed batch case."""
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.PQ, "id-1")
    clear_matching(store, frozenset(), (ContextKey.PQ,))
    assert store.read().pq == "id-1"


def test_clear_matching_matches_any_of_several_values(tmp_path: Path) -> None:
    """A batch delete clears whichever key matches any deleted id."""
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set_many({ContextKey.SESSION: "id-2", ContextKey.PQ: "id-9"})
    clear_matching(
        store, frozenset({"id-1", "id-2"}), (ContextKey.SESSION, ContextKey.PQ)
    )
    context = store.read()
    assert context.session is None
    assert context.pq == "id-9"


# ---------------------------------------------------------------------------
# resolve_context_value
# ---------------------------------------------------------------------------


def test_resolve_prefers_explicit_argument(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "from-file")
    resolved = resolve_context_value(
        ContextKey.SESSION, "from-arg", store=store, enabled=True
    )
    assert resolved.value == "from-arg"
    assert resolved.provenance is ContextProvenance.ARGUMENT


def test_resolve_falls_back_to_file(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "from-file")
    resolved = resolve_context_value(
        ContextKey.SESSION, None, store=store, enabled=True
    )
    assert resolved.value == "from-file"
    assert resolved.provenance is ContextProvenance.FILE


def test_resolve_returns_unset_when_nothing_supplies_a_value(tmp_path: Path) -> None:
    store = ContextStore.for_runtime_dir(tmp_path)
    resolved = resolve_context_value(
        ContextKey.SESSION, None, store=store, enabled=True
    )
    assert resolved.value is None
    assert resolved.provenance is ContextProvenance.UNSET


def test_resolve_treats_empty_argument_as_supplied_not_omitted(tmp_path: Path) -> None:
    """An empty id is reported, never swapped for the sticky target.

    Falling back here would act on a *different* target than the one
    named on the command line -- the failure mode the sticky context is
    supposed to prevent. The empty value is passed through so the
    downstream tool rejects it naming the id.
    """
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "from-file")
    resolved = resolve_context_value(ContextKey.SESSION, "", store=store, enabled=True)
    assert resolved.value == ""
    assert resolved.provenance is ContextProvenance.ARGUMENT


def test_resolve_disabled_skips_file(tmp_path: Path) -> None:
    """``enabled=False`` considers only an explicit argument."""
    store = ContextStore.for_runtime_dir(tmp_path)
    store.set(ContextKey.SESSION, "from-file")
    resolved = resolve_context_value(
        ContextKey.SESSION, None, store=store, enabled=False
    )
    assert resolved.value is None
    assert resolved.provenance is ContextProvenance.UNSET


# ---------------------------------------------------------------------------
# require_context_value
# ---------------------------------------------------------------------------


def test_require_context_value_returns_resolved_value(tmp_path: Path) -> None:
    runtime = make_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "community:community:dev")
    assert (
        require_context_value(runtime, ContextKey.SESSION, None)
        == "community:community:dev"
    )


def test_require_context_value_prefers_explicit(tmp_path: Path) -> None:
    runtime = make_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "from-file")
    assert require_context_value(runtime, ContextKey.SESSION, "explicit") == "explicit"


def test_require_context_value_raises_when_unset(tmp_path: Path) -> None:
    runtime = make_runtime(tmp_path)
    with pytest.raises(CliError) as exc_info:
        require_context_value(runtime, ContextKey.SESSION, None)
    assert exc_info.value.code is ErrorCode.CONTEXT_NOT_SET
    assert "dhcli context set session" in str(exc_info.value)


def test_require_context_value_error_names_the_kind_of_value(tmp_path: Path) -> None:
    """'system' holds a name, not an id."""
    runtime = make_runtime(tmp_path)
    with pytest.raises(CliError) as exc_info:
        require_context_value(runtime, ContextKey.SYSTEM, None)
    assert "No system name was given" in str(exc_info.value)


def test_require_context_value_error_omits_dead_remedy_when_disabled(
    tmp_path: Path,
) -> None:
    """With the fallback off, 'context set' would not help -- so do not suggest it.

    ``--no-context`` / ``context.enabled=false`` skips the file step, so
    setting the key changes nothing while it is disabled.
    """
    base = make_runtime(tmp_path)
    cli = base.config.cli.model_copy(update={"context": ContextConfig(enabled=False)})
    runtime = dataclasses.replace(
        base, config=base.config.model_copy(update={"cli": cli})
    )
    runtime.context_store.set(ContextKey.SESSION, "from-file")
    with pytest.raises(CliError) as exc_info:
        require_context_value(runtime, ContextKey.SESSION, None)
    message = str(exc_info.value)
    assert exc_info.value.code is ErrorCode.CONTEXT_NOT_SET
    assert "dhcli context set" not in message
    assert "--no-context" in message


# ---------------------------------------------------------------------------
# resolve_for_runtime
# ---------------------------------------------------------------------------


def test_resolve_for_runtime_reads_the_file(tmp_path: Path) -> None:
    runtime = make_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "community:community:dev")
    resolved = resolve_for_runtime(runtime, ContextKey.SESSION, None)
    assert resolved.value == "community:community:dev"
    assert resolved.provenance is ContextProvenance.FILE


def test_resolve_for_runtime_prefers_explicit(tmp_path: Path) -> None:
    runtime = make_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "from-file")
    resolved = resolve_for_runtime(runtime, ContextKey.SESSION, "explicit")
    assert resolved.value == "explicit"
    assert resolved.provenance is ContextProvenance.ARGUMENT


def test_resolve_for_runtime_returns_unset_rather_than_raising(tmp_path: Path) -> None:
    """The soft-fallback entry point: an unset key is a value, not an error."""
    runtime = make_runtime(tmp_path)
    resolved = resolve_for_runtime(runtime, ContextKey.SYSTEM, None)
    assert resolved.value is None
    assert resolved.provenance is ContextProvenance.UNSET


def test_resolve_for_runtime_honors_disabled_fallback(tmp_path: Path) -> None:
    """It reads ``context.enabled`` so no caller has to know where it lives."""
    base = make_runtime(tmp_path)
    base.context_store.set(ContextKey.SESSION, "from-file")
    cli = base.config.cli.model_copy(update={"context": ContextConfig(enabled=False)})
    runtime = dataclasses.replace(
        base, config=base.config.model_copy(update={"cli": cli})
    )
    resolved = resolve_for_runtime(runtime, ContextKey.SESSION, None)
    assert resolved.value is None
    assert resolved.provenance is ContextProvenance.UNSET


def test_resolve_for_runtime_treats_blank_stored_value_as_unset(tmp_path: Path) -> None:
    """A hand-edited blank must never become a command's target.

    Deliberately asymmetric with an explicit blank argument, which is
    returned as-is so the caller can report it.
    """
    runtime = make_runtime(tmp_path)
    path = runtime.context_store.path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"session": ""}', encoding="utf-8")
    assert resolve_for_runtime(runtime, ContextKey.SESSION, None).value is None
    assert resolve_for_runtime(runtime, ContextKey.SESSION, "").value == ""


# ---------------------------------------------------------------------------
# require_context_target
# ---------------------------------------------------------------------------


def _confirming_runtime(tmp_path: Path, *, no_input: bool = False) -> Runtime:
    """Build a runtime with ``context.confirm_destructive`` enabled."""
    base = make_runtime(tmp_path, no_input=no_input)
    cli = base.config.cli.model_copy(
        update={"context": ContextConfig(enabled=True, confirm_destructive=True)}
    )
    return dataclasses.replace(base, config=base.config.model_copy(update={"cli": cli}))


def test_require_context_target_confirms_file_value_when_enabled(
    tmp_path: Path,
) -> None:
    """An accepted confirmation returns the context value and names the target."""
    runtime = _confirming_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "community:community:dev")
    with (
        patch("deephaven_mcp.cli._context.can_prompt", return_value=True),
        patch("deephaven_mcp.cli._context.confirm", return_value=True) as confirm,
    ):
        value = require_context_target(
            runtime, ContextKey.SESSION, None, action="Run script in", yes=False
        )
    assert value == "community:community:dev"
    label = confirm.call_args.args[0]
    assert "Run script in session 'community:community:dev'" in label
    assert "sticky context" in label


def test_require_context_target_declined_raises_operation_canceled(
    tmp_path: Path,
) -> None:
    """Declining is a deliberate cancel, not a failure of the operation."""
    runtime = _confirming_runtime(tmp_path)
    runtime.context_store.set(ContextKey.PQ, "enterprise:prod:1")
    with (
        patch("deephaven_mcp.cli._context.can_prompt", return_value=True),
        patch("deephaven_mcp.cli._context.confirm", return_value=False) as confirm,
        pytest.raises(CliError) as exc_info,
    ):
        require_context_target(runtime, ContextKey.PQ, None, action="Delete", yes=False)
    assert exc_info.value.code is ErrorCode.OPERATION_CANCELED
    # 'PQ' is an acronym: the prompt must not render the wire form 'pq'.
    assert confirm.call_args.args[0].startswith("Delete PQ 'enterprise:prod:1'")


def test_require_context_target_never_confirms_an_explicit_value(
    tmp_path: Path,
) -> None:
    """Naming the target on the command line is already the statement of intent."""
    runtime = _confirming_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "from-file")
    with (
        patch("deephaven_mcp.cli._context.can_prompt", return_value=True),
        patch("deephaven_mcp.cli._context.confirm") as confirm,
    ):
        value = require_context_target(
            runtime, ContextKey.SESSION, "explicit", action="Delete", yes=False
        )
    assert value == "explicit"
    confirm.assert_not_called()


def test_require_context_target_yes_skips_the_prompt(tmp_path: Path) -> None:
    runtime = _confirming_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "community:community:dev")
    with (
        patch("deephaven_mcp.cli._context.can_prompt", return_value=True),
        patch("deephaven_mcp.cli._context.confirm") as confirm,
    ):
        value = require_context_target(
            runtime, ContextKey.SESSION, None, action="Delete", yes=True
        )
    assert value == "community:community:dev"
    confirm.assert_not_called()


def test_require_context_target_silent_when_setting_disabled(tmp_path: Path) -> None:
    """The default configuration acts silently, matching prevailing CLI behavior."""
    runtime = make_runtime(tmp_path)
    runtime.context_store.set(ContextKey.SESSION, "community:community:dev")
    with (
        patch("deephaven_mcp.cli._context.can_prompt", return_value=True),
        patch("deephaven_mcp.cli._context.confirm") as confirm,
    ):
        value = require_context_target(
            runtime, ContextKey.SESSION, None, action="Delete", yes=False
        )
    assert value == "community:community:dev"
    confirm.assert_not_called()


def test_require_context_target_proceeds_when_prompting_unavailable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A non-interactive caller proceeds rather than failing closed.

    Enabling the setting must not break agents and CI: where no question
    can be asked there is nothing to confirm, and refusing instead would
    make ``--yes`` mandatory boilerplate on every such invocation.
    """
    runtime = _confirming_runtime(tmp_path, no_input=True)
    runtime.context_store.set(ContextKey.SESSION, "community:community:dev")
    with (
        patch("deephaven_mcp.cli._context.can_prompt", return_value=False),
        patch("deephaven_mcp.cli._context.confirm") as confirm,
        caplog.at_level("DEBUG", logger="deephaven_mcp.cli._context"),
    ):
        value = require_context_target(
            runtime, ContextKey.SESSION, None, action="Delete", yes=False
        )
    assert value == "community:community:dev"
    confirm.assert_not_called()
    # Proceeding without asking is a decision worth a trace.
    assert "Skipping confirmation" in caplog.text


def test_require_context_target_raises_when_unset(tmp_path: Path) -> None:
    runtime = _confirming_runtime(tmp_path)
    with pytest.raises(CliError) as exc_info:
        require_context_target(
            runtime, ContextKey.SESSION, None, action="Delete", yes=False
        )
    assert exc_info.value.code is ErrorCode.CONTEXT_NOT_SET
