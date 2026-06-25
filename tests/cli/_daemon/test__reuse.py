"""Tests for ``deephaven_mcp.cli._daemon._reuse``."""

from __future__ import annotations

import pytest

from deephaven_mcp.cli._daemon._reuse import (
    ReuseDecision,
    decide_reuse,
    describe_difference,
)
from deephaven_mcp.config.schema import DaemonReuseAction, DaemonReusePolicy
from deephaven_mcp.daemon_registry import DaemonBuildIdentity

_BASE = DaemonBuildIdentity(
    version="1.2.3",
    venv="/venv/a",
    fingerprint="a" * 64,
)


def _variant(**overrides: str) -> DaemonBuildIdentity:
    fields = {
        "version": _BASE.version,
        "venv": _BASE.venv,
        "fingerprint": _BASE.fingerprint,
    }
    fields.update(overrides)
    return DaemonBuildIdentity(**fields)


# ---------------------------------------------------------------------------
# decide_reuse
# ---------------------------------------------------------------------------


def test_matching_identity_ignores() -> None:
    decision = decide_reuse(_BASE, _BASE, DaemonReusePolicy(), can_spawn=True)
    assert decision.action is DaemonReuseAction.IGNORE
    assert decision.differing == ()


@pytest.mark.parametrize(
    ("recorded", "field"),
    [
        (_variant(version="9.9.9"), "version"),
        (_variant(venv="/venv/b"), "venv"),
        (_variant(fingerprint="b" * 64), "fingerprint"),
    ],
)
def test_single_field_diff_uses_its_action(
    recorded: DaemonBuildIdentity, field: str
) -> None:
    # Set the field under test to 'warn' and the others to 'ignore' so the
    # resolved action is unambiguously that field's. Build via the constructor
    # (not model_copy) so Pydantic coerces the string to the enum member.
    actions = {"version": "ignore", "venv": "ignore", "fingerprint": "ignore"}
    actions[field] = "warn"
    policy = DaemonReusePolicy(**actions)
    decision = decide_reuse(_BASE, recorded, policy, can_spawn=True)
    assert decision.action is DaemonReuseAction.WARN
    assert decision.differing == (field,)


def test_most_severe_action_wins_across_fields() -> None:
    # version and fingerprint both differ; refuse (version) beats warn (fingerprint).
    recorded = _variant(version="9.9.9", fingerprint="b" * 64)
    policy = DaemonReusePolicy(version="refuse", venv="ignore", fingerprint="warn")
    decision = decide_reuse(_BASE, recorded, policy, can_spawn=True)
    assert decision.action is DaemonReuseAction.REFUSE
    assert set(decision.differing) == {"version", "fingerprint"}


def test_restart_degrades_to_refuse_when_cannot_spawn() -> None:
    recorded = _variant(version="9.9.9")
    policy = DaemonReusePolicy(version="restart", venv="restart", fingerprint="warn")
    decision = decide_reuse(_BASE, recorded, policy, can_spawn=False)
    assert decision.action is DaemonReuseAction.REFUSE


def test_restart_kept_when_can_spawn() -> None:
    recorded = _variant(version="9.9.9")
    policy = DaemonReusePolicy(version="restart", venv="restart", fingerprint="warn")
    decision = decide_reuse(_BASE, recorded, policy, can_spawn=True)
    assert decision.action is DaemonReuseAction.RESTART


def test_all_ignore_policy_ignores_a_real_diff() -> None:
    # A field differs, but its configured action is 'ignore', so the decision
    # collapses to IGNORE with the differing field still recorded.
    recorded = _variant(version="9.9.9")
    policy = DaemonReusePolicy(version="ignore", venv="ignore", fingerprint="ignore")
    decision = decide_reuse(_BASE, recorded, policy, can_spawn=True)
    assert decision.action is DaemonReuseAction.IGNORE
    assert decision.differing == ("version",)


def test_reuse_decision_is_frozen() -> None:
    decision = ReuseDecision(action=DaemonReuseAction.IGNORE, differing=())
    with pytest.raises(AttributeError):
        decision.action = DaemonReuseAction.REFUSE  # type: ignore[misc]


# ---------------------------------------------------------------------------
# describe_difference
# ---------------------------------------------------------------------------


def test_describe_version_diff_names_values() -> None:
    recorded = _variant(version="9.9.9")
    detail = describe_difference(_BASE, recorded)
    assert "version" in detail
    assert "daemon=9.9.9" in detail
    assert "cli=1.2.3" in detail


def test_describe_venv_diff_names_values() -> None:
    recorded = _variant(venv="/venv/b")
    detail = describe_difference(_BASE, recorded)
    assert "venv (daemon=/venv/b, cli=/venv/a)" in detail


def test_describe_fingerprint_is_truncated() -> None:
    recorded = _variant(fingerprint="b" * 64)
    detail = describe_difference(_BASE, recorded)
    # The 64-char digest is shortened to its first 12 characters.
    assert "source fingerprint" in detail
    assert "b" * 12 in detail
    assert "b" * 13 not in detail


def test_describe_multiple_fields_joined() -> None:
    recorded = _variant(version="9.9.9", venv="/venv/b")
    detail = describe_difference(_BASE, recorded)
    assert "version" in detail
    assert "venv" in detail
    assert "; " in detail
