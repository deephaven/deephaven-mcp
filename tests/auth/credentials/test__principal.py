"""Tests for deephaven_mcp.auth._principal."""

import pytest

from deephaven_mcp.auth.credentials import Principal


def test_principal_minimal_fields():
    p = Principal(subject="alice", display_name="Alice")
    assert p.subject == "alice"
    assert p.display_name == "Alice"
    assert p.raw == {}


def test_principal_with_raw_claims():
    p = Principal(
        subject="alice",
        display_name="Alice",
        raw={"backend": "psk"},
    )
    assert p.raw == {"backend": "psk"}


def test_principal_is_frozen():
    p = Principal(subject="alice", display_name="Alice")
    with pytest.raises((AttributeError, Exception)):
        p.subject = "bob"  # type: ignore[misc]


def test_principal_default_raw_is_independent_between_instances():
    p1 = Principal(subject="a", display_name="A")
    p2 = Principal(subject="b", display_name="B")
    # default_factory => different dict instances.
    assert p1.raw is not p2.raw
