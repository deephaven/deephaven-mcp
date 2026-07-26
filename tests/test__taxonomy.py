"""Tests for :mod:`deephaven_mcp._taxonomy`.

These tests pin the public surface, the StrEnum string-equality and
JSON-serialization invariants that callers rely on, and the
:class:`SystemRef` NamedTuple's tuple-compatibility contract that
keeps existing positional-unpacking call sites working.
"""

from __future__ import annotations

import json

import pytest

from deephaven_mcp._taxonomy import (
    COMMUNITY_SYSTEM_NAME,
    SessionOrigin,
    SystemRef,
    SystemType,
)

# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


def test_all_lists_every_public_symbol() -> None:
    from deephaven_mcp import _taxonomy

    assert set(_taxonomy.__all__) == {
        "COMMUNITY_SYSTEM_NAME",
        "SessionOrigin",
        "SystemRef",
        "SystemType",
    }


def test_community_system_name_matches_system_type() -> None:
    assert COMMUNITY_SYSTEM_NAME == "community"
    assert COMMUNITY_SYSTEM_NAME == SystemType.COMMUNITY


def test_every_name_in_all_is_resolvable() -> None:
    from deephaven_mcp import _taxonomy

    for name in _taxonomy.__all__:
        assert hasattr(_taxonomy, name), name


# ---------------------------------------------------------------------------
# SystemType
# ---------------------------------------------------------------------------


def test_system_type_values() -> None:
    assert SystemType.COMMUNITY.value == "community"
    assert SystemType.ENTERPRISE.value == "enterprise"


def test_system_type_str_equality_with_lowercase_value() -> None:
    """StrEnum members compare equal to their lowercase value strings."""
    assert SystemType.COMMUNITY == "community"
    assert SystemType.ENTERPRISE == "enterprise"


def test_system_type_str_returns_lowercase_value() -> None:
    """``__str__`` returns the lowercase string value, matching ``.value``."""
    assert str(SystemType.COMMUNITY) == "community"
    assert str(SystemType.ENTERPRISE) == "enterprise"


def test_system_type_fstring_matches_value() -> None:
    """f-string interpolation produces the same lowercase value."""
    assert f"{SystemType.COMMUNITY}" == "community"
    assert f"{SystemType.ENTERPRISE}" == "enterprise"


def test_system_type_serializes_as_lowercase_value() -> None:
    """JSON sees the StrEnum string content, not ``__str__``."""
    assert json.dumps(SystemType.COMMUNITY) == '"community"'
    assert json.dumps(SystemType.ENTERPRISE) == '"enterprise"'


def test_system_type_round_trips_through_json() -> None:
    payload = {"type": SystemType.COMMUNITY}
    decoded = json.loads(json.dumps(payload))
    assert decoded == {"type": "community"}
    assert SystemType(decoded["type"]) is SystemType.COMMUNITY


def test_system_type_construction_from_string() -> None:
    assert SystemType("community") is SystemType.COMMUNITY
    assert SystemType("enterprise") is SystemType.ENTERPRISE
    with pytest.raises(ValueError):
        SystemType("nonsense")


# ---------------------------------------------------------------------------
# SessionOrigin
# ---------------------------------------------------------------------------


def test_origin_values() -> None:
    assert SessionOrigin.STATIC.value == "static"
    assert SessionOrigin.DYNAMIC.value == "dynamic"
    assert SessionOrigin.DISCOVERED.value == "discovered"


def test_origin_str_equality_with_lowercase_value() -> None:
    assert SessionOrigin.STATIC == "static"
    assert SessionOrigin.DYNAMIC == "dynamic"
    assert SessionOrigin.DISCOVERED == "discovered"


def test_origin_serializes_as_lowercase_value() -> None:
    assert json.dumps(SessionOrigin.STATIC) == '"static"'
    assert json.dumps(SessionOrigin.DYNAMIC) == '"dynamic"'
    assert json.dumps(SessionOrigin.DISCOVERED) == '"discovered"'


# ---------------------------------------------------------------------------
# SystemRef
# ---------------------------------------------------------------------------


def test_system_ref_field_access() -> None:
    ref = SystemRef(name="prod", type=SystemType.ENTERPRISE)
    assert ref.name == "prod"
    assert ref.type is SystemType.ENTERPRISE


def test_system_ref_unpacks_positionally() -> None:
    """Backward-compat: existing ``for name, type_str in ...`` keeps working."""
    ref = SystemRef(name="community", type=SystemType.COMMUNITY)
    name, type_str = ref
    assert name == "community"
    assert type_str is SystemType.COMMUNITY


def test_system_ref_compares_equal_to_plain_tuple() -> None:
    """Backward-compat: existing assertions on ``[(name, type), ...]`` keep working."""
    ref = SystemRef(name="community", type=SystemType.COMMUNITY)
    assert ref == ("community", "community")
    assert ref == ("community", SystemType.COMMUNITY)


def test_system_ref_inequality_on_name_or_type() -> None:
    a = SystemRef(name="prod", type=SystemType.ENTERPRISE)
    assert a != SystemRef(name="dev", type=SystemType.ENTERPRISE)
    assert a != SystemRef(name="prod", type=SystemType.COMMUNITY)


def test_system_ref_is_hashable() -> None:
    """NamedTuple instances are hashable; useful for set / dict-key membership tests."""
    refs = {
        SystemRef(name="community", type=SystemType.COMMUNITY),
        SystemRef(name="prod", type=SystemType.ENTERPRISE),
    }
    assert SystemRef(name="prod", type=SystemType.ENTERPRISE) in refs
