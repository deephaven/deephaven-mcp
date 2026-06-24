"""Tests for :mod:`deephaven_mcp.resource_manager._session_id`.

Covers both validated identifier types in this module:

- :class:`SessionId` construction and validation against the
  resource-name character class, str-subclass behaviors, and
  :meth:`SessionId.from_int`.
- :class:`QualifiedSessionId` — a frozen, slotted dataclass with three
  validated fields: construction from components (the dataclass
  ``__init__``), parsing via :meth:`from_str`, :meth:`__str__`
  wire-form rendering, frozen-instance immutability, value equality,
  and hashability.
"""

from __future__ import annotations

import json

import pytest

from deephaven_mcp._exceptions import InvalidSessionNameError
from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.resource_manager._session_id import (
    QualifiedSessionId,
    SessionId,
)

# ---------------------------------------------------------------------------
# SessionId construction and validation
# ---------------------------------------------------------------------------


def test_session_id_constructs_from_valid_string() -> None:
    sid = SessionId("my_worker")
    assert sid == "my_worker"
    assert isinstance(sid, SessionId)
    assert isinstance(sid, str)


def test_session_id_accepts_alphanumeric_and_punctuation() -> None:
    for value in ("a", "0", "A0", "worker_1", "my.session", "name-with-dash"):
        sid = SessionId(value)
        assert sid == value


def test_session_id_accepts_purely_digit_string() -> None:
    """Enterprise serials stringify to digit-only ids; must round-trip."""
    sid = SessionId("42")
    assert sid == "42"


def test_session_id_rejects_empty() -> None:
    with pytest.raises(InvalidSessionNameError):
        SessionId("")


def test_session_id_rejects_colon() -> None:
    """Colons would break qualified_session_id parsing — must be rejected."""
    with pytest.raises(InvalidSessionNameError):
        SessionId("a:b")


def test_session_id_rejects_space() -> None:
    with pytest.raises(InvalidSessionNameError):
        SessionId("has space")


def test_session_id_rejects_non_ascii() -> None:
    with pytest.raises(InvalidSessionNameError):
        SessionId("worker\u00e9")


def test_session_id_rejects_leading_underscore() -> None:
    """Resource-name pattern requires an alphanumeric leading character."""
    with pytest.raises(InvalidSessionNameError):
        SessionId("_leading")


def test_session_id_rejects_leading_dot() -> None:
    with pytest.raises(InvalidSessionNameError):
        SessionId(".dot")


def test_session_id_idempotent_on_session_id_input() -> None:
    inner = SessionId("worker")
    outer = SessionId(inner)
    # `type(value) is cls` short-circuit returns same object.
    assert outer is inner


# ---------------------------------------------------------------------------
# str-subclass behaviors
# ---------------------------------------------------------------------------


def test_session_id_equals_plain_str() -> None:
    assert SessionId("worker") == "worker"
    assert "worker" == SessionId("worker")


def test_session_id_isinstance_str() -> None:
    assert isinstance(SessionId("worker"), str)


def test_session_id_isinstance_discriminates() -> None:
    """A plain str is not a SessionId, even though SessionId is a str."""
    assert not isinstance("worker", SessionId)


def test_session_id_hashes_like_str() -> None:
    assert hash(SessionId("worker")) == hash("worker")


def test_session_id_usable_as_dict_key_and_finds_str() -> None:
    d: dict[str, str] = {SessionId("worker"): "found"}
    assert d["worker"] == "found"


def test_session_id_usable_in_set_and_finds_str() -> None:
    s: set[str] = {SessionId("worker")}
    assert "worker" in s


def test_session_id_f_string_formats_as_str() -> None:
    assert f"x:{SessionId('worker')}" == "x:worker"
    assert f"x:{SessionId('42')}" == "x:42"


def test_session_id_json_serializes_as_str() -> None:
    """SessionId is a str subclass — json.dumps emits it as a JSON string."""
    assert json.dumps(SessionId("worker")) == '"worker"'


# ---------------------------------------------------------------------------
# SessionId.from_int
# ---------------------------------------------------------------------------


def test_from_int_happy_path() -> None:
    sid = SessionId.from_int(42)
    assert sid == "42"
    assert isinstance(sid, SessionId)


def test_from_int_zero() -> None:
    sid = SessionId.from_int(0)
    assert sid == "0"


def test_from_int_large_value() -> None:
    """No u64 ceiling — controller serials can be any non-negative int."""
    big = 2**100
    sid = SessionId.from_int(big)
    assert sid == str(big)


def test_from_int_rejects_negative() -> None:
    with pytest.raises(InvalidSessionNameError):
        SessionId.from_int(-1)


def test_from_int_round_trip() -> None:
    """``SessionId.from_int(int(str(sid)))`` returns an equal SessionId."""
    sid = SessionId.from_int(42)
    again = SessionId.from_int(int(str(sid)))
    assert again == sid


def test_from_int_accepts_int_subclass() -> None:
    """The factory accepts arbitrary int-likes (e.g. CorePlusQuerySerial)."""

    class _IntLike(int):
        pass

    sid = SessionId.from_int(_IntLike(123))
    assert sid == "123"


# ---------------------------------------------------------------------------
# QualifiedSessionId — construction from components
# ---------------------------------------------------------------------------


def test_construct_community() -> None:
    qsid = QualifiedSessionId(SystemType.COMMUNITY, "community", SessionId("my_worker"))
    assert qsid.system_type is SystemType.COMMUNITY
    assert qsid.system_name == "community"
    assert qsid.session_id == SessionId("my_worker")


def test_construct_enterprise() -> None:
    qsid = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId.from_int(42))
    assert qsid.system_type is SystemType.ENTERPRISE
    assert qsid.system_name == "prod"
    assert qsid.session_id == SessionId("42")


def test_construct_rejects_invalid_system_name() -> None:
    """``__post_init__`` validates ``system_name`` via ``validate_resource_name``."""
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId(SystemType.COMMUNITY, "bad system", SessionId.from_int(1))


def test_construct_accepts_allowed_punctuation_in_system_name() -> None:
    qsid = QualifiedSessionId(
        SystemType.ENTERPRISE, "test-env_v2", SessionId.from_int(2841957462)
    )
    assert qsid.system_name == "test-env_v2"


# ---------------------------------------------------------------------------
# QualifiedSessionId.from_str — parse the wire string
# ---------------------------------------------------------------------------


def test_from_str_community_id() -> None:
    qsid = QualifiedSessionId.from_str("community:community:my_worker")
    assert qsid.system_type is SystemType.COMMUNITY
    assert qsid.system_name == "community"
    assert qsid.session_id == SessionId("my_worker")


def test_from_str_enterprise_id() -> None:
    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    assert qsid.system_type is SystemType.ENTERPRISE
    assert qsid.system_name == "prod"
    assert qsid.session_id == SessionId("42")


def test_from_str_rejects_too_few_segments() -> None:
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str("enterprise:prod")


def test_from_str_rejects_extra_colons_in_session_id() -> None:
    """``split(':', 2)`` produces a 3-tuple, but the suffix would then fail
    :class:`SessionId` validation, so the construction still fails."""
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str("enterprise:prod:has:extra")


def test_from_str_rejects_empty_string() -> None:
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str("")


def test_from_str_rejects_empty_segments() -> None:
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str("enterprise::42")
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str(":prod:42")
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str("enterprise:prod:")


def test_from_str_rejects_unknown_system_type() -> None:
    with pytest.raises(InvalidSessionNameError) as exc:
        QualifiedSessionId.from_str("docs:central:1")
    msg = str(exc.value)
    assert "system_type segment" in msg
    assert "'docs'" in msg


def test_from_str_rejects_invalid_system_segment() -> None:
    """Space in the system_name segment fails resource-name validation."""
    with pytest.raises(InvalidSessionNameError) as exc:
        QualifiedSessionId.from_str("enterprise:bad system:1")
    assert "enterprise:bad system:1" in str(exc.value)


def test_from_str_rejects_invalid_session_id_segment() -> None:
    """Space in the trailing segment fails :class:`SessionId` validation."""
    with pytest.raises(InvalidSessionNameError) as exc:
        QualifiedSessionId.from_str("enterprise:prod:has space")
    assert "enterprise:prod:has space" in str(exc.value)


def test_from_str_rejects_session_id_with_leading_underscore() -> None:
    with pytest.raises(InvalidSessionNameError):
        QualifiedSessionId.from_str("enterprise:prod:_leading")


# ---------------------------------------------------------------------------
# Wire-form rendering via ``__str__``
# ---------------------------------------------------------------------------


def test_str_community() -> None:
    qsid = QualifiedSessionId(SystemType.COMMUNITY, "community", SessionId("my_worker"))
    assert str(qsid) == "community:community:my_worker"


def test_str_enterprise() -> None:
    qsid = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId.from_int(42))
    assert str(qsid) == "enterprise:prod:42"


def test_f_string_renders_wire_form() -> None:
    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    assert f"id={qsid}" == "id=enterprise:prod:42"


# ---------------------------------------------------------------------------
# Round-trip between component and wire forms
# ---------------------------------------------------------------------------


def test_round_trip_components_to_wire_and_back() -> None:
    sid = SessionId("my_worker")
    built = QualifiedSessionId(SystemType.COMMUNITY, "community", sid)
    parsed = QualifiedSessionId.from_str(str(built))
    assert parsed == built
    assert parsed.session_id == sid


def test_round_trip_wire_to_components_and_back() -> None:
    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    rebuilt = QualifiedSessionId(qsid.system_type, qsid.system_name, qsid.session_id)
    assert rebuilt == qsid


# ---------------------------------------------------------------------------
# Frozen dataclass semantics
# ---------------------------------------------------------------------------


def test_frozen_rejects_field_mutation() -> None:
    """``@dataclass(frozen=True)`` raises on attempts to mutate fields."""
    from dataclasses import FrozenInstanceError

    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    with pytest.raises(FrozenInstanceError):
        qsid.system_name = "other"  # type: ignore[misc]


def test_slots_reject_new_attribute() -> None:
    """``slots=True`` excludes ``__dict__``; arbitrary attribute writes fail."""
    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    assert not hasattr(qsid, "__dict__")
    with pytest.raises((AttributeError, Exception)):
        qsid.unknown_attr = "x"  # type: ignore[attr-defined]


def test_equality_is_value_based() -> None:
    a = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId("42"))
    b = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId("42"))
    assert a == b
    assert a is not b


def test_inequality_on_different_components() -> None:
    a = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId("42"))
    b = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId("43"))
    c = QualifiedSessionId(SystemType.ENTERPRISE, "stage", SessionId("42"))
    d = QualifiedSessionId(SystemType.COMMUNITY, "prod", SessionId("42"))
    assert a != b
    assert a != c
    assert a != d


def test_not_equal_to_plain_str() -> None:
    """A :class:`QualifiedSessionId` is not a :class:`str`; equality with
    a wire-form string is False."""
    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    assert qsid != "enterprise:prod:42"


def test_hashable_and_keys_by_value() -> None:
    a = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId("42"))
    b = QualifiedSessionId(SystemType.ENTERPRISE, "prod", SessionId("42"))
    d: dict[QualifiedSessionId, str] = {a: "found"}
    assert d[b] == "found"


def test_not_isinstance_str() -> None:
    qsid = QualifiedSessionId.from_str("enterprise:prod:42")
    assert not isinstance(qsid, str)


# ---------------------------------------------------------------------------
# Explicit JSON serialization at the boundary
# ---------------------------------------------------------------------------


def test_json_dumps_requires_stringify() -> None:
    """A bare :class:`QualifiedSessionId` is not JSON-serializable; the
    boundary must use ``str(qsid)``."""
    qsid = QualifiedSessionId.from_str("community:community:my_worker")
    with pytest.raises(TypeError):
        json.dumps(qsid)
    assert json.dumps(str(qsid)) == '"community:community:my_worker"'
