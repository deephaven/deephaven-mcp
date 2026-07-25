"""Tests for :mod:`deephaven_mcp.config._fields`."""

from __future__ import annotations

import pytest

from deephaven_mcp._exceptions import (
    ConfigurationFieldMissingError,
    ConfigurationPathError,
)
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._fields import get_field, has_field, set_field, unset_field

# ---------------------------------------------------------------------------
# get_field
# ---------------------------------------------------------------------------


def test_get_field_empty_path_returns_whole_dict() -> None:
    data = {"a": 1}
    assert get_field(data, FieldPath.ROOT) is data


def test_get_field_nested() -> None:
    data = {"a": {"b": {"c": 1}}}
    assert get_field(data, FieldPath(("a", "b", "c"))) == 1


def test_get_field_missing_leaf_raises() -> None:
    with pytest.raises(ConfigurationFieldMissingError, match="a.b is not set"):
        get_field({"a": {}}, FieldPath(("a", "b")))


def test_get_field_missing_intermediate_raises() -> None:
    with pytest.raises(ConfigurationFieldMissingError, match="a is not set"):
        get_field({}, FieldPath(("a", "b")))


def test_get_field_non_object_intermediate_raises() -> None:
    with pytest.raises(ConfigurationPathError, match="not an object"):
        get_field({"a": 1}, FieldPath(("a", "b")))


def test_get_field_non_object_error_is_not_field_missing() -> None:
    # The path-shape error stays the base class so callers can map it
    # to a different error code than field absence.
    with pytest.raises(ConfigurationPathError) as excinfo:
        get_field({"a": 1}, FieldPath(("a", "b")))
    assert not isinstance(excinfo.value, ConfigurationFieldMissingError)


# ---------------------------------------------------------------------------
# has_field
# ---------------------------------------------------------------------------


def test_has_field_empty_path_is_always_present() -> None:
    assert has_field({"a": 1}, FieldPath.ROOT) is True


def test_has_field_present_leaf() -> None:
    assert has_field({"a": {"b": 1}}, FieldPath(("a", "b"))) is True


def test_has_field_missing_leaf() -> None:
    assert has_field({"a": {}}, FieldPath(("a", "b"))) is False


def test_has_field_missing_intermediate() -> None:
    assert has_field({}, FieldPath(("a", "b"))) is False


def test_has_field_non_object_intermediate() -> None:
    assert has_field({"a": 1}, FieldPath(("a", "b"))) is False


# ---------------------------------------------------------------------------
# set_field
# ---------------------------------------------------------------------------


def test_set_field_leaf() -> None:
    out = set_field({"a": 1}, FieldPath(("b",)), 2)
    assert out == {"a": 1, "b": 2}


def test_set_field_does_not_mutate_input() -> None:
    data = {"a": {"b": 1}}
    out = set_field(data, FieldPath(("a", "c")), 2)
    assert data == {"a": {"b": 1}}
    assert out == {"a": {"b": 1, "c": 2}}


def test_set_field_creates_intermediates() -> None:
    out = set_field({}, FieldPath(("a", "b", "c")), 1)
    assert out == {"a": {"b": {"c": 1}}}


def test_set_field_intermediate_not_object_raises() -> None:
    with pytest.raises(ConfigurationPathError, match="not an object"):
        set_field({"a": 1}, FieldPath(("a", "b")), 2)


def test_set_field_intermediate_explicit_null_raises() -> None:
    # An explicit JSON null is present-but-not-an-object, exactly like
    # a scalar; it must not be silently replaced by a fresh object.
    with pytest.raises(ConfigurationPathError, match="not an object"):
        set_field({"a": None}, FieldPath(("a", "b")), 2)


def test_set_field_empty_path_replaces_not_merges() -> None:
    # Assignment semantics: the value replaces the file's contents
    # outright; nothing from the previous contents survives.
    out = set_field({"a": {"x": 1}}, FieldPath.ROOT, {"a": {"y": 2}, "b": 3})
    assert out == {"a": {"y": 2}, "b": 3}


def test_set_field_empty_path_returns_copy_of_value() -> None:
    value = {"a": 1}
    out = set_field({}, FieldPath.ROOT, value)
    assert out == value
    assert out is not value


def test_set_field_empty_path_rejects_scalar() -> None:
    with pytest.raises(ConfigurationPathError, match="whole-file path"):
        set_field({}, FieldPath.ROOT, 5)


# ---------------------------------------------------------------------------
# unset_field
# ---------------------------------------------------------------------------


def test_unset_field_removes_leaf() -> None:
    out = unset_field({"a": {"b": 1, "c": 2}}, FieldPath(("a", "b")))
    assert out == {"a": {"c": 2}}


def test_unset_field_does_not_mutate_input() -> None:
    data = {"a": {"b": 1}}
    unset_field(data, FieldPath(("a", "b")))
    assert data == {"a": {"b": 1}}


def test_unset_field_empty_path_raises() -> None:
    with pytest.raises(ConfigurationPathError, match="whole file"):
        unset_field({"a": 1}, FieldPath.ROOT)


def test_unset_field_missing_leaf_raises() -> None:
    with pytest.raises(ConfigurationFieldMissingError, match="is not set"):
        unset_field({"a": {}}, FieldPath(("a", "b")))


def test_unset_field_missing_intermediate_raises() -> None:
    with pytest.raises(ConfigurationFieldMissingError, match="is not set"):
        unset_field({}, FieldPath(("a", "b")))


def test_unset_field_non_object_intermediate_raises() -> None:
    # Present-but-not-an-object is a path-shape error, not absence.
    with pytest.raises(ConfigurationPathError, match="not an object") as excinfo:
        unset_field({"a": 1}, FieldPath(("a", "b")))
    assert not isinstance(excinfo.value, ConfigurationFieldMissingError)
