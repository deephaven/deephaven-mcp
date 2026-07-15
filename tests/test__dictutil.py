"""Tests for ``deephaven_mcp._dictutil``."""

from __future__ import annotations

from deephaven_mcp._dictutil import deep_merge


def test_flat_override_wins():
    assert deep_merge({"a": 1, "b": 2}, {"b": 3}) == {"a": 1, "b": 3}


def test_nested_dicts_merge_key_by_key():
    base = {"a": {"x": 1, "y": 2}, "b": 5}
    override = {"a": {"y": 20, "z": 30}}
    assert deep_merge(base, override) == {"a": {"x": 1, "y": 20, "z": 30}, "b": 5}


def test_non_dict_value_replaces_dict_outright():
    assert deep_merge({"a": {"x": 1}}, {"a": 7}) == {"a": 7}


def test_dict_value_replaces_non_dict_outright():
    assert deep_merge({"a": 7}, {"a": {"x": 1}}) == {"a": {"x": 1}}


def test_new_keys_are_added():
    assert deep_merge({}, {"a": {"b": 1}}) == {"a": {"b": 1}}


def test_empty_override_is_identity():
    base = {"a": {"x": 1}}
    assert deep_merge(base, {}) == base


def test_inputs_are_not_mutated():
    base = {"a": {"x": 1}}
    override = {"a": {"y": 2}}
    result = deep_merge(base, override)
    assert base == {"a": {"x": 1}}
    assert override == {"a": {"y": 2}}
    # The merged nested dict is a new object, not the base's.
    assert result["a"] is not base["a"]
