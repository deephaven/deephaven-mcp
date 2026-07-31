"""Nested field access over raw (wire-format) configuration dicts.

Complements :mod:`deephaven_mcp.config._logical_paths` (which resolves a
logical path to *which file* and *which field path within it*) with
the mechanics of reading, setting, and unsetting a value at that field
path inside the file's raw ``dict``. Used by the ``dhcli config
get/set/unset`` verbs.
"""

from __future__ import annotations

__all__ = [
    "get_field",
    "has_field",
    "set_field",
    "unset_field",
]

from typing import Any

from deephaven_mcp._exceptions import (
    ConfigurationFieldMissingError,
    ConfigurationPathError,
)
from deephaven_mcp.config._field_path import FieldPath


def get_field(data: dict[str, Any], field_path: FieldPath) -> Any:
    """Return the value at ``field_path`` within ``data``.

    See :func:`has_field` for the non-raising counterpart, when
    presence itself (not the value) is the question.

    Args:
        data (dict[str, Any]): The raw (unexpanded) file contents.
        field_path (FieldPath): Field path within the file; empty
            returns ``data`` itself.

    Returns:
        Any: The value found at ``field_path``.

    Raises:
        ConfigurationFieldMissingError: When any segment is absent.
        ConfigurationPathError: When an intermediate segment's value
            is not a JSON object.
    """
    node: Any = data
    walked: list[str] = []
    for segment in field_path:
        if not isinstance(node, dict):
            raise ConfigurationPathError(
                f"{FieldPath(walked)} is not an object; cannot descend into "
                f"{segment!r}"
            )
        if segment not in node:
            raise ConfigurationFieldMissingError(
                f"{FieldPath((*walked, segment))} is not set"
            )
        node = node[segment]
        walked.append(segment)
    return node


def has_field(data: dict[str, Any], field_path: FieldPath) -> bool:
    """Return whether a value is set at ``field_path`` within ``data``.

    The non-raising counterpart to :func:`get_field`.

    Args:
        data (dict[str, Any]): The raw (unexpanded) file contents.
        field_path (FieldPath): Field path to check; empty is always
            present (it names ``data`` itself).

    Returns:
        bool: Whether a value is present at ``field_path``.
    """
    try:
        get_field(data, field_path)
    except ConfigurationPathError:
        return False
    return True


def set_field(
    data: dict[str, Any], field_path: FieldPath, value: Any
) -> dict[str, Any]:
    """Return a copy of ``data`` with ``value`` set at ``field_path``.

    Intermediate objects are created as needed. When ``field_path`` is
    empty, ``value`` (which must be a dict) **replaces** ``data``
    outright — assignment semantics, the same as at any other depth,
    never a merge.

    Args:
        data (dict[str, Any]): The raw (unexpanded) file contents.
            Not mutated.
        field_path (FieldPath): Field path to set.
        value (Any): The value to assign.

    Returns:
        dict[str, Any]: A new dict with the value set.

    Raises:
        ConfigurationPathError: When ``field_path`` is empty and ``value`` is
            not a dict, or an intermediate segment's existing value
            (including an explicit JSON ``null``) is not a JSON object.
    """
    if not field_path:
        if not isinstance(value, dict):
            raise ConfigurationPathError(
                "cannot set a non-object value at the whole-file path; set "
                "a specific field instead"
            )
        return dict(value)
    out = dict(data)
    node = out
    walked: list[str] = []
    for segment in field_path.parent:
        if segment not in node:
            existing: dict[str, Any] = {}
        elif not isinstance(node[segment], dict):
            raise ConfigurationPathError(
                f"{FieldPath((*walked, segment))} is not an object; cannot "
                "set a field inside it"
            )
        else:
            existing = dict(node[segment])
        node[segment] = existing
        node = existing
        walked.append(segment)
    node[field_path.last] = value
    return out


def unset_field(data: dict[str, Any], field_path: FieldPath) -> dict[str, Any]:
    """Return a copy of ``data`` with the value at ``field_path`` removed.

    Args:
        data (dict[str, Any]): The raw (unexpanded) file contents.
            Not mutated.
        field_path (FieldPath): Field path to remove; must be
            non-empty (removing the whole file is not this function's
            job — use ``config session/system remove``).

    Returns:
        dict[str, Any]: A new dict with the field removed.

    Raises:
        ConfigurationFieldMissingError: When an intermediate segment or
            the final segment is absent.
        ConfigurationPathError: When ``field_path`` is empty, or an
            intermediate segment's value is not a JSON object.
    """
    if not field_path:
        raise ConfigurationPathError(
            "cannot unset the whole file; remove the entity instead"
        )
    out = dict(data)
    node = out
    walked: list[str] = []
    for segment in field_path.parent:
        if segment not in node:
            raise ConfigurationFieldMissingError(
                f"{FieldPath((*walked, segment))} is not set"
            )
        if not isinstance(node[segment], dict):
            raise ConfigurationPathError(
                f"{FieldPath((*walked, segment))} is not an object; cannot "
                "unset a field inside it"
            )
        existing = dict(node[segment])
        node[segment] = existing
        node = existing
        walked.append(segment)
    last = field_path.last
    if last not in node:
        raise ConfigurationFieldMissingError(f"{field_path} is not set")
    del node[last]
    return out
