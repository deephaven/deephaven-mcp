"""Schema-derived inventory of settable configuration fields.

Walks the Pydantic schema of one configuration file kind
(:class:`~deephaven_mcp.config._file_kinds.ConfigFileKind`) and
enumerates every settable field as a :class:`SettableField`: its
wire-format path, JSON type, required/secret flags, scalar default,
and description. Powers the ``dhcli config keys`` verb.

The walk is depth-first over nested models, in schema declaration
order. Model fields marked ``non_wire`` in their
``json_schema_extra`` metadata (loader-injected fields such as
``name``) are omitted. Discriminated unions (outbound credentials)
and open dicts/lists are reported as single ``object`` / ``array``
fields rather than recursed into.
"""

from __future__ import annotations

__all__ = [
    "JsonTypeName",
    "SettableField",
    "settable_fields",
]

import re
import types
from dataclasses import dataclass, replace
from enum import Enum
from typing import Annotated, Literal, TypeGuard, Union, get_args, get_origin

from pydantic import BaseModel, SecretStr

from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind

JsonTypeName = Literal["string", "integer", "number", "boolean", "array", "object"]
"""JSON type name of a settable field's value."""


@dataclass(frozen=True, slots=True)
class SettableField:
    """One settable configuration field of a file kind's schema."""

    path: FieldPath
    """Wire-format path of the field relative to its file (e.g.
    ``FieldPath(("auth", "credentials"))``)."""

    json_type: JsonTypeName
    """JSON type of the field's value."""

    required: bool
    """Whether the field must be supplied: it carries no default and
    every enclosing block is itself required."""

    secret: bool
    """Whether the field's value contains secret material (any
    :class:`pydantic.SecretStr` anywhere in its annotation)."""

    default: str | int | float | bool | None = None
    """The schema default when the field is optional and its default
    is a plain JSON scalar; ``None`` otherwise. ``None`` therefore
    conflates three cases: the field is required, the default is
    itself ``None``, and the default is not a plain scalar."""

    description: str | None = None
    """Plain-text description derived from the field's docstring, or
    ``None`` when the schema declares none."""


def _strip_optional(annotation: object) -> object:
    """Return ``annotation`` with ``Annotated`` metadata and ``| None`` removed.

    Pydantic normally hoists ``Annotated`` metadata into the
    :class:`~pydantic.fields.FieldInfo` before the walk sees it; the
    unwrap here is a defensive layer for any annotation that reaches
    this module still wrapped.

    Args:
        annotation (object): A type annotation, possibly wrapped in
            ``Annotated`` and/or an optional union.

    Returns:
        object: The single non-``None`` member when ``annotation`` is
            a two-member union with ``None`` (itself unwrapped from
            any ``Annotated``); otherwise ``annotation`` unchanged
            apart from the ``Annotated`` unwrap.
    """
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        members = [a for a in get_args(annotation) if a is not type(None)]
        if len(members) == 1:
            return _strip_optional(members[0])
    return annotation


def _as_model_type(annotation: object) -> type[BaseModel] | None:
    """Return the Pydantic model class behind ``annotation``, if any.

    Args:
        annotation (object): A field annotation, possibly optional.

    Returns:
        type[BaseModel] | None: The model class when ``annotation``
            is a model (or an optional model); ``None`` otherwise.
    """
    annotation = _strip_optional(annotation)
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    return None


def _json_type_name(annotation: object) -> JsonTypeName:
    """Return the JSON type name for a leaf field annotation.

    Args:
        annotation (object): The field's type annotation, possibly
            optional.

    Returns:
        JsonTypeName: The JSON type of values the field accepts.
    """
    annotation = _strip_optional(annotation)
    origin = get_origin(annotation)
    if origin is Literal:
        return _json_type_name(type(get_args(annotation)[0]))
    if origin is list:
        return "array"
    if origin is dict:
        return "object"
    if annotation is bool:
        return "boolean"
    if annotation is int:
        return "integer"
    if annotation is float:
        return "number"
    if annotation is str or annotation is SecretStr:
        return "string"
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return _json_type_name(type(next(iter(annotation)).value))
    return "object"


def _contains_secret(annotation: object) -> bool:
    """Return whether ``annotation`` references :class:`pydantic.SecretStr`.

    Recurses through generic arguments (unions, ``Annotated``,
    containers) and into the fields of any Pydantic model class
    reachable from ``annotation``.

    Args:
        annotation (object): A field annotation.

    Returns:
        bool: ``True`` when a :class:`~pydantic.SecretStr` appears
            anywhere within ``annotation``.
    """
    if annotation is SecretStr:
        return True
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return any(
            _contains_secret(field_info.annotation)
            for field_info in annotation.model_fields.values()
        )
    return any(_contains_secret(argument) for argument in get_args(annotation))


def _is_json_scalar(value: object) -> TypeGuard[str | int | float | bool | None]:
    """Return whether ``value`` is a plain JSON scalar.

    Args:
        value (object): A candidate default value.

    Returns:
        TypeGuard[str | int | float | bool | None]: ``True`` for
            ``None``, :class:`str`, :class:`int`,
            :class:`float`, and :class:`bool` values (including
            subclasses such as string-valued enum members, which
            serialize as their scalar value); ``False`` otherwise.
    """
    return value is None or isinstance(value, (str, int, float, bool))


_RST_LITERAL_RE = re.compile(r"``([^`]*)``")
"""Matches reStructuredText double-backtick literals for stripping
into plain text."""


def _to_plain_text(description: str | None) -> str | None:
    """Convert a docstring-derived description to single-line plain text.

    Args:
        description (str | None): The raw field description, possibly
            containing line breaks and reStructuredText literals.

    Returns:
        str | None: The description with whitespace collapsed and
            double-backtick literals stripped to their bare contents;
            ``None`` when ``description`` is ``None``.
    """
    if description is None:
        return None
    collapsed = " ".join(description.split())
    return _RST_LITERAL_RE.sub(r"\1", collapsed)


def _model_settable_fields(model: type[BaseModel]) -> list[SettableField]:
    """Flatten ``model``'s fields into a list of :class:`SettableField`.

    Recurses depth-first into nested model fields, prefixing child
    paths with the field name and demoting ``required`` for children
    of optional blocks. Skips fields marked ``non_wire`` in their
    ``json_schema_extra`` metadata.

    Args:
        model (type[BaseModel]): The schema class to walk.

    Returns:
        list[SettableField]: One entry per settable field, in schema
            declaration order.
    """
    fields: list[SettableField] = []
    for name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra
        if isinstance(extra, dict) and extra.get("non_wire"):
            continue
        path = FieldPath((name,))
        nested_model = _as_model_type(field_info.annotation)
        if nested_model is not None:
            fields.extend(
                replace(
                    child,
                    path=path + child.path,
                    required=child.required and field_info.is_required(),
                )
                for child in _model_settable_fields(nested_model)
            )
            continue
        required = field_info.is_required()
        default = field_info.default if _is_json_scalar(field_info.default) else None
        fields.append(
            SettableField(
                path=path,
                json_type=_json_type_name(field_info.annotation),
                required=required,
                secret=_contains_secret(field_info.annotation),
                default=default,
                description=_to_plain_text(field_info.description),
            )
        )
    return fields


def settable_fields(kind: ConfigFileKind) -> list[SettableField]:
    """Return every settable field of one configuration file kind.

    Args:
        kind (ConfigFileKind): The file kind whose schema to walk.

    Returns:
        list[SettableField]: One entry per settable field, in schema
            declaration order (nested models flattened depth-first),
            with paths in wire format relative to the file.
    """
    return _model_settable_fields(kind.schema)
