"""Schema-guided redaction of raw, not-yet-validated configuration data.

``dhcli config get`` prints what is actually on disk, and must keep
working on a tree too broken to load — so it cannot use
:func:`~deephaven_mcp._pydantic.dump_redacted`, which needs a validated
model instance. :func:`redact_raw` fills that gap using the field
inventory that
:func:`~deephaven_mcp.config._settable_fields.settable_fields` already
derives from the same schemas, so ``dhcli config keys`` and ``dhcli
config get`` agree on which fields are secret.

Three rules define what it redacts:

- **Redaction starts at the deepest secret field.**
  :attr:`~deephaven_mcp.config._settable_fields.SettableField.secret`
  is annotation-wide, so an enclosing block is flagged alongside the
  field that makes it secret; starting at the deepest flagged path
  keeps the surrounding structure visible.
- **Within that field, each value is replaced but every key survives,
  as does the ``type`` discriminator.** Which *kind* of credential is
  configured is a diagnostic, not a secret, so an ``auth`` block
  renders as ``{"credentials": {"type": "psk", "token":
  "[REDACTED]"}}``. An anonymous block carries no secret at all and so
  passes through untouched and uncounted.
- **A bare templating reference is left verbatim.** ``"${env:DH_PSK}"``
  names an environment variable and discloses nothing, so redacting it
  would cost the diagnostic that matters most (is this field wired to
  the variable I think it is?) and buy no secrecy. A literal in the
  same field *is* the secret and is replaced.

A key the schema does not declare is left verbatim: it cannot be a
schema secret, and it is exactly what an operator runs ``config get``
to find.
"""

from __future__ import annotations

__all__ = [
    "RawRedaction",
    "redact_raw",
]

from dataclasses import dataclass
from functools import cache
from typing import Any

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._settable_fields import settable_fields
from deephaven_mcp.config._templating import is_single_placeholder

_DISCRIMINATOR = "type"
"""Key tagging which member of a discriminated union applies. It names
the *kind* of credential, never the credential, so it survives
redaction — without it an auth block reduces to an opaque blob."""


@dataclass(frozen=True, slots=True)
class RawRedaction:
    """Outcome of one :func:`redact_raw` call."""

    value: Any
    """The input value with every secret-bearing field replaced by
    :data:`~deephaven_mcp._redaction.REDACTED`. Shares unmodified
    sub-objects with the input; the input itself is never mutated."""

    count: int
    """How many values were replaced. ``0`` means nothing was redacted,
    so revealing the value would disclose nothing."""


@cache
def _redaction_points(kind: ConfigFileKind) -> frozenset[FieldPath]:
    """Return the deepest secret field paths of one file kind's schema.

    Cached because the result depends only on the schema, which is
    fixed at import time.

    Args:
        kind (ConfigFileKind): The file kind whose schema to inspect.

    Returns:
        frozenset[FieldPath]: Every path flagged secret that has no
            secret descendant, in wire format relative to the file.
    """
    secret = {field.path for field in settable_fields(kind) if field.secret}
    return frozenset(
        path
        for path in secret
        if not any(other != path and other.has_prefix(path) for other in secret)
    )


def redact_raw(
    kind: ConfigFileKind,
    value: Any,
    *,
    at: FieldPath = FieldPath.ROOT,
) -> RawRedaction:
    """Redact the secret-bearing fields of a raw configuration value.

    Args:
        kind (ConfigFileKind): The file kind ``value`` came from. Used
            for its schema-derived field inventory only; nothing is
            validated, so ``value`` need not be valid.
        value (Any): The raw JSON-parsed value: a whole file's mapping,
            a subtree, or a single scalar.
        at (FieldPath): Where ``value`` sits within the file, in wire
            format. :attr:`~deephaven_mcp.config._field_path.FieldPath.ROOT`
            (the default) means ``value`` is the whole file.

    Returns:
        RawRedaction: The redacted value and the number of values
            replaced.
    """
    return _redact(_redaction_points(kind), at, value)


def _redact(points: frozenset[FieldPath], path: FieldPath, value: Any) -> RawRedaction:
    """Redact ``value``, sitting at ``path``, against the redaction points.

    Args:
        points (frozenset[FieldPath]): The deepest secret field paths.
        path (FieldPath): Where ``value`` sits within its file.
        value (Any): The raw value to redact.

    Returns:
        RawRedaction: The redacted value and the replacement count.
    """
    # ``has_prefix`` is true for the point itself and for anything under
    # it, so addressing a field *inside* a secret block (``config get
    # ...auth.credentials.token``) cannot slip past the check.
    if any(path.has_prefix(point) for point in points):
        return _scrub(value)
    if isinstance(value, dict):
        return _redact_mapping(points, path, value)
    # A list is opaque to the field inventory: any secret inside one is
    # flagged at the list's own path, handled above. Reaching here means
    # nothing within can be a schema secret.
    return RawRedaction(value, 0)


def _scrub(value: Any) -> RawRedaction:
    """Replace the disclosing material inside one secret field's value.

    Args:
        value (Any): The raw value sitting at a redaction point.

    Returns:
        RawRedaction: The scrubbed value and the replacement count.
            ``0`` means the field held nothing disclosing — an
            anonymous credentials block, or a value that only
            references a secret stored elsewhere.
    """
    if isinstance(value, str):
        # A lone ``${...}`` reference points at the secret rather than
        # being it, so it survives.
        if is_single_placeholder(value):
            return RawRedaction(value, 0)
        return RawRedaction(REDACTED, 1)
    if isinstance(value, dict):
        scrubbed: dict[str, Any] = {}
        count = 0
        for key, item in value.items():
            if key == _DISCRIMINATOR:
                # Which kind of credential this is, never the credential
                # itself. Keeping it is the difference between a useful
                # and a useless view of an auth block.
                scrubbed[key] = item
                continue
            outcome = _scrub(item)
            scrubbed[key] = outcome.value
            count += outcome.count
        return RawRedaction(scrubbed, count)
    if isinstance(value, list):
        results = [_scrub(item) for item in value]
        return RawRedaction(
            [result.value for result in results],
            sum(result.count for result in results),
        )
    # A non-string scalar at a secret field is invalid data that would
    # still disclose whatever it holds, so it fails closed.
    return RawRedaction(REDACTED, 1)


def _redact_mapping(
    points: frozenset[FieldPath], path: FieldPath, data: dict[str, Any]
) -> RawRedaction:
    """Redact each key of a raw mapping.

    Args:
        points (frozenset[FieldPath]): The deepest secret field paths.
        path (FieldPath): Where ``data`` sits within its file.
        data (dict[str, Any]): The raw mapping.

    Returns:
        RawRedaction: A new mapping with the same key order, and the
            replacement count.
    """
    redacted: dict[str, Any] = {}
    count = 0
    for key, item in data.items():
        outcome = _redact(points, path + key, item)
        redacted[key] = outcome.value
        count += outcome.count
    return RawRedaction(redacted, count)
