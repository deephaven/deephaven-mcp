"""Redaction of sensitive values from user-facing output.

This module owns redaction for ``deephaven_mcp``: the textual markers
substituted for a withheld value, and the JSON redactor built on them.
A marker appears when a sensitive value (auth token, password, private
key, PSK, etc.) has been stripped from logs, config dumps, or object
representations, or when a value could not be parsed well enough to
redact and so was suppressed wholesale.

Keeping the markers and the redactor together means every redaction site
(config redactors, launcher command scrubbing, MCP tool JSON redaction,
credential ``__repr__`` methods) produces the same output format, and any
future change to a marker is a one-line edit.

A redactor here reports what it withheld rather than leaving callers to
infer it by comparing its output against the input: see :class:`Redaction`.
Two are offered, differing in what they know about the document:
:func:`redact_json_sensitive_fields` scrubs values by key name in a
document of unknown shape, while :func:`project_json_fields` rebuilds a
document of known shape from a declared allowlist.

Consumers should import these names rather than hard-coding the literal
marker strings, except in test assertions where hard-coding the literal
is preferred so the test fails loudly if a canonical value ever
changes.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import NamedTuple

__all__ = [
    "REDACTED",
    "AllowedField",
    "Redaction",
    "UNPARSEABLE",
    "parse_json_no_duplicate_keys",
    "project_json_fields",
    "redact_json_sensitive_fields",
]

_LOGGER = logging.getLogger(__name__)


REDACTED: str = "[REDACTED]"
"""Canonical placeholder substituted for any sensitive value in logs,
config dumps, or ``__repr__`` output. The bracketed uppercase form is
the widely-recognized convention in ops tooling and log aggregators."""

UNPARSEABLE: str = "[UNPARSEABLE]"
"""Canonical placeholder substituted for a whole value that could not be
parsed, and so was suppressed rather than selectively redacted: an
unparseable value may hide a secret anywhere in it."""

_SENSITIVE_JSON_KEYS: frozenset[str] = frozenset(
    {"password", "passwd", "token", "secret", "api_key", "apikey", "api_secret"}
)
"""JSON object keys whose values are redacted in nested-JSON output."""


class Redaction(NamedTuple):
    """A redacted value together with what producing it cost."""

    text: str | None
    """The value to report, or ``None`` when the input held nothing."""

    withheld: bool
    """True when redaction replaced, dropped, or suppressed content."""


@dataclass(frozen=True)
class AllowedField:
    """One key an allowlist projection is permitted to report."""

    name: str
    """The JSON object key."""

    type: type
    """The type the value must hold; a value of any other type is dropped."""

    secret: bool = False
    """When True the value is never reported: presence of the key alone yields
    ``REDACTED``, whatever the value turns out to be."""


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build an object from ``pairs``, refusing one that repeats a key.

    Args:
        pairs (list[tuple[str, object]]): Key/value pairs as parsed, before
            collapsing.

    Returns:
        dict[str, object]: The object, when every key is distinct.

    Raises:
        ValueError: When a key repeats.
    """
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("JSON object repeats a key")
    return dict(pairs)


def parse_json_no_duplicate_keys(text: str) -> object:
    """Parse JSON text, rejecting any object that repeats a key.

    ``json.loads`` keeps only the last value for a repeated key, so a redactor
    built on it drops the others while the raw text still carries them.

    Args:
        text (str): JSON text to parse.

    Returns:
        object: The parsed value.

    Raises:
        ValueError: When ``text`` is not valid JSON, or an object in it repeats
            a key. ``json.JSONDecodeError`` is a subclass.
    """
    return json.loads(text, object_pairs_hook=_reject_duplicate_keys)


def _redact_recursive(obj: object) -> tuple[object, bool]:
    """Redact values of sensitive keys, reporting whether anything was replaced.

    Args:
        obj (object): A parsed JSON value.

    Returns:
        tuple[object, bool]: The redacted value, and True when at least one
            sensitive value was replaced anywhere within it.
    """
    if isinstance(obj, dict):
        redacted: dict[str, object] = {}
        withheld = False
        for key, value in obj.items():
            if key.lower() in _SENSITIVE_JSON_KEYS:
                redacted[key] = REDACTED
                withheld = True
            else:
                redacted[key], nested = _redact_recursive(value)
                withheld = withheld or nested
        return redacted, withheld
    if isinstance(obj, list):
        items: list[object] = []
        withheld = False
        for item in obj:
            value, nested = _redact_recursive(item)
            items.append(value)
            withheld = withheld or nested
        return items, withheld
    return obj, False


def redact_json_sensitive_fields(json_str: str | None) -> Redaction:
    """Parse a JSON string and redact values whose keys match known-sensitive names.

    Args:
        json_str (str | None): The JSON string to scan, or ``None``.

    Returns:
        Redaction: ``text`` is ``None`` for empty/``None`` input, ``UNPARSEABLE``
            (with a warning log) when the string is not valid JSON or repeats an
            object key, and otherwise a re-serialized JSON string with sensitive
            values replaced by ``[REDACTED]``. ``withheld`` reports whether any
            content was replaced or suppressed.
    """
    if not json_str:
        return Redaction(None, False)
    try:
        parsed = parse_json_no_duplicate_keys(json_str)
    except ValueError:
        _LOGGER.warning(
            "[_redaction:redact_json_sensitive_fields] Suppressing JSON field: "
            "not valid JSON, or an object in it repeats a key"
        )
        return Redaction(UNPARSEABLE, True)
    redacted, withheld = _redact_recursive(parsed)
    return Redaction(json.dumps(redacted), withheld)


def project_json_fields(
    stored: str, fields: Sequence[AllowedField], *, label: str
) -> Redaction:
    """Rebuild a stored JSON object from an allowlist of fields.

    The complement of :func:`redact_json_sensitive_fields`, which removes known-bad
    keys: this reports only known-good ones. The result is constructed from
    ``fields`` rather than edited from ``stored``, so an unknown key, a repeated
    key, or an unexpected type cannot carry a value through. A secret field is
    reported as ``REDACTED`` on presence alone, its value never inspected.

    Args:
        stored (str): The raw value read from the source.
        fields (Sequence[AllowedField]): The keys that may be reported, in
            output order.
        label (str): Name of the field being projected, for the suppression log.

    Returns:
        Redaction: ``text`` is a JSON object holding only the allowed keys, or
            ``UNPARSEABLE`` when ``stored`` is not a JSON object or repeats a key.
            ``withheld`` reports whether the projection lost anything ``stored``
            held.
    """
    try:
        parsed = parse_json_no_duplicate_keys(stored)
    except ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        _LOGGER.warning(
            f"[_redaction:project_json_fields] Suppressing {label}: not a JSON "
            "object, or an object in it repeats a key"
        )
        return Redaction(UNPARSEABLE, True)
    projected: dict[str, object] = {}
    withheld = False
    for field in fields:
        if field.name not in parsed:
            continue
        if field.secret:
            projected[field.name] = REDACTED
            withheld = True
        elif isinstance(parsed[field.name], field.type):
            projected[field.name] = parsed[field.name]
    # A stored key absent from the projection was dropped rather than reported.
    withheld = withheld or set(parsed) != set(projected)
    return Redaction(json.dumps(projected), withheld)
