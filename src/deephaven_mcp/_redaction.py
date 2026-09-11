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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import NamedTuple, NoReturn

__all__ = [
    "REDACTED",
    "AllowedField",
    "Redaction",
    "UNPARSEABLE",
    "parse_json_strict",
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

    secret_when: Callable[[object], bool] | None = None
    """Decides, from the stored value, whether to withhold it, yielding ``REDACTED``
    when it returns True. ``None`` reports the value whenever it holds :attr:`type`.
    Runs before that type check and receives the value exactly as parsed, so it must
    fail closed on a type it does not expect."""


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


def _reject_json_constant(constant: str) -> NoReturn:
    """Reject a constant Python's JSON parser accepts but the JSON spec does not.

    Args:
        constant (str): The token matched - ``NaN``, ``Infinity``, or ``-Infinity``.

    Raises:
        ValueError: Always.
    """
    raise ValueError(f"JSON does not allow the constant {constant}")


def parse_json_strict(text: str) -> object:
    """Parse JSON text, refusing what the JSON spec does not allow.

    Stricter than :func:`json.loads` in two ways, each of which would otherwise
    let a redactor built on it report something misleading:

    - A repeated object key is rejected. ``json.loads`` keeps only the last value,
      so a redactor would drop the others while the raw text still carries them.
    - ``NaN``/``Infinity``/``-Infinity`` are rejected. ``json.loads`` accepts these
      Python extensions and ``json.dumps`` writes them back, so a redacted value
      would not itself be valid JSON.

    Args:
        text (str): JSON text to parse.

    Returns:
        object: The parsed value.

    Raises:
        ValueError: When ``text`` is not valid JSON, an object in it repeats a key,
            or it holds a non-standard constant. ``json.JSONDecodeError`` is a
            subclass.
    """
    return json.loads(
        text,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )


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
        parsed = parse_json_strict(json_str)
    except ValueError:
        _LOGGER.warning(
            "[_redaction:redact_json_sensitive_fields] Suppressing JSON field: "
            "not parseable as strict JSON"
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
    key, or an unexpected type cannot carry a value through. A field whose
    ``secret_when`` predicate accepts the stored value is reported as ``REDACTED``
    instead, the predicate running before the type check.

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
        parsed = parse_json_strict(stored)
    except ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        _LOGGER.warning(
            f"[_redaction:project_json_fields] Suppressing {label}: not a JSON "
            "object, or not parseable as strict JSON"
        )
        return Redaction(UNPARSEABLE, True)
    projected: dict[str, object] = {}
    withheld = False
    for field in fields:
        if field.name not in parsed:
            continue
        value = parsed[field.name]
        if field.secret_when is not None and field.secret_when(value):
            projected[field.name] = REDACTED
            withheld = True
        elif isinstance(value, field.type):
            projected[field.name] = value
    # A stored key absent from the projection was dropped rather than reported.
    withheld = withheld or set(parsed) != set(projected)
    return Redaction(json.dumps(projected), withheld)
