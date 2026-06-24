"""Validation of names embedded in Deephaven MCP resource identifiers.

A "resource name" is any string the system embeds into something that must
round-trip through every context it appears in:

- a colon-separated session_id segment (``<type>:<system>:<id>``),
- a Docker container name,
- a Python process tag,
- the filename stem of a config file (``community/sessions/<X>.json``),
- a JSON pointer path,
- a shell argument.

The rule is the same in every case (ASCII alphanumerics plus ``_``, ``.``,
``-``, starting with an alphanumeric, non-empty), so it lives in one place.

This module is a leaf utility at the top of :mod:`deephaven_mcp` so that any
subpackage (sessions, resource_manager, ...) can import it without pulling in
package-init side effects from the others.
"""

from __future__ import annotations

__all__ = ["validate_resource_name"]

import re
from typing import Final

from deephaven_mcp._exceptions import InvalidSessionNameError

_RESOURCE_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"
)
"""Regex defining the resource-name rule.

Allowed: ASCII alphanumerics plus ``_``, ``.``, ``-``. First character must be
alphanumeric (no leading dash, dot, or underscore). Empty strings are rejected. Display
names (e.g. enterprise PQ names from the DHE controller) are NOT subject to
this rule; they travel through verbatim.
"""


def validate_resource_name(value: str, *, field: str) -> str:
    """Return ``value`` unchanged after asserting it is a valid resource name.

    Valid resource names are non-empty ASCII strings drawn from
    ``[A-Za-z0-9_.-]`` and starting with an alphanumeric character.

    Args:
        value: The string to validate.
        field: Human-readable label used in the error message
            (e.g. ``"session_name"``, ``"system"``).

    Returns:
        str: The validated ``value``.

    Raises:
        InvalidSessionNameError: When ``value`` is empty, contains any
            character outside ``[A-Za-z0-9_.-]``, or does not start with an
            alphanumeric (a leading ``-``, ``.``, or ``_`` is rejected).
    """
    if not value:
        raise InvalidSessionNameError(
            f"{field} must be a non-empty string of "
            f"[A-Za-z0-9_.-] starting with a letter or digit; got empty string"
        )
    if not _RESOURCE_NAME_PATTERN.match(value):
        bad_chars = sorted({ch for ch in value if not re.match(r"[A-Za-z0-9_.-]", ch)})
        if bad_chars:
            detail = (
                f"contains illegal character(s) "
                f"{', '.join(repr(c) for c in bad_chars)}"
            )
        else:
            detail = f"must not start with {value[0]!r}"
        raise InvalidSessionNameError(
            f"{field} {value!r}: {detail} "
            f"(allowed: [A-Za-z0-9_.-], must start with a letter or digit)"
        )
    return value
