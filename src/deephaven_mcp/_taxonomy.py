"""Classification vocabulary for systems and sessions.

Defines :class:`SystemType` (community vs enterprise),
:class:`SessionOrigin` (static / dynamic / discovered), and
:class:`SystemRef` (a ``(name, type)`` pair).
"""

from __future__ import annotations

import enum
from typing import NamedTuple

__all__ = [
    "SessionOrigin",
    "SystemRef",
    "SystemType",
]


class SystemType(enum.StrEnum):
    """Kind of Deephaven backend a session connects to.

    String inheritance: as a :class:`enum.StrEnum`, members compare
    equal to their underlying lowercase string value
    (``SystemType.COMMUNITY == "community"``) and serialize through
    :mod:`json` as that value.

    ``str(SystemType.COMMUNITY)`` returns ``"community"`` (the value),
    matching the JSON form and every f-string interpolation. Callers
    that need the uppercase Python member name read ``.name`` directly.
    """

    COMMUNITY = "community"
    """Open-source Deephaven Community / Core deployment. Uses
    :class:`~deephaven_mcp.client.CoreSession` and the community
    client libraries."""

    ENTERPRISE = "enterprise"
    """Commercial Deephaven Enterprise / Core+ deployment. Uses
    :class:`~deephaven_mcp.client.CorePlusSession` and a
    factory-based session-creation flow."""

    def __str__(self) -> str:
        """Return the lowercase string value of the member."""
        return self.value


class SessionOrigin(enum.StrEnum):
    """How a session came to be known to MCP.

    Defined for every session, community or enterprise. ``None`` in
    tool-response payloads is reserved for the genuinely unknown
    (a future manager kind not yet classified); every manager
    constructed today reports one of the three values below.
    """

    STATIC = "static"
    """Declared in configuration at server startup — community
    sessions from ``community/sessions/*.json``. No enterprise
    session is static today, but the value is available if a future
    enterprise configuration mechanism declares sessions ahead of
    time."""

    DYNAMIC = "dynamic"
    """Created at runtime by an MCP tool —
    ``session_community_create`` for community sessions,
    ``session_enterprise_create`` for enterprise sessions. The
    session would not exist without an explicit MCP call."""

    DISCOVERED = "discovered"
    """Pre-existing on the source system and surfaced to MCP —
    enterprise persistent queries read from the DHE controller. The
    session predates MCP's awareness of it and outlives MCP
    shutdown."""


class SystemRef(NamedTuple):
    """Reference to a single system in the configured catalog.

    As a :class:`typing.NamedTuple`, instances unpack positionally as
    ``(name, type)`` and compare equal to plain tuples.
    """

    name: str
    """The system identifier. For community, this is the literal
    ``"community"`` (the umbrella convention also used by the
    ``list_systems`` MCP tool and the fully qualified session id
    grammar). For enterprise, this is the per-system ``system_name``
    from the configuration file."""

    type: SystemType
    """:data:`SystemType.COMMUNITY` for the umbrella community row;
    :data:`SystemType.ENTERPRISE` for every enterprise row."""
