"""Classification vocabulary for systems and sessions.

Defines :class:`SystemType` (community vs enterprise),
:class:`SessionOrigin` (static vs dynamic, community-only), and
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

    Members:
        COMMUNITY: Open-source Deephaven Community / Core deployment.
            Uses :class:`~deephaven_mcp.client.CoreSession` and the
            community client libraries.
        ENTERPRISE: Commercial Deephaven Enterprise / Core+ deployment.
            Uses :class:`~deephaven_mcp.client.CorePlusSession` and a
            factory-based session-creation flow.
    """

    COMMUNITY = "community"
    ENTERPRISE = "enterprise"

    def __str__(self) -> str:
        """Return the lowercase string value of the member."""
        return self.value


class SessionOrigin(enum.StrEnum):
    """How a community session came to exist.

    Defined for community sessions only; ``None`` for enterprise
    sessions (the concept is meaningless there).

    Members:
        STATIC: Declared in ``community/sessions/*.json`` at server
            startup.
        DYNAMIC: Created at runtime via the
            ``session_community_create`` MCP tool.
    """

    STATIC = "static"
    DYNAMIC = "dynamic"


class SystemRef(NamedTuple):
    """Reference to a single system in the configured catalog.

    As a :class:`typing.NamedTuple`, instances unpack positionally as
    ``(name, type)`` and compare equal to plain tuples.

    Attributes:
        name (str): The system identifier. For community, this is the
            literal ``"community"`` (the umbrella convention also used
            by the ``list_systems`` MCP tool and the fully qualified
            session id grammar). For enterprise, this is the
            per-system ``system_name`` from the configuration file.
        type (SystemType): :data:`SystemType.COMMUNITY` for the
            umbrella community row; :data:`SystemType.ENTERPRISE` for
            every enterprise row.
    """

    name: str
    type: SystemType
