"""Wire-format ``auth`` block shared by session and system declarations.

:class:`AuthConfig` validates the ``auth`` block of a community
session file, an enterprise system file, and the community
``session_creation.defaults`` block. Today it carries only
``credentials``; unknown keys are rejected.

Schema::

    {
        "credentials": {
            "type": "psk",
            "token": "${env:DH_LOCAL_PSK}"
        }
    }
"""

from __future__ import annotations

__all__ = [
    "AuthConfig",
]

from deephaven_mcp._pydantic import RedactableSchema
from deephaven_mcp.auth.credentials import CredentialsUnion


class AuthConfig(RedactableSchema):
    """Validated ``auth`` block of a session or system declaration."""

    credentials: CredentialsUnion
    """How to authenticate the outbound connection to the target server."""
