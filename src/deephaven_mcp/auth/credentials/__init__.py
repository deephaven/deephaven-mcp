"""Outbound bearer credential models for the multiplexed MCP server.

Holds the secret-bearing Pydantic models used by community and
enterprise session-creation entry points
(:meth:`deephaven_mcp.client.CoreSession.from_credentials` and
:meth:`deephaven_mcp.client.CorePlusSessionFactory.from_credentials`).

Five concrete kinds cover both backends:

- :class:`AnonymousCredentials` — anonymous community auth.
- :class:`PSKCredentials` — community pre-shared-key auth.
- :class:`PasswordCredentials` — community Basic and enterprise
  username/password (with optional ``effective_user``).
- :class:`PrivateKeyCredentials` — enterprise private-key auth.
- :class:`CustomTokenCredentials` — escape hatch for arbitrary
  Java auth-handler classes (community).

The :data:`CredentialsUnion` type alias is the discriminated-union
annotation that containing config models use when declaring a
``credentials`` field so Pydantic dispatches on the ``type`` discriminator
at parse time.

TLS material (server-trust bundle + optional mTLS client identity)
lives in the peer package :mod:`deephaven_mcp.auth.tls`.
"""

from ._credentials import (
    AnonymousCredentials,
    Credentials,
    CredentialsUnion,
    CustomTokenCredentials,
    PasswordCredentials,
    PrivateKeyCredentials,
    PSKCredentials,
)

__all__ = [
    "AnonymousCredentials",
    "Credentials",
    "CredentialsUnion",
    "CustomTokenCredentials",
    "PSKCredentials",
    "PasswordCredentials",
    "PrivateKeyCredentials",
]
