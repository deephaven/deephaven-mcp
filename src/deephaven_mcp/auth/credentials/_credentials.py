"""Outbound bearer credentials for Deephaven workers.

Defines the typed credential models the MCP server uses to authenticate
*to* a Deephaven Community or Enterprise worker. Each concrete model
represents one bearer mechanism. Transport-layer TLS material lives in
:mod:`deephaven_mcp.auth.tls`.

Class hierarchy
---------------

:class:`Credentials` is the abstract base; concrete kinds:

- :class:`AnonymousCredentials` — no bearer material.
- :class:`PSKCredentials` — Deephaven Community pre-shared key.
- :class:`PasswordCredentials` — username/password, with optional
  ``effective_user`` operate-as identity.
- :class:`PrivateKeyCredentials` — UTF-8 text of a PEM private-key
  file (Enterprise private-key auth).
- :class:`CustomTokenCredentials` — escape hatch for arbitrary Java
  auth-handler class names.

Sensitivity
-----------

Every secret-bearing field is typed :class:`pydantic.SecretStr`:

- ``PSKCredentials.token``
- ``PasswordCredentials.password``
- ``PrivateKeyCredentials.key_text``
- ``CustomTokenCredentials.auth_token``

``repr(creds)`` and ``str(creds)`` mask the secret value as
``SecretStr('**********')``; ``model.model_dump(mode="json",
context={"redact": True})`` produces the project's canonical
:data:`~deephaven_mcp._redaction.REDACTED` sentinel. Consumers that
need the secret text call ``.get_secret_value()`` explicitly.

Subclasses are ``frozen=True`` Pydantic models; equality and hashing
derive from the field values.
"""

from __future__ import annotations

__all__ = [
    "AnonymousCredentials",
    "Credentials",
    "CredentialsUnion",
    "CustomTokenCredentials",
    "PSKCredentials",
    "PasswordCredentials",
    "PrivateKeyCredentials",
]

from typing import Annotated, Literal

from pydantic import Field, SecretStr

from deephaven_mcp._pydantic import RedactableSchema


class Credentials(RedactableSchema):
    """Abstract base class for outbound bearer credentials.

    Concrete kinds inherit from :class:`Credentials`. The base itself
    declares no fields and cannot be instantiated directly —
    attempting :class:`Credentials` raises :class:`TypeError` at
    ``__new__`` time.

    Each concrete subclass declares a ``type`` field as a
    ``Literal[<name>]`` so the :data:`CredentialsUnion` discriminator
    can dispatch on it during JSON parsing.
    """

    def __new__(cls, *args: object, **kwargs: object) -> Credentials:
        """Forbid direct instantiation of the abstract base class."""
        if cls is Credentials:
            raise TypeError(
                "Credentials is an abstract base class; instantiate a "
                "concrete subclass instead."
            )
        return super().__new__(cls)


class AnonymousCredentials(Credentials):
    """No bearer material; suitable for anonymous-auth Community workers.

    Carries no fields beyond the discriminator.
    """

    type: Literal["anonymous"] = "anonymous"
    """Discriminator tag for the :data:`CredentialsUnion`; marks this
    object as anonymous (no bearer material)."""


class PSKCredentials(Credentials):
    """Pre-shared-key bearer material for Deephaven Community PSK auth."""

    type: Literal["psk"] = "psk"
    """Discriminator tag for the :data:`CredentialsUnion`; marks this
    object as Community pre-shared-key credentials."""

    token: SecretStr
    """The pre-shared key."""


class PasswordCredentials(Credentials):
    """Username/password bearer material with optional operate-as identity."""

    type: Literal["password"] = "password"
    """Discriminator tag for the :data:`CredentialsUnion`; marks this
    object as username/password credentials."""

    username: str
    """The authenticating username."""

    password: SecretStr
    """The user's password."""

    effective_user: str | None = None
    """Optional identity to operate as after authenticating. ``None``
    means the authenticated user is also the effective user."""


class PrivateKeyCredentials(Credentials):
    """Private-key bearer material for Deephaven Enterprise private-key auth."""

    type: Literal["private_key"] = "private_key"
    """Discriminator tag for the :data:`CredentialsUnion`; marks this
    object as Enterprise private-key credentials."""

    key_text: SecretStr
    """The Deephaven private key as UTF-8 text (proprietary base64
    keypair format, typically the contents of a
    ``priv-<keyname>.base64.txt`` file — not a PEM file)."""


class CustomTokenCredentials(Credentials):
    """Escape-hatch credential for arbitrary Java auth-handler class names.

    The full ``auth_type`` and resolved ``auth_token`` are forwarded
    verbatim to ``pydeephaven.Session``.
    """

    type: Literal["custom"] = "custom"
    """Discriminator tag for the :data:`CredentialsUnion`; marks this
    object as the escape-hatch credential for a custom Java auth
    handler."""

    auth_type: str
    """Fully-qualified Java class name of the auth handler (e.g.
    ``"com.example.MyHandler"``)."""

    auth_token: SecretStr
    """Opaque token whose format is dictated by the custom handler."""


CredentialsUnion = Annotated[
    AnonymousCredentials
    | PSKCredentials
    | PasswordCredentials
    | PrivateKeyCredentials
    | CustomTokenCredentials,
    Field(discriminator="type"),
]
"""Discriminated-union annotation for outbound credentials.

Pydantic dispatches on the ``type`` field at validation time, so
parsing ``{"type": "psk", "token": "x"}`` directly produces a
:class:`PSKCredentials` instance.
"""
