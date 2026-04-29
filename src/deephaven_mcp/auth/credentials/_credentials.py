"""Mechanism-only credential dataclasses: the secret-bearing "what" of authentication.

Each concrete subclass represents a single kind of bearer material (a
shared token, a username/password pair, a private key). The types are
defined purely by the *shape* of the material they carry; this module
is intentionally agnostic about which consumer accepts which kind.
Deciding whether a given credential can authenticate against a given
system is the responsibility of whichever consumer takes one as input
(for example a session factory's ``match`` statement or a registry's
acceptance check).

Class hierarchy
---------------

:class:`Credentials` is an :class:`abc.ABC` and the single type used in
function signatures throughout the codebase. The three concrete
subclasses inherit from it:

- :class:`PSKCredentials` — a single pre-shared key string verified by
  the authenticator.
- :class:`PasswordCredentials` — username/password (and optional
  identity to operate as).
- :class:`PrivateKeyCredentials` — UTF-8 text of a private-key file.

Consumers branch on concrete type via ``match`` or ``isinstance`` checks
local to the consuming module; this package exposes no helpers for
such branching.

Sensitivity
-----------
These dataclasses carry secret material (passwords, PSKs, decoded key
bytes). They must never be logged, serialised, or persisted. Call sites
should drop the secret as soon as it has been exchanged for a
long-lived handle (e.g. a session object).

As a defence-in-depth measure, every concrete subclass overrides
``__repr__`` to redact its secret fields. ``repr(creds)``,
``f"{creds}"``, ``logger.info("%s", creds)``, and any other route that
goes through ``__repr__``/``__str__`` therefore produce
``"PSKCredentials(psk=[REDACTED])"`` rather than the plaintext secret.
Call sites that genuinely need the secret must read the typed attribute
(``creds.psk``, ``creds.password``, ``creds.key_text``) explicitly.

Identity and hashing
--------------------
The concrete subclasses are frozen dataclasses, so Python synthesises
``__eq__`` and ``__hash__`` from their fields, including the secret
fields. Consumers may therefore use a credential object directly as a
cache key: two structurally equal credentials hash and compare equal
and share a cached resource. Hash and equality never produce strings,
so this does not leak secrets.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from deephaven_mcp._redaction import REDACTED

__all__ = [
    "Credentials",
    "PasswordCredentials",
    "PrivateKeyCredentials",
    "PSKCredentials",
]


class Credentials(ABC):  # noqa: B024 - abstract via __new__ (see below)
    """Abstract base class for mechanism-only credentials.

    Every concrete credential kind is a subclass of
    :class:`Credentials`. The base class carries no fields and no
    behaviour because credentials are pure data — dispatching on the
    concrete type is the responsibility of whichever consumer takes a
    :class:`Credentials` as input.

    Direct instantiation is forbidden at runtime via :meth:`__new__`:
    attempting ``Credentials()`` raises :class:`TypeError`. The empty
    :attr:`__slots__` makes the base compatible with subclasses that
    use ``@dataclass(slots=True)``.
    """

    __slots__ = ()

    def __new__(cls, *args: object, **kwargs: object) -> Credentials:
        """Forbid direct instantiation of the abstract base class."""
        if cls is Credentials:
            raise TypeError(
                "Credentials is an abstract base class; instantiate a "
                "concrete subclass instead."
            )
        return super().__new__(cls)


@dataclass(frozen=True, slots=True, repr=False)
class PSKCredentials(Credentials):
    """Pre-shared key bearer material.

    Carries the PSK string the caller presented and the authenticator
    verified (typically by constant-time comparison against a configured
    value). Consumers that need to forward the key to an upstream
    service can read :attr:`psk` directly; consumers that only need to
    know "the caller is authenticated" can ignore the field.

    The auto-generated dataclass ``__repr__`` is disabled
    (``repr=False``) and replaced with a secret-redacting implementation
    so that accidental ``f"{creds}"`` or ``logger.info("%s", creds)``
    cannot leak the PSK.

    Attributes:
        psk (str): The verified pre-shared key.
    """

    psk: str

    def __repr__(self) -> str:
        """Return a redacted representation that never reveals the PSK."""
        return f"PSKCredentials(psk={REDACTED})"


@dataclass(frozen=True, slots=True, repr=False)
class PasswordCredentials(Credentials):
    """Username/password bearer material, with optional operate-as identity.

    Carries the fields needed for classic password authentication plus
    an optional "operate as" identity for consumers that support
    sudo-style delegation. Whether a given consumer honours
    :attr:`effective_user` is a property of that consumer; this
    dataclass is only responsible for carrying the value.

    The auto-generated dataclass ``__repr__`` is disabled
    (``repr=False``) and replaced with a secret-redacting implementation
    so that accidental ``f"{creds}"`` or ``logger.info("%s", creds)``
    cannot leak the password. ``username`` and ``effective_user`` are
    not secrets and remain visible for debugging.

    Attributes:
        username (str): The authenticating username.
        password (str): The user's password.
        effective_user (str | None): Optional identity to operate as
            after authenticating. When ``None``, the authenticated user
            is also the effective user.
    """

    username: str
    password: str
    effective_user: str | None = None

    def __repr__(self) -> str:
        """Return a representation that redacts the password field only."""
        return (
            f"PasswordCredentials(username={self.username!r}, "
            f"password={REDACTED}, effective_user={self.effective_user!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class PrivateKeyCredentials(Credentials):
    """Private-key bearer material.

    Carries the decoded contents of a private-key file as UTF-8 text.
    Keeping the payload in memory (rather than as a file path) lets
    consumers present the key to the underlying auth API without
    requiring filesystem access on the server side; how the text is
    consumed (e.g. wrapped in a text stream, parsed as PEM) is the
    consumer's concern.

    The auto-generated dataclass ``__repr__`` is disabled
    (``repr=False``) and replaced with a secret-redacting implementation
    so that accidental ``f"{creds}"`` or ``logger.info("%s", creds)``
    cannot leak the key material. The key length in characters is
    shown because it is useful for debugging and does not reveal key
    content.

    Attributes:
        key_text (str): Decoded keyfile contents — the raw text of the
            private-key file. Always valid UTF-8: the producing backend
            (:class:`~deephaven_mcp.auth.backends.PrivateKeyBackend`)
            validates the bytes it receives on the wire and raises
            :class:`~deephaven_mcp.auth.backends.AuthenticationError` on
            invalid UTF-8, so downstream consumers can rely on the
            ``str`` type without re-validating.
    """

    key_text: str

    def __repr__(self) -> str:
        """Return a representation that shows key length but redacts contents."""
        return (
            f"PrivateKeyCredentials(key_text={REDACTED}; "
            f"{len(self.key_text)} chars)"
        )
