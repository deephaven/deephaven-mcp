"""Verified caller identity — the non-secret "who" half of authentication.

:class:`Principal` is one of the two outputs an authenticator produces
after inspecting a request's headers; the other is a
:class:`~deephaven_mcp.auth.credentials.Credentials` instance. They
encode orthogonal facts and exist so the two can be handled with
different policies:

======================  ====================  ====================
Aspect                  :class:`Principal`    :class:`Credentials`
======================  ====================  ====================
Answers                 "who is calling?"     "what bearer material
                                              authenticates downstream?"
Contains secrets        No                    Yes
Safe to log             Yes                   Never
Lifetime on the server  Full request/session  Dropped as soon as it is
                                              exchanged for a session
======================  ====================  ====================

Where a :class:`Principal` comes from
-------------------------------------

An authenticator (see :class:`~deephaven_mcp.auth.backends.AuthBackend`)
implements a two-phase contract:

1. ``authenticate(headers) -> Principal | None`` — inspects the request
   headers and verifies them. On success it returns a
   :class:`Principal` built from the verified values; the *existence* of
   the returned :class:`Principal` is the proof that authentication
   succeeded. On "not my request" it returns ``None`` so the chain can
   try the next authenticator.
2. ``derive_credentials(principal, headers) -> Credentials`` — combines
   the already-verified :class:`Principal` with the original headers to
   produce the secret-bearing :class:`Credentials` instance used by
   downstream session factories.

The split lets the non-secret identity be constructed, stashed, and
logged freely while the secret material is only materialised at the
moment it is needed and is not retained on the :class:`Principal`.

Field semantics
---------------

- ``subject`` — the canonical, stable identifier for the caller. For
  shared-bearer flows this is conventionally a fixed string (e.g.
  ``"community"``) because there is only one logical caller; for
  per-user flows it is the authenticated username. Consumers that need
  a username (e.g. Deephaven Enterprise password auth) read it from
  ``subject`` so the identity verified during ``authenticate`` is the
  same identity used during ``derive_credentials``.
- ``display_name`` — free-form human-readable label for log output;
  authenticators set it equal to ``subject`` when they have nothing else
  to add.
- ``raw`` — authenticator-defined bag of non-secret extras, useful for
  auditing and for forward-compatibility with richer identity providers
  (IdP claims, etc.). Never carries secret material.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["Principal"]


@dataclass(frozen=True, slots=True)
class Principal:
    """Verified caller identity produced by an authenticator.

    Returning a :class:`Principal` from an authenticator's
    ``authenticate`` call is the proof that the request's headers were
    verified; consumers that receive a :class:`Principal` may therefore
    treat its fields as trusted.

    A :class:`Principal` is non-secret by design (see the module
    docstring). It is paired with, but distinct from, a
    :class:`~deephaven_mcp.auth.credentials.Credentials` value: the
    principal says *who*, the credentials carry the secret material used
    to authenticate *downstream*.

    Attributes:
        subject (str): Canonical, stable identifier for the caller. Used
            as the log/audit key and, where applicable, as the username
            that downstream consumers (e.g. a Deephaven Enterprise
            session factory) authenticate as.
        display_name (str): Human-readable label for log output. May
            equal ``subject`` when the authenticator has nothing else to
            add.
        raw (dict[str, str]): Authenticator-defined non-secret extras
            (for example the name of the authenticator that produced
            this principal, or IdP claims). Never contains secret
            material. Intentionally untyped because its schema is
            authenticator-defined.
    """

    subject: str
    display_name: str
    raw: dict[str, str] = field(default_factory=dict)
