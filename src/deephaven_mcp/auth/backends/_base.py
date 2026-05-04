"""The :class:`AuthBackend` abstract base class and related error type.

A backend is a small, stateless object that inspects a mapping of HTTP
request headers and decides whether the request carries valid
credentials. It implements a **two-phase contract** whose split is the
reason :class:`~deephaven_mcp.auth.credentials.Principal` and
:class:`~deephaven_mcp.auth.credentials.Credentials` are separate types:

1. :meth:`AuthBackend.authenticate` verifies the headers and returns a
   :class:`Principal` on success (or ``None`` to pass the request to the
   next backend). The returned principal is **non-secret** identity
   only; its existence is the proof that authentication succeeded.
2. :meth:`AuthBackend.derive_credentials` is called only for the winning
   backend and turns the already-verified :class:`Principal` plus the
   original headers into a secret-bearing
   :class:`~deephaven_mcp.auth.credentials.Credentials` instance that
   downstream session factories use to authenticate to Deephaven.

This split lets the middleware log "who" freely and lets the secret
material be materialised only at the moment a consumer needs it, rather
than being carried around on a long-lived identity object.

Backend chaining
----------------
The middleware tries every registered backend in registration order. The
first backend whose :meth:`authenticate` returns a :class:`Principal`
wins; the middleware then calls its :meth:`derive_credentials` and
attaches both the principal and the credentials to the ASGI scope.
Returning ``None`` from :meth:`authenticate` means "not my request";
raising :class:`AuthenticationError` means "my request, but the
credential is invalid" — that short-circuits the chain with ``401``.

Abstract base class with shared helpers
---------------------------------------
:class:`AuthBackend` is an :class:`abc.ABC` so that:

- forgetting to implement :meth:`authenticate`,
  :meth:`derive_credentials`, or :meth:`_challenge_scheme` fails loudly
  at construction with a clear :class:`TypeError`, rather than silently
  at call time with :class:`AttributeError`;
- subclasses share a single source of truth for ``realm`` storage, the
  default-realm constant, the lowercase username-header name, and the
  ``WWW-Authenticate`` challenge format. Subclasses customise the
  challenge by overriding two small hooks (:meth:`_challenge_scheme`
  and, optionally, :meth:`_challenge_headers`) instead of reimplementing
  the format string;
- the helper :meth:`_make_principal` produces a non-secret
  :class:`Principal` with ``raw={"backend": self.name, ...}`` so every
  backend reports its identity consistently.

The class also enforces, via :meth:`__init_subclass__`, that every
non-abstract concrete subclass declares a non-empty class-level
:attr:`name` attribute used in logs and ``WWW-Authenticate`` challenges.

:class:`AuthenticationError` is re-exported from
:mod:`deephaven_mcp._exceptions` for convenience.
"""

from __future__ import annotations

import abc
import inspect
from collections.abc import Mapping
from typing import ClassVar

from ..._exceptions import AuthenticationError
from ..credentials import Credentials, Principal

__all__ = ["AuthBackend", "AuthenticationError"]

_DEFAULT_REALM = "deephaven-mcp"
"""Default realm advertised in the ``WWW-Authenticate`` challenge header."""


class AuthBackend(abc.ABC):
    """Abstract base class implemented by every authentication backend.

    Instances are expected to be cheap to construct and safe to share
    across requests (backends should hold only immutable configuration;
    any mutable state belongs to the call site).

    Subclasses must:

    - Declare a non-empty class-level :attr:`name` attribute (a short,
      stable identifier such as ``"psk"`` or ``"password"``). Class
      definition fails with :class:`TypeError` otherwise.
    - Implement :meth:`authenticate` and :meth:`derive_credentials`
      (both async).
    - Implement :meth:`_challenge_scheme` to return the
      ``WWW-Authenticate`` scheme token (e.g. ``"Bearer"``).

    Subclasses may optionally override:

    - :meth:`_challenge_headers` to add a ``headers="…"`` clause to
      their challenge.
    - :meth:`challenge` to return a fully bespoke string (rare; the
      default formats ``_challenge_scheme`` + ``realm`` + optional
      ``_challenge_headers`` consistently).

    The module-level constant :data:`_DEFAULT_REALM` holds the default
    ``WWW-Authenticate`` realm and is not a class attribute because it
    is a fixed string, not a per-subclass override. HTTP header names
    consumed by backends live in
    :mod:`deephaven_mcp.auth.backends._headers`.
    """

    name: ClassVar[str]
    """Short, stable identifier used in logs and the ``WWW-Authenticate``
    challenge header (for example ``"psk"``). Must be set by every
    concrete subclass; enforced by :meth:`__init_subclass__`.
    """

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Validate that concrete subclasses declare a non-empty ``name``.

        Skipped for further abstract subclasses (those that still have
        unimplemented :func:`abc.abstractmethod` members), so the check
        only fires on classes that can actually be instantiated.

        Raises:
            TypeError: If a concrete subclass does not define a
                non-empty class-level ``name`` string.
        """
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        name = cls.__dict__.get("name", getattr(cls, "name", None))
        if not isinstance(name, str) or not name:
            raise TypeError(
                f"{cls.__qualname__} must declare a non-empty class-level "
                f"'name' attribute (got {name!r})."
            )

    def __init__(self, *, realm: str | None = None) -> None:
        """Initialize shared backend state.

        Args:
            realm (str | None): Realm string advertised in the
                ``WWW-Authenticate`` challenge header. Defaults to
                :data:`_DEFAULT_REALM` (``"deephaven-mcp"``) when
                ``None``.
        """
        self.realm = realm if realm is not None else _DEFAULT_REALM

    @abc.abstractmethod
    async def authenticate(self, headers: Mapping[str, str]) -> Principal | None:
        """Phase 1: verify the request headers and return verified identity.

        Implementations inspect ``headers`` for the specific fields this
        backend knows how to verify and, on success, return a
        :class:`Principal` containing the verified identity. The
        principal **must not** carry secret material (passwords, key
        bytes, tokens); such material is re-read from ``headers`` during
        :meth:`derive_credentials` if it is needed downstream.

        The three return outcomes map to three middleware behaviours:

        - :class:`Principal` — this backend claims the request and has
          verified it; the middleware will call :meth:`derive_credentials`
          on this backend next.
        - ``None`` — this backend does not recognise the request (e.g.
          the header it looks for is absent); the middleware proceeds to
          the next backend in the chain.
        - Raising :class:`AuthenticationError` — this backend claims the
          request but the credential is invalid; the middleware
          short-circuits the chain with ``401 Unauthorized``.

        Args:
            headers (Mapping[str, str]): Case-insensitive view of the
                request headers. Implementations must not assume any
                specific casing; headers are lowercased by the
                middleware before this call.

        Returns:
            Principal | None: A :class:`Principal` built from the
                verified headers, or ``None`` to pass the request to the
                next backend.

        Raises:
            AuthenticationError: If the backend recognises the request
                (e.g. the expected header is present) but the credential
                is invalid (e.g. the token does not match).
        """

    @abc.abstractmethod
    async def derive_credentials(
        self, principal: Principal, headers: Mapping[str, str]
    ) -> Credentials:
        """Phase 2: build the secret-bearing credentials for the principal.

        Called by the middleware only for the backend whose
        :meth:`authenticate` returned ``principal`` (i.e. exactly once
        per authenticated request). The result is attached to the ASGI
        scope and ultimately handed to a downstream session factory
        (for example
        :meth:`~deephaven_mcp.client.CorePlusSessionFactory.from_credentials`).

        Implementations typically combine verified fields from
        ``principal`` (e.g. ``principal.subject`` as the username) with
        fields re-read from ``headers`` (e.g. the password, the key
        bytes) to construct the appropriate concrete
        :class:`~deephaven_mcp.auth.credentials.Credentials` subclass.
        Re-reading from ``headers`` rather than stashing secrets on the
        :class:`Principal` keeps the principal non-secret.

        Args:
            principal (Principal): The principal returned by this
                backend's :meth:`authenticate` for this request. Its
                fields have already been verified and may be trusted.
            headers (Mapping[str, str]): The same case-insensitive
                header view that was passed to :meth:`authenticate`,
                available for re-reading values that were not captured
                on the :class:`Principal`.

        Returns:
            Credentials: A concrete
                :class:`~deephaven_mcp.auth.credentials.Credentials`
                subclass appropriate for this backend. The caller is
                expected to consume it promptly (exchange it for a
                long-lived session handle) and drop it; credentials must
                not be logged or persisted.
        """

    @abc.abstractmethod
    def _challenge_scheme(self) -> str:
        """Return the ``WWW-Authenticate`` scheme token for this backend.

        Examples: ``"Bearer"``, ``"DeephavenPassword"``,
        ``"DeephavenPrivateKey"``.

        Returns:
            str: The scheme token used as the first whitespace-separated
                token of the ``WWW-Authenticate`` challenge string.
        """

    def _challenge_headers(self) -> tuple[str, ...]:
        """Return the headers this backend expects, for the challenge string.

        Backends that document the headers a client must supply (typical
        for username/password and private-key flows) override this to
        return a non-empty tuple of lowercase header names. The default
        empty tuple omits the ``headers="…"`` clause from the challenge
        entirely (typical for bearer-token flows like PSK).

        Returns:
            tuple[str, ...]: Lowercase header names, or ``()`` for none.
        """
        return ()

    def challenge(self) -> str:
        """Return the value for a ``WWW-Authenticate`` challenge header.

        Emitted by the middleware on ``401`` responses. Multiple backends
        contribute one challenge each, and the middleware joins them with
        ``", "`` on the single response header.

        The default implementation formats
        ``f'{scheme} realm="{realm}"'`` and appends
        ``f', headers="{h1}, {h2}, …"'`` if :meth:`_challenge_headers`
        returns a non-empty tuple. Subclasses with bespoke needs may
        override this method directly.

        Returns:
            str: A single ``WWW-Authenticate`` scheme/parameter string
                (for example ``'Bearer realm="deephaven-mcp"'``).
        """
        base = f'{self._challenge_scheme()} realm="{self.realm}"'
        headers = self._challenge_headers()
        if not headers:
            return base
        return f'{base}, headers="{", ".join(headers)}"'

    @staticmethod
    def _require_header(headers: Mapping[str, str], header_name: str) -> str | None:
        """Return the value of *header_name*, or ``None`` if absent.

        Raises :class:`AuthenticationError` if the header is present but
        empty, so callers can distinguish "not my header" (``None``) from
        "my header but malformed" (exception) without repeating the
        two-step guard in every backend.

        Args:
            headers (Mapping[str, str]): Lowercase-keyed request headers.
            header_name (str): The lowercase header name to look up.

        Returns:
            str | None: The non-empty header value, or ``None`` if the
                header is absent.

        Raises:
            AuthenticationError: If the header is present but empty.
        """
        value = headers.get(header_name)
        if value is None:
            return None
        if not value:
            raise AuthenticationError(f"{header_name} header must not be empty.")
        return value

    def _make_principal(
        self,
        subject: str,
        *,
        display_name: str | None = None,
        extra_raw: Mapping[str, str] | None = None,
    ) -> Principal:
        """Build a :class:`Principal` tagged with this backend's name.

        Centralises the convention that every authenticator records its
        identity in ``Principal.raw["backend"]`` so that audit logs and
        cross-backend debugging can attribute a verified request to the
        backend that accepted it.

        Args:
            subject (str): Canonical caller identifier
                (:attr:`Principal.subject`).
            display_name (str | None): Human-readable label
                (:attr:`Principal.display_name`). Defaults to
                ``subject`` when ``None``.
            extra_raw (Mapping[str, str] | None): Additional non-secret
                claims to merge into :attr:`Principal.raw` alongside the
                ``"backend"`` key. Keys in ``extra_raw`` take precedence
                over the auto-set ``"backend"`` key only if explicitly
                provided.

        Returns:
            Principal: A frozen :class:`Principal` whose ``raw`` dict
                contains at least ``{"backend": self.name}``.
        """
        raw: dict[str, str] = {"backend": self.name}
        if extra_raw:
            raw.update(extra_raw)
        return Principal(
            subject=subject,
            display_name=display_name if display_name is not None else subject,
            raw=raw,
        )
