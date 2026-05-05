"""Username/password authentication backend.

Verifies that the caller has supplied a username/password pair (and an
optional operate-as identity) in dedicated HTTP headers, and packages
the values into a :class:`PasswordCredentials` for downstream consumers
that will perform the real authentication against an upstream system.

Headers
-------
- ``X-Deephaven-Username`` (required): the authenticating username.
- ``X-Deephaven-Password`` (required): the password.
- ``X-Deephaven-Effective-User`` (optional): identity to operate as
  after authenticating. Only honoured when the backend is constructed
  with ``allow_effective_user=True``; otherwise present-and-set raises
  :class:`~deephaven_mcp.auth.backends.AuthenticationError`.

The backend performs *syntactic* validation only — it checks that the
required headers are present and well-formed. It does not contact any
upstream system: that round-trip happens later, when a consumer (for
example a session factory) spends the derived credential.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from ..credentials import PasswordCredentials, Principal
from ._base import AuthBackend, AuthenticationError
from ._headers import HEADER_EFFECTIVE_USER, HEADER_PASSWORD, HEADER_USERNAME

__all__ = ["PasswordBackend"]


class PasswordBackend(AuthBackend):
    """Authenticate requests carrying username/password headers.

    Attributes:
        name (str): ``"password"``. Used in logs and ``WWW-Authenticate``
            challenges.
        allow_effective_user (bool): Whether a non-empty
            ``X-Deephaven-Effective-User`` header is permitted.
        realm (str): Realm advertised in the ``WWW-Authenticate``
            challenge header. Inherited from :class:`AuthBackend`.
    """

    name: ClassVar[str] = "password"

    def __init__(
        self,
        *,
        allow_effective_user: bool = False,
        realm: str | None = None,
    ) -> None:
        """Initialize the backend.

        Args:
            allow_effective_user (bool): If ``True``, accept an optional
                ``X-Deephaven-Effective-User`` header and propagate its
                value into :class:`PasswordCredentials`. If ``False``
                (default), the header must be absent or empty; a
                present-and-non-empty value causes the request to be
                rejected with :class:`AuthenticationError`.
            realm (str | None): Realm string advertised in the
                ``WWW-Authenticate`` challenge header. Defaults to
                :data:`~deephaven_mcp.auth.backends._base._DEFAULT_REALM`
                (``"deephaven-mcp"``).
        """
        super().__init__(realm=realm)
        self.allow_effective_user = allow_effective_user

    async def authenticate(self, headers: Mapping[str, str]) -> Principal | None:
        """Return a :class:`Principal` iff the password headers are present.

        Returns ``None`` when the request is not claiming password auth
        (``X-Deephaven-Password`` absent); this lets a chain fall through
        to other backends. Raises when the request is malformed
        (only one of the required headers present, empty username, or
        effective-user header used while disallowed).

        Args:
            headers (Mapping[str, str]): Lowercase-keyed request headers.

        Returns:
            Principal | None: ``Principal(subject=<username>, …)`` on
                success; ``None`` if ``X-Deephaven-Password`` is absent.

        Raises:
            AuthenticationError: If ``X-Deephaven-Password`` is present
                but ``X-Deephaven-Username`` is missing or empty; if
                ``X-Deephaven-Password`` is empty; or if
                ``X-Deephaven-Effective-User`` is present while
                ``allow_effective_user`` is ``False``.
        """
        password = self._require_header(headers, HEADER_PASSWORD)
        if password is None:
            return None
        username = self._require_header(headers, HEADER_USERNAME)
        if not username:
            raise AuthenticationError(
                f"{HEADER_USERNAME} header is required with {HEADER_PASSWORD}."
            )
        effective_user = headers.get(HEADER_EFFECTIVE_USER)
        if effective_user and not self.allow_effective_user:
            raise AuthenticationError(
                f"{HEADER_EFFECTIVE_USER} header is not permitted on this " f"server."
            )

        extra_raw = {"effective_user": effective_user} if effective_user else None
        return self._make_principal(username, extra_raw=extra_raw)

    async def derive_credentials(
        self, principal: Principal, headers: Mapping[str, str]
    ) -> PasswordCredentials:
        """Return a :class:`PasswordCredentials` for the authenticated principal.

        Re-reads the password and effective-user headers; ``username``
        comes from ``principal.subject`` so the two sources stay in sync.

        Args:
            principal (Principal): Principal previously returned by
                :meth:`authenticate`.
            headers (Mapping[str, str]): Lowercase-keyed request headers.

        Returns:
            PasswordCredentials: Credentials carrying the username,
                password, and optional effective-user identity for a
                downstream consumer to spend against an upstream system.
        """
        password = headers[HEADER_PASSWORD]
        effective_user: str | None = None
        if self.allow_effective_user:
            raw_effective = headers.get(HEADER_EFFECTIVE_USER)
            if raw_effective:
                effective_user = raw_effective
        return PasswordCredentials(
            username=principal.subject,
            password=password,
            effective_user=effective_user,
        )

    def _challenge_scheme(self) -> str:
        """Return the ``DeephavenPassword`` scheme token for the challenge.

        Returns:
            str: ``"DeephavenPassword"``.
        """
        return "DeephavenPassword"

    def _challenge_headers(self) -> tuple[str, ...]:
        """Return the headers required by this backend.

        Returns:
            tuple[str, ...]: Lowercase names of the required username
                and password headers, included in the
                ``WWW-Authenticate`` challenge so callers know which
                headers to send.
        """
        return (HEADER_USERNAME, HEADER_PASSWORD)
