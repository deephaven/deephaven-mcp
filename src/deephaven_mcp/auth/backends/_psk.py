"""Pre-shared-key (PSK) authentication backend.

Verifies a single configured PSK presented by the caller in the HTTP
``X-Deephaven-PSK`` header, using a constant-time comparison. Suitable
for "Jupyter-style" deployments where one shared secret authenticates
every request and there is no per-user identity to distinguish.

On success the backend returns a :class:`Principal` whose ``subject``
defaults to ``"psk"`` (configurable per instance) and a
:class:`PSKCredentials` carrying the verified key, so that consumers
that need to forward the PSK to an upstream service can do so.
"""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from typing import ClassVar

from ..credentials import Principal, PSKCredentials
from ._base import AuthBackend, AuthenticationError
from ._headers import HEADER_PSK

__all__ = ["PSKBackend"]


class PSKBackend(AuthBackend):
    """Authenticate requests against a configured pre-shared key.

    Attributes:
        name (str): ``"psk"``. Used in logs and ``WWW-Authenticate``
            challenges.
        expected_psk (str): The key the request must present.
        principal_subject (str): ``Principal.subject`` value returned on
            successful authentication.
        realm (str): Realm advertised in the ``WWW-Authenticate``
            challenge header. Inherited from :class:`AuthBackend`.
    """

    name: ClassVar[str] = "psk"

    def __init__(
        self,
        expected_psk: str,
        *,
        principal_subject: str = "psk",
        realm: str | None = None,
    ) -> None:
        """Initialize the backend.

        Args:
            expected_psk (str): The pre-shared key the client must
                present. Must be non-empty; an empty string would make
                every unauthenticated request succeed and is always a
                configuration bug.
            principal_subject (str): Identity surfaced on the
                :class:`Principal` returned by :meth:`authenticate`.
                Defaults to ``"psk"``.
            realm (str | None): Realm string advertised in the
                ``WWW-Authenticate`` challenge header. Defaults to
                :data:`~deephaven_mcp.auth.backends._base._DEFAULT_REALM`
                (``"deephaven-mcp"``).

        Raises:
            ValueError: If ``expected_psk`` is empty.
        """
        if not expected_psk:
            raise ValueError("PSKBackend requires a non-empty PSK.")
        super().__init__(realm=realm)
        self.expected_psk = expected_psk
        self.principal_subject = principal_subject

    async def authenticate(self, headers: Mapping[str, str]) -> Principal | None:
        """Return a :class:`Principal` iff the PSK header matches the configured PSK.

        A missing ``X-Deephaven-PSK`` header returns ``None`` (the
        request is not claiming PSK auth, and a multi-backend chain
        could legitimately pass it to another backend). A present
        ``X-Deephaven-PSK`` header whose value is empty or does not
        match raises :class:`AuthenticationError`.

        Args:
            headers (Mapping[str, str]): Lowercase-keyed request headers.

        Returns:
            Principal | None: ``Principal(subject=self.principal_subject,
                ...)`` on success; ``None`` if the ``X-Deephaven-PSK``
                header is absent.

        Raises:
            AuthenticationError: If the header is present but empty, or
                its value does not match the configured PSK.
        """
        presented = self._require_header(headers, HEADER_PSK)
        if presented is None:
            return None
        if not hmac.compare_digest(presented, self.expected_psk):
            raise AuthenticationError(f"Invalid pre-shared key in {HEADER_PSK} header.")

        return self._make_principal(self.principal_subject)

    async def derive_credentials(
        self, _principal: Principal, headers: Mapping[str, str]
    ) -> PSKCredentials:
        """Return a :class:`PSKCredentials` carrying the PSK from the request.

        Forwards the **observed** ``X-Deephaven-PSK`` header value rather
        than the server's configured :attr:`expected_psk`. The two are
        byte-equal at this point — :meth:`authenticate` only returns a
        :class:`Principal` after :func:`hmac.compare_digest` confirmed
        equality — but reading from ``headers`` here mirrors how
        :class:`PasswordBackend` and :class:`PrivateKeyBackend` build
        their credentials and is robust to a future world in which
        :attr:`expected_psk` becomes mutable (e.g. config hot-reload).
        Forwarding the value the client actually presented keeps the
        credential bound to *this* request, eliminating any
        time-of-check/time-of-use gap.

        Args:
            _principal (Principal): Unused; present to satisfy the
                :class:`AuthBackend` contract.
            headers (Mapping[str, str]): Lowercase-keyed request
                headers. The ``X-Deephaven-PSK`` entry is required and
                guaranteed to be present by :meth:`authenticate`.

        Returns:
            PSKCredentials: The verified pre-shared key, available to
                downstream consumers (for example a session manager
                forwarding the key to an upstream worker).
        """
        return PSKCredentials(psk=headers[HEADER_PSK])

    def _challenge_scheme(self) -> str:
        """Return the ``DeephavenPSK`` scheme token for the challenge string.

        Returns:
            str: ``"DeephavenPSK"``.
        """
        return "DeephavenPSK"

    def _challenge_headers(self) -> tuple[str, ...]:
        """Return the header required by this backend.

        Returns:
            tuple[str, ...]: A one-tuple containing the lowercase
                ``X-Deephaven-PSK`` header name, included in the
                ``WWW-Authenticate`` challenge so callers know which
                header to send.
        """
        return (HEADER_PSK,)
