"""Private-key authentication backend.

Accepts private-key authentication where the client delivers the key as
a base64-encoded blob in an HTTP header (never a filesystem path — the
server never touches client-side files). The backend produces a
:class:`PrivateKeyCredentials` whose decoded bytes a downstream consumer
spends against an upstream system.

Headers
-------
- ``X-Deephaven-Username`` (required): the username the key
  authenticates as. Stored on the resulting :attr:`Principal.subject`.
- ``X-Deephaven-Private-Key`` (required): Base64-encoded contents of the
  caller's private-key file.

Like the password backend, :meth:`authenticate` performs only syntactic
validation; the real round-trip authentication happens when a consumer
spends the derived :class:`PrivateKeyCredentials`.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from typing import ClassVar

from ..credentials import Principal, PrivateKeyCredentials
from ._base import AuthBackend, AuthenticationError
from ._headers import HEADER_PRIVATE_KEY, HEADER_USERNAME

__all__ = ["PrivateKeyBackend"]


class PrivateKeyBackend(AuthBackend):
    """Authenticate requests carrying a private-key header.

    Attributes:
        name (str): ``"private_key"``. Used in logs and
            ``WWW-Authenticate`` challenges.
        realm (str): Realm advertised in the ``WWW-Authenticate``
            challenge header. Inherited from :class:`AuthBackend`.
    """

    name: ClassVar[str] = "private_key"

    def __init__(self, *, realm: str | None = None) -> None:
        """Initialize the backend.

        Args:
            realm (str | None): Realm string advertised in the
                ``WWW-Authenticate`` challenge header. Defaults to
                :data:`~deephaven_mcp.auth.backends._base._DEFAULT_REALM`
                (``"deephaven-mcp"``).
        """
        super().__init__(realm=realm)

    async def authenticate(self, headers: Mapping[str, str]) -> Principal | None:
        """Return a :class:`Principal` iff the private-key headers are present.

        Returns ``None`` when the request is not claiming private-key auth
        (``X-Deephaven-Private-Key`` absent). Raises when the request is
        malformed (missing username, empty key header, or key not valid
        base64).

        Args:
            headers (Mapping[str, str]): Lowercase-keyed request headers.

        Returns:
            Principal | None: ``Principal(subject=<username>, …)`` on
                success; ``None`` if ``X-Deephaven-Private-Key`` is absent.

        Raises:
            AuthenticationError: If the key header is present but empty;
                if ``X-Deephaven-Username`` is missing or empty; or if
                the key header is not valid base64.
        """
        key_header = headers.get(HEADER_PRIVATE_KEY)
        if key_header is None:
            return None
        if not key_header:
            raise AuthenticationError(f"{HEADER_PRIVATE_KEY} header must not be empty.")
        username = headers.get(HEADER_USERNAME)
        if not username:
            raise AuthenticationError(
                f"{HEADER_USERNAME} header is required with " f"{HEADER_PRIVATE_KEY}."
            )
        # Validate base64 at authenticate time so malformed requests fail
        # at the edge rather than deep inside the consumer that spends
        # the credential.
        try:
            base64.b64decode(key_header, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise AuthenticationError(
                f"{HEADER_PRIVATE_KEY} header is not valid base64."
            ) from exc

        return self._make_principal(username)

    async def derive_credentials(
        self, principal: Principal, headers: Mapping[str, str]
    ) -> PrivateKeyCredentials:
        """Return a :class:`PrivateKeyCredentials` for the authenticated principal.

        The key header was already validated as base64 during
        :meth:`authenticate`; we decode again here (rather than cache the
        decoded bytes on the principal) to keep :class:`Principal`
        secret-free. Additionally, the decoded bytes are converted to
        text here, at the producing edge, so that downstream consumers
        of :class:`PrivateKeyCredentials` can rely on the
        ``key_text: str`` field without re-validating.

        Deephaven private-key files are documented as plain text files
        (conventional extension ``priv-<username>.base64.txt``) whose
        structured content is ASCII: ``///#`` comment lines plus
        ``key value`` lines where the values are base64-encoded DER.
        See
        https://deephaven.io/enterprise/gplus/docs/sys-admin/configuration/public-and-private-keys
        for the format reference.

        We decode as UTF-8 rather than strict ASCII so that human-written
        comment lines are allowed to contain Unicode (e.g. non-English
        team names or notes). Since ASCII is a strict subset of UTF-8,
        every conformant file is accepted; only genuinely binary garbage
        is rejected. Parse-level validation of the key material itself
        is the upstream ``SessionManager``'s responsibility, not ours.

        Args:
            principal (Principal): Unused; required by the
                :class:`AuthBackend` contract.
            headers (Mapping[str, str]): Lowercase-keyed request headers.

        Returns:
            PrivateKeyCredentials: Credentials carrying the decoded
                key text for a downstream consumer to spend against an
                upstream system.

        Raises:
            AuthenticationError: If the base64-decoded bytes are not
                valid UTF-8. Deephaven private-key files are text, so
                this indicates a malformed credential on the client
                side.
        """
        del principal
        key_bytes = base64.b64decode(headers[HEADER_PRIVATE_KEY], validate=True)
        try:
            key_text = key_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AuthenticationError(
                f"{HEADER_PRIVATE_KEY} header decodes to bytes that are not "
                f"valid UTF-8."
            ) from exc
        return PrivateKeyCredentials(key_text=key_text)

    def _challenge_scheme(self) -> str:
        """Return the ``DeephavenPrivateKey`` scheme token for the challenge.

        Returns:
            str: ``"DeephavenPrivateKey"``.
        """
        return "DeephavenPrivateKey"

    def _challenge_headers(self) -> tuple[str, ...]:
        """Return the headers required by this backend.

        Returns:
            tuple[str, ...]: Lowercase names of the required username
                and private-key headers, included in the
                ``WWW-Authenticate`` challenge so callers know which
                headers to send.
        """
        return (HEADER_USERNAME, HEADER_PRIVATE_KEY)
