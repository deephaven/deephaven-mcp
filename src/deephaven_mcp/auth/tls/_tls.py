"""TLS Pydantic models: :class:`ClientCertificate` and :class:`TlsConfig`.

These models act simultaneously as the wire-format schema (parsed
from the optional ``tls`` block on a community session config) and
as the runtime objects passed to ``pydeephaven.Session``. Any
file indirection in the source JSON is resolved at file-load time by
:mod:`deephaven_mcp.config._templating` (``"${file:/path/to/file.pem}"``),
so the models themselves carry only the decoded PEM text and never
the paths.

JSON shape::

    "tls": {
        "root_certs": "${file:/etc/ssl/dh-ca.pem}",
        "client_certificate": {
            "cert_chain": "${file:/etc/ssl/client.pem}",
            "private_key": "${file:/etc/ssl/client.key}"
        }
    }

The presence of the ``tls`` block (even ``"tls": {}``) enables TLS
for the session; absence means plaintext. Both sub-fields of
:class:`TlsConfig` are optional and independent.
"""

from __future__ import annotations

__all__ = [
    "ClientCertificate",
    "TlsConfig",
]

from pydantic import SecretStr

from deephaven_mcp._pydantic import RedactableSchema


class ClientCertificate(RedactableSchema):
    """Paired client certificate chain + matching private key for mTLS.

    Wire format (parsed via :meth:`model_validate`)::

        {"cert_chain": "${file:/path/to/chain.pem}",
         "private_key": "${file:/path/to/key.pem}"}

    The ``${file:...}`` placeholders are expanded by the templating
    engine at config-load time, so the validated model carries the
    decoded PEM text directly.
    """

    cert_chain: str
    """PEM-encoded certificate chain, decoded as UTF-8. Public
    material; dumped verbatim (no redaction)."""

    private_key: SecretStr
    """PEM-encoded private key matching ``cert_chain``, decoded as
    UTF-8. Wrapped in :class:`pydantic.SecretStr` so ``repr`` and
    redacted dumps mask the value."""


class TlsConfig(RedactableSchema):
    """All TLS material for a single community session.

    An empty instance (``TlsConfig()``) means "use TLS with the
    system-default trust store and no mTLS"; populated fields override
    each individual default.

    Wire format (parsed via :meth:`model_validate`)::

        {"root_certs": "${file:/etc/ssl/dh-ca.pem}",
         "client_certificate": {"cert_chain": ..., "private_key": ...}}
    """

    root_certs: str | None = None
    """Optional PEM-encoded server-trust bundle (CA certificates the
    client uses to verify the server). ``None`` falls back to the
    system default trust store. Public material; dumped verbatim."""

    client_certificate: ClientCertificate | None = None
    """Optional mTLS client identity (paired cert chain + private
    key). ``None`` means the session uses TLS but does not present
    a client certificate (no mTLS)."""
