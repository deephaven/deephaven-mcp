"""Transport-layer TLS material for outbound Deephaven sessions.

Pydantic models describing the TLS configuration the MCP server
passes to ``pydeephaven.Session`` when opening a community session:

- :class:`ClientCertificate` — paired PEM certificate chain and its
  matching private key, used for mutual-TLS (mTLS) client
  authentication. The private key is typed
  :class:`pydantic.SecretStr` so it is masked in ``repr`` and
  redacted in ``model_dump(context={"redact": True})``.
- :class:`TlsConfig` — bundle holding optional server-trust material
  (``root_certs``) and an optional :class:`ClientCertificate`. It is
  the complete TLS contract for a single community session.

Both models accept resolved literal PEM text only. File indirection is
expressed in the source JSON via ``"${file:/path/to/file.pem}"``
templating, which :mod:`deephaven_mcp.config._templating` resolves
before the model is validated; the models themselves carry only the
decoded text and never a path.
"""

from ._tls import ClientCertificate, TlsConfig

__all__ = [
    "ClientCertificate",
    "TlsConfig",
]
