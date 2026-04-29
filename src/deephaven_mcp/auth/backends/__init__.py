"""Authentication backend Protocol, concrete backends, and chain runner.

This subpackage contains the **mechanism layer** of the ``auth``
framework:

- :class:`AuthBackend` — the Protocol every backend implements, plus the
  :class:`AuthenticationError` raised on invalid credentials.
- :func:`authenticate_and_resolve` — the pure-function chain runner used
  by the middleware and by non-HTTP callers (e.g. a future CLI, tests).
- Concrete backends (:class:`PSKBackend`, :class:`PasswordBackend`,
  :class:`PrivateKeyBackend`) — pluggable implementations registered by
  each MCP server at startup.
- ``HEADER_*`` constants — the lowercase HTTP header names that make up
  the MCP auth wire protocol. External consumers (client SDKs,
  integration tests, CLI tools) should import these rather than
  hard-coding the strings; see :mod:`._headers` for details.

Depends on :mod:`deephaven_mcp.auth.credentials` for the types it
produces; never imports from :mod:`deephaven_mcp.auth.middleware`.
"""

from ._base import AuthBackend, AuthenticationError
from ._headers import (
    HEADER_EFFECTIVE_USER,
    HEADER_PASSWORD,
    HEADER_PRIVATE_KEY,
    HEADER_PSK,
    HEADER_USERNAME,
)
from ._password import PasswordBackend
from ._private_key import PrivateKeyBackend
from ._psk import PSKBackend
from ._resolve import authenticate_and_resolve

__all__ = [
    "AuthBackend",
    "AuthenticationError",
    "HEADER_EFFECTIVE_USER",
    "HEADER_PASSWORD",
    "HEADER_PRIVATE_KEY",
    "HEADER_PSK",
    "HEADER_USERNAME",
    "PasswordBackend",
    "PSKBackend",
    "PrivateKeyBackend",
    "authenticate_and_resolve",
]
