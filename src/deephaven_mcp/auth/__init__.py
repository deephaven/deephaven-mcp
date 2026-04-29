"""Authentication framework for the Deephaven MCP servers.

The framework is split into three subpackages with strictly linear
dependencies (``middleware → backends → credentials``); consumers import
from the subpackage they actually need:

- :mod:`deephaven_mcp.auth.credentials` — pure-data types for verified
  identity and bearer material. Depends only on the standard library.
- :mod:`deephaven_mcp.auth.backends` — the :class:`AuthBackend` Protocol,
  concrete backend implementations, and the pure-function chain runner
  :func:`authenticate_and_resolve`. Depends on ``credentials``.
- :mod:`deephaven_mcp.auth.middleware` — ASGI middleware that adapts the
  chain runner to Starlette/HTTP. Depends on ``backends``.

This top-level package intentionally re-exports **nothing**; every public
symbol is imported directly from the subpackage that owns it (for example
``from deephaven_mcp.auth.credentials import PasswordCredentials``). This
keeps the layering visible at every use-site.
"""
