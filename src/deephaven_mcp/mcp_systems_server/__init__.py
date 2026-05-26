"""deephaven_mcp.mcp_systems_server package.

Implements the multiplexed Deephaven MCP systems server: a single
process that hosts every configured Deephaven Community session and
Deephaven Enterprise system in one tool manifest.

There is one CLI entry point — ``dh-mcp-systems-server`` (see
``[project.scripts]`` in ``pyproject.toml``) — and two transports:

- ``stdio`` (default): no authentication; the OS pipe is the trust
  boundary. Suitable for local-IDE integrations such as Claude Desktop.
- ``http``: streamable-HTTP gated by a single PSK shared via the
  ``X-Deephaven-PSK`` header. Bind address is restricted to loopback
  (``127.0.0.1``, ``::1``, or ``localhost``); no TLS is performed.

Configuration is read from a per-user directory tree validated by
:class:`~deephaven_mcp.config.MultiSystemConfigManager`; ``server.json``
inside that tree carries the PSK. There is no per-request authentication,
no auth backends, and no ``mcp_reload`` tool — configuration changes
require a restart.

Key modules:

- :mod:`.server`: CLI parsing, transport selection, PSK resolution, and
  the ``main`` entry point.
- :mod:`._lifespan`: FastMCP lifespan factory that loads configuration,
  builds the multi-system registry, and starts one evictor per child
  registry.
- :mod:`._tools`: MCP tool implementations registered on the
  multiplexed server.

Reusable inbound-auth primitives (the PSK middleware mounted by this
server's HTTP transport) live in :mod:`deephaven_mcp.auth.middleware`,
not here.
"""
