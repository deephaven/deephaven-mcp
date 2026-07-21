"""Schema for ``server.json``.

Defines the :class:`ServerConfig` Pydantic model that owns the
optional top-level ``server.json`` file under the MCP configuration
directory. The model carries every operator-tunable knob for the
systems-server process: HTTP transport selection, bind address and
port, the FastMCP server name advertised in the handshake, and the
PSK that gates HTTP transport. Secret fields are redacted in
``model_dump(context={"redact": True})``. Env-var or file
indirection in the source JSON is resolved at file-load time by
:mod:`deephaven_mcp.config._templating`; the model itself sees only literal
values.

Loader: :func:`load_server`.

Schema::

    {
        "transport": "stdio",                  // or "http"
        "host": "127.0.0.1",                   // HTTP bind; must be loopback
        "port": 8000,                          // HTTP TCP port
        "server_name": "deephaven-mcp-systems",
        "psk": "<literal-pre-shared-key>",     // required for HTTP transport
        "daemon": {                            // local-daemon tunables
            "idle_shutdown_seconds": 3600,     // 0 = disable auto-shutdown
            "process_name": "dh-mcp-systems-server"
        }
    }

Every field is optional and has a schema-level default. Authors who
want to pull a value from an environment variable write
``"<field>": "${env:NAME}"`` in the source JSON.
"""

from __future__ import annotations

__all__ = [
    "DaemonProcessConfig",
    "ServerConfig",
    "load_server",
]

import logging
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr

from deephaven_mcp._pydantic import RedactableSchema
from deephaven_mcp.config._loaders import load_named_json

_LOGGER = logging.getLogger(__name__)


class DaemonProcessConfig(RedactableSchema):
    """Daemon-mode tunables for ``dh-mcp-systems-server --daemon``.

    All fields are optional and have safe defaults. Consulted only
    when the server is launched in daemon mode by the ``dhcli`` CLI;
    ignored under stdio and the foreground HTTP transport.

    Wire format::

        {
            "idle_shutdown_seconds": 3600,
            "process_name": "dh-mcp-systems-server"
        }
    """

    idle_shutdown_seconds: Annotated[int, Field(ge=0)] = 3600
    """Number of seconds of MCP inactivity after which the daemon
    gracefully exits. ``0`` disables auto-shutdown (the daemon then
    runs until killed). Activity is defined as any successful,
    PSK-authenticated MCP request; failed PSK checks do not reset
    the timer."""

    process_name: str = "dh-mcp-systems-server"
    """Expected process-name token used by the CLI's liveness check
    when validating a registry entry. The CLI cross-checks the
    recorded PID's process name (or command line on Linux/macOS)
    against this value; a mismatch causes the registry entry to be
    treated as stale and discarded."""


class ServerConfig(RedactableSchema):
    """Validated contents of ``server.json``.

    Wire format (every field optional)::

        {
            "transport": "stdio",
            "host": "127.0.0.1",
            "port": 8000,
            "server_name": "deephaven-mcp-systems",
            "psk": "<literal>"
        }
    """

    transport: Literal["stdio", "http"] = "stdio"
    """Transport the systems server exposes by default. ``"stdio"``
    speaks the MCP protocol over the process's stdin/stdout (the
    usual launch-via-client mode); ``"http"`` exposes the streamable-
    HTTP transport on the configured loopback host/port. The
    ``--transport`` CLI flag overrides this when supplied."""

    host: str = "127.0.0.1"
    """HTTP transport bind address. Must be a loopback host
    (``"127.0.0.1"``, ``"::1"``, or ``"localhost"``); the server
    rejects non-loopback values. Ignored under stdio. The ``--host``
    CLI flag overrides this when supplied. In daemon mode
    (``--daemon``) this field is constrained more tightly: only
    ``"127.0.0.1"`` is accepted, because the daemon registry wire
    format and the CLI's MCP client both assume IPv4 loopback. The
    daemon refuses to start with any other value."""

    port: Annotated[int, Field(gt=0, lt=65536)] = 8000
    """HTTP transport TCP port. Ignored under stdio. The ``--port``
    CLI flag overrides this when supplied. In daemon mode
    (``--daemon``) this field is ignored: the daemon always binds an
    ephemeral kernel-chosen port (``0``) and publishes it via the
    daemon registry so the CLI can discover it."""

    server_name: str = "deephaven-mcp-systems"
    """The FastMCP server name advertised in MCP handshakes. Visible
    to clients as the server's identity string; rarely needs
    overriding outside of multi-server testbeds."""

    psk: SecretStr | None = None
    """Pre-shared key required by the HTTP transport (omit for
    stdio, which has no network-exposed surface to protect). The
    HTTP transport rejects any request whose ``X-Deephaven-PSK``
    header does not match this value. The ``--psk`` CLI flag
    overrides this when supplied. In daemon mode (``--daemon``)
    the PSK is auto-generated per-process and this field is
    ignored."""

    daemon: DaemonProcessConfig = Field(default_factory=lambda: DaemonProcessConfig())
    """Daemon-mode tunables (idle shutdown, startup deadline,
    process name). Consulted only when the server is launched
    with ``--daemon``. All fields have safe defaults."""


async def load_server(config_dir: Path) -> ServerConfig | None:
    """Load and validate ``server.json`` if it exists.

    Args:
        config_dir (Path): The audited configuration root.

    Returns:
        ServerConfig | None: ``None`` when ``server.json`` is absent.

    Raises:
        ConfigurationError: When the file exists but cannot be parsed
            or fails validation.
    """
    path = config_dir / "server.json"
    if not path.is_file():
        _LOGGER.info(
            "[_server:load_server] server.json absent; "
            "HTTP transport will be unavailable."
        )
        return None
    return await load_named_json(
        ServerConfig,
        path=path,
        config_dir=config_dir,
        error_label="server.json",
        log_label="_server:server.json",
        logger=_LOGGER,
    )
