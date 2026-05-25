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
        "psk": "<literal-pre-shared-key>"      // required for HTTP transport
    }

Every field is optional and has a schema-level default. Authors who
want to pull a value from an environment variable write
``"<field>": "${env:NAME}"`` in the source JSON.
"""

from __future__ import annotations

__all__ = [
    "ServerConfig",
    "load_server",
]

import logging
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr

from deephaven_mcp._pydantic import RedactableSchema

from ._loaders import load_named_json

_LOGGER = logging.getLogger(__name__)


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
    CLI flag overrides this when supplied."""

    port: Annotated[int, Field(gt=0, lt=65536)] = 8000
    """HTTP transport TCP port. Ignored under stdio. The ``--port``
    CLI flag overrides this when supplied."""

    server_name: str = "deephaven-mcp-systems"
    """The FastMCP server name advertised in MCP handshakes. Visible
    to clients as the server's identity string; rarely needs
    overriding outside of multi-server testbeds."""

    psk: SecretStr | None = None
    """Pre-shared key required by the HTTP transport (omit for
    stdio, which has no network-exposed surface to protect). The
    HTTP transport rejects any request whose ``X-Deephaven-PSK``
    header does not match this value. The ``--psk`` CLI flag
    overrides this when supplied."""


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
