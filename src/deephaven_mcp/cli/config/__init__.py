"""Configuration models for the ``dh-mcp`` CLI.

The CLI reads an optional ``cli.json`` file alongside ``server.json``
in the standard MCP configuration directory. The file is validated
against :class:`CliConfig` (Pydantic v2) and supplies user-default
values for CLI options such as the output format and the request
timeout. Every value can still be overridden per-invocation by an
explicit CLI flag.
"""

from __future__ import annotations

__all__ = [
    "CliConfig",
    "DaemonConfig",
    "DaemonTimeouts",
    "OutputConfig",
    "RequestConfig",
    "RequestTimeouts",
    "load_cli",
]

from ._cli import (
    CliConfig,
    DaemonConfig,
    DaemonTimeouts,
    OutputConfig,
    RequestConfig,
    RequestTimeouts,
    load_cli,
)
