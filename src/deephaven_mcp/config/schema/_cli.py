"""Schema and loader for ``cli.json``.

The CLI honors an optional ``cli.json`` file in the same
configuration directory consumed by the systems server. The file is
validated against :class:`CliConfig` and supplies per-user defaults
for cosmetic and behavioral options such as the output format, the
request timeout, and whether the daemon is auto-started on demand.

Loader: :func:`load_cli`.

The schema is organised into three top-level domain sections:

- :class:`OutputConfig` — presentation knobs (currently ``format``).
- :class:`DaemonControlConfig` — CLI-side daemon lifecycle settings, including a
  :class:`DaemonTimeouts` sub-section for daemon-lifecycle timeouts.
- :class:`RequestConfig` — outbound MCP request settings, including a
  :class:`RequestTimeouts` sub-section for request-level timeouts.

Each section has its own ``timeouts:`` sub-section reserved from day one
(on the sections that have any time-shaped knobs), so future timeouts
slot in without a breaking schema change. This mirrors the project's
existing :class:`~deephaven_mcp.client._timeouts.CommunityClientTimeouts`
/ :class:`~deephaven_mcp.client._timeouts.EnterpriseClientTimeouts`
pattern.

Wire format (JSON5; ``//`` comments are accepted)::

    {
        "output": {
            "format": "human"               // "human" | "json" | "yaml"
        },
        "daemon": {
            "auto_start": true,
            "timeouts": {
                "startup_deadline_seconds": 30,
                "kill_after_seconds": 10
            }
        },
        "request": {
            "timeouts": {
                "default_seconds": 60
            }
        }
    }
"""

from __future__ import annotations

__all__ = [
    "CliConfig",
    "DaemonControlConfig",
    "DaemonTimeouts",
    "OutputConfig",
    "RequestConfig",
    "RequestTimeouts",
    "load_cli",
]

import logging
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

from deephaven_mcp._pydantic import RedactableSchema
from deephaven_mcp.config._loaders import load_named_json

_LOGGER = logging.getLogger(__name__)


class OutputConfig(RedactableSchema):
    """CLI output / presentation settings.

    Future fields will cover additional presentation knobs (color,
    row/column limits, pager preference, etc.).
    """

    format: Literal["human", "json", "yaml"] = "json"
    """Default output format. Defaults to ``"json"`` because the CLI is
    machine-first (primarily driven by AI agents); ``"yaml"`` also emits
    a deterministically sorted structured document, and ``"human"`` emits
    terminal-friendly output for interactive use. Overridden per
    invocation by ``-o/--output`` or ``DH_MCP_OUTPUT``."""


class DaemonTimeouts(RedactableSchema):
    """Timeouts the CLI applies to daemon-lifecycle operations."""

    startup_deadline_seconds: Annotated[int, Field(gt=0)] = 30
    """Maximum number of seconds the CLI waits for a freshly spawned
    daemon to write its registry file (``daemon.json``). After this
    deadline the CLI gives up, releases the spawn lock, and reports
    a startup failure. Strictly a CLI-side patience knob — the daemon
    binary itself never reads it."""

    kill_after_seconds: Annotated[int, Field(gt=0)] = 10
    """Maximum number of seconds the CLI waits after sending
    ``SIGTERM`` to the daemon before escalating to ``SIGKILL``.
    Applies to ``dh-mcp daemon stop`` and ``dh-mcp daemon restart``.
    Strictly a CLI-side patience knob — the daemon binary itself
    never reads it."""


class DaemonControlConfig(RedactableSchema):
    """CLI-side daemon lifecycle settings.

    Distinct from the daemon-side ``DaemonProcessConfig`` in
    ``server.json``: these knobs govern how the CLI *interacts with*
    the daemon, not how the daemon configures itself.
    """

    auto_start: bool = True
    """When ``True`` (default), the CLI spawns the local daemon
    on demand if it is not already running. When ``False``, tool
    subcommands exit with a non-zero status before attempting any
    tool call unless the daemon has been started explicitly via
    ``dh-mcp daemon start``."""

    timeouts: DaemonTimeouts = Field(default_factory=DaemonTimeouts)
    """Daemon-lifecycle timeouts."""


class RequestTimeouts(RedactableSchema):
    """Timeouts the CLI applies to outbound MCP requests."""

    default_seconds: Annotated[int, Field(gt=0)] = 60
    """Default maximum number of seconds to wait for an MCP tool call
    to complete. Applied per request unless a per-call override is
    supplied (e.g., via ``--timeout``); surfaced as exit code ``2``
    when exceeded."""


class RequestConfig(RedactableSchema):
    """CLI-side request / RPC settings.

    Future fields will cover non-timeout request behavior such as
    retry policy, concurrency limits, and per-request defaults.
    """

    timeouts: RequestTimeouts = Field(default_factory=RequestTimeouts)
    """Outbound-request timeouts."""


class CliConfig(RedactableSchema):
    """Validated contents of ``cli.json``.

    All fields are optional and carry schema-level defaults; a
    missing or empty ``cli.json`` yields an all-defaults model. The
    CLI reads this model once at startup; per-invocation CLI flags
    override the loaded defaults.
    """

    output: OutputConfig = Field(default_factory=OutputConfig)
    """Output / presentation settings."""

    daemon: DaemonControlConfig = Field(default_factory=DaemonControlConfig)
    """CLI-side daemon lifecycle settings."""

    request: RequestConfig = Field(default_factory=RequestConfig)
    """CLI-side request / RPC settings."""


async def load_cli(config_dir: Path) -> CliConfig:
    """Load and validate ``cli.json``, falling back to defaults when absent.

    ``cli.json`` is optional: every field on :class:`CliConfig` carries
    a sensible default, so a missing file is functionally equivalent to
    an empty object. Centralizing the substitution here means
    :class:`ConfigTree.cli` is always populated and consumers never
    have to choose between a parsed value and a default.

    Args:
        config_dir (Path): The audited configuration root. The same
            directory used by the systems server; ``cli.json`` lives
            alongside ``server.json``.

    Returns:
        CliConfig: The validated model when ``cli.json`` exists;
            ``CliConfig()`` (all defaults) when the file is absent.

    Raises:
        ConfigurationError: When the file exists but cannot be
            parsed, template-expanded, or validated.
    """
    path = config_dir / "cli.json"
    if not path.is_file():
        _LOGGER.info("[_cli:load_cli] cli.json absent; using all-defaults CliConfig.")
        return CliConfig()
    return await load_named_json(
        CliConfig,
        path=path,
        config_dir=config_dir,
        error_label="cli.json",
        log_label="_cli:cli.json",
        logger=_LOGGER,
    )
