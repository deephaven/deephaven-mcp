"""Schema and loader for ``cli.json``.

The CLI honors an optional ``cli.json`` file in the same
configuration directory consumed by the systems server. The file is
validated against :class:`CliConfig` and supplies per-user defaults
for cosmetic and behavioral options such as the output format, the
request timeout, and whether the daemon is auto-started on demand.

Loader: :func:`load_cli`.

The schema is organized into four top-level domain sections:

- :class:`OutputConfig` — presentation knobs (currently ``format``).
- :class:`DaemonControlConfig` — CLI-side daemon lifecycle settings, including a
  :class:`DaemonTimeouts` sub-section for daemon-lifecycle timeouts.
- :class:`RequestConfig` — outbound MCP request settings, including a
  :class:`RequestTimeouts` sub-section for request-level timeouts.
- :class:`DocsConfig` — docs MCP server settings (endpoint URL), including a
  :class:`DocsTimeouts` sub-section for docs-request timeouts.

Each section has its own ``timeouts:`` sub-section reserved from day one
(on the sections that have any time-shaped knobs), so future timeouts
slot in without a breaking schema change. This mirrors the project's
existing :class:`~deephaven_mcp.client._timeouts.CommunityClientTimeouts`
/ :class:`~deephaven_mcp.client._timeouts.EnterpriseClientTimeouts`
pattern.

Wire format (JSON5; ``//`` comments are accepted)::

    {
        "output": {
            "format": "human"               // "human" | "json" | "json-pretty" | "yaml"
        },
        "daemon": {
            "auto_start": true,
            "reuse": {
                "version": "refuse",        // ignore | warn | restart | refuse
                "venv": "refuse",
                "fingerprint": "warn"
            },
            "timeouts": {
                "startup_deadline_seconds": 30,
                "kill_after_seconds": 10
            }
        },
        "request": {
            "timeouts": {
                "default_seconds": 60
            }
        },
        "docs": {
            "url": "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp",
            "timeouts": {
                "request_seconds": 120
            }
        }
    }
"""

from __future__ import annotations

__all__ = [
    "CliConfig",
    "DaemonControlConfig",
    "DaemonReuseAction",
    "DaemonReusePolicy",
    "DaemonTimeouts",
    "DocsConfig",
    "DocsTimeouts",
    "OutputConfig",
    "RequestConfig",
    "RequestTimeouts",
    "load_cli",
]

import logging
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, field_validator

from deephaven_mcp._pydantic import RedactableSchema
from deephaven_mcp.config._loaders import load_named_json

_LOGGER = logging.getLogger(__name__)


class OutputConfig(RedactableSchema):
    """CLI output / presentation settings.

    Future fields will cover additional presentation knobs (color,
    row/column limits, pager preference, etc.).
    """

    format: Literal["human", "json", "json-pretty", "yaml"] = "json"
    """Default output format. Defaults to ``"json"`` — compact
    single-line JSON — because the CLI is machine-first (primarily
    driven by AI agents); ``"json-pretty"`` emits the same document
    indented for human reading, ``"yaml"`` also emits a
    deterministically sorted structured document, and ``"human"`` emits
    terminal-friendly output for interactive use. Overridden per
    invocation by ``-o/--output`` or ``DHCLI_OUTPUT``."""


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
    Applies to ``dhcli daemon stop`` and ``dhcli daemon restart``.
    Strictly a CLI-side patience knob — the daemon binary itself
    never reads it."""


class DaemonReuseAction(StrEnum):
    """What the CLI does about a daemon-build difference on one identity field.

    The single source of truth for the action vocabulary: it types the
    :class:`DaemonReusePolicy` fields (so Pydantic validates ``cli.json``
    values against it) and is the symbolic constant the CLI's reuse engine
    branches on. Each member carries its severity rank via the
    ``(value, severity)`` tuple, ordered least to most severe
    (``ignore < warn < restart < refuse``); when several identity fields
    differ, the reuse engine selects the most-severe action by
    :attr:`severity`. Adding a member without a rank is a ``TypeError`` at
    class-construction time.
    """

    IGNORE = ("ignore", 0)
    """Reuse the running daemon silently."""

    WARN = ("warn", 1)
    """Reuse the running daemon but emit a warning to stderr."""

    RESTART = ("restart", 2)
    """Stop the running daemon and spawn a fresh one. Degrades to
    :attr:`REFUSE` when spawning is not permitted (auto-start disabled)."""

    REFUSE = ("refuse", 3)
    """Decline to reuse the running daemon and raise an error."""

    severity: int

    def __new__(cls, value: str, severity: int) -> DaemonReuseAction:
        """Bind the string value and its severity rank together.

        ``StrEnum``'s default ``__new__`` accepts a single string value.
        Extending it to a ``(value, severity)`` tuple makes severity a
        first-class attribute; adding a member without a rank fails at
        class-construction time when this initializer raises ``TypeError``
        for the missing argument.
        """
        member = str.__new__(cls, value)
        member._value_ = value
        member.severity = severity
        return member


class DaemonReusePolicy(RedactableSchema):
    """Per-field action when a live daemon is a different build than the CLI.

    On every reuse decision the CLI compares the running daemon's recorded
    build identity (version + venv + source fingerprint) against its own.
    Each field below is the action for one differing identity field; when
    several fields differ the *most severe* action wins, ordered
    ``ignore < warn < restart < refuse``. ``restart`` degrades to ``refuse``
    when auto-start is disabled (a restart implies a spawn).
    """

    version: DaemonReuseAction = DaemonReuseAction.REFUSE
    """Action when the daemon's ``deephaven-mcp`` package version differs
    from the CLI's. Defaults to ``refuse``: a different release may carry
    protocol or behavior drift, so the CLI declines to reuse it."""

    venv: DaemonReuseAction = DaemonReuseAction.REFUSE
    """Action when the daemon's virtualenv (``sys.prefix``) differs from the
    CLI's. Defaults to ``refuse``: the venv is the only signal for the
    surrounding environment (which ``deephaven-server`` is installed, the
    executable-resolution path), which the source fingerprint cannot see."""

    fingerprint: DaemonReuseAction = DaemonReuseAction.WARN
    """Action when only the source fingerprint differs (same version and
    venv) — an in-place code edit. Defaults to ``warn`` so developers iterate
    without restarting, and so timestamp-only churn (reinstall, restore,
    ``touch``) never hard-blocks an end user."""


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
    ``dhcli daemon start``."""

    reuse: DaemonReusePolicy = Field(default_factory=DaemonReusePolicy)
    """Per-field policy for reusing a daemon that is a different build
    than the CLI."""

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


class DocsTimeouts(RedactableSchema):
    """Timeouts the CLI applies to docs MCP server requests."""

    request_seconds: Annotated[int, Field(gt=0)] = 120
    """Maximum number of seconds to wait for a docs server tool call
    to complete. Docs queries are answered by an LLM backend, so the
    default is higher than the daemon request default. Overridden per
    invocation by ``--timeout``; surfaced as exit code ``2`` when
    exceeded."""


class DocsConfig(RedactableSchema):
    """Docs MCP server settings for the ``dhcli docs`` commands."""

    url: str = "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp"
    """Streamable-HTTP endpoint of the docs MCP server. Must be an
    ``http://`` or ``https://`` URL with a host. Defaults to the
    Deephaven-hosted production docs server; point it at another
    endpoint (e.g. a self-hosted ``dh-mcp-docs-server``) to query that
    instead."""

    timeouts: DocsTimeouts = Field(default_factory=DocsTimeouts)
    """Docs-request timeouts."""

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        """Reject ``docs.url`` values that are not http(s) URLs with a host.

        The field is a streamable-HTTP endpoint; catching an empty
        value, a malformed URL, or an unsupported scheme here surfaces
        the mistake as ``config_invalid`` at load time instead of a
        later ``mcp_request_failed``.

        Args:
            value (str): The raw ``url`` field value.

        Returns:
            str: ``value``, unchanged, when valid.

        Raises:
            ValueError: When ``value`` is not an ``http://`` or
                ``https://`` URL with a non-empty host, its port is
                non-numeric or out of range, or it contains userinfo
                credentials.
        """
        message = (
            f"docs.url must be an http:// or https:// URL with a host, got {value!r}"
        )
        try:
            parts = urlsplit(value)
            # ``hostname``, not ``netloc``: a hostless authority such
            # as ``http://@/mcp`` or ``http://:8000/mcp`` has a
            # non-empty netloc but no host to connect to. ``port``
            # raises for a non-numeric or out-of-range port.
            hostname = parts.hostname
            _ = parts.port
        except ValueError as exc:
            raise ValueError(message) from exc
        if parts.scheme not in ("http", "https") or not hostname:
            raise ValueError(message)
        # The docs transport takes no credentials, and the configured
        # URL is surfaced verbatim (docs status payload, error
        # messages) — rejecting userinfo eagerly keeps secrets out of
        # terminal output and logs.
        if parts.username is not None or parts.password is not None:
            raise ValueError(
                "docs.url must not contain userinfo credentials "
                "(user:password@); the docs server takes no credentials "
                "and the URL is echoed in output and error messages"
            )
        return value


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

    docs: DocsConfig = Field(default_factory=DocsConfig)
    """Docs MCP server settings."""


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
        error_label="CLI config",
        log_label="_cli:cli.json",
        logger=_LOGGER,
    )
