"""Configuration-directory tree model :class:`ConfigTree` and its loader.

Loads the per-file configuration tree under a single user-private
directory and produces a :class:`ConfigTree` Pydantic model that
mirrors the on-disk layout one-for-one (``server.json`` ->
``server``, ``cli.json`` -> ``cli``, ``community/`` -> ``community``,
``enterprise/`` -> ``enterprise``). Used by the multiplexed MCP
systems server to drive registry construction at startup, and by the
``dhcli`` CLI for ``config show`` / ``config validate``.

Directory layout (under the directory returned by
:func:`deephaven_mcp.config.resolve_config_dir`)::

    server.json                      # server-process tunables (transport, host, port, PSK, ...)
    cli.json                         # dhcli CLI user defaults (optional)
    community/
      settings.json                  # community-wide globals (optional)
      sessions/
        <name>.json                  # one file per static community session
        ...
    enterprise/
      settings.json                  # enterprise-wide globals (optional)
      systems/
        <name>.json                  # one file per enterprise system
        ...

All files are JSON or JSON5. Filename stems are validated against
the ``session_name`` / ``system_name`` field inside each file by the
per-section schemas. The directory permission audit (POSIX strict,
Windows best-effort) runs before any file is parsed.
"""

from __future__ import annotations

__all__ = [
    "ConfigTree",
    "ConfigTreeLoader",
    "no_systems_configured_message",
]

import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

from pydantic import Field

from deephaven_mcp._exceptions import ConfigurationError, InternalError
from deephaven_mcp._pydantic import StrictSchema
from deephaven_mcp._taxonomy import COMMUNITY_SYSTEM_NAME, SystemRef, SystemType

from . import resolve_config_dir, verify_config_directory_permissions
from .schema import (
    CliConfig,
    CommunityConfig,
    EnterpriseConfig,
    ServerConfig,
    load_cli,
    load_community,
    load_enterprise,
    load_server,
)

_LOGGER = logging.getLogger(__name__)


def no_systems_configured_message(config_dir: Path) -> str:
    """Return the operator guidance for a tree that declares no usable system.

    Args:
        config_dir (Path): The configuration directory named in the message.

    Returns:
        str: The shared message emitted by every zero-system enforcement
            point (systems-server startup and CLI daemon acquisition).
    """
    return (
        f"No systems configured in {config_dir}. Add at least one of: a "
        "community session file under community/sessions/, a "
        "community/settings.json with a session_creation block (enables "
        "dynamically created sessions), or an enterprise system file under "
        "enterprise/systems/. Run 'dhcli config init' for guided setup."
    )


class ConfigTree(StrictSchema):
    """Validated parse of the configuration directory tree.

    Mirrors the on-disk layout one-for-one. Every per-file section is
    optional so the type can also represent partial trees (e.g. a
    deployment that runs only community sessions).

    The type is consumed by both the multiplexed systems server (which
    drives registry construction from ``community`` + ``enterprise``)
    and the ``dhcli`` CLI (which dumps the redacted tree for
    ``config show`` and reconstructs it for ``config validate``).
    """

    config_dir: Path
    """The configuration directory the manager loaded from, after
    resolution by :func:`deephaven_mcp.config.resolve_config_dir`
    (explicit constructor arg, ``$DH_AI_DATA_DIR/config``, or the
    platform default user-data root's ``config`` subdirectory)."""

    server: ServerConfig | None = None
    """Parsed ``server.json``. ``None`` when the file does not exist
    (the HTTP transport is then unavailable; stdio still works)."""

    cli: CliConfig = Field(default_factory=CliConfig)
    """Parsed ``cli.json`` when present, otherwise an all-defaults
    :class:`CliConfig`. Always populated: :func:`load_cli` substitutes
    defaults for an absent file. Unused by the server itself; the
    ``dhcli`` CLI reads this for output formatting and request
    timeouts (with top-level flag overrides applied)."""

    community: CommunityConfig | None = None
    """Parsed community configuration. ``None`` when both
    ``community/settings.json`` is absent and ``community/sessions/``
    is missing or empty (no community sessions are then served)."""

    enterprise: EnterpriseConfig | None = None
    """Parsed enterprise configuration. ``None`` when both
    ``enterprise/settings.json`` is absent and ``enterprise/systems/``
    is missing or empty (no enterprise systems are then served)."""

    def _community_is_servable(self) -> bool:
        """Return whether the community section can serve a session.

        The community section is servable only when it declares at least
        one static session or a ``session_creation`` block (which enables
        dynamically created sessions). A settings-only section (e.g.
        ``community/settings.json`` holding only timeouts) is not.

        Returns:
            bool: True when the community section can serve or create a
                session.
        """
        return self.community is not None and (
            bool(self.community.sessions)
            or self.community.settings.session_creation is not None
        )

    def list_systems(self) -> list[SystemRef]:
        """Return a :class:`SystemRef` for every servable system.

        Only servable systems are listed: the community umbrella appears
        only when the community section can serve or create a session
        (see :meth:`_community_is_servable`) — a settings-only section is
        omitted — and each enterprise system appears in declaration order.

        Returns:
            list[SystemRef]: One entry per servable system, in stable
                order: the umbrella community row first (when servable,
                with ``name == "community"``), then each enterprise
                system in declaration (filename) order.
        """
        out: list[SystemRef] = []
        if self._community_is_servable():
            out.append(SystemRef(name=COMMUNITY_SYSTEM_NAME, type=SystemType.COMMUNITY))
        if self.enterprise is not None:
            for name in self.enterprise.systems:
                out.append(SystemRef(name=name, type=SystemType.ENTERPRISE))
        return out

    def has_usable_system(self) -> bool:
        """Return whether the tree declares at least one servable system.

        Equivalent to a non-empty :meth:`list_systems`: a servable
        community section (a static session or a ``session_creation``
        block) or at least one enterprise system. A settings-only
        section does not count.

        Returns:
            bool: True when at least one system is servable.
        """
        return bool(self.list_systems())


async def _safe[T](
    errors: list[str],
    label: str,
    loader: Callable[[Path], Awaitable[T]],
    config_dir: Path,
) -> T | None:
    """Run ``loader(config_dir)`` and append any error to ``errors``."""
    try:
        return await loader(config_dir)
    except ConfigurationError as exc:
        errors.append(f"{label}: {exc}")
        return None


class ConfigTreeLoader:
    """Explicit-initialization loader for the configuration directory tree.

    Reads ``server.json``, ``cli.json``, ``community/settings.json``,
    ``community/sessions/<name>.json``, ``enterprise/settings.json``,
    and ``enterprise/systems/<name>.json`` from a single user-private
    directory; validates every file and aggregates the results into
    a :class:`ConfigTree`.

    Lifecycle (strict; the class enforces it):

    1. :meth:`__init__` resolves the configuration directory and
       does no I/O. :attr:`config_dir` is available immediately.
    2. :meth:`initialize` must be awaited exactly once. It audits
       the directory and loads every file.
    3. After successful initialization, :attr:`config` returns the
       validated :class:`ConfigTree` synchronously.
    """

    def __init__(self, config_dir: Path | None = None) -> None:
        """Resolve the configuration directory without performing any I/O.

        Args:
            config_dir (Path | None): Configuration directory. When
                ``None``, resolves via
                :func:`deephaven_mcp.config.resolve_config_dir`.
        """
        self._config_dir = resolve_config_dir(config_dir)
        self._cache: ConfigTree | None = None

    @property
    def config_dir(self) -> Path:
        """The resolved configuration directory."""
        return self._config_dir

    @property
    def config(self) -> ConfigTree:
        """Return the loaded configuration.

        Raises:
            InternalError: If :meth:`initialize` has not been
                awaited yet.
        """
        if self._cache is None:
            raise InternalError(
                "ConfigTreeLoader.config accessed before initialize(). "
                "Call `await loader.initialize()` exactly once before reading "
                "`loader.config`."
            )
        return self._cache

    async def initialize(self) -> ConfigTree:
        """Audit the configuration directory and load every file.

        Single-shot: the call performs exactly one audit pass and
        one load pass with no retries, no backoff, and no
        transient-failure handling. Every error (permission audit
        failure, missing-or-malformed file, validation failure)
        propagates as a :class:`ConfigurationError` on the first
        occurrence. A second call on the same instance raises
        :class:`InternalError`; the caller constructs a new
        loader to reload.

        Returns:
            ConfigTree: The validated, fully-populated configuration
                tree.

        A zero-system tree is a valid result and is returned like any
        other: the zero-system invariant is enforced by system
        consumers (systems-server startup and CLI daemon acquisition
        via :meth:`ConfigTree.has_usable_system`), not by this loader.

        Raises:
            ConfigurationError: When the permission audit fails or
                any configuration file cannot be loaded or validated.
            InternalError: When :meth:`initialize` has already been
                awaited on this instance.
        """
        if self._cache is not None:
            raise InternalError(
                "ConfigTreeLoader.initialize() called more than once "
                "on the same instance."
            )
        self._cache = await self._load()
        return self._cache

    async def _load(self) -> ConfigTree:
        """Audit the resolved config dir and load every file."""
        config_dir = self._config_dir
        _LOGGER.info(
            f"[tree:ConfigTreeLoader._load] Loading configuration " f"from {config_dir}"
        )
        verify_config_directory_permissions(config_dir)

        errors: list[str] = []
        server = await _safe(errors, "server.json", load_server, config_dir)
        cli = await _safe(errors, "cli.json", load_cli, config_dir)
        community = await _safe(errors, "community", load_community, config_dir)
        enterprise = await _safe(errors, "enterprise", load_enterprise, config_dir)

        if errors:
            joined = "\n".join(f"  - {e}" for e in errors)
            msg = f"Configuration directory {config_dir} contains errors:\n{joined}"
            _LOGGER.error(f"[tree:ConfigTreeLoader._load] {msg}")
            raise ConfigurationError(msg)

        # A zero-system tree is a valid configuration and loads cleanly
        # here: consumers that do not need a system (docs commands,
        # daemon stop, the config authoring/inspection verbs, config
        # validate/show) must be able to load it. The zero-system
        # invariant is enforced only where a system is actually required
        # -- systems-server startup and the CLI daemon-acquisition path
        # (see ConfigTree.has_usable_system / no_systems_configured_message).

        # ``cli`` is ``None`` only when ``_safe`` swallowed an error,
        # which would have already triggered the ``raise`` above. The
        # ``or`` keeps the type checker happy for the fall-through.
        result = ConfigTree(
            config_dir=config_dir,
            server=server,
            cli=cli or CliConfig(),
            community=community,
            enterprise=enterprise,
        )
        _LOGGER.info(
            f"[tree:ConfigTreeLoader._load] Loaded "
            f"{len(result.list_systems())} system(s) from {config_dir}"
        )
        return result
