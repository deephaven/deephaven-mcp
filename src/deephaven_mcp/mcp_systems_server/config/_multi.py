"""Multi-system configuration manager and top-level :class:`MultiSystemConfig`.

Loads the per-file configuration tree under a single user-private
directory and produces a :class:`MultiSystemConfig` Pydantic model
holding the validated server, community, and enterprise
configurations. Used by the multiplexed MCP systems server to drive
registry construction at startup.

Directory layout (under :func:`default_config_dir` or
``$DH_MCP_CONFIG_DIR``)::

    server.json                      # server-process tunables (transport, host, port, PSK, ...)
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
    "MultiSystemConfig",
    "MultiSystemConfigManager",
]

import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

from deephaven_mcp._exceptions import ConfigurationError, InternalError
from deephaven_mcp._pydantic import StrictSchema
from deephaven_mcp._taxonomy import SystemRef, SystemType
from deephaven_mcp.config import (
    resolve_config_dir,
    verify_config_directory_permissions,
)

from ._community import CommunityConfig, load_community
from ._enterprise import EnterpriseConfig, load_enterprise
from ._server import ServerConfig, load_server

_LOGGER = logging.getLogger(__name__)


class MultiSystemConfig(StrictSchema):
    """Top-level container for the multi-system configuration tree."""

    config_dir: Path
    """The configuration directory the manager loaded from, after
    resolving the source (explicit constructor arg, the
    ``DH_MCP_CONFIG_DIR`` env var, or the platform default)."""

    server: ServerConfig | None = None
    """Parsed ``server.json``. ``None`` when the file does not exist
    (the HTTP transport is then unavailable; stdio still works)."""

    community: CommunityConfig | None = None
    """Parsed community configuration. ``None`` when both
    ``community/settings.json`` is absent and ``community/sessions/``
    is missing or empty (no community sessions are then served)."""

    enterprise: EnterpriseConfig | None = None
    """Parsed enterprise configuration. ``None`` when both
    ``enterprise/settings.json`` is absent and ``enterprise/systems/``
    is missing or empty (no enterprise systems are then served)."""

    def list_systems(self) -> list[SystemRef]:
        """Return a :class:`SystemRef` for every configured system.

        Returns:
            list[SystemRef]: One entry per system, in stable order:
                the umbrella community row first (when present, with
                ``name == "community"``), then each enterprise
                system in declaration (filename) order.
        """
        out: list[SystemRef] = []
        if self.community is not None:
            out.append(SystemRef(name="community", type=SystemType.COMMUNITY))
        if self.enterprise is not None:
            for name in self.enterprise.systems:
                out.append(SystemRef(name=name, type=SystemType.ENTERPRISE))
        return out


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


class MultiSystemConfigManager:
    """Explicit-initialization loader for the per-file MCP configuration tree.

    Reads ``server.json``, ``community/settings.json``,
    ``community/sessions/<name>.json``, ``enterprise/settings.json``,
    and ``enterprise/systems/<name>.json`` from a single user-private
    directory; validates every file and aggregates the results into
    a :class:`MultiSystemConfig`.

    Lifecycle (strict; the class enforces it):

    1. :meth:`__init__` resolves the configuration directory and
       does no I/O. :attr:`config_dir` is available immediately.
    2. :meth:`initialize` must be awaited exactly once. It audits
       the directory and loads every file.
    3. After successful initialization, :attr:`config` returns the
       validated :class:`MultiSystemConfig` synchronously.
    """

    def __init__(self, config_dir: Path | None = None) -> None:
        """Resolve the configuration directory without performing any I/O."""
        self._config_dir = resolve_config_dir(config_dir)
        self._cache: MultiSystemConfig | None = None

    @property
    def config_dir(self) -> Path:
        """The resolved configuration directory."""
        return self._config_dir

    @property
    def config(self) -> MultiSystemConfig:
        """Return the loaded configuration.

        Raises:
            InternalError: If :meth:`initialize` has not been
                awaited yet.
        """
        if self._cache is None:
            raise InternalError(
                "MultiSystemConfigManager.config accessed before initialize(). "
                "Call `await manager.initialize()` exactly once before reading "
                "`manager.config`."
            )
        return self._cache

    async def initialize(self) -> MultiSystemConfig:
        """Audit the configuration directory and load every file.

        Single-shot: the call performs exactly one audit pass and
        one load pass with no retries, no backoff, and no
        transient-failure handling. Every error (permission audit
        failure, missing-or-malformed file, validation failure)
        propagates as a :class:`ConfigurationError` on the first
        occurrence. A second call on the same instance raises
        :class:`InternalError`; the caller constructs a new
        manager to reload.

        Returns:
            MultiSystemConfig: The validated, fully-populated
                configuration tree.

        Raises:
            ConfigurationError: When the permission audit fails or
                any configuration file cannot be loaded or validated.
            InternalError: When :meth:`initialize` has already been
                awaited on this instance.
        """
        if self._cache is not None:
            raise InternalError(
                "MultiSystemConfigManager.initialize() called more than once "
                "on the same instance."
            )
        self._cache = await self._load()
        return self._cache

    async def _load(self) -> MultiSystemConfig:
        """Audit the resolved config dir and load every file."""
        config_dir = self._config_dir
        _LOGGER.info(
            f"[_multi:MultiSystemConfigManager._load] Loading configuration "
            f"from {config_dir}"
        )
        verify_config_directory_permissions(config_dir)

        errors: list[str] = []
        server = await _safe(errors, "server.json", load_server, config_dir)
        community = await _safe(errors, "community", load_community, config_dir)
        enterprise = await _safe(errors, "enterprise", load_enterprise, config_dir)

        if errors:
            joined = "\n".join(f"  - {e}" for e in errors)
            msg = f"Configuration directory {config_dir} contains errors:\n{joined}"
            _LOGGER.error(f"[_multi:MultiSystemConfigManager._load] {msg}")
            raise ConfigurationError(msg)

        if community is None and enterprise is None:
            msg = (
                f"No systems configured in {config_dir}. Add at least one "
                "community session under community/sessions/ or one "
                "enterprise system under enterprise/systems/."
            )
            _LOGGER.error(f"[_multi:MultiSystemConfigManager._load] {msg}")
            raise ConfigurationError(msg)

        result = MultiSystemConfig(
            config_dir=config_dir,
            server=server,
            community=community,
            enterprise=enterprise,
        )
        _LOGGER.info(
            f"[_multi:MultiSystemConfigManager._load] Loaded "
            f"{len(result.list_systems())} system(s) from {config_dir}"
        )
        return result
