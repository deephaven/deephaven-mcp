"""Schemas and loader for the ``enterprise/`` section of the MCP config tree.

Contains:

- :class:`EnterpriseSettings` - the Pydantic schema for
  ``enterprise/settings.json``.
- :class:`EnterpriseConfig` - the umbrella that
  :class:`~deephaven_mcp.mcp_systems_server.config.ConfigTreeLoader`
  produces after loading ``enterprise/settings.json`` and every
  ``enterprise/systems/<name>.json`` file.
- :func:`load_enterprise` - the section loader the manager invokes.

Sibling of :mod:`deephaven_mcp.mcp_systems_server.config._community`.

The per-system declaration type itself
(:class:`~deephaven_mcp.sessions.EnterpriseSystemConfig`) and its
nested session-creation models live in
:mod:`deephaven_mcp.sessions` - they are domain value types,
produced both by this loader and by runtime callers.
"""

from __future__ import annotations

__all__ = [
    "EnterpriseConfig",
    "EnterpriseSettings",
    "EnterpriseTimeouts",
    "load_enterprise",
]

import logging
from pathlib import Path

from pydantic import Field

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._pydantic import StrictSchema
from deephaven_mcp.client._timeouts import EnterpriseClientTimeouts
from deephaven_mcp.config._loaders import load_named_json, load_named_json_with_stem
from deephaven_mcp.mcp_systems_server._tools._pq_config import PqToolsConfig
from deephaven_mcp.mcp_systems_server._tools._response_limits import ResponseLimits
from deephaven_mcp.resource_manager._evictor import EvictionTimeouts
from deephaven_mcp.sessions import EnterpriseSystemConfig

_LOGGER = logging.getLogger(__name__)


class EnterpriseTimeouts(StrictSchema):
    """All operator-tunable duration knobs for the enterprise section.

    Single umbrella for every duration the operator may tune in
    ``enterprise/settings.json``. Two typed sub-blocks split by
    consumer:

    - :attr:`client`: deadlines the Deephaven client library applies
      to outbound enterprise RPCs and persistent-query state waits.
    - :attr:`eviction`: MCP-side idle-session eviction policy
      (applied uniformly to every enterprise system).
    """

    client: EnterpriseClientTimeouts = Field(default_factory=EnterpriseClientTimeouts)
    """Client-layer timeouts for outbound Deephaven Enterprise RPCs
    and persistent-query state waits. Defaults to a default-
    constructed :class:`EnterpriseClientTimeouts` when absent from
    the JSON."""

    eviction: EvictionTimeouts = Field(default_factory=EvictionTimeouts)
    """MCP-side idle-session eviction policy applied uniformly to
    every enterprise system. Defaults to a default-constructed
    :class:`EvictionTimeouts` when absent from the JSON."""


class EnterpriseSettings(StrictSchema):
    """Validated contents of ``enterprise/settings.json``."""

    timeouts: EnterpriseTimeouts = Field(default_factory=EnterpriseTimeouts)
    """All operator-tunable duration knobs for the enterprise section,
    grouped under :attr:`~EnterpriseTimeouts.client` (outbound RPC
    deadlines + persistent-query state waits) and
    :attr:`~EnterpriseTimeouts.eviction` (MCP-side idle-session
    sweeper applied uniformly to every enterprise system). Defaults
    to a default-constructed :class:`EnterpriseTimeouts` when absent
    from the JSON. Independent of the worker-side auto-delete timeout
    (``EnterpriseSessionCreationDefaults.auto_delete_timeout``)."""

    pq_tools: PqToolsConfig = Field(default_factory=PqToolsConfig)
    """Defaults for the persistent-query MCP tools (concurrency cap
    only; timeouts live alongside in ``timeouts.*`` on this same
    settings file). PQ tools are enterprise-only, which is why this
    block sits on :class:`EnterpriseSettings` rather than
    :class:`ServerConfig`. See
    :class:`deephaven_mcp.mcp_systems_server._tools._pq_config.PqToolsConfig`."""

    response_limits: ResponseLimits = Field(default_factory=ResponseLimits)
    """Operator-tunable thresholds for the tool-side response-size
    guard applied when an enterprise tool projects how large a
    serialized response will be. See
    :class:`deephaven_mcp.mcp_systems_server._tools._response_limits.ResponseLimits`."""


class EnterpriseConfig(StrictSchema):
    """Validated enterprise configuration block.

    Sibling of
    :class:`~deephaven_mcp.mcp_systems_server.config.CommunityConfig`.
    Duration knobs live on :class:`EnterpriseSettings` under
    ``timeouts.client`` (outbound RPC + state-wait deadlines) and
    ``timeouts.eviction`` (MCP-side idle-session sweeper, applied
    uniformly to every enterprise system).
    """

    settings: EnterpriseSettings
    """Validated contents of ``enterprise/settings.json`` (defaulted
    to a default-constructed instance when the file is absent)."""

    systems: dict[str, EnterpriseSystemConfig]
    """Validated per-system declarations, keyed by system name
    (filename stem). Empty dict when ``enterprise/systems/`` is
    absent or empty."""


async def load_enterprise(config_dir: Path) -> EnterpriseConfig | None:
    """Load and validate the enterprise section if any enterprise files exist.

    Args:
        config_dir (Path): The audited configuration root.

    Returns:
        EnterpriseConfig | None: ``None`` when both
            ``enterprise/settings.json`` is absent and
            ``enterprise/systems/`` is empty or missing.

    Raises:
        ConfigurationError: When any enterprise file fails validation.
    """
    section_dir = config_dir / "enterprise"
    settings_path = section_dir / "settings.json"
    systems_dir = section_dir / "systems"

    settings_present = settings_path.is_file()
    settings: EnterpriseSettings
    if settings_present:
        settings = await load_named_json(
            EnterpriseSettings,
            path=settings_path,
            config_dir=config_dir,
            error_label="enterprise/settings.json",
            log_label="_enterprise:enterprise/settings.json",
            logger=_LOGGER,
        )
    else:
        settings = EnterpriseSettings()

    systems: dict[str, EnterpriseSystemConfig] = {}
    if systems_dir.is_dir():
        for path in sorted(systems_dir.glob("*.json")):
            name = path.stem
            if name == "community":
                raise ConfigurationError(
                    f"enterprise/systems/{path.name}: 'community' is "
                    f"reserved for the community umbrella system and "
                    f"cannot be used as an enterprise system_name."
                )
            system = await load_named_json_with_stem(
                EnterpriseSystemConfig,
                path=path,
                config_dir=config_dir,
                error_label=f"enterprise system '{name}'",
                log_label=f"_enterprise:enterprise/systems/{path.name}",
                logger=_LOGGER,
            )
            systems[system.name] = system

    if not settings_present and not systems:
        return None
    return EnterpriseConfig(settings=settings, systems=systems)
