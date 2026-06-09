"""Schemas and orchestration for the systems-server config tree.

Loads, validates, and manages the per-file configuration tree that
the multiplexed ``dh-mcp-systems-server`` reads at startup. The
configuration models are Pydantic v2 ``BaseModel`` subclasses: they
act simultaneously as the JSON wire-format schema (parsed from the
on-disk files) and as the runtime objects passed to registries and
session factories.

Public API:

- :class:`ConfigTreeLoader` — coroutine-safe loader for the
  per-file configuration tree (server, cli, community, enterprise).
- :class:`ConfigTree` and the per-section umbrella models
  (:class:`ServerConfig`, :class:`CommunityConfig`,
  :class:`EnterpriseConfig`) plus their nested settings schemas
  (:class:`CommunitySettings`, :class:`EnterpriseSettings`,
  :class:`CommunitySecurity`, :class:`CommunitySessionCreation`,
  :class:`CommunitySessionCreationDefaults`).
- Per-session/per-system **declaration** types
  (``CommunitySessionConfig``, ``EnterpriseSystemConfig`` and the
  enterprise session-creation nested types) live in
  :mod:`deephaven_mcp.sessions`; they are domain value types
  produced by this loader and by runtime callers alike.
- Taxonomy types re-exported from :mod:`deephaven_mcp._taxonomy`:
  :class:`SystemType`, :class:`SystemRef`, :class:`SessionOrigin`.

The reusable file-loading + templating primitives that this package
sits on top of live in :mod:`deephaven_mcp.config`; that package
also re-exports
:class:`~deephaven_mcp._exceptions.ConfigurationError` and the
configuration-directory helpers.

Validation and redaction are handled directly by the models:

- Validation: call ``Model.model_validate(data)`` (raises
  :class:`pydantic.ValidationError`); the loader translates that
  into :class:`~deephaven_mcp._exceptions.ConfigurationError` for
  the public layer.
- Redaction: call
  ``model.model_dump(mode="json", context={"redact": True})``;
  every :class:`pydantic.SecretStr` field is replaced with the
  project's :data:`~deephaven_mcp._redaction.REDACTED` sentinel.
"""

__all__ = [
    # Loader + top-level model
    "ConfigTreeLoader",
    "ConfigTree",
    # Per-section umbrella models
    "DaemonConfig",
    "ServerConfig",
    "CommunityConfig",
    "CommunitySettings",
    "EnterpriseConfig",
    # Nested settings schemas (exposed for callers that want typed access
    # to specific sub-sections).
    "CommunitySecurity",
    "CommunitySessionCreation",
    "CommunitySessionCreationDefaults",
    "CommunityTimeouts",
    "EnterpriseSettings",
    "EnterpriseTimeouts",
    # Taxonomy
    "SessionOrigin",
    "SystemRef",
    "SystemType",
]

from deephaven_mcp._taxonomy import SessionOrigin, SystemRef, SystemType

from ._community import (
    CommunityConfig,
    CommunitySecurity,
    CommunitySessionCreation,
    CommunitySessionCreationDefaults,
    CommunitySettings,
    CommunityTimeouts,
)
from ._enterprise import EnterpriseConfig, EnterpriseSettings, EnterpriseTimeouts
from ._server import DaemonConfig, ServerConfig
from ._tree import ConfigTree, ConfigTreeLoader
