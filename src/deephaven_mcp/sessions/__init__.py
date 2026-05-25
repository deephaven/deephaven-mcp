"""Domain value types for declaring Deephaven sessions and systems.

This package owns the **declaration** types — the typed descriptions
of how to connect to a Deephaven Community session or a Deephaven
Enterprise system. The types live here, not in
:mod:`deephaven_mcp.config`, because they describe domain objects
that happen to be loaded from configuration files today: the same
classes are produced by the dynamic-session tools at runtime and
could be produced by future non-file sources.

Distinct from live-session types:

- :class:`CommunitySessionConfig` describes *how to connect to* a
  community session. The live session itself is
  :class:`deephaven_mcp.client.CoreSession`.
- :class:`EnterpriseSystemConfig` describes *how to connect to* an
  enterprise system. The live factory is
  :class:`deephaven_mcp.client.CorePlusSessionFactory`.

The ``Config`` suffix is intentional: it disambiguates these
declaration value types from the many live-session and
session-manager classes that share the same domain prose
(``CoreSession``, ``CommunitySessionManager``,
``CommunitySessionRegistry``, etc.).
"""

__all__ = [
    "CommunitySessionConfig",
    "EnterpriseSessionCreation",
    "EnterpriseSessionCreationDefaults",
    "EnterpriseSystemConfig",
]

from ._community import CommunitySessionConfig
from ._enterprise import (
    EnterpriseSessionCreation,
    EnterpriseSessionCreationDefaults,
    EnterpriseSystemConfig,
)
