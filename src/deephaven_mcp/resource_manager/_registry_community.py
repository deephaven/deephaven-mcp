"""Registry for Deephaven Community sessions.

This module provides :class:`CommunitySessionRegistry`, which loads static
community sessions from configuration and supports dynamic session mutation
(add / remove / count) via its :class:`MutableSessionRegistry` base.
"""

import logging
import sys

if sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp import config

from ._manager import StaticCommunitySessionManager
from ._registry import MutableSessionRegistry

_LOGGER = logging.getLogger(__name__)


class CommunitySessionRegistry(MutableSessionRegistry):
    """Registry for community sessions — both static (from config) and dynamically created.

    Loads static sessions from the ``community.sessions`` section of the MCP config
    at initialization.  Inherits ``add_session``, ``remove_session``, and
    ``count_added_sessions`` from ``MutableSessionRegistry`` for sessions created
    after initialization.
    """

    @override
    async def _load_items(self, config_manager: config.ConfigManager) -> None:
        """Load static session configurations from the ``community.sessions`` config section.

        Reads ``config_data["community"]["sessions"]`` and creates a
        :class:`StaticCommunitySessionManager` for each entry.

        Args:
            config_manager (config.ConfigManager): Source of configuration data.
        """
        config_data = await config_manager.get_config()
        community_sessions_config = config_data.get("community", {}).get("sessions", {})

        _LOGGER.info(
            f"[{self.__class__.__name__}] Found {len(community_sessions_config)} community session configurations to load."
        )

        for session_name, session_config in community_sessions_config.items():
            _LOGGER.info(
                f"[{self.__class__.__name__}] Loading session configuration for '{session_name}'..."
            )
            mgr = StaticCommunitySessionManager(session_name, session_config)
            self._items[mgr.full_name] = mgr
