"""
Specialized registry for managing Deephaven community sessions.

This module provides the `CommunitySessionRegistry`, which is responsible for loading and
managing `CommunitySessionManager` instances based on the application configuration.
It builds upon the generic `BaseRegistry` to provide an async-compatible,
coroutine-safe container for community sessions.
"""

import logging
from typing import override

from deephaven_mcp import config
from ._base_registry import BaseRegistry
from ._session_manager import CommunitySessionManager

_LOGGER = logging.getLogger(__name__)


class CommunitySessionRegistry(BaseRegistry[CommunitySessionManager]):
    """
    A registry for managing `CommunitySessionManager` instances.

    This class inherits from `BaseRegistry` and implements the `_load_items`
    method to discover and create `CommunitySessionManager` objects from the
    application's configuration data.
    """

    @override
    async def _load_items(self, config_manager: config.ConfigManager) -> None:
        """
        Loads session configurations and creates CommunitySessionManager instances.

        Args:
            config_manager: The configuration manager to use for loading session configurations.
        """
        config_data = await config_manager.get_config()
        community_sessions_config = config_data.get("community", {}).get("sessions", {})

        _LOGGER.info(
            "[%s] Found %d community session configurations to load.",
            self.__class__.__name__,
            len(community_sessions_config),
        )

        for session_name, session_config in community_sessions_config.items():
            _LOGGER.info("[%s] Loading session configuration for '%s'...", self.__class__.__name__, session_name)
            self._items[session_name] = CommunitySessionManager(
                session_name, session_config
            )
