"""
Async Deephaven MCP configuration management.

This module provides async functions to load, validate, and manage configuration for Deephaven MCP from a JSON file.
Configuration is loaded from a file specified by the DH_MCP_CONFIG_FILE environment variable using native async file I/O (aiofiles).

Features:
    - Coroutine-safe, cached loading of configuration using asyncio.Lock.
    - Strict validation of configuration structure and values.
    - Helper functions to access community session-specific config, and community session names.
    - Logging of configuration loading, environment variable value, and validation steps.
    - Uses aiofiles for non-blocking, native async config file reads.

Configuration Schema:
---------------------
The configuration file must be a JSON object. It may contain the following top-level key:

  - `community_sessions` (dict, optional):
      A dictionary mapping community session names (str) to client session configuration dicts.
      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
      If this key is absent, it implies no community sessions are configured.
      Each community session configuration dict may contain any of the following fields (all are optional):

        - `host` (str): Hostname or IP address of the community worker.
        - `port` (int): Port number for the community worker connection.
        - `auth_type` (str): Authentication type. Allowed values include:
            * "token": Use a bearer token for authentication.
            * "basic": Use HTTP Basic authentication.
            * "anonymous": No authentication required.
        - `auth_token` (str): The authentication token or password. May be empty if `auth_type` is "anonymous".
        - `never_timeout` (bool): If True, sessions to this community worker never time out.
        - `session_type` (str): Programming language for the session. Common values include:
            * "python": For Python-based Deephaven instances.
            * "groovy": For Groovy-based Deephaven instances.
        - `use_tls` (bool): Whether to use TLS/SSL for the connection.
        - `tls_root_certs` (str, optional): Path to a PEM file containing root certificates to trust for TLS.
        - `client_cert_chain` (str, optional): Path to a PEM file containing the client certificate chain for mutual TLS.
        - `client_private_key` (str, optional): Path to a PEM file containing the client private key for mutual TLS.

      Notes:
        - All fields are optional; if a field is omitted, the consuming code may use an internal default value for that field, or the feature may be disabled.
        - All file paths should be absolute, or relative to the process working directory.
        - If `use_tls` is True and any of the optional TLS fields are provided, they must point to valid PEM files.
        - Sensitive fields (`auth_token`, `client_private_key`) are redacted from logs for security.
        - Unknown fields are not allowed and will cause validation to fail.

Validation rules:
  - If the `community_sessions` key is present, its value must be a dictionary.
  - Within each session configuration, all field values must have the correct type if present.
  - No unknown fields are permitted in session configurations.
  - If TLS fields are provided, referenced files must exist and be readable.

Configuration JSON Specification:
---------------------------------
- The configuration file must be a JSON object.
- It may optionally contain a `"community_sessions"` top-level key:
    - `"community_sessions"`: If present, this must be a dictionary mapping community session names to client session configuration dicts for connecting to community workers. It cannot be an empty dictionary if provided.

Example Valid Configuration (without community_sessions):
---------------------------
```json
{}
```

Example Valid Configuration (with community_sessions):
---------------------------
```json
{
    "community_sessions": {
        "local": {
            "host": "localhost",
            "port": 10000,
            "auth_type": "token",
            "auth_token": "your-token-here",
            "never_timeout": true,
            "session_type": "python",
            "use_tls": true,
            "tls_root_certs": "/path/to/certs.pem",
            "client_cert_chain": "/path/to/client-cert.pem",
            "client_private_key": "/path/to/client-key.pem"
        },
        "remote": {
            "host": "remote-server.example.com",
            "port": 10000,
            "auth_type": "basic",
            "auth_token": "basic-auth-token",
            "never_timeout": false,
            "session_type": "groovy",
            "use_tls": true
        }
    }
}
```

Example Invalid Configurations:
------------------------------
1. Invalid: Session field with wrong type
```json
{
    "community_sessions": {
        "local": {
            "host": 12345,  // Should be a string, not an integer
            "port": "not-a-port"  // Should be an integer, not a string
        }
    }
}
```

Performance Considerations:
--------------------------
- Uses native async file I/O (aiofiles) to avoid blocking the event loop.
- Employs an `asyncio.Lock` to ensure coroutine-safe, cached configuration loading.
- Designed for high-throughput, concurrent environments.

Usage Patterns:
-----------------------------------------------------------------------------
- The configuration may optionally include a `community_sessions` dictionary as a top-level key.
- Loading a community session configuration:
    >>> session_config = await config_manager.get_community_session_config('local_session_name')
    >>> # connection = connect(**session_config)  # Example usage
- Listing available configured community sessions:
    >>> session_names = await config_manager.get_community_session_names()
    >>> for session_name in session_names:
    ...     print(f"Available community session: {session_name}")

Environment Variables:
---------------------
- `DH_MCP_CONFIG_FILE`: Path to the Deephaven MCP configuration JSON file.

Security:
---------
- Sensitive information (such as authentication tokens) is redacted in logs.
- Environment variable values are logged for debugging.

Async/Await & I/O:
------------------
- All configuration loading is async and coroutine-safe.
- File I/O uses `aiofiles` for non-blocking reads.
"""

import asyncio
import json
import logging
import os
from time import perf_counter
from typing import Any, cast

import aiofiles

from .community_session import (
    CommunitySessionConfigurationError,
    redact_community_session_config,
    validate_community_sessions_config,
)
from .enterprise_session import (
    validate_enterprise_sessions_config,
    EnterpriseSessionConfigurationError,  
)


_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""
str: Name of the environment variable specifying the path to the Deephaven MCP config file.
"""


class ConfigManager:
    _REQUIRED_TOP_LEVEL_KEYS: set[str] = (
        set()
    )  # Defines mandatory top-level keys in the config
    _ALLOWED_TOP_LEVEL_KEYS: set[str] = {
        "community_sessions",
        "enterprise_sessions",
    }  # Defines all allowed top-level keys

    """
    Async configuration manager for Deephaven MCP configuration.

    This class encapsulates all logic for loading, validating, and caching the configuration for Deephaven MCP.
    """

    def __init__(self) -> None:
        """
        Initialize a new ConfigManager instance.

        Sets up the internal configuration cache and an asyncio.Lock for coroutine safety.
        Typically, only one instance (DEFAULT_CONFIG_MANAGER) should be used in production.
        """
        self._cache: dict[str, Any] | None = None
        self._lock = asyncio.Lock()

    async def clear_config_cache(self) -> None:
        """
        Clear the cached Deephaven configuration (coroutine-safe).

        This will force the next configuration access to reload from disk. Useful for tests or when the config file has changed.

        Returns:
            None

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> await config_manager.clear_config_cache()
        """
        _LOGGER.debug("Clearing Deephaven configuration cache...")
        async with self._lock:
            self._cache = None

        _LOGGER.debug("Configuration cache cleared.")

    async def set_config_cache(self, config: dict[str, Any]) -> None:
        """
        Set the in-memory configuration cache (coroutine-safe, for testing only).

        Args:
            config (dict[str, Any]): The configuration dictionary to set as the cache. This will be validated before caching.

        Returns:
            None

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> await config_manager.set_config_cache({'community_sessions': {'example_session': {}}})
        """
        async with self._lock:
            self._cache = self.validate_config(config)

    async def get_config(self) -> dict[str, Any]:
        """
        Load and validate the Deephaven MCP application configuration from disk (coroutine-safe).

        Uses `aiofiles` for async file I/O and caches the result. If the cache is present,
        returns it; otherwise, loads from disk and validates.

        Returns:
            dict[str, Any]: The loaded and validated configuration dictionary.

        Raises:
            RuntimeError: If the `DH_MCP_CONFIG_FILE` environment variable is not set, or the file cannot be read.
            ValueError: If the config file is invalid (e.g., not JSON, missing required keys, incorrect types).

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> # and DH_MCP_CONFIG_FILE is set appropriately
            >>> # import os
            >>> # os.environ['DH_MCP_CONFIG_FILE'] = '/path/to/config.json'
            >>> config_dict = await config_manager.get_config()
            >>> print(config_dict['community_sessions']['local_session_name']['host'])
            'localhost'
        """
        _LOGGER.debug("Loading Deephaven MCP application configuration...")
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug("Using cached Deephaven MCP application configuration.")
                return self._cache

            _LOGGER.info("Loading Deephaven MCP application configuration from disk...")
            start_time = perf_counter()

            if CONFIG_ENV_VAR not in os.environ:
                _LOGGER.error(f"Environment variable {CONFIG_ENV_VAR} is not set.")
                raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} is not set.")

            config_path = os.environ[CONFIG_ENV_VAR]
            _LOGGER.info(
                f"Environment variable {CONFIG_ENV_VAR} is set to: {config_path}"
            )

            try:
                async with aiofiles.open(config_path) as f:
                    data = json.loads(await f.read())
            except Exception as e:
                _LOGGER.error(f"Failed to load config file {config_path}: {e}")
                raise

            validated = self.validate_config(data)
            self._cache = validated
            _LOGGER.info(
                f"Deephaven community session configuration loaded and validated successfully in {perf_counter() - start_time:.3f} seconds"
            )
            return validated

    async def get_community_session_config(self, session_name: str) -> dict[str, Any]:
        """
        Retrieves the configuration for a specific community session by its name.

        Args:
            session_name (str): The name of the community session to retrieve. This argument is required.

        Returns:
            dict[str, Any]: The configuration dictionary for the specified community session.

        Raises:
            CommunitySessionConfigurationError: If the community session is not found or config is invalid.

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> local_session_config = await config_manager.get_community_session_config('local_session_name')
        """
        _LOGGER.debug(f"Getting community session config for session: {session_name!r}")
        config = await self.get_config()
        community_sessions_map = config.get("community_sessions", {})

        if session_name not in community_sessions_map:
            _LOGGER.error(
                f"Community session {session_name} not found in configuration"
            )
            raise CommunitySessionConfigurationError(
                f"Community session {session_name} not found in configuration"
            )

        _LOGGER.debug(
            f"Retrieved configuration for community session '{session_name}': {redact_community_session_config(community_sessions_map[session_name])}"
        )
        return cast(dict[str, Any], community_sessions_map[session_name])

    async def get_community_session_names(self) -> list[str]:
        """
        Retrieves a list of all configured community session names.

        Returns:
            list[str]: A list of community session names.

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> session_names = await config_manager.get_community_session_names()
            >>> for session_name in session_names:
            ...     print(f"Available community session: {session_name}")
        """
        _LOGGER.debug("Getting list of all community session names")
        config = await self.get_config()
        community_sessions_map = config.get("community_sessions", {})
        session_names = list(community_sessions_map.keys())

        _LOGGER.debug(
            f"Found {len(session_names)} community session(s): {session_names}"
        )
        return session_names

    async def get_enterprise_session_config(self, session_name: str) -> dict[str, Any]:
        """
        Retrieves the configuration for a specific enterprise session by its name.

        Args:
            session_name (str): The name of the enterprise session to retrieve. Required.

        Returns:
            dict[str, Any]: The configuration dictionary for the specified enterprise session.

        Raises:
            EnterpriseSessionConfigurationError: If the enterprise session is not found.

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> enterprise_config = await config_manager.get_enterprise_session_config('prod_cluster')
        """
        _LOGGER.debug(f"Getting enterprise session config for session: {session_name!r}")
        config = await self.get_config()
        enterprise_sessions_map = config.get("enterprise_sessions", {})

        if not isinstance(enterprise_sessions_map, dict) or session_name not in enterprise_sessions_map:
            _LOGGER.error(
                f"Enterprise session '{session_name}' not found in configuration or 'enterprise_sessions' is not a dict."
            )
            raise EnterpriseSessionConfigurationError(
                f"Enterprise session '{session_name}' not found in configuration."
            )
        
        # TODO: Implement redaction for enterprise sessions if needed, similar to community sessions.
        # For now, returning the raw config.
        _LOGGER.debug(
            f"Retrieved configuration for enterprise session '{session_name}': {enterprise_sessions_map[session_name]}" # Add redaction if sensitive
        )
        return cast(dict[str, Any], enterprise_sessions_map[session_name])

    async def get_enterprise_session_names(self) -> list[str]:
        """
        Retrieves a list of all configured enterprise session names.

        Returns:
            list[str]: A list of enterprise session names.

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> session_names = await config_manager.get_enterprise_session_names()
            >>> for session_name in session_names:
            ...     print(f"Available enterprise session: {session_name}")
        """
        _LOGGER.debug("Getting list of all enterprise session names")
        config = await self.get_config()
        enterprise_sessions_map = config.get("enterprise_sessions", {})
        
        if not isinstance(enterprise_sessions_map, dict):
             _LOGGER.warning("'enterprise_sessions' is not a dictionary, returning empty list of names.")
             return []

        session_names = list(enterprise_sessions_map.keys())

        _LOGGER.debug(
            f"Found {len(session_names)} enterprise session(s): {session_names}"
        )
        return session_names

    @staticmethod
    def validate_config(config: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the Deephaven MCP application configuration dictionary.

        Ensures that the configuration has the correct top-level structure.
        The 'community_sessions' and 'enterprise_sessions' keys are optional. If present, they delegate to
        their respective validation functions (`validate_community_sessions_config` and
        `validate_enterprise_sessions_config`) for detailed validation of their content.
        Only known top-level keys are allowed.

        Args:
            config (dict[str, Any]): The configuration dictionary to validate.
                                     May optionally include a 'community_sessions' dictionary as a top-level key.

        Returns:
            dict[str, Any]: The validated configuration dictionary.

        Raises:
            ValueError: If the config has unknown top-level keys,
                        or if 'community_sessions' is present and `validate_community_sessions_config` finds issues.

        Example:
            >>> validated_config = ConfigManager.validate_config({'community_sessions': {'local_session': {}}})
            >>> validated_config_empty = ConfigManager.validate_config({}) # Also valid
        """
        top_level_keys = set(config.keys())

        # Check for missing required keys
        missing_keys = ConfigManager._REQUIRED_TOP_LEVEL_KEYS - top_level_keys
        if missing_keys:
            _LOGGER.error(
                f"Missing required top-level keys in Deephaven MCP config: {missing_keys}"
            )
            raise ValueError(
                f"Missing required top-level keys in Deephaven MCP config: {missing_keys}"
            )

        # Check for unknown keys (keys present that are not in allowed_top_level)
        unknown_keys = top_level_keys - ConfigManager._ALLOWED_TOP_LEVEL_KEYS
        if unknown_keys:
            _LOGGER.error(
                f"Unknown top-level keys in Deephaven MCP config: {unknown_keys}"
            )
            raise ValueError(
                f"Unknown top-level keys in Deephaven MCP config: {unknown_keys}"
            )

        # Validate 'community_sessions' if present
        if "community_sessions" in top_level_keys:
            validate_community_sessions_config(config.get("community_sessions"))

        # Validate 'enterprise_sessions' if present
        if "enterprise_sessions" in top_level_keys:
            validate_enterprise_sessions_config(config.get("enterprise_sessions"))

        return config
