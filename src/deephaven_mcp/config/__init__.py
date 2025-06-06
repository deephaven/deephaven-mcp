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
The configuration file must be a JSON object with one top-level key:

  - `community_sessions` (dict, required):
      A dictionary mapping community session names (str) to client session configuration dicts.
      Each configuration defines how to connect to a specific community worker.
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
  - The `community_sessions` key must be present and its value must be a non-empty dictionary.
  - Within each session configuration, all field values must have the correct type if present.
  - No unknown fields are permitted in session configurations.
  - If TLS fields are provided, referenced files must exist and be readable.

Configuration JSON Specification:
---------------------------------
- The configuration file must be a JSON object with one top-level key:
    - `"community_sessions"`: a dictionary mapping community session names to client session configuration dicts for connecting to community workers.

Example Valid Configuration:
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
1. Invalid: `community_sessions` is empty
```json
{
    "community_sessions": {}
}
```
(Reason: The `community_sessions` dictionary cannot be empty.)

2. Invalid: Session field with wrong type
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
- The configuration **must** include a `community_sessions` dictionary as a top-level key.
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

_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""
str: Name of the environment variable specifying the path to the Deephaven MCP config file.
"""


class ConfigManager:
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

    @staticmethod
    def validate_config(config: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the Deephaven MCP application configuration dictionary.

        Ensures that the configuration has the correct top-level structure, specifically
        requiring a 'community_sessions' key, and then delegates to
        `validate_community_sessions_config` for detailed validation of its content.

        Args:
            config (dict[str, Any]): The configuration dictionary to validate.
                                     Must include a 'community_sessions' dictionary as a top-level key.

        Returns:
            dict[str, Any]: The validated configuration dictionary.

        Raises:
            ValueError: If the config is missing the 'community_sessions' key, has other unknown top-level keys,
                        or if `validate_community_sessions_config` finds issues.

        Example:
            >>> validated_config = ConfigManager.validate_config({'community_sessions': {'local_session': {}}})
        """
        required_top_level = {"community_sessions"}
        allowed_top_level = required_top_level
        top_level_keys = set(config.keys())

        unknown_keys = top_level_keys - allowed_top_level
        if unknown_keys:
            _LOGGER.error(
                f"Unknown top-level keys in Deephaven MCP config: {unknown_keys}"
            )
            raise ValueError(
                f"Unknown top-level keys in Deephaven MCP config: {unknown_keys}"
            )

        missing_keys = required_top_level - top_level_keys
        if missing_keys:
            _LOGGER.error(
                f"Missing required top-level keys in Deephaven MCP config: {missing_keys}"
            )
            raise ValueError(
                f"Missing required top-level keys in Deephaven MCP config: {missing_keys}"
            )

        # Validate the 'community_sessions' structure and its contents
        validate_community_sessions_config(config["community_sessions"])

        return config
