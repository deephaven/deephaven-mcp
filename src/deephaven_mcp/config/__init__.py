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
The configuration file must be a JSON object. It may contain the following top-level keys:

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
        - `auth_token` (str, optional): The direct authentication token or password. May be empty if `auth_type` is "anonymous". Use this OR `auth_token_env_var`, but not both.
        - `auth_token_env_var` (str, optional): The name of an environment variable from which to read the authentication token. Use this OR `auth_token`, but not both.
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

  - `enterprise_systems` (dict, optional):
      A dictionary mapping enterprise system names (str) to system configuration dicts.
      If this key is present, its value must be a dictionary (which can be empty).
      Each enterprise system configuration dict is validated according to the schema defined in
      `src/deephaven_mcp/config/enterprise_system.py`. Key fields typically include:

        - `connection_json_url` (str): URL to the server's connection.json file.
        - `auth_type` (str, enum): Authentication type. Allowed values include:
            * "password": Use username and password for authentication.
            * "private_key": Use a private key for SAML or similar token-based authentication.
        - Conditional fields based on `auth_type`:
            - If `auth_type` is "password":
                - `username` (str, required): The username.
                - `password` (str, optional): The password.
                - `password_env_var` (str, optional): Environment variable for the password.
                  (Note: `password` and `password_env_var` are mutually exclusive.)
            - If `auth_type` is "private_key":
                - `private_key` (str, required): Path to the private key file.

      Notes:
        - For the detailed schema of individual enterprise system configurations, please refer to the
          `src/deephaven_mcp/config/enterprise_system.py` module and the DEVELOPER_GUIDE.md.
        - Sensitive fields are redacted from logs.
        - Unknown fields within an enterprise system configuration will cause a warning and be ignored, but unknown top-level keys in the main config will fail validation.

Validation rules:
  - If the `community_sessions` key is present, its value must be a dictionary.
  - Within each session configuration, all field values must have the correct type if present.
  - No unknown fields are permitted in session configurations.
  - If TLS fields are provided, referenced files must exist and be readable.
  - If the `enterprise_systems` key is present, its value must be a dictionary.
  - Each enterprise system configuration is validated according to its specific schema.

Configuration JSON Specification:
---------------------------------
- The configuration file must be a JSON object.
- It may optionally contain `"community_sessions"` and/or `"enterprise_systems"` top-level keys:
    - `"community_sessions"`: If present, this must be a dictionary mapping community session names to client session configuration dicts for connecting to community workers. An empty dictionary is allowed (e.g., {}).
    - `"enterprise_systems"`: If present, this must be a dictionary mapping enterprise system names to system configuration dicts. An empty dictionary is allowed (e.g., {}).

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
    },
    "enterprise_systems": {
        "prod_cluster": {
            "connection_json_url": "https://enterprise.example.com/iris/connection.json",
            "auth_type": "api_key",
            "api_key_env_var": "PROD_CLUSTER_API_KEY"
        },
        "dev_system": {
            "connection_json_url": "http://localhost:8080/iris/connection.json",
            "auth_type": "password",
            "username": "dev_user",
            "password_env_var": "DEV_SYSTEM_PASSWORD"
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

__all__ = [
    # Errors and core config
    "McpConfigurationError",
    "CommunitySessionConfigurationError",
    "EnterpriseSystemConfigurationError",
    "ConfigManager",
    "CONFIG_ENV_VAR",
    "validate_config",
    "get_named_config",
    "get_all_config_names",
    "get_config_path",
    "load_and_validate_config",
    # Community session API
    "validate_community_sessions_config",
    "validate_single_community_session_config",
    "redact_community_session_config",
    # Enterprise system API
    "validate_enterprise_systems_config",
    "validate_single_enterprise_system",
    "redact_enterprise_system_config",
    "redact_enterprise_systems_map",
]

import asyncio
import json
import logging
import os
from collections.abc import Callable
from typing import Any, cast

import aiofiles

from ._community_session import (
    validate_community_sessions_config,
    validate_single_community_session_config,
    redact_community_session_config,
)
from ._enterprise_system import (
    validate_enterprise_systems_config,
    validate_single_enterprise_system,
    redact_enterprise_system_config,
    redact_enterprise_systems_map,
)
from .errors import (
    CommunitySessionConfigurationError,
    EnterpriseSystemConfigurationError,
    McpConfigurationError,
)

_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""
str: Name of the environment variable specifying the path to the Deephaven MCP config file.
"""

_REQUIRED_TOP_LEVEL_KEYS: set[str] = set()
"""Set of top-level keys that MUST be present in the configuration file."""
_ALLOWED_TOP_LEVEL_KEYS: set[str] = {"community_sessions", "enterprise_systems"}
"""Set of all allowed top-level keys in the configuration file."""


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

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """
        PRIVATE: Set the in-memory configuration cache (coroutine-safe, for testing/internal use only).

        This private method allows tests or advanced users to inject a configuration dictionary directly
        into the manager's cache, bypassing file I/O. The configuration will be validated before caching.
        This is useful for unit tests or scenarios where you want to avoid reading from disk.

        Args:
            config (dict[str, Any]): The configuration dictionary to set as the cache. This will be validated before caching.

        Returns:
            None

        Raises:
            McpConfigurationError: If the provided configuration is invalid.

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> await config_manager._set_config_cache({'community_sessions': {'example_session': {}}})
        """
        async with self._lock:
            self._cache = validate_config(config)

    async def get_config(self) -> dict[str, Any]:
        """
        Load and validate the Deephaven MCP application configuration from disk (coroutine-safe).

        This method loads the configuration from the file path specified by the DH_MCP_CONFIG_FILE
        environment variable, validates its structure and contents, and caches the result for
        subsequent calls. If the cache is already populated, it returns the cached configuration.
        All file I/O is performed asynchronously using aiofiles, and the method is coroutine-safe.
        If the configuration file or its contents are invalid, detailed errors are logged and
        exceptions are raised.

        Returns:
            dict[str, Any]: The loaded and validated configuration dictionary. Returns an empty
                dictionary if the config file path is not set or the file is empty (but valid JSON like {}).

        Raises:
            RuntimeError: If the DH_MCP_CONFIG_FILE environment variable is not set.
            McpConfigurationError: If the config file is invalid (e.g., not JSON, missing required keys,
                incorrect types, or fails validation).

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

            config_path = get_config_path()
            validated = await load_and_validate_config(config_path)
            self._cache = validated
            _log_config_summary(validated)
            return validated


async def get_named_config(
    config_manager: "ConfigManager",
    section: str,
    name: str,
) -> dict[str, Any]:
    """
    Retrieve a named config dict from a section, with logging and error handling.

    Args:
        config_manager (ConfigManager): The ConfigManager instance to use for config retrieval.
        section (str): The top-level config section (e.g., 'community_sessions', 'enterprise_systems').
        name (str): The specific item name to retrieve.

    Returns:
        dict[str, Any]: The configuration dictionary for the specified item.

    Raises:
        ValueError: If the named item is not found in the config section.
    """
    _LOGGER.debug(f"Getting config for '{section}:{name}'")
    config = await config_manager.get_config()
    section_map = config.get(section, {})

    # get redact fn
    redact_fn: Callable[[dict[str, Any]], dict[str, Any]]
    if section == "community_sessions":
        redact_fn = redact_community_session_config
    elif section == "enterprise_systems":
        redact_fn = redact_enterprise_system_config
    else:
        raise ValueError(f"Invalid section: {section}")

    if name not in section_map:
        _LOGGER.error(f"Config for '{section}:{name}' not found in configuration")
        raise ValueError(f"Config for '{section}:{name}' not found in configuration")
    _LOGGER.debug(
        f"Retrieved configuration for '{section}:{name}': {redact_fn(section_map[name])}"
    )
    return cast(dict[str, Any], section_map[name])


async def get_all_config_names(
    config_manager: "ConfigManager",
    section: str,
) -> list[str]:
    """
    Retrieve all names from a given config section.

    Args:
        config_manager (ConfigManager): The ConfigManager instance to use for config retrieval.
        section (str): The top-level config section to extract names from (e.g., 'community_sessions', 'enterprise_systems').

    Returns:
        list[str]: List of keys (names) in the specified config section, or empty list if not found or not a dict.
    """
    _LOGGER.debug(f"Getting list of all names from config section '{section}'")
    config = await config_manager.get_config()
    section_map = config.get(section, {})
    if not isinstance(section_map, dict):
        _LOGGER.warning(
            f"'{section}' is not a dictionary, returning empty list of names."
        )
        return []
    names = list(section_map.keys())
    _LOGGER.debug(f"Found {len(names)} {section} item(s): {names}")
    return names


async def _load_config_from_file(config_path: str) -> dict[str, Any]:
    """
    Load and parse the Deephaven MCP configuration from a JSON file asynchronously.

    Args:
        config_path (str): The file path to the configuration JSON file.

    Returns:
        dict[str, Any]: The parsed configuration as a dictionary.

    Raises:
        McpConfigurationError: If the file is not found, cannot be read, is not valid JSON, or any other I/O error occurs.

    Example:
        >>> config = await _load_config_from_file('/path/to/config.json')
        >>> print(config['community_sessions'])
    """
    try:
        async with aiofiles.open(config_path) as f:
            content = await f.read()
        return cast(dict[str, Any], json.loads(content))
    except FileNotFoundError:
        _LOGGER.error(f"Configuration file not found: {config_path}")
        raise McpConfigurationError(
            f"Configuration file not found: {config_path}"
        ) from None
    except PermissionError:
        _LOGGER.error(
            f"Permission denied when trying to read configuration file: {config_path}"
        )
        raise McpConfigurationError(
            f"Permission denied when trying to read configuration file: {config_path}"
        ) from None
    except json.JSONDecodeError as e:
        _LOGGER.error(f"Invalid JSON in configuration file {config_path}: {e}")
        raise McpConfigurationError(
            f"Invalid JSON in configuration file {config_path}: {e}"
        ) from e
    except Exception as e:
        _LOGGER.error(
            f"Unexpected error loading or parsing config file {config_path}: {e}"
        )
        raise McpConfigurationError(
            f"Unexpected error loading or parsing config file {config_path}: {e}"
        ) from e


def get_config_path() -> str:
    """
    Retrieve the configuration file path from the environment variable.

    This function retrieves the path to the Deephaven MCP configuration JSON file from the environment variable specified by CONFIG_ENV_VAR.

    Returns:
        str: The path to the Deephaven MCP configuration JSON file as specified by the CONFIG_ENV_VAR environment variable.

    Raises:
        RuntimeError: If the CONFIG_ENV_VAR environment variable is not set.

    Example:
        >>> os.environ['DH_MCP_CONFIG_FILE'] = '/path/to/config.json'
        >>> path = get_config_path()
        >>> print(path)
        '/path/to/config.json'
    """
    if CONFIG_ENV_VAR not in os.environ:
        _LOGGER.error(f"Environment variable {CONFIG_ENV_VAR} is not set.")
        raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} is not set.")
    config_path = os.environ[CONFIG_ENV_VAR]
    _LOGGER.info(f"Environment variable {CONFIG_ENV_VAR} is set to: {config_path}")
    return config_path


async def load_and_validate_config(config_path: str) -> dict[str, Any]:
    """
    Load and validate the Deephaven MCP configuration from a JSON file.

    This function loads the configuration from the specified file path, parses it as JSON,
    and validates it according to the expected schema. All exceptions are logged and
    re-raised as McpConfigurationError for unified error handling.

    Args:
        config_path (str): The path to the configuration JSON file.

    Returns:
        dict[str, Any]: The loaded and validated configuration dictionary.

    Raises:
        McpConfigurationError: If the file cannot be read, is not valid JSON, or fails validation.

    Example:
        >>> config = await _load_and_validate_config('/path/to/config.json')
        >>> print(config['enterprise_systems'])
    """
    try:
        data = await _load_config_from_file(config_path)
        return validate_config(data)
    except (
        CommunitySessionConfigurationError,
        EnterpriseSystemConfigurationError,
    ) as specific_e:
        _LOGGER.error(
            f"Configuration validation failed for {config_path}: {specific_e}"
        )
        raise McpConfigurationError(
            f"Configuration validation failed: {specific_e}"
        ) from specific_e
    except ValueError as ve:
        _LOGGER.error(f"General configuration validation error for {config_path}: {ve}")
        raise McpConfigurationError(
            f"General configuration validation error: {ve}"
        ) from ve
    except Exception as e:
        _LOGGER.error(f"Error loading configuration file {config_path}: {e}")
        raise McpConfigurationError(f"Error loading configuration file: {e}") from e


def _log_config_summary(config: dict[str, Any]) -> None:
    """
    Log a summary of the loaded Deephaven MCP configuration.

    This function logs the names and (redacted) details of all configured community sessions and
    enterprise systems. If no sessions or systems are configured, it logs that information as well.

    Args:
        config (dict[str, Any]): The loaded and validated configuration dictionary.

    Example:
        >>> config = {'community_sessions': {'local': {...}}, 'enterprise_systems': {}}
        >>> _log_config_summary(config)
    """
    community_sessions = config.get("community_sessions", {})
    if community_sessions:
        _LOGGER.info("Configured Community Sessions:")
        for name, details in community_sessions.items():
            _LOGGER.info(
                f"  Session '{name}': {redact_community_session_config(details)}"
            )
    else:
        _LOGGER.info("No Community Sessions configured.")
    enterprise_systems = config.get("enterprise_systems", {})
    if enterprise_systems:
        _LOGGER.info("Configured Enterprise Systems:")
        for name, details in enterprise_systems.items():
            _LOGGER.info(
                f"  System '{name}': {redact_enterprise_system_config(details)}"
            )
    else:
        _LOGGER.info("No Enterprise Systems configured.")


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Validate the Deephaven MCP application configuration dictionary.

    This function ensures that the configuration dictionary conforms to the expected schema for Deephaven MCP.
    The configuration may contain the following top-level keys:
      - 'community_sessions' (dict, optional):
            A dictionary mapping community session names (str) to session configuration dicts.
            Each session configuration dict may contain any of the following fields (all optional):
                - 'host' (str): Hostname or IP address.
                - 'port' (int): Port number.
                - 'auth_type' (str): Authentication type ('token', 'basic', 'anonymous').
                - 'auth_token' (str, optional): Direct authentication token or password.
                - 'auth_token_env_var' (str, optional): Environment variable for authentication token.
                - 'never_timeout' (bool): If True, session never times out.
                - 'session_type' (str): Programming language for the session ('python', 'groovy', etc.).
                - 'use_tls' (bool): Whether to use TLS/SSL.
                - 'tls_root_certs' (str, optional): Path to PEM file with root certificates.
                - 'client_cert_chain' (str, optional): Path to client certificate chain PEM file.
                - 'client_private_key' (str, optional): Path to client private key PEM file.
            All fields are optional; unknown fields are not allowed and will cause validation to fail.
      - 'enterprise_systems' (dict, optional):
            A dictionary mapping enterprise system names (str) to enterprise system configuration dicts.
            Each enterprise system configuration dict must include:
                - 'connection_json_url' (str, required): URL to the server's connection.json file.
                - 'auth_type' (str, required): One of:
                    * 'password':
                        - 'username' (str, required): The username.
                        - 'password' (str, optional): The password.
                        - 'password_env_var' (str, optional): Environment variable for the password.
                          (Exactly one of 'password' or 'password_env_var' must be provided, but not both.)
                    * 'private_key':
                        - 'private_key' (str, required): The private key.
            All fields not listed above are disallowed and will cause validation to fail.

    Validation Rules:
        - Only known top-level keys are allowed ('community_sessions', 'enterprise_systems').
        - All present sections are validated according to their schema.
        - Missing required top-level keys (if any) will cause validation to fail.
        - Unknown or misspelled keys will cause validation to fail.
        - All field types must be correct if present.
        - Sensitive fields are redacted from logs.

    Args:
        config (dict[str, Any]): The configuration dictionary to validate.

    Returns:
        dict[str, Any]: The validated configuration dictionary.

    Raises:
        McpConfigurationError: If required top-level keys are missing or unknown keys are present.
        CommunitySessionConfigurationError: If a community session config is invalid.
        EnterpriseSystemConfigurationError: If an enterprise system config is invalid.

    Example:
        >>> validated_config = validate_config({'community_sessions': {'local_session': {}}, 'enterprise_systems': {'prod_cluster': {}}})
        >>> validated_config_empty = validate_config({})  # Also valid
    """
    top_level_keys = set(config.keys())

    # Check for missing required keys
    missing_keys = _REQUIRED_TOP_LEVEL_KEYS - top_level_keys
    if missing_keys:
        _LOGGER.error(
            f"Missing required top-level keys in Deephaven MCP config: {missing_keys}"
        )
        raise McpConfigurationError(
            f"Missing required top-level keys in Deephaven MCP config: {missing_keys}"
        )
    unknown_keys = top_level_keys - _ALLOWED_TOP_LEVEL_KEYS
    if unknown_keys:
        _LOGGER.error(f"Unknown top-level keys in Deephaven MCP config: {unknown_keys}")
        raise McpConfigurationError(
            f"Unknown top-level keys in Deephaven MCP config: {unknown_keys}"
        )
    if "community_sessions" in top_level_keys:
        validate_community_sessions_config(config["community_sessions"])

    # Validate 'enterprise_systems' if present
    if "enterprise_systems" in top_level_keys:
        validate_enterprise_systems_config(config["enterprise_systems"])

    _LOGGER.info("Configuration validation passed.")
    return config
