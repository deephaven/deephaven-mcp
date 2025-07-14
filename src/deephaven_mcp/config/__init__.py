"""
Async Deephaven MCP configuration management.

This module provides async functions to load, validate, and manage configuration for Deephaven MCP from a JSON file.
Configuration is loaded from a file specified by the DH_MCP_CONFIG_FILE environment variable using native async file I/O (aiofiles).

Features:
    - Coroutine-safe, cached loading of configuration using asyncio.Lock.
    - Strict validation of configuration structure and values.
    - Helper functions to access configuration sections and retrieve configuration names.
    - Logging of configuration loading, environment variable value, and validation steps.
    - Uses aiofiles for non-blocking, native async config file reads.

Configuration Schema:
---------------------
The configuration file must be a JSON object. It may contain the following top-level keys:

  - `community` (dict, optional):
      A dictionary mapping community configuration.
      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
      If this key is absent, it implies no community configuration is present.
      Each community configuration dict may contain any of the following fields (all are optional):

        - `sessions` (dict, optional):
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

  - `enterprise` (dict, optional):
      A dictionary mapping enterprise configuration.
      If this key is present, its value must be a dictionary (which can be empty).
      Each enterprise configuration dict is validated according to the schema defined in
      `src/deephaven_mcp/config/enterprise_system.py`. Key fields typically include:

        - `systems` (dict, optional):
            A dictionary mapping enterprise system names (str) to system configuration dicts.
            If this key is present, its value must be a dictionary (which can be empty).
            Each enterprise system configuration dict must include:
                - `connection_json_url` (str, required): URL to the server's connection.json file.
                - `auth_type` (str, required): One of:
                    * "password":
                        - `username` (str, required): The username.
                        - `password` (str, optional): The password.
                        - `password_env_var` (str, optional): Environment variable for the password.
                          (Note: `password` and `password_env_var` are mutually exclusive.)
                    * "private_key":
                        - `private_key` (str, required): The private key.

      Notes:
        - For the detailed schema of individual enterprise system configurations, please refer to the
          `src/deephaven_mcp/config/enterprise_system.py` module and the DEVELOPER_GUIDE.md.
        - Sensitive fields are redacted from logs.
        - Unknown fields at any level will cause validation to fail.

Validation rules:
  - If the `community` key is present, its value must be a dictionary.
  - Within each community configuration, all field values must have the correct type if present.
  - No unknown fields are permitted at any level of the configuration.
  - If TLS fields are provided, referenced files must exist and be readable.
  - If the `enterprise` key is present, its value must be a dictionary.
  - Each enterprise system configuration is validated according to its specific schema.

Configuration JSON Specification:
---------------------------------
- The configuration file must be a JSON object.
- It may optionally contain `"community"` and/or `"enterprise"` top-level keys:
    - `"community"`: If present, this must be a dictionary containing community configuration.
    - `"enterprise"`: If present, this must be a dictionary containing enterprise configuration.

Example Valid Configuration (without community sessions):
---------------------------
```json
{}
```

Example Valid Configuration (with community and enterprise sections):
---------------------------
```json
{
    "community": {
        "sessions": {
            "local": {
                "host": "localhost",
                "port": 10000
            }
        }
    },
    "enterprise": {
        "systems": {
            "prod_cluster": {
                "connection_json_url": "https://enterprise.example.com/iris/connection.json",
                "auth_type": "password",
                "username": "admin",
                "password_env_var": "MY_PASSWORD"
            }
        }
    }
}
```

Example Invalid Configurations:
------------------------------
1. Invalid: Session field with wrong type
```json
{
    "community": {
        "sessions": {
            "local": {
                "host": 12345,  // Should be a string, not an integer
                "port": "not-a-port"  // Should be an integer, not a string
            }
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
- The configuration may optionally include a `community` dictionary.
- Accessing configuration sections:
    >>> config_manager = ConfigManager()
    >>> await config_manager.load_config("/path/to/deephaven_mcp.json")
    >>> config = await config_manager.get_config()
    >>> config_section = get_config_section(config, ["community", "sessions"])
    >>> # Access specific session data from config_section
- Listing available configured names:
    >>> session_names = get_all_config_names(config, ["community", "sessions"])
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
    # Core config
    "ConfigManager",
    "CONFIG_ENV_VAR",
    "validate_config",
    "get_config_section",
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
import copy
import json
import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

import aiofiles

from ._community_session import (
    redact_community_session_config,
    validate_community_sessions_config,
    validate_single_community_session_config,
)
from ._enterprise_system import (
    redact_enterprise_system_config,
    redact_enterprise_systems_map,
    validate_enterprise_systems_config,
    validate_single_enterprise_system,
)
from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    EnterpriseSystemConfigurationError,
    ConfigurationError,
)

_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""
str: Name of the environment variable specifying the path to the Deephaven MCP config file.
"""


@dataclass
class _ConfigPathSpec:
    """Specification for a valid configuration path."""

    required: bool
    expected_type: type
    validator: Callable[[Any], None] | None = None
    redactor: Callable[[Any], Any] | None = (
        None  # Function to redact sensitive data for logging
    )


# Schema defining all valid configuration paths
_SCHEMA_PATHS: dict[tuple[str, ...], _ConfigPathSpec] = {
    ("community",): _ConfigPathSpec(
        required=False, expected_type=dict, validator=None  # Validated by nested paths
    ),
    ("community", "sessions"): _ConfigPathSpec(
        required=False,
        expected_type=dict,
        validator=validate_community_sessions_config,
        redactor=lambda sessions_dict: (
            {
                name: redact_community_session_config(config)
                for name, config in sessions_dict.items()
            }
            if isinstance(sessions_dict, dict)
            else sessions_dict
        ),
    ),
    ("enterprise",): _ConfigPathSpec(
        required=False, expected_type=dict, validator=None  # Validated by nested paths
    ),
    ("enterprise", "systems"): _ConfigPathSpec(
        required=False,
        expected_type=dict,
        validator=validate_enterprise_systems_config,
        redactor=lambda systems_dict: (
            {
                name: redact_enterprise_system_config(config)
                for name, config in systems_dict.items()
            }
            if isinstance(systems_dict, dict)
            else systems_dict
        ),
    ),
}


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
            ConfigurationError: If the provided configuration is invalid.

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> await config_manager._set_config_cache({'community': {'sessions': {'example_session': {}}}})
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
            ConfigurationError: If the config file is invalid (e.g., not JSON, missing required keys,
                incorrect types, or fails validation).

        Example:
            >>> # Assuming config_manager is an instance of ConfigManager
            >>> # and DH_MCP_CONFIG_FILE is set appropriately
            >>> # import os
            >>> # os.environ['DH_MCP_CONFIG_FILE'] = '/path/to/config.json'
            >>> config_dict = await config_manager.get_config()
            >>> print(config_dict['community'])
        """
        _LOGGER.debug(
            "[ConfigManager] Loading Deephaven MCP application configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[ConfigManager] Using cached Deephaven MCP application configuration."
                )
                return self._cache

            config_path = get_config_path()
            validated = await load_and_validate_config(config_path)
            self._cache = validated
            _log_config_summary(validated)
            return validated


def get_config_section(
    config: dict[str, Any],
    section: Sequence[str],
) -> Any:
    """
    Retrieve a config subsection by path. Raises KeyError if not found.

    Args:
        config (dict[str, Any]): The configuration dictionary to navigate.
        section (Sequence[str]): The path to the config section (e.g., ['community', 'sessions', 'foo']).

    Returns:
        Any: The config subsection at the given path.

    Raises:
        KeyError: If the section path does not exist.
    """
    _LOGGER.debug(f"Getting config section for path: {section}")
    curr = config
    for key in section:
        if not isinstance(curr, dict) or key not in curr:
            raise KeyError(f"Section path {section} does not exist in configuration")
        curr = curr[key]
    return curr


def get_all_config_names(
    config: dict[str, Any],
    section: Sequence[str],
) -> list[str]:
    """
    Retrieve all names from a given config section path.

    Args:
        config (dict[str, Any]): The configuration dictionary to search within.
        section (Sequence[str]): The path to the config section (e.g., ['community', 'sessions']).

    Returns:
        list[str]: A list of names from the given config section. If the section doesn't exist, returns an empty list.
    """
    _LOGGER.debug(f"Getting list of all names from config section path: {section}")
    try:
        section_obj = get_config_section(config, section)
    except KeyError:
        _LOGGER.warning(
            f"Section path {section} does not exist, returning empty list of names."
        )
        return []

    if not isinstance(section_obj, dict):
        _LOGGER.warning(
            f"Section at path {section} is not a dictionary, returning empty list of names."
        )
        return []

    names = list(section_obj.keys())
    _LOGGER.debug(f"Found {len(names)} names in section {section}: {names}")
    return names


async def _load_config_from_file(config_path: str) -> dict[str, Any]:
    """
    Load and parse the Deephaven MCP configuration from a JSON file asynchronously.

    Args:
        config_path (str): The file path to the configuration JSON file.

    Returns:
        dict[str, Any]: The parsed configuration as a dictionary.

    Raises:
        ConfigurationError: If the file is not found, cannot be read, is not valid JSON, or any other I/O error occurs.

    Example:
        >>> config = await _load_config_from_file('/path/to/config.json')
        >>> print(config['community'])
    """
    try:
        async with aiofiles.open(config_path) as f:
            content = await f.read()
        return cast(dict[str, Any], json.loads(content))
    except FileNotFoundError:
        _LOGGER.error(f"Configuration file not found: {config_path}")
        raise ConfigurationError(
            f"Configuration file not found: {config_path}"
        ) from None
    except PermissionError:
        _LOGGER.error(
            f"Permission denied when trying to read configuration file: {config_path}"
        )
        raise ConfigurationError(
            f"Permission denied when trying to read configuration file: {config_path}"
        ) from None
    except json.JSONDecodeError as e:
        _LOGGER.error(f"Invalid JSON in configuration file {config_path}: {e}")
        raise ConfigurationError(
            f"Invalid JSON in configuration file {config_path}: {e}"
        ) from e
    except Exception as e:
        _LOGGER.error(
            f"Unexpected error loading or parsing config file {config_path}: {e}"
        )
        raise ConfigurationError(
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
    re-raised as ConfigurationError for unified error handling.

    Args:
        config_path (str): The path to the configuration JSON file.

    Returns:
        dict[str, Any]: The loaded and validated configuration dictionary.

    Raises:
        ConfigurationError: If the file cannot be read, is not valid JSON, or fails validation.

    Example:
        >>> config = await _load_and_validate_config('/path/to/config.json')
        >>> print(config['enterprise'])
    """
    try:
        data = await _load_config_from_file(config_path)
        return validate_config(data)
    except Exception as e:
        _LOGGER.error(f"Error loading configuration file {config_path}: {e}")
        raise ConfigurationError(f"Error loading configuration file: {e}") from e


def _apply_redaction_to_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Apply all configured redaction functions to a deep copy of the config.

    Args:
        config (dict[str, Any]): The configuration dictionary to redact.

    Returns:
        dict[str, Any]: A deep copy of the config with sensitive data redacted.
    """
    config_copy = copy.deepcopy(config)

    # Apply redaction functions for each configured path
    for path_tuple, spec in _SCHEMA_PATHS.items():
        if spec.redactor is not None:
            try:
                section = get_config_section(config_copy, list(path_tuple))
                redacted_section = spec.redactor(section)

                # Navigate to the parent and set the redacted section
                current = config_copy
                for key in path_tuple[:-1]:
                    current = current[key]
                current[path_tuple[-1]] = redacted_section

            except KeyError:
                # Section doesn't exist, skip redaction
                continue

    return config_copy


def _log_config_summary(config: dict[str, Any]) -> None:
    """
    Log a summary of the loaded Deephaven MCP configuration.

    This function logs the configuration with sensitive data redacted as formatted JSON.

    Args:
        config (dict[str, Any]): The loaded and validated configuration dictionary.

    Example:
        >>> config = {'community': {'sessions': {'local': {...}}}}
        >>> _log_config_summary(config)
    """
    _LOGGER.info("[ConfigManager] Configuration summary:")

    # Create a redacted copy of the config for logging
    redacted_config = _apply_redaction_to_config(config)

    # Log the redacted config as formatted JSON
    try:
        formatted_config = json.dumps(redacted_config, indent=2, sort_keys=True)
        _LOGGER.info(f"[ConfigManager] Loaded configuration:\n{formatted_config}")
    except (TypeError, ValueError) as e:
        _LOGGER.warning(f"[ConfigManager] Failed to format config as JSON: {e}")
        _LOGGER.info(f"[ConfigManager] Loaded configuration: {redacted_config}")


def _validate_unknown_keys(
    data: dict[str, Any], path: tuple[str, ...], valid_keys: set[str]
) -> None:
    """Check for unknown keys at the current path level."""
    unknown_keys = set(data.keys()) - valid_keys
    if unknown_keys:
        _LOGGER.error(f"Unknown keys at config path {path}: {unknown_keys}")
        raise ConfigurationError(
            f"Unknown keys at config path {path}: {unknown_keys}"
        )


def _validate_required_keys(
    data: dict[str, Any], path: tuple[str, ...], required_keys: set[str]
) -> None:
    """Check for missing required keys at the current path level."""
    missing_keys = required_keys - set(data.keys())
    if missing_keys:
        _LOGGER.error(f"Missing required keys at config path {path}: {missing_keys}")
        raise ConfigurationError(
            f"Missing required keys at config path {path}: {missing_keys}"
        )


def _validate_key_type_and_value(
    key: str, value: Any, spec: "_ConfigPathSpec", path: tuple[str, ...]
) -> None:
    """Validate type and value for a single configuration key.
    
    Performs two types of validation:
    1. Type validation - ensures the value matches the expected type in the spec
    2. Specialized validation - if a validator is provided in the spec, runs it
       and handles any configuration exceptions
    
    Args:
        key: The configuration key being validated
        value: The value to validate
        spec: The configuration path specification containing type and validator
        path: The parent path tuple (will be combined with key to form current_path)
    
    Raises:
        ConfigurationError: If validation fails for type or specialized validation
    """
    current_path = path + (key,)

    # Type validation
    if not isinstance(value, spec.expected_type):
        _LOGGER.error(
            f"Config path {current_path} must be of type {spec.expected_type.__name__}, got {type(value).__name__}"
        )
        raise ConfigurationError(
            f"Config path {current_path} must be of type {spec.expected_type.__name__}, got {type(value).__name__}"
        )

    # Specialized validation
    if spec.validator:
        try:
            spec.validator(value)
        except (
            CommunitySessionConfigurationError,
            EnterpriseSystemConfigurationError,
        ) as e:
            raise ConfigurationError(
                f"Invalid configuration for {'.'.join(current_path)}: {e}"
            ) from e


def _should_recurse_into_nested_dict(current_path: tuple[str, ...]) -> bool:
    """Check if there are nested schema paths for the current path.
    
    Determines if we should continue recursing into a dictionary by checking if any
    schema paths exist that are children of the current path (i.e., they start with
    the current path and have at least one more component).
    
    Args:
        current_path: The current path tuple to check for children
        
    Returns:
        bool: True if there are nested paths that extend beyond the current path
    """
    return any(
        nested_path[: len(current_path)] == current_path
        and len(nested_path) > len(current_path)
        for nested_path in _SCHEMA_PATHS.keys()
    )


def _validate_section(data: dict[str, Any], path: tuple[str, ...]) -> None:
    """Validate a configuration section in a single pass.
    
    Performs comprehensive validation of a configuration section including:
    1. Checking for unknown keys not in the schema
    2. Checking for missing required keys
    3. Validating each key's type and value
    4. Recursively validating nested dictionary sections
    
    Args:
        data: The dictionary containing configuration data to validate
        path: The current path tuple representing the location in the config
        
    Raises:
        ConfigurationError: If validation fails for any reason
    """
    # Get specs for the current path level
    current_specs = {
        nested_path[len(path)]: spec
        for nested_path, spec in _SCHEMA_PATHS.items()
        if len(nested_path) == len(path) + 1 and nested_path[: len(path)] == path
    }

    # Check for unknown keys
    valid_keys = set(current_specs.keys())
    _validate_unknown_keys(data, path, valid_keys)

    # Check for missing required keys
    required_keys = {key for key, spec in current_specs.items() if spec.required}
    _validate_required_keys(data, path, required_keys)

    # Validate each present key
    for key, value in data.items():
        if key in current_specs:
            spec = current_specs[key]
            current_path = path + (key,)

            _validate_key_type_and_value(key, value, spec, path)

            # Recurse into nested dictionaries
            if isinstance(value, dict) and _should_recurse_into_nested_dict(
                current_path
            ):
                _validate_section(value, current_path)


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    """
        Validate the Deephaven MCP application configuration dictionary.

        This function ensures that the configuration dictionary conforms to the expected schema for Deephaven MCP.
        The configuration may contain the following top-level keys:
          - 'community' (dict, optional):
                A dictionary mapping community configuration.
                If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
                If this key is absent, it implies no community configuration is present.
                Each community configuration dict may contain any of the following fields (all are optional):

                  - 'sessions' (dict, optional):
                      A dictionary mapping community session names (str) to client session configuration dicts.
                      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
                      If this key is absent, it implies no community sessions are configured.
                      Each community session configuration dict may contain any of the following fields (all are optional):

                        - 'host' (str): Hostname or IP address of the community worker.
                        - 'port' (int): Port number for the community worker connection.
                        - 'auth_type' (str): Authentication type. Allowed values include:
                            * "token": Use a bearer token for authentication.
                            * "basic": Use HTTP Basic authentication.
                            * "anonymous": No authentication required.
                        - 'auth_token' (str, optional): The direct authentication token or password. May be empty if `auth_type` is "anonymous". Use this OR `auth_token_env_var`, but not both.
                        - 'auth_token_env_var' (str, optional): The name of an environment variable from which to read the authentication token. Use this OR `auth_token`, but not both.
                        - 'never_timeout' (bool): If True, sessions to this community worker never time out.
                        - 'session_type' (str): Programming language for the session. Common values include:
                            * "python": For Python-based Deephaven instances.
                            * "groovy": For Groovy-based Deephaven instances.
                        - 'use_tls' (bool): Whether to use TLS/SSL for the connection.
                        - 'tls_root_certs' (str, optional): Path to a PEM file containing root certificates to trust for TLS.
                        - 'client_cert_chain' (str, optional): Path to a PEM file containing the client certificate chain for mutual TLS.
                        - 'client_private_key' (str, optional): Path to a PEM file containing the client private key for mutual TLS.

          - 'enterprise' (dict, optional):
                A dictionary mapping enterprise configuration.
                If this key is present, its value must be a dictionary (which can be empty).
                Each enterprise configuration dict is validated according to the schema defined in
                `src/deephaven_mcp/config/enterprise_system.py`. Key fields typically include:

                  - 'systems' (dict, optional):
                      A dictionary mapping enterprise system names (str) to system configuration dicts.
                      If this key is present, its value must be a dictionary (which can be empty).
                      Each enterprise system configuration dict must include:
                          - 'connection_json_url' (str, required): URL to the server's connection.json file.
                          - 'auth_type' (str, required): One of:
                              * "password":
                                  - 'username' (str, required): The username.
                                  - 'password' (str, optional): The password.
                                  - 'password_env_var' (str, optional): Environment variable for the password.
                                    (Note: `password` and `password_env_var` are mutually exclusive.)
                              * "private_key":
                                  - 'private_key' (str, required): The private key.

    Validation Rules:
      - Only known keys are allowed at each level of nesting.
      - All present sections are validated according to their schema.
      - Unknown or misspelled keys at any level will cause validation to fail.
      - All field types must be correct if present.
      - Sensitive fields are redacted from logs.

    Args:
        config (dict[str, Any]): The configuration dictionary to validate.

    Returns:
        dict[str, Any]: The validated configuration dictionary.

    Raises:
        ConfigurationError: If validation fails due to unknown keys, wrong types, or invalid nested configurations.
            This includes cases where community session or enterprise system configurations are invalid.

    Example:
        >>> validated_config = validate_config({'community': {'sessions': {'local_session': {}}}, 'enterprise': {'systems': {'prod_cluster': {}}}})
        >>> validated_config_empty = validate_config({})  # Also valid
    """
    if not isinstance(config, dict):
        raise ConfigurationError("Configuration must be a dictionary")

    # Validate the entire configuration
    _validate_section(config, ())

    _LOGGER.info("Configuration validation passed.")
    return config
