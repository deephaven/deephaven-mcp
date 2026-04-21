"""Async Deephaven MCP configuration management.

This module provides async functions to load, validate, and manage configuration for Deephaven MCP from a JSON or JSON5 file.
Configuration is loaded from a file specified by the DH_MCP_CONFIG_FILE environment variable using native async file I/O (aiofiles).
The configuration file supports both standard JSON and JSON5 formats. JSON5 allows single-line (//) and multi-line (/* */) comments, trailing commas, and other JSON5 features.

Two Config Formats, Two Manager Classes:
-----------------------------------------
This module supports two distinct configuration file formats, one per server type:

  1. **Community server** (``dh-mcp-community-server``): Use :class:`CommunityServerConfigManager`.
     The config file uses ``community`` and ``security`` as optional top-level keys (described in
     full below). No enterprise-related keys are allowed.

  2. **Enterprise server** (``dh-mcp-enterprise-server``): Use :class:`EnterpriseServerConfigManager`.
     The config file is a *flat* dict with all enterprise system fields at the top level — there are
     no ``community`` or ``security`` sections. The enterprise schema is documented fully in the
     "Enterprise Server Configuration Schema" section below.

Features:
    - Coroutine-safe, cached loading of configuration using asyncio.Lock.
    - Strict validation of configuration structure and values.
    - Logging of configuration loading, environment variable value, and validation steps.
    - Uses aiofiles for non-blocking, native async config file reads.

Community Server Configuration Schema:
---------------------------------------
The community config file must be a JSON or JSON5 object. JSON5 allows single-line (//) and multi-line (/* */) comments, trailing commas, and other JSON5 features.
It may contain the following top-level keys (all optional):

  - `security` (dict, optional):
      A dictionary containing security-related configuration for all session types.
      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
      If this key is absent, all security settings use their secure defaults.
      The security configuration dict may contain:

        - `community` (dict, optional):
            Security settings specific to community sessions.
            If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
            May contain:

              - `credential_retrieval_mode` (str, optional, default: "none"): Controls which community session credentials
                can be retrieved programmatically via the session_community_credentials MCP tool. Valid values:
                  * "none": Credential retrieval disabled (secure default)
                  * "dynamic_only": Only allow retrieval for auto-generated tokens (dynamic sessions)
                  * "static_only": Only allow retrieval for pre-configured tokens (static sessions)
                  * "all": Allow retrieval for both dynamic and static session credentials

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

              - `host` (str): Hostname or IP address of the community server.
              - `port` (int): Port number for the community server connection.
              - `auth_type` (str): Authentication type. Common values include:
                  * "PSK" or "io.deephaven.authentication.psk.PskAuthenticationHandler": Pre-shared key authentication (shorthand and full class name).
                  * "Anonymous": Default, no authentication required.
                  * "Basic": HTTP Basic authentication (requires username:password format in auth_token).
                  * Custom authenticator strings are also valid.
              - `auth_token` (str, optional): The direct authentication token or password. May be empty if `auth_type` is "Anonymous". Use this OR `auth_token_env_var`, but not both.
              - `auth_token_env_var` (str, optional): The name of an environment variable from which to read the authentication token. Use this OR `auth_token`, but not both.
              - `never_timeout` (bool): If True, sessions to this community server never time out.
              - `session_type` (str): Programming language for the session. Common values include:
                  * "python": For Python-based Deephaven instances.
                  * "groovy": For Groovy-based Deephaven instances.
              - `use_tls` (bool): Whether to use TLS/SSL for the connection.
              - `tls_root_certs` (str | None, optional): Path to a PEM file containing root certificates to trust for TLS.
              - `client_cert_chain` (str | None, optional): Path to a PEM file containing the client certificate chain for mutual TLS.
              - `client_private_key` (str | None, optional): Path to a PEM file containing the client private key for mutual TLS.

        - `session_creation` (dict, optional):
            Configuration for dynamically creating community sessions on demand.
            If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
            If this key is absent, dynamic session creation is not configured.

      Notes:
        - All fields are optional; if a field is omitted, the consuming code may use an internal default value for that field, or the feature may be disabled.
        - All file paths should be absolute, or relative to the process working directory.
        - If `use_tls` is True and any of the optional TLS fields are provided, they must point to valid PEM files.
        - Sensitive fields (`auth_token`, `client_private_key`) are redacted from logs for security.
        - Unknown fields are not allowed and will cause validation to fail.

Community Config Validation rules:
  - If the `community` key is present, its value must be a dictionary.
  - Within each community configuration, all field values must have the correct type if present.
  - No unknown fields are permitted at any level of the configuration.
  - If TLS fields are provided, referenced files must exist and be readable.
  - Only `community` and `security` are valid top-level keys; any other key will cause validation to fail.
    (Note: the enterprise server uses a completely separate flat config format via EnterpriseServerConfigManager —
    enterprise system fields such as `system_name` are not valid in community config files.)

Enterprise Server Configuration Schema:
-----------------------------------------
The enterprise config file is a flat JSON or JSON5 object. All fields sit at the top level; there
are no ``community`` or ``security`` sections. Each ``dh-mcp-enterprise-server`` instance is
configured for exactly one enterprise system.

Required fields:

  - `system_name` (str): Human-readable identifier for this enterprise system.
      Used as the ``source`` component in all session identifiers (e.g. ``"enterprise:prod:my-pq"``).

  - `connection_json_url` (str): Full URL to the Core+ ``connection.json`` endpoint
      (e.g. ``"https://dhe.example.com/iris/connection.json"``).

  - `auth_type` (str): Authentication method. Must be one of:
      * ``"password"``: Username/password authentication. Requires ``username`` and either
        ``password`` or ``password_env_var`` (mutually exclusive).
      * ``"private_key"``: Private key file authentication. Requires ``private_key_path``.

Authentication fields (required when auth_type is "password"):

  - `username` (str): Username for authentication.
  - `password` (str): Password in plaintext. Use this OR ``password_env_var``, not both.
  - `password_env_var` (str): Name of an environment variable holding the password.
      Use this OR ``password``, not both. Preferred over hardcoding the password.

Authentication fields (required when auth_type is "private_key"):

  - `private_key_path` (str): Filesystem path to the private key file used for authentication.

Optional fields:

  - `connection_timeout` (int | float, > 0): Connection timeout in seconds.
      Default: ``10.0``. Booleans are not accepted even though bool is a subclass of int.

  - `session_creation` (dict, optional): Session lifecycle configuration.
      If absent, dynamic session creation uses server defaults.

Enterprise Config Validation rules:
  - ``system_name``, ``connection_json_url``, and ``auth_type`` are always required.
  - ``auth_type`` must be exactly ``"password"`` or ``"private_key"``; no custom values.
  - For ``"password"`` auth: ``username`` is required; exactly one of ``password`` or
    ``password_env_var`` must be present.
  - For ``"private_key"`` auth: ``private_key_path`` is required.
  - ``connection_timeout`` must be a positive number if present; booleans are rejected.
  - ``max_concurrent_sessions`` must be a non-negative integer if present.
  - Unknown fields generate a warning but do not cause validation to fail.
  - Sensitive field ``password`` is redacted in logs.

Environment Variables:
---------------------
- `DH_MCP_CONFIG_FILE`: Path to the Deephaven MCP configuration JSON or JSON5 file.

Security:
---------
- Sensitive information (such as authentication tokens and passwords) is redacted in logs.
- Environment variable values are logged for debugging.

Async/Await & I/O:
------------------
- All configuration loading is async and coroutine-safe.
- File I/O uses `aiofiles` for non-blocking reads.

"""

__all__ = [
    # Config manager base and concrete types
    "ConfigManager",
    "CommunityServerConfigManager",
    "EnterpriseServerConfigManager",
    # Constants
    "CONFIG_ENV_VAR",
    # Validators used by external callers
    "validate_enterprise_config",
    "validate_single_community_session_config",
    # Redaction used by external callers
    "redact_community_session_config",
    "redact_enterprise_system_config",
    # Constants used by external callers
    "DEFAULT_CONNECTION_TIMEOUT_SECONDS",
    # Exceptions
    "CommunitySessionConfigurationError",
    "EnterpriseSystemConfigurationError",
    "ConfigurationError",
]

import abc
import asyncio
import logging
import os
from collections.abc import Callable
from typing import Any, cast

import aiofiles
import json5

from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    ConfigurationError,
    EnterpriseSystemConfigurationError,
)

from ._community import (
    _apply_redaction_to_config,
    _validate_community_config,
    redact_community_session_config,
    validate_single_community_session_config,
)
from ._enterprise import (
    DEFAULT_CONNECTION_TIMEOUT_SECONDS,
    redact_enterprise_system_config,
    validate_enterprise_config,
)

_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""Name of the environment variable specifying the path to the Deephaven MCP config file."""

class ConfigManager(abc.ABC):
    """
    Abstract base class for Deephaven MCP configuration managers.

    Provides the common interface and shared infrastructure for coroutine-safe,
    cached configuration loading. Concrete subclasses implement config-format-specific
    loading and validation logic.

    Subclasses:
        - :class:`CommunityServerConfigManager`: Loads community-format config files.
        - :class:`EnterpriseServerConfigManager`: Loads flat enterprise-format config files.

    Common features:
        - **Coroutine-safe**: Uses asyncio.Lock to prevent concurrent loads.
        - **Caching**: Loads configuration once; subsequent calls return the cached value.
        - **Cache control**: :meth:`clear_config_cache` forces reload on next access.
    """

    def __init__(self, config_path: str | None = None) -> None:
        """Initialize a new ConfigManager instance.

        Sets up the internal configuration cache and an asyncio.Lock for coroutine safety.

        Args:
            config_path (str | None): Optional explicit path to the configuration file.
                If provided, this takes precedence over the ``DH_MCP_CONFIG_FILE`` environment
                variable. If ``None`` (default), the environment variable is used.
        """
        self._config_path = config_path
        self._cache: dict[str, Any] | None = None
        self._lock = asyncio.Lock()

    async def clear_config_cache(self) -> None:
        """Clear the cached Deephaven configuration (coroutine-safe).

        Forces the next configuration access to reload from disk. Useful for tests
        or when the config file has changed.
        """
        _LOGGER.debug(
            "[ConfigManager:clear_config_cache] Clearing Deephaven configuration cache..."
        )
        async with self._lock:
            self._cache = None

        _LOGGER.debug("[ConfigManager:clear_config_cache] Configuration cache cleared.")

    @abc.abstractmethod
    async def get_config(self) -> dict[str, Any]:
        """Load and return the validated configuration (coroutine-safe).

        Subclasses must implement format-specific loading and validation.
        """
        ...

    @abc.abstractmethod
    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Inject a configuration dictionary into the cache (for testing).

        Subclasses must validate ``config`` against their schema before caching.
        """
        ...


class EnterpriseServerConfigManager(ConfigManager):
    """ConfigManager for the DHE MCP server (``dh-mcp-enterprise-server``).

    Reads a *flat* enterprise config file where the system fields sit at the top level
    (no system-name nesting).  Validates the config as a single enterprise system and
    returns it directly.

    Config file format (flat)::

        {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password_env_var": "DHE_PASSWORD",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4, "programming_language": "Python"}
            }
        }

    ``get_config()`` returns the flat config above directly — no wrapping.
    ``EnterpriseSessionRegistry`` uses the ``system_name`` field as the source name
    in all enterprise session identifiers (e.g. ``"enterprise:prod:my-pq"``).
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the flat enterprise config file.

        Returns:
            dict[str, Any]: The flat enterprise system config dict (fields at top level).

        Raises:
            RuntimeError: If no config path is provided and ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails enterprise validation.
        """
        _LOGGER.debug(
            "[EnterpriseServerConfigManager:get_config] Loading enterprise server configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[EnterpriseServerConfigManager:get_config] Using cached configuration."
                )
                return self._cache

            resolved_path = self._config_path if self._config_path is not None else _get_config_path()
            flat_config = await _load_and_validate_enterprise_config(resolved_path)
            self._cache = flat_config
            _log_config_summary(
                flat_config,
                label="EnterpriseServerConfigManager:get_config",
                redactor=redact_enterprise_system_config,
            )
            _LOGGER.info(
                "[EnterpriseServerConfigManager:get_config] Enterprise configuration loaded successfully."
            )
            return flat_config

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Set the in-memory configuration cache for testing (coroutine-safe).

        Validates ``config`` against the enterprise system schema before caching.
        Implements the abstract :meth:`ConfigManager._set_config_cache` method using
        enterprise validation rather than community validation.

        Args:
            config (dict[str, Any]): The flat enterprise configuration dictionary to cache.

        Raises:
            EnterpriseSystemConfigurationError: If the provided configuration is invalid.
        """
        async with self._lock:
            self._cache = validate_enterprise_config(config)


class CommunityServerConfigManager(ConfigManager):
    """ConfigManager for the DHC MCP server (``dh-mcp-community-server``).

    Reads a community config file. The format uses ``community`` and ``security`` as optional
    top-level keys; validation enforces the community schema defined in
    :mod:`deephaven_mcp.config._community`.

    Config file format::

        {
            "security": {
                "community": {"credential_retrieval_mode": "dynamic_only"}
            },
            "community": {
                "sessions": {
                    "local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "..."}
                },
                "session_creation": {"defaults": {"launch_method": "python"}}
            }
        }
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the community config file (coroutine-safe).

        Returns:
            dict[str, Any]: The validated community configuration dictionary.

        Raises:
            RuntimeError: If no config path is provided and ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails community validation.
        """
        _LOGGER.debug(
            "[CommunityServerConfigManager:get_config] Loading Deephaven MCP application configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[CommunityServerConfigManager:get_config] Using cached Deephaven MCP application configuration."
                )
                return self._cache

            resolved_path = self._config_path if self._config_path is not None else _get_config_path()
            validated = await _load_and_validate_community_config(resolved_path)
            self._cache = validated
            _log_config_summary(
                validated,
                label="CommunityServerConfigManager:get_config",
                redactor=_apply_redaction_to_config,
            )
            _LOGGER.info(
                "[CommunityServerConfigManager:get_config] Community configuration loaded successfully."
            )
            return self._cache

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Inject a community configuration dictionary into the cache (coroutine-safe).

        Validates ``config`` against the community schema before caching.

        Args:
            config (dict[str, Any]): The community configuration dictionary to cache.

        Raises:
            ConfigurationError: If the provided configuration is invalid.
        """
        async with self._lock:
            self._cache = _validate_community_config(config)


async def _load_config_from_file(config_path: str) -> dict[str, Any]:
    """Load and parse the Deephaven MCP configuration from a JSON file using async I/O.

    Uses aiofiles for non-blocking file reads, ensuring the event loop is not blocked
    during file I/O operations. All JSON parsing and I/O errors are caught and wrapped
    as ConfigurationError with descriptive messages.

    Args:
        config_path (str): The absolute or relative path to the configuration JSON file.

    Returns:
        dict[str, Any]: The parsed configuration cast as a dictionary. The caller is
            responsible for ensuring the JSON root is an object; no runtime enforcement
            is performed beyond the cast.

    Raises:
        ConfigurationError: For any of the following conditions:
            - File not found (FileNotFoundError)
            - Permission denied (PermissionError)
            - Invalid JSON/JSON5 syntax (ValueError)
            - Any other I/O error (Exception)

    Example:
        >>> import asyncio
        >>> config = asyncio.run(_load_config_from_file('/path/to/config.json'))
        >>> print(config['community'])
    """
    try:
        async with aiofiles.open(config_path) as f:
            content = await f.read()
        return cast(dict[str, Any], json5.loads(content))
    except FileNotFoundError:
        _LOGGER.error(
            f"[_load_config_from_file] Configuration file not found: {config_path}"
        )
        raise ConfigurationError(
            f"Configuration file not found: {config_path}"
        ) from None
    except PermissionError:
        _LOGGER.error(
            f"[_load_config_from_file] Permission denied when trying to read configuration file: {config_path}"
        )
        raise ConfigurationError(
            f"Permission denied when trying to read configuration file: {config_path}"
        ) from None
    except ValueError as e:
        _LOGGER.error(
            f"[_load_config_from_file] Invalid JSON/JSON5 in configuration file {config_path}: {e}"
        )
        raise ConfigurationError(
            f"Invalid JSON/JSON5 in configuration file {config_path}: {e}"
        ) from e
    except Exception as e:
        _LOGGER.error(
            f"[_load_config_from_file] Unexpected error reading configuration file {config_path}: {e}"
        )
        raise ConfigurationError(
            f"Unexpected error loading or parsing config file {config_path}: {e}"
        ) from e


def _get_config_path() -> str:
    """Retrieve the configuration file path from the DH_MCP_CONFIG_FILE environment variable.

    Returns:
        str: The absolute or relative path to the Deephaven MCP configuration JSON file.

    Raises:
        RuntimeError: If the DH_MCP_CONFIG_FILE environment variable is not set.
    """
    if CONFIG_ENV_VAR not in os.environ:
        _LOGGER.error(
            f"[_get_config_path] Environment variable {CONFIG_ENV_VAR} is not set."
        )
        raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} is not set.")
    config_path = os.environ[CONFIG_ENV_VAR]
    _LOGGER.info(
        f"[_get_config_path] Environment variable {CONFIG_ENV_VAR} is set to: {config_path}"
    )
    return config_path


async def _load_and_validate_config(
    config_path: str,
    validator: Callable[[dict[str, Any]], dict[str, Any]],
    caller: str,
) -> dict[str, Any]:
    """Load a config file and run a validator; wrap any error as ConfigurationError.

    Args:
        config_path (str): Path to the JSON/JSON5 config file.
        validator (Callable[[dict[str, Any]], dict[str, Any]]): Function that validates and returns the parsed dict.
        caller (str): Caller label used in error log messages when loading or validation fails.

    Returns:
        dict[str, Any]: The fully validated configuration dictionary.

    Raises:
        ConfigurationError: For any failure during loading or validation.
    """
    try:
        data = await _load_config_from_file(config_path)
        return validator(data)
    except Exception as e:
        _LOGGER.error(f"[{caller}] Error loading configuration file {config_path}: {e}")
        raise ConfigurationError(f"Error loading configuration file: {e}") from e


async def _load_and_validate_community_config(config_path: str) -> dict[str, Any]:
    """Load, parse, and validate the community configuration from a JSON/JSON5 file."""
    return await _load_and_validate_config(
        config_path, _validate_community_config, "_load_and_validate_community_config"
    )


async def _load_and_validate_enterprise_config(config_path: str) -> dict[str, Any]:
    """Load, parse, and validate the flat enterprise configuration from a JSON/JSON5 file."""
    return await _load_and_validate_config(
        config_path, validate_enterprise_config, "_load_and_validate_enterprise_config"
    )


def _log_config_summary(
    config: dict[str, Any],
    label: str = "ConfigManager:get_config",
    redactor: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> None:
    """Log a summary of the loaded Deephaven MCP configuration.

    This function logs the configuration with sensitive data redacted as formatted JSON.
    Sensitive fields (auth tokens, passwords, private keys, etc.) are replaced with
    "[REDACTED]" before logging. The configuration is logged at INFO level as pretty-printed
    JSON. If JSON serialization fails, the config is logged as a Python dict representation.

    Args:
        config (dict[str, Any]): The loaded and validated configuration dictionary.
        label (str): Log prefix label identifying the caller (e.g. class and method name).
            Defaults to ``"ConfigManager:get_config"``.
        redactor (Callable[[dict[str, Any]], dict[str, Any]] | None): Optional function to
            redact sensitive fields before logging. If ``None``, uses
            :func:`._community._apply_redaction_to_config` (community schema-driven redaction).
            Pass ``redact_enterprise_system_config`` for enterprise configs.

    Example:
        >>> config = {'community': {'sessions': {'local': {'auth_token': 'secret'}}}}
        >>> _log_config_summary(config)
        # Logs: {"community": {"sessions": {"local": {"auth_token": "[REDACTED]"}}}}
    """
    _LOGGER.info(f"[{label}] Configuration summary:")

    # Create a redacted copy of the config for logging
    redacted_config = redactor(config) if redactor is not None else _apply_redaction_to_config(config)

    # Log the redacted config as formatted JSON
    try:
        formatted_config = json5.dumps(redacted_config, indent=2, sort_keys=True)
        _LOGGER.info(
            f"[{label}] Loaded configuration:\n{formatted_config}"
        )
    except (TypeError, ValueError) as e:
        _LOGGER.warning(
            f"[{label}] Failed to format config as JSON: {e}"
        )
        _LOGGER.info(
            f"[{label}] Loaded configuration: {redacted_config}"
        )
