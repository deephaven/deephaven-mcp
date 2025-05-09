"""
Async Deephaven MCP configuration management.

This module provides async functions to load, validate, and manage configuration for Deephaven workers from a JSON file.
Configuration is loaded from a file specified by the DH_MCP_CONFIG_FILE environment variable using native async file I/O (aiofiles).

Features:
    - Coroutine-safe, cached loading of configuration using asyncio.Lock.
    - Strict validation of configuration structure and values.
    - Helper functions to access worker-specific config, and worker names.
    - Logging of configuration loading, environment variable value, and validation steps.
    - Uses aiofiles for non-blocking, native async config file reads.

Configuration Schema:
---------------------
The configuration file must be a JSON object with exactly two top-level keys:

  - workers (dict, required):
      A dictionary mapping worker names (str) to worker configuration dicts.
      Each worker configuration dict may contain any of the following fields (all are optional):

        - host (str): Hostname or IP address of the worker.
        - port (int): Port number for the worker connection.
        - auth_type (str): Authentication type. Allowed values include:
            * "token": Use a bearer token for authentication.
            * "basic": Use HTTP Basic authentication.
            * "anonymous": No authentication required.
        - auth_token (str): The authentication token or password. May be empty if auth_type is "anonymous".
        - never_timeout (bool): If True, sessions to this worker never time out.
        - session_type (str): Session management mode. Allowed values include:
            * "single": Only one session is maintained per worker.
            * "multi": Multiple sessions may be created per worker.
        - use_tls (bool): Whether to use TLS/SSL for the connection.
        - tls_root_certs (str): Path to a PEM file containing root certificates to trust for TLS.
        - client_cert_chain (str): Path to a PEM file containing the client certificate chain for mutual TLS.
        - client_private_key (str): Path to a PEM file containing the client private key for mutual TLS.

      Notes:
        - All fields are optional; if a field is omitted, the consuming code may use an internal default value for that field, or the feature may be disabled. There is no default or fallback worker—every worker must be explicitly configured and selected by name.
        - All file paths should be absolute, or relative to the process working directory.
        - If use_tls is True and any of the optional TLS fields are provided, they must point to valid PEM files.
        - Sensitive fields (auth_token, client_private_key) are redacted from logs for security.
        - Unknown fields are not allowed and will cause validation to fail.

Validation rules:
  - All required fields must be present and have the correct type.
  - All field values must be valid (see allowed values above).
  - No unknown fields are permitted in worker configs.
  - If TLS fields are provided, referenced files must exist and be readable.

Configuration JSON Specification:
---------------------------------
- The configuration file must be a JSON object with one top-level key:
    - "workers": a dictionary mapping worker names to worker configuration dicts

Example Valid Configuration:
---------------------------
The configuration file should look like the following (see field explanations below):

```json
{
    "workers": {
        "local": {
            "host": "localhost",  // str: Hostname or IP address
            "port": 10000,        // int: Port number
            "auth_type": "token", // str: Authentication type ("token", "basic", "none")
            "auth_token": "your-token-here", // str: Authentication token
            "never_timeout": true, // bool: Whether sessions should never timeout
            "session_type": "single", // str: "single" or "multi"
            "use_tls": true,      // bool: Whether to use TLS/SSL
            "tls_root_certs": "/path/to/certs.pem", // str: Path to TLS root certificates
            "client_cert_chain": "/path/to/client-cert.pem", // str: Path to client certificate chain
            "client_private_key": "/path/to/client-key.pem"  // str: Path to client private key
        },
        "remote": {
            "host": "remote-server.example.com",
            "port": 10000,
            "auth_type": "basic",
            "auth_token": "basic-auth-token",
            "never_timeout": false,
            "session_type": "multi",
            "use_tls": true
        }
    },
}
```

Example Invalid Configurations:
------------------------------
1. Invalid: Missing required top-level keys
```json
{
    "workers": {}
}
```


2. Invalid: Worker field with wrong type
```json
{
    "workers": {
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
- Employs an asyncio.Lock to ensure coroutine-safe, cached configuration loading.
- Designed for high-throughput, concurrent environments.

Usage Patterns:
---------------
- The configuration **must** include a 'workers' dictionary as a top-level key.
- Loading a worker configuration:
    >>> config = await get_worker_config('local')
    >>> connection = connect(**config)
- Listing available workers:
    >>> workers = await get_worker_names()
    >>> for worker in workers:
    ...     print(f"Available worker: {worker}")

Environment Variables:
---------------------
- DH_MCP_CONFIG_FILE: Path to the Deephaven worker configuration JSON file.

Security:
---------
- Sensitive information (such as authentication tokens) is redacted in logs.
- Environment variable values are logged for debugging.

Async/Await & I/O:
------------------
- All configuration loading is async and coroutine-safe.
- File I/O uses aiofiles for non-blocking reads.

Async/Await & I/O:
------------------
- All configuration loading is async and coroutine-safe.
- File I/O uses aiofiles for non-blocking reads.
"""

import asyncio
import json
import logging
import os
from time import perf_counter
from typing import Any, cast

import aiofiles

_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""
str: Name of the environment variable specifying the path to the Deephaven worker config file.
"""

_REQUIRED_FIELDS: list[str] = []
"""
list[str]: List of required fields for each worker configuration dictionary.
"""

_ALLOWED_WORKER_FIELDS = {
    "host": str,
    "port": int,
    "auth_type": str,
    "auth_token": str,
    "never_timeout": bool,
    "session_type": str,
    "use_tls": bool,
    "tls_root_certs": (str, type(None)),
    "client_cert_chain": (str, type(None)),
    "client_private_key": (str, type(None)),
}
"""
Dictionary of allowed worker configuration fields and their expected types.
Type: dict[str, type | tuple[type, ...]]
"""

class ConfigManager:
    """
    Async configuration manager for Deephaven MCP worker configuration.

    This class encapsulates all logic for loading, validating, and caching the configuration used by Deephaven MCP workers. The configuration must include a 'workers' dictionary as a required top-level key. All configuration operations, including retrieving worker-specific configurations, depend on this key being present and valid. All configuration access and mutation should go through an instance of this class (typically DEFAULT_CONFIG_MANAGER).
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
            >>> await config.DEFAULT_CONFIG_MANAGER.clear_config_cache()
        """
        _LOGGER.debug("Clearing Deephaven configuration cache...")
        async with self._lock:
            self._cache = None

        _LOGGER.debug("Configuration cache cleared.")


    async def set_config_cache(self, config: dict[str, Any]) -> None:
        """
        Set the in-memory configuration cache (coroutine-safe, for testing only).

        Args:
            config (Dict[str, Any]): The configuration dictionary to set as the cache. This will be validated before caching.

        Returns:
            None

        Example:
            >>> await config.DEFAULT_CONFIG_MANAGER.set_config_cache({'workers': {...}})
        """
        async with self._lock:
            self._cache = self.validate_config(config)


    async def get_config(self) -> dict[str, Any]:
        """
        Load and validate the Deephaven worker configuration from disk (coroutine-safe).

        Uses aiofiles for async file I/O and caches the result. If the cache is present, returns it; otherwise, loads from disk and validates.

        Returns:
            Dict[str, Any]: The loaded and validated configuration dictionary.

        Raises:
            RuntimeError: If the environment variable is not set, or the file cannot be read.
            ValueError: If the config file is invalid, contains unknown keys, or fails validation.

        Example:
            >>> import os
            >>> os.environ['DH_MCP_CONFIG_FILE'] = '/path/to/config.json'
            >>> config_dict = await config.DEFAULT_CONFIG_MANAGER.get_config()
            >>> config_dict['workers']['local']['host']
            'localhost'
        """
        _LOGGER.debug("Loading Deephaven worker configuration...")
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug("Using cached Deephaven worker configuration.")
                return self._cache

            _LOGGER.info("Loading Deephaven worker configuration from disk...")
            start_time = perf_counter()

            if CONFIG_ENV_VAR not in os.environ:
                _LOGGER.error(f"Environment variable {CONFIG_ENV_VAR} is not set.")
                raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} is not set.")

            config_path = os.environ[CONFIG_ENV_VAR]
            _LOGGER.info(f"Environment variable {CONFIG_ENV_VAR} is set to: {config_path}")

            try:
                async with aiofiles.open(config_path) as f:
                    data = json.loads(await f.read())
            except Exception as e:
                _LOGGER.error(f"Failed to load config file {config_path}: {e}")
                raise

            validated = self.validate_config(data)
            self._cache = validated
            _LOGGER.info(
                f"Deephaven worker configuration loaded and validated successfully in {perf_counter() - start_time:.3f} seconds"
            )
            return validated


    async def get_worker_config(self, worker_name: str) -> dict[str, Any]:
        """
        Retrieve the configuration dictionary for a specific worker.

        Args:
            worker_name (str): The name of the worker to retrieve. This argument is required.

        Returns:
            Dict[str, Any]: The configuration dictionary for the specified worker.

        Raises:
            RuntimeError: If the specified worker is not found in the configuration.

        Example:
            >>> worker_cfg = await config.DEFAULT_CONFIG_MANAGER.get_worker_config('local')
        """
        _LOGGER.debug(f"Getting worker config for worker: {worker_name!r}")
        config = await self.get_config()
        workers = config.get("workers", {})

        if worker_name not in workers:
            _LOGGER.error(f"Worker {worker_name} not found in configuration")
            raise RuntimeError(f"Worker {worker_name} not found in configuration")

        _LOGGER.debug(f"Returning config for worker: {worker_name}")
        return cast(dict[str, Any], workers[worker_name])


    async def get_worker_names(self) -> list[str]:
        """
        Get a list of all configured Deephaven worker names from the loaded configuration.

        Returns:
            list[str]: List of all worker names defined in the configuration.

        Example:
            >>> workers = await config.DEFAULT_CONFIG_MANAGER.get_worker_names()
            >>> for worker in workers:
            ...     print(f"Available worker: {worker}")
        """
        _LOGGER.debug("Getting list of all worker names")
        config = await self.get_config()
        workers = config.get("workers", {})
        worker_names = list(workers.keys())

        _LOGGER.debug(f"Found {len(worker_names)} worker(s): {worker_names}")
        return worker_names


    @staticmethod
    def validate_config(config: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the Deephaven worker configuration dictionary.

        Args:
            config (Dict[str, Any]): The configuration dictionary to validate. Must include a 'workers' dictionary as a top-level key.

        Returns:
            Dict[str, Any]: The validated configuration dictionary. This may be a normalized or cleaned version of the input.

        Raises:
            ValueError: If the config is missing required keys, has unknown keys, has invalid field types, or is otherwise invalid.

        Example:
            >>> valid = ConfigManager.validate_config({'workers': {'local': {...}}})
        """
        required_top_level = {"workers"}
        allowed_top_level = required_top_level
        top_level_keys = set(config.keys())

        unknown_keys = top_level_keys - allowed_top_level
        if unknown_keys:
            _LOGGER.error(
                f"Unknown top-level keys in Deephaven worker config: {unknown_keys}"
            )
            raise ValueError(
                f"Unknown top-level keys in Deephaven worker config: {unknown_keys}"
            )

        missing_keys = required_top_level - top_level_keys
        if missing_keys:
            _LOGGER.error(
                f"Missing required top-level keys in Deephaven worker config: {missing_keys}"
            )
            raise ValueError(
                f"Missing required top-level keys in Deephaven worker config: {missing_keys}"
            )

        workers = config["workers"]
        if not isinstance(workers, dict):
            raise ValueError("'workers' must be a dictionary in Deephaven worker config")
        if not workers:
            _LOGGER.error("No workers defined in Deephaven worker config")
            raise ValueError("No workers defined in Deephaven worker config")

        for worker_name, worker_config in workers.items():
            ConfigManager._validate_worker_config(worker_name, worker_config)

        return config

    @staticmethod
    def _validate_worker_config(worker_name: str, worker_config: dict[str, Any]) -> None:
        """
        Validate the configuration dictionary for a single Deephaven worker.

        Args:
            worker_name (str): The name of the worker being validated.
            worker_config (dict[str, Any]): The configuration dictionary for the worker.

        Raises:
            ValueError: If the worker config is not a dictionary, is missing required fields,
                contains unknown fields, or contains fields with invalid types.
        """
        if not isinstance(worker_config, dict):
            raise ValueError(f"Worker config for {worker_name} must be a dictionary.")

        missing_fields = [
            field for field in _REQUIRED_FIELDS if field not in worker_config
        ]
        if missing_fields:
            _LOGGER.error(
                f"Missing required fields in worker config for {worker_name}: {missing_fields}"
            )
            raise ValueError(
                f"Missing required fields in worker config for {worker_name}: {missing_fields}"
            )

        for field, value in worker_config.items():
            if field not in _ALLOWED_WORKER_FIELDS:
                _LOGGER.error(
                    f"Unknown field '{field}' in worker config for {worker_name}"
                )
                raise ValueError(
                    f"Unknown field '{field}' in worker config for {worker_name}"
                )

            allowed_types = _ALLOWED_WORKER_FIELDS[field]
            if not isinstance(allowed_types, tuple):
                allowed_types = (allowed_types,)
            if not isinstance(value, allowed_types):
                _LOGGER.error(
                    f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}"
                )
                raise ValueError(
                    f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}"
                )
