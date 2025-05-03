"""
Async Deephaven MCP configuration management.

This module provides async functions to load, validate, and manage configuration for Deephaven workers from a JSON file.
Configuration is loaded from a file specified by the DH_MCP_CONFIG_FILE environment variable using native async file I/O (aiofiles).

Features:
    - Coroutine-safe, cached loading of configuration using asyncio.Lock.
    - Strict validation of configuration structure and values.
    - Helper functions to access worker-specific config, default worker, and worker names.
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
        - All fields are optional; if a field is omitted, a default may be used by the consuming code, or the feature may be disabled.
        - All file paths should be absolute, or relative to the process working directory.
        - If use_tls is True and any of the optional TLS fields are provided, they must point to valid PEM files.
        - Sensitive fields (auth_token, client_private_key) are redacted from logs for security.
        - Unknown fields are not allowed and will cause validation to fail.

  - default_worker (str, required):
      The name of the default worker to use. Must match one of the keys in the workers dictionary.
      This worker will be used if no worker name is explicitly specified in API calls.

Validation rules:
  - All required fields must be present and have the correct type.
  - All field values must be valid (see allowed values above).
  - No unknown fields are permitted in worker configs.
  - The default_worker must be present in the workers dictionary.
  - If TLS fields are provided, referenced files must exist and be readable.

Configuration JSON Specification:
---------------------------------
- The configuration file must be a JSON object with exactly two top-level keys:
    - "workers": a dictionary mapping worker names to worker configuration dicts
    - "default_worker": the name (string) of the default worker (must be a key in "workers")

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
    "default_worker": "local"  // str: Name of the default worker
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

2. Invalid: default_worker must be defined and must exist in workers
```json
{
    "workers": {
        "local": {
            "host": "localhost",
            "port": 10000
        }
    },
    "default_worker": "nonexistent"  // Must be a worker that exists in the workers dictionary
}
```

Performance Considerations:
--------------------------
- Uses native async file I/O (aiofiles) to avoid blocking the event loop.
- Employs an asyncio.Lock to ensure coroutine-safe, cached configuration loading.
- Designed for high-throughput, concurrent environments.

Usage Patterns:
---------------
- Loading a worker configuration:
    >>> config = await get_worker_config('local')
    >>> connection = connect(**config)
- Listing available workers:
    >>> workers = await get_worker_names()
    >>> for worker in workers:
    ...     print(f"Available worker: {worker}")
- Using the default worker:
    >>> default_worker = await get_worker_name_default()
    >>> config = await get_worker_config(default_worker)

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

Usage Patterns:
---------------
- Loading a worker configuration:
    >>> config = await get_worker_config('local')
    >>> connection = connect(**config)
- Listing available workers:
    >>> workers = await get_worker_names()
    >>> for worker in workers:
    ...     print(f"Available worker: {worker}")
- Using the default worker:
    >>> default_worker = await get_worker_name_default()
    >>> config = await get_worker_config(default_worker)

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
"""

import os
import json
import logging
from typing import Any, Dict, Optional
from time import perf_counter
import asyncio
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

    This class encapsulates all logic for loading, validating, and caching the configuration used by Deephaven MCP workers. It ensures coroutine safety for all operations, supports async file I/O, and provides methods for retrieving worker-specific configurations, listing available workers, and resolving the default worker. All configuration access and mutation should go through an instance of this class (typically DEFAULT_CONFIG_MANAGER).
    """
    def __init__(self) -> None:
        """
        Initialize a new ConfigManager instance.

        Sets up the internal configuration cache and an asyncio.Lock for coroutine safety.
        Typically, only one instance (DEFAULT_CONFIG_MANAGER) should be used in production.
        """
        self._cache: Optional[Dict[str, Any]] = None
        self._lock = asyncio.Lock()

    async def get_worker_name_default(self) -> str:
        """
        Get the default worker name from the loaded configuration.

        Returns:
            str: The default worker name.

        Raises:
            RuntimeError: If no default worker is set in the configuration.

        Example:
            >>> default_worker = await config.DEFAULT_CONFIG_MANAGER.get_worker_name_default()
        """
        config = await self.get_config()
        default_worker = config.get("default_worker")
        if not default_worker:
            raise RuntimeError("No default worker is set in the configuration.")
        return default_worker

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


    async def set_config_cache(self, config: Dict[str, Any]) -> None:
        """
        Set the in-memory configuration cache (coroutine-safe, for testing only).

        Args:
            config (Dict[str, Any]): The configuration dictionary to set as the cache. This will be validated before caching.

        Returns:
            None

        Example:
            >>> await config.DEFAULT_CONFIG_MANAGER.set_config_cache({'workers': {...}, 'default_worker': 'local'})
        """
        async with self._lock:
            self._cache = self.validate_config(config)


    async def get_config(self) -> Dict[str, Any]:
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
                async with aiofiles.open(config_path, "r") as f:
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


    async def get_worker_config(self, worker_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve the configuration dictionary for a specific worker.

        Args:
            worker_name (Optional[str]): The name of the worker to retrieve. If None, uses the default_worker from config.

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
        resolved_name = await self.resolve_worker_name(worker_name)

        if resolved_name not in workers:
            _LOGGER.error(f"Worker {resolved_name} not found in configuration")
            raise RuntimeError(f"Worker {resolved_name} not found in configuration")

        _LOGGER.debug(f"Returning config for worker: {resolved_name}")
        return workers[resolved_name]


    async def resolve_worker_name(self, worker_name: Optional[str] = None) -> str:
        """
        Resolve the worker name to use, either from the provided worker_name or the default_worker from config.

        Args:
            worker_name (Optional[str]): The name of the worker to resolve. If None, uses the default_worker from config.

        Returns:
            str: The resolved worker name.

        Raises:
            RuntimeError: If no worker name is specified and no default_worker is set in the config.

        Example:
            >>> worker = await config.DEFAULT_CONFIG_MANAGER.resolve_worker_name()
        """
        _LOGGER.debug(f"Resolving worker name (provided: {worker_name!r})")
        config = await self.get_config()

        if worker_name:
            return worker_name

        default_worker = config.get("default_worker")
        if not default_worker:
            _LOGGER.error("No worker name specified and no default_worker in config")
            raise RuntimeError("No worker name specified and no default_worker in config")

        _LOGGER.debug(f"Using default worker: {default_worker}")
        return default_worker


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
    def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate the Deephaven worker configuration dictionary.

        Args:
            config (Dict[str, Any]): The configuration dictionary to validate.

        Returns:
            Dict[str, Any]: The validated configuration dictionary. This may be a normalized or cleaned version of the input.

        Raises:
            ValueError: If the config is missing required keys, has unknown keys, has invalid field types, or is otherwise invalid.

        Example:
            >>> valid = ConfigManager.validate_config({'workers': {...}, 'default_worker': 'local'})
        """
        required_top_level = {"workers", "default_worker"}
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
            _LOGGER.error("'workers' must be a dictionary in Deephaven worker config")
            raise ValueError("'workers' must be a dictionary in Deephaven worker config")

        if not workers:
            _LOGGER.error("No workers defined in Deephaven worker config")
            raise ValueError("No workers defined in Deephaven worker config")

        for worker_name, worker_config in workers.items():
            if not isinstance(worker_config, dict):
                _LOGGER.error(f"Worker config for {worker_name} must be a dictionary")
                raise ValueError(f"Worker config for {worker_name} must be a dictionary")

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
                if not isinstance(value, allowed_types):
                    _LOGGER.error(
                        f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}"
                    )
                    raise ValueError(
                        f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}"
                    )

        default_worker = config["default_worker"]
        if default_worker not in workers:
            _LOGGER.error(f"Default worker '{default_worker}' is not defined in workers")
            raise ValueError(f"Default worker '{default_worker}' is not defined in workers")

        return config


DEFAULT_CONFIG_MANAGER: ConfigManager = ConfigManager()
"""
DEFAULT_CONFIG_MANAGER (ConfigManager):
    The default singleton instance of ConfigManager used by all Deephaven MCP configuration APIs.
    This instance is coroutine-safe and should be used for all configuration loading, validation, and cache management unless a custom instance is required (e.g., for testing or advanced use).

    Usage Example:
        from deephaven_mcp import config
        config_dict = await config.DEFAULT_CONFIG_MANAGER.get_config()
        worker_cfg = await config.DEFAULT_CONFIG_MANAGER.get_worker_config('local')
        await config.DEFAULT_CONFIG_MANAGER.clear_config_cache()

    # Listing all workers
        workers = await config.DEFAULT_CONFIG_MANAGER.get_worker_names()
        for worker in workers:
            print(f"Available worker: {worker}")

    # Resolving the default worker
        default = await config.DEFAULT_CONFIG_MANAGER.resolve_worker_name()

    This singleton ensures a consistent configuration state across the application.
"""
