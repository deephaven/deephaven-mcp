"""
Configuration management for Deephaven worker servers.

This module provides thread-safe loading, validation, and access to Deephaven worker
configuration from a JSON file specified by the DH_MCP_CONFIG_FILE environment variable.
It ensures robust error handling, strict schema validation, atomic cache operations, and
provides helpers for retrieving worker names and the default worker for session management.

Features:
    - Thread-safe, reentrant loading and caching of configuration from JSON.
    - Strict validation of configuration structure, allowed fields, and required fields.
    - Access to individual worker configs, worker lists, and the default worker.
    - Only 'workers' and 'default_worker' allowed as top-level keys.
    - Atomic cache clearing for safe reloads.
    - Designed for use by other modules and tools in the dhmcp package.
"""

import os
import json
import logging
import threading
from typing import Optional, Dict, Any
from time import perf_counter

_LOGGER = logging.getLogger(__name__)

_CONFIG_CACHE: Optional[Dict[str, Any]] = None
_CONFIG_CACHE_LOCK = threading.RLock()
"""
_CONFIG_CACHE (Optional[dict]): Holds the loaded Deephaven worker configuration, or None if not loaded.
_CONFIG_CACHE_LOCK (threading.RLock): Ensures thread-safe, reentrant access to the configuration cache.
"""

def clear_config_cache() -> None:
    """
    Atomically clear the Deephaven configuration cache.

    Acquires the configuration cache lock and sets the cached config to None.
    This ensures that future config loads will re-read from disk. Thread-safe and
    safe to call concurrently or recursively (uses a reentrant lock).
    """
    _LOGGER.debug("Clearing Deephaven configuration cache...")
    global _CONFIG_CACHE
    
    with _CONFIG_CACHE_LOCK:
        _CONFIG_CACHE = None

    _LOGGER.debug("Configuration cache cleared.")

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""
str: Name of the environment variable specifying the path to the Deephaven worker config file.
"""

_REQUIRED_FIELDS = []
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

def load_config() -> Dict[str, Any]:
    """
    Load and validate the Deephaven worker configuration from the JSON file specified
    by the DH_MCP_CONFIG_FILE environment variable. Uses a thread-safe cache to avoid
    repeated disk reads and validation.

    Returns:
        Dict[str, Any]: The loaded and validated configuration dictionary.

    Raises:
        RuntimeError: If the environment variable is not set, or the file cannot be read.
        ValueError: If the config file is not a JSON object, contains unknown keys, or fails validation.
    """
    _LOGGER.debug("Loading Deephaven worker configuration...")
    global _CONFIG_CACHE

    # Thread-safe read of the config cache
    with _CONFIG_CACHE_LOCK:
        if _CONFIG_CACHE is not None:
            _LOGGER.debug("Using cached Deephaven worker configuration.")
            return _CONFIG_CACHE

        # Only one thread proceeds to load and cache the config
        _LOGGER.info("Loading Deephaven worker configuration from disk...")
        start_time = perf_counter()
        config_path = os.environ.get(CONFIG_ENV_VAR)
        if not config_path:
            _LOGGER.error(f"Environment variable {CONFIG_ENV_VAR} must be set to the path of the Deephaven worker config file.")
            raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} must be set to the path of the Deephaven worker config file.")

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except Exception as e:
            _LOGGER.error(f"Failed to load Deephaven worker config from {config_path}: {str(e)}")
            raise RuntimeError(f"Failed to load Deephaven worker config from {config_path}: {str(e)}") from e

        # Validate config structure
        if not isinstance(config, dict):
            _LOGGER.error("Deephaven worker config must be a JSON object")
            raise ValueError("Deephaven worker config must be a JSON object")

        # Validate top-level keys
        top_level_keys = set(config.keys())
        allowed_top_level = {'workers', 'default_worker'}
        unknown_keys = top_level_keys - allowed_top_level
        if unknown_keys:
            _LOGGER.error(f"Unknown top-level keys in Deephaven worker config: {unknown_keys}")
            raise ValueError(f"Unknown top-level keys in Deephaven worker config: {unknown_keys}")

        # Validate workers
        workers = config.get('workers', {})
        if not isinstance(workers, dict):
            _LOGGER.error("'workers' must be a dictionary in Deephaven worker config")
            raise ValueError("'workers' must be a dictionary in Deephaven worker config")

        for worker_name, worker_config in workers.items():
            if not isinstance(worker_config, dict):
                _LOGGER.error(f"Worker config for {worker_name} must be a dictionary")
                raise ValueError(f"Worker config for {worker_name} must be a dictionary")

            # Check required fields
            missing_fields = [field for field in _REQUIRED_FIELDS if field not in worker_config]
            if missing_fields:
                _LOGGER.error(f"Missing required fields in worker config for {worker_name}: {missing_fields}")
                raise ValueError(f"Missing required fields in worker config for {worker_name}: {missing_fields}")

            # Check allowed fields and types
            for field, value in worker_config.items():
                if field not in _ALLOWED_WORKER_FIELDS:
                    _LOGGER.error(f"Unknown field '{field}' in worker config for {worker_name}")
                    raise ValueError(f"Unknown field '{field}' in worker config for {worker_name}")

                allowed_types = _ALLOWED_WORKER_FIELDS[field]
                if not isinstance(value, allowed_types):
                    _LOGGER.error(f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}")
                    raise ValueError(f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}")

        # Cache the validated config
        _CONFIG_CACHE = config
        load_time = perf_counter() - start_time
        _LOGGER.info(f"Deephaven worker configuration loaded and validated successfully in {load_time:.3f} seconds")
        return config

def resolve_worker_name(worker_name: Optional[str] = None) -> str:
    """
    Resolve the worker name to use, either from the provided worker_name or the default_worker from config.

    Args:
        worker_name (str, optional): The name of the worker to retrieve. If None, uses the default_worker from config.

    Returns:
        str: The resolved worker name.

    Raises:
        RuntimeError: If no worker name is specified (via argument or default_worker in config).
    """
    _LOGGER.debug(f"Resolving worker name (provided: {worker_name!r})")
    config = load_config()

    if worker_name:
        return worker_name

    default_worker = config.get('default_worker')
    if not default_worker:
        _LOGGER.error("No worker name specified and no default_worker in config")
        raise RuntimeError("No worker name specified and no default_worker in config")

    _LOGGER.debug(f"Using default worker: {default_worker}")
    return default_worker

def get_worker_config(worker_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve the configuration dictionary for a specific worker.

    Args:
        worker_name (str, optional): The name of the worker to retrieve. If None, uses the default_worker from config.

    Returns:
        dict: The configuration dictionary for the specified worker.

    Raises:
        RuntimeError: If no workers are defined, the worker is not found, or no default_worker is set.
    """
    _LOGGER.debug(f"Getting worker config for worker: {worker_name!r}")
    config = load_config()
    workers = config.get('workers', {})

    if not workers:
        _LOGGER.error("No workers defined in configuration")
        raise RuntimeError("No workers defined in configuration")

    worker_name = resolve_worker_name(worker_name)
    if worker_name not in workers:
        _LOGGER.error(f"Worker {worker_name} not found in configuration")
        raise RuntimeError(f"Worker {worker_name} not found in configuration")

    _LOGGER.debug(f"Returning config for worker: {worker_name}")
    return workers[worker_name]

def deephaven_worker_names() -> list[str]:
    """
    Get a list of all configured Deephaven worker names from the loaded configuration.

    Returns:
        list[str]: List of worker names defined in the configuration.
    """
    _LOGGER.debug("Getting list of all worker names")
    config = load_config()
    workers = config.get('workers', {})
    worker_names = list(workers.keys())
    _LOGGER.debug(f"Found {len(worker_names)} worker(s): {worker_names}")
    return worker_names

def deephaven_default_worker() -> Optional[str]:
    """
    Get the name of the default Deephaven worker, as set in the configuration.

    Returns:
        str or None: The default worker name, or None if not set in the config.
    """
    _LOGGER.debug("Getting default worker name")
    config = load_config()
    default_worker = config.get('default_worker')
    _LOGGER.debug(f"Default worker: {default_worker}")
    return default_worker
