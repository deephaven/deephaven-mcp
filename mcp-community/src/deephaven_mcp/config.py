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
    logging.info("CALL: clear_config_cache called with no arguments")
    logging.info("Clearing Deephaven configuration cache...")
    global _CONFIG_CACHE
    
    with _CONFIG_CACHE_LOCK:
        _CONFIG_CACHE = None

    logging.info("Configuration cache cleared.")

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

def _load_config() -> Dict[str, Any]:
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
    logging.info("CALL: _load_config called with no arguments")
    global _CONFIG_CACHE

    # Thread-safe read of the config cache
    with _CONFIG_CACHE_LOCK:
        if _CONFIG_CACHE is not None:
            logging.debug("Using cached Deephaven worker configuration.")
            return _CONFIG_CACHE

        # Only one thread proceeds to load and cache the config
        logging.info("Loading Deephaven worker configuration...")
        config_path = os.environ.get(CONFIG_ENV_VAR)
        if not config_path:
            logging.error(f"Environment variable {CONFIG_ENV_VAR} must be set to the path of the Deephaven worker config file.")
            raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} must be set to the path of the Deephaven worker config file.")

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load Deephaven worker config from {config_path}: {str(e)}")
            raise RuntimeError(f"Failed to load Deephaven worker config from {config_path}: {str(e)}") from e

        # Validate config structure
        if not isinstance(config, dict):
            logging.error("Deephaven worker config must be a JSON object")
            raise ValueError("Deephaven worker config must be a JSON object")

        # Validate top-level keys
        top_level_keys = set(config.keys())
        allowed_top_level = {'workers', 'default_worker'}
        unknown_keys = top_level_keys - allowed_top_level
        if unknown_keys:
            logging.error(f"Unknown top-level keys in Deephaven worker config: {unknown_keys}")
            raise ValueError(f"Unknown top-level keys in Deephaven worker config: {unknown_keys}")

        # Validate workers
        workers = config.get('workers', {})
        if not isinstance(workers, dict):
            logging.error("'workers' must be a dictionary in Deephaven worker config")
            raise ValueError("'workers' must be a dictionary in Deephaven worker config")

        for worker_name, worker_config in workers.items():
            if not isinstance(worker_config, dict):
                logging.error(f"Worker config for {worker_name} must be a dictionary")
                raise ValueError(f"Worker config for {worker_name} must be a dictionary")

            # Check required fields
            missing_fields = [field for field in _REQUIRED_FIELDS if field not in worker_config]
            if missing_fields:
                logging.error(f"Missing required fields in worker config for {worker_name}: {missing_fields}")
                raise ValueError(f"Missing required fields in worker config for {worker_name}: {missing_fields}")

            # Check allowed fields and types
            for field, value in worker_config.items():
                if field not in _ALLOWED_WORKER_FIELDS:
                    logging.error(f"Unknown field '{field}' in worker config for {worker_name}")
                    raise ValueError(f"Unknown field '{field}' in worker config for {worker_name}")

                allowed_types = _ALLOWED_WORKER_FIELDS[field]
                if not isinstance(value, allowed_types):
                    logging.error(f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}")
                    raise ValueError(f"Field '{field}' in worker config for {worker_name} must be of type {allowed_types}")

        # Cache the validated config
        _CONFIG_CACHE = config
        logging.info("Deephaven worker configuration loaded and validated successfully")
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
    logging.info(f"CALL: resolve_worker_name called with worker_name={worker_name!r}")
    config = _load_config()

    if worker_name:
        return worker_name

    default_worker = config.get('default_worker')
    if not default_worker:
        logging.error("No worker name specified and no default_worker in config")
        raise RuntimeError("No worker name specified and no default_worker in config")

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
    logging.info(f"CALL: get_worker_config called with worker_name={worker_name!r}")
    config = _load_config()
    workers = config.get('workers', {})

    if not workers:
        logging.error("No workers defined in configuration")
        raise RuntimeError("No workers defined in configuration")

    worker_name = resolve_worker_name(worker_name)
    if worker_name not in workers:
        logging.error(f"Worker {worker_name} not found in configuration")
        raise RuntimeError(f"Worker {worker_name} not found in configuration")

    return workers[worker_name]

def deephaven_worker_names() -> list[str]:
    """
    Get a list of all configured Deephaven worker names from the loaded configuration.

    Returns:
        list[str]: List of worker names defined in the configuration.
    """
    logging.info("CALL: deephaven_worker_names called with no arguments")
    config = _load_config()
    workers = config.get('workers', {})
    return list(workers.keys())

def deephaven_default_worker() -> Optional[str]:
    """
    Get the name of the default Deephaven worker, as set in the configuration.

    Returns:
        str or None: The default worker name, or None if not set in the config.
    """
    logging.info("CALL: deephaven_default_worker called with no arguments")
    config = _load_config()
    return config.get('default_worker')
