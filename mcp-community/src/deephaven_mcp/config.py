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
