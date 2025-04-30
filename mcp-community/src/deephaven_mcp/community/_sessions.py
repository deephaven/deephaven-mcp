"""
Session management for Deephaven workers.

This module provides thread-safe creation, caching, and lifecycle management of Deephaven Session objects.
Sessions are configured using validated worker configuration from _config.py. Session reuse is automatic:
if a cached session is alive, it is returned; otherwise, a new session is created and cached.

Features:
    - Thread-safe, reentrant session cache keyed by worker name (or default).
    - Automatic session reuse, liveness checking, and resource cleanup.
    - Secure loading of certificate files for TLS connections.
    - Tools for cache clearing and atomic reloads.
    - Designed for use by other dhmcp modules and MCP tools.

Thread Safety:
    All public functions are thread-safe and use appropriate locking mechanisms.
    The session cache is protected by a reentrant lock (_SESSION_CACHE_LOCK).

Error Handling:
    - All certificate loading operations are wrapped in try-except blocks.
    - Session creation failures are logged and raised to the caller.
    - Session closure failures are logged but do not prevent other operations.
"""

from typing import Optional
from pydeephaven import Session
import logging
import threading
from deephaven_mcp import config


_SESSION_CACHE = {}
_SESSION_CACHE_LOCK = threading.RLock()
"""
_SESSION_CACHE (dict): Module-level cache for Deephaven sessions, keyed by worker name (or '__default__').
_SESSION_CACHE_LOCK (threading.RLock): Ensures thread-safe, reentrant access to the session cache.
"""

def clear_all_sessions() -> None:
    """
    Atomically clear all Deephaven sessions and their cache.

    This function:
    1. Acquires the session cache lock
    2. Iterates through all cached sessions
    3. Attempts to close each alive session
    4. Clears the session cache

    Error Handling:
        - Any exceptions during session closure are logged but do not prevent other sessions from being closed
        - The cache is always cleared regardless of errors

    Thread Safety:
        This function is thread-safe and uses the session cache lock to prevent race conditions.

    Returns:
        None
    """
    logging.info("CALL: clear_all_sessions called with no arguments")
    logging.info("Clearing Deephaven session cache...")
    
    def _close_session_safely(worker_key, session):
        """
        Safely close a Deephaven session if it is alive.

        This helper function attempts to close a session while handling any potential errors.
        It is used internally by clear_all_sessions to clean up sessions.

        Args:
            worker_key (str): The cache key for the worker (used for logging)
            session (Session): The Deephaven session instance to close

        Error Handling:
            - Any exceptions during session closure are caught and logged
            - The function continues execution regardless of errors
        """
        logging.info(f"CALL: _close_session_safely called with worker_key={worker_key!r}, session={session!r}")
        try:
            if hasattr(session, "is_alive") and session.is_alive:
                session.close()
                logging.info(f"Closed alive Deephaven session for worker: {worker_key}")
        except Exception as exc:
            logging.warning(f"Failed to close session for worker {worker_key}: {exc}")

    with _SESSION_CACHE_LOCK:
        # Iterate over a copy to avoid mutation during iteration
        for worker_key, session in list(_SESSION_CACHE.items()):
            _close_session_if_alive(worker_key, session)
        _SESSION_CACHE.clear()
        logging.info("Session cache cleared.")

def get_or_create_session(worker_name: Optional[str] = None) -> Session:
    """
    Get or create a Deephaven Session for the specified worker.

    This function implements a caching pattern where sessions are reused when possible.
    The process is as follows:
    1. Check if a cached session exists for the worker
    2. If cached session exists and is alive, return it
    3. Otherwise, create a new session using worker configuration
    4. Cache the new session and return it

    Args:
        worker_name (str, optional): Name of the Deephaven worker to use. If None,
            uses the default_worker from config.

    Returns:
        Session: A configured, live Deephaven Session instance for the worker.

    Raises:
        RuntimeError: If required configuration fields are missing or invalid.
        Exception: If session creation fails or certificates cannot be loaded.

    Thread Safety:
        This function is thread-safe and uses the session cache lock to prevent race conditions.
        It is also reentrant, allowing nested calls to safely share the same lock.

    Note:
        This function handles TLS certificate loading and configuration for secure connections.
        All sensitive information (like auth tokens and private keys) is redacted from logs.
    """
    logging.info(f"CALL: get_or_create_session called with worker_name={worker_name!r}")
    resolved_worker = config.resolve_worker_name(worker_name)
    logging.info(f"Resolving worker name: {worker_name} -> {resolved_worker}")

    # First, check and create the session in a single atomic lock block
    with _SESSION_CACHE_LOCK:
        session = _SESSION_CACHE.get(resolved_worker)
        if session is not None:
            try:
                if session.is_alive:
                    logging.info(f"Returning cached Deephaven session for worker: {resolved_worker}")
                    return session
                else:
                    logging.info(f"Cached Deephaven session for worker '{resolved_worker}' is not alive. Recreating.")
            except Exception as e:
                logging.warning(f"Error checking session liveness for worker '{resolved_worker}': {e}. Recreating session.")

        # At this point, we need to create a new session and update the cache
        cfg = config.get_worker_config(worker_name)
        host = cfg.get("host", None)
        port = cfg.get("port", None)
        auth_type = cfg.get("auth_type", "Anonymous")
        auth_token = cfg.get("auth_token", "")
        never_timeout = cfg.get("never_timeout", False)
        session_type = cfg.get("session_type", "python")
        use_tls = cfg.get("use_tls", False)
        tls_root_certs = cfg.get("tls_root_certs", None)
        client_cert_chain = cfg.get("client_cert_chain", None)
        client_private_key = cfg.get("client_private_key", None)

        # Load certificate files as bytes if provided as file paths, with logging (outside lock if slow)
        def _load_bytes(path):
            """
            Helper to load bytes from a file path, or return None if path is None.
            """
            logging.info(f"CALL: _load_bytes called with path={path!r}")
            if path is None:
                return None
            try:
                with open(path, "rb") as f:
                    return f.read()
            except Exception as e:
                logging.error(f"Failed to load certificate/key file: {path}: {e}")
                raise

        if tls_root_certs:
            logging.info(f"Loading TLS root certs from: {cfg.get('tls_root_certs')}")
            tls_root_certs = _load_bytes(tls_root_certs)
            logging.info("Loaded TLS root certs successfully.")
        else:
            logging.info("No TLS root certs provided for session.")
 
        if client_cert_chain:
            logging.info(f"Loading client cert chain from: {cfg.get('client_cert_chain')}")
            client_cert_chain = _load_bytes(client_cert_chain)
            logging.info("Loaded client cert chain successfully.")
        else:
            logging.info("No client cert chain provided for session.")
 
        if client_private_key:
            logging.info(f"Loading client private key from: {cfg.get('client_private_key')}")
            client_private_key = _load_bytes(client_private_key)
            logging.info("Loaded client private key successfully.")
        else:
            logging.info("No client private key provided for session.")

        # Redact sensitive info for logging
        log_cfg = dict(cfg)
        if "auth_token" in log_cfg:
            log_cfg["auth_token"] = "<redacted>"
 
        if "client_private_key" in log_cfg:
            log_cfg["client_private_key"] = "<redacted>"
 
        if "client_cert_chain" in log_cfg:
            log_cfg["client_cert_chain"] = "<redacted>"
 
        if "tls_root_certs" in log_cfg:
            log_cfg["tls_root_certs"] = "<redacted>"
 
        logging.info(f"Creating Deephaven Session with config: {log_cfg} (worker cache key: {resolved_worker})")

        session = Session(
            host=host,
            port=port,
            auth_type=auth_type,
            auth_token=auth_token,
            never_timeout=never_timeout,
            session_type=session_type,
            use_tls=use_tls,
            tls_root_certs=tls_root_certs,
            client_cert_chain=client_cert_chain,
            client_private_key=client_private_key,
        )
        logging.info(f"Session created for worker '{resolved_worker}', adding to cache.")
        _SESSION_CACHE[resolved_worker] = session
        logging.info(f"Session cached for worker '{resolved_worker}'. Returning session.")
        return session
