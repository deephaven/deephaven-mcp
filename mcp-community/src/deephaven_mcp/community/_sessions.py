"""
Async session management for Deephaven workers.

This module provides asyncio-compatible, coroutine-safe creation, caching, and lifecycle management of Deephaven Session objects.
Sessions are configured using validated worker configuration from _config.py. Session reuse is automatic:
if a cached session is alive, it is returned; otherwise, a new session is created and cached.

Features:
    - Coroutine-safe session cache keyed by worker name (or default), protected by an asyncio.Lock.
    - Automatic session reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for cache clearing and atomic reloads.
    - Designed for use by other dhmcp modules and MCP tools.

Async Safety:
    All public functions are async and use asyncio.Lock for coroutine safety.
    The session cache is protected by _SESSION_CACHE_LOCK (asyncio.Lock).

Error Handling:
    - All certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Session creation failures are logged and raised to the caller.
    - Session closure failures are logged but do not prevent other operations.

Dependencies:
    - Requires aiofiles for async file I/O.
"""

from typing import Optional, Dict
from pydeephaven import Session
import logging
import asyncio
import time
import aiofiles
from deephaven_mcp import config

_LOGGER = logging.getLogger(__name__)


_SESSION_CACHE: Dict[str, Session] = {}
_SESSION_CACHE_LOCK = asyncio.Lock()
"""
_SESSION_CACHE (dict): Module-level cache for Deephaven sessions, keyed by worker name (or '__default__').
_SESSION_CACHE_LOCK (asyncio.Lock): Ensures coroutine-safe access to the session cache for all async operations.
"""


async def clear_all_sessions() -> None:
    """
    Atomically clear all Deephaven sessions and their cache (async).

    This function:
    1. Acquires the async session cache lock
    2. Iterates through all cached sessions
    3. Attempts to close each alive session (using await asyncio.to_thread)
    4. Clears the session cache

    Error Handling:
        - Any exceptions during session closure are logged but do not prevent other sessions from being closed
        - The cache is always cleared regardless of errors

    Async Safety:
        This function is coroutine-safe and uses asyncio.Lock to prevent race conditions.

    Returns:
        None
    """
    start_time = time.time()
    _LOGGER.info("Clearing Deephaven session cache...")
    _LOGGER.info(f"Current session cache size: {len(_SESSION_CACHE)}")

    async def _close_session_safely(worker_key, session):
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
        _LOGGER.debug(f"Attempting to close session for worker: {worker_key}")
        try:
            if hasattr(session, "is_alive") and session.is_alive:
                _LOGGER.info(f"Closing alive session for worker: {worker_key}")
                await asyncio.to_thread(session.close)
                _LOGGER.info(f"Successfully closed session for worker: {worker_key}")
            else:
                _LOGGER.debug(f"Session for worker {worker_key} is already closed")
        except Exception as exc:
            _LOGGER.error(f"Failed to close session for worker {worker_key}: {exc}")
            _LOGGER.debug(
                f"Session state after error: is_alive={hasattr(session, 'is_alive') and session.is_alive}",
                exc_info=True,
            )

    async with _SESSION_CACHE_LOCK:
        # Iterate over a copy to avoid mutation during iteration
        num_sessions = len(_SESSION_CACHE)
        _LOGGER.info(f"Processing {num_sessions} cached sessions...")
        for worker_key, session in list(_SESSION_CACHE.items()):
            await _close_session_safely(worker_key, session)
        _SESSION_CACHE.clear()
        _LOGGER.info(
            f"Session cache cleared. Processed {num_sessions} sessions in {time.time() - start_time:.2f}s"
        )


async def get_or_create_session(worker_name: Optional[str] = None) -> Session:
    """
    Async get-or-create for a Deephaven Session for the specified worker.

    This function implements a caching pattern where sessions are reused when possible.
    The process is as follows:
    1. Check if a cached session exists for the worker
    2. If cached session exists and is alive, return it
    3. Otherwise, create a new session using worker configuration (awaits async config functions)
    4. Loads any required certificate/key files using native async file I/O (aiofiles)
    5. Cache the new session and return it

    Args:
        worker_name (str, optional): Name of the Deephaven worker to use. If None,
            uses the default_worker from config.

    Returns:
        Session: A configured, live Deephaven Session instance for the worker.

    Raises:
        RuntimeError: If required configuration fields are missing or invalid.
        Exception: If session creation fails or certificates cannot be loaded.

    Async Safety:
        This function is coroutine-safe and uses asyncio.Lock to prevent race conditions.

    Note:
        This function handles TLS certificate loading and configuration for secure connections using aiofiles for async file I/O.
        All sensitive information (like auth tokens and private keys) is redacted from logs.
    """
    start_time = time.time()
    _LOGGER.info(f"Getting or creating session for worker: {worker_name}")
    resolved_worker = await config.resolve_worker_name(worker_name)
    _LOGGER.info(f"Resolved worker name: {worker_name} -> {resolved_worker}")
    _LOGGER.info(f"Session cache size: {len(_SESSION_CACHE)}")

    async with _SESSION_CACHE_LOCK:
        session = _SESSION_CACHE.get(resolved_worker)
        if session is not None:
            try:
                if session.is_alive:
                    _LOGGER.info(
                        f"Found and returning cached session for worker: {resolved_worker}"
                    )
                    _LOGGER.debug(
                        f"Session state: host={session.host}, port={session.port}, auth_type={session.auth_type}"
                    )
                    return session
                else:
                    _LOGGER.info(
                        f"Cached session for worker '{resolved_worker}' is not alive. Recreating."
                    )
            except Exception as e:
                _LOGGER.warning(
                    f"Error checking session liveness for worker '{resolved_worker}': {e}. Recreating session."
                )

        # At this point, we need to create a new session and update the cache
        _LOGGER.info(f"Creating new session for worker: {resolved_worker}")
        worker_cfg = await config.get_worker_config(resolved_worker)
        host = worker_cfg.get("host", None)
        port = worker_cfg.get("port", None)
        auth_type = worker_cfg.get("auth_type", "Anonymous")
        auth_token = worker_cfg.get("auth_token", "")
        never_timeout = worker_cfg.get("never_timeout", False)
        session_type = worker_cfg.get("session_type", "python")
        use_tls = worker_cfg.get("use_tls", False)
        tls_root_certs = worker_cfg.get("tls_root_certs", None)
        client_cert_chain = worker_cfg.get("client_cert_chain", None)
        client_private_key = worker_cfg.get("client_private_key", None)

        # Log configuration details (redacting sensitive info)
        log_cfg = dict(worker_cfg)

        if "auth_token" in log_cfg:
            log_cfg["auth_token"] = "REDACTED"

        _LOGGER.info(f"Session configuration: {log_cfg}")

        async def _load_bytes(path):
            _LOGGER.info(f"Loading certificate/key file: {path}")
            if path is None:
                return None
            try:
                async with aiofiles.open(path, "rb") as f:
                    return await f.read()
            except Exception as e:
                _LOGGER.error(f"Failed to load certificate/key file: {path}: {e}")
                raise

        if tls_root_certs:
            _LOGGER.info(f"Loading TLS root certs from: {worker_cfg.get('tls_root_certs')}")
            tls_root_certs = await _load_bytes(tls_root_certs)
            _LOGGER.info("Loaded TLS root certs successfully.")
        else:
            _LOGGER.debug("No TLS root certs provided for session.")

        if client_cert_chain:
            _LOGGER.info(
                f"Loading client cert chain from: {worker_cfg.get('client_cert_chain')}"
            )
            client_cert_chain = await _load_bytes(client_cert_chain)
            _LOGGER.info("Loaded client cert chain successfully.")
        else:
            _LOGGER.debug("No client cert chain provided for session.")

        if client_private_key:
            _LOGGER.info(
                f"Loading client private key from: {worker_cfg.get('client_private_key')}"
            )
            client_private_key = await _load_bytes(client_private_key)
            _LOGGER.info("Loaded client private key successfully.")
        else:
            _LOGGER.debug("No client private key provided for session.")

        # Redact sensitive info for logging
        log_cfg = {
            "worker_name": resolved_worker,
            "host": host,
            "port": port,
            "auth_type": auth_type,
            "auth_token": "REDACTED" if auth_token else None,
            "never_timeout": never_timeout,
            "session_type": session_type,
            "use_tls": use_tls,
            "tls_root_certs": "REDACTED" if tls_root_certs else None,
            "client_cert_chain": "REDACTED" if client_cert_chain else None,
            "client_private_key": "REDACTED" if client_private_key else None,
        }

        _LOGGER.info(
            f"Creating new Deephaven Session with config: {log_cfg} (worker cache key: {resolved_worker})"
        )

        session = await asyncio.to_thread(
            Session,
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

        _LOGGER.info(
            f"Successfully created session for worker: {resolved_worker}, adding to cache."
        )
        _SESSION_CACHE[resolved_worker] = session
        _LOGGER.info(
            f"Session cached for worker: {resolved_worker}. Returning session."
        )
        return session
