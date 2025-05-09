"""
Async session management for Deephaven workers.

This module provides asyncio-compatible, coroutine-safe creation, caching, and lifecycle management of Deephaven Session objects.
Sessions are configured using validated worker configuration from _config.py. Session reuse is automatic:
if a cached session is alive, it is returned; otherwise, a new session is created and cached.

Features:
    - Coroutine-safe session cache keyed by worker name, protected by an asyncio.Lock.
    - Automatic session reuse, liveness checking, and resource cleanup.
    - Native async file I/O for secure loading of certificate files (TLS, client certs/keys) using aiofiles.
    - Tools for cache clearing and atomic reloads.
    - Designed for use by other MCP modules and MCP tools.

Async Safety:
    All public functions are async and use an instance-level asyncio.Lock (self._lock) for coroutine safety.
    Each SessionManager instance encapsulates its own session cache and lock.

Error Handling:
    - All certificate loading operations are wrapped in try-except blocks and use aiofiles for async file I/O.
    - Session creation failures are logged and raised to the caller.
    - Session closure failures are logged but do not prevent other operations.

Dependencies:
    - Requires aiofiles for async file I/O.
"""

from typing import Optional, Dict, Any, Type
from types import TracebackType
from pydeephaven import Session
import logging
import asyncio
import time
import aiofiles
from deephaven_mcp import config

_LOGGER = logging.getLogger(__name__)


class SessionCreationError(Exception):
    """Raised when a Deephaven Session cannot be created."""
    pass


class SessionManager:
    """
    Manages Deephaven Session objects, including creation, caching, and lifecycle.
 
    Usage:
        - Instantiate with a ConfigManager instance:
            cfg_mgr = ...  # Your ConfigManager
            mgr = SessionManager(cfg_mgr)
        - Use in async context for deterministic cleanup:
            async with SessionManager(cfg_mgr) as mgr:
                ...
            # Sessions are automatically cleared on exit

    Notes:
        - Each SessionManager instance is fully isolated and must be provided a ConfigManager.
    """
    def __init__(self, config_manager: config.ConfigManager):
        """
        Initialize a new SessionManager instance.

        Args:
            config_manager (ConfigManager): The configuration manager to use for worker config lookup.

        This constructor sets up the internal session cache (mapping worker names to Session objects)
        and an asyncio.Lock to ensure coroutine safety for all session management operations.

        Example:
            cfg_mgr = ...  # Your ConfigManager instance
            mgr = SessionManager(cfg_mgr)
        """
        self._cache: Dict[str, Session] = {}
        self._lock = asyncio.Lock()
        self._config_manager = config_manager

    async def __aenter__(self) -> "SessionManager":
        """
        Enter the async context manager for SessionManager.

        Returns:
            SessionManager: The current instance (self).

        Usage:
            async with SessionManager() as mgr:
                # Use mgr to create, cache, and reuse sessions
                ...
            # On exit, all sessions are automatically cleaned up.
        """
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType]
    ) -> None:
        """
        Exit the async context manager for SessionManager, ensuring resource cleanup.

        On exit, all cached sessions are cleared via clear_all_sessions().
        This guarantees no lingering sessions after the context block, which is useful for tests,
        scripts, and advanced workflows that require deterministic resource management.

        Args:
            exc_type (type): Exception type if raised in the context, else None.
            exc (Exception): Exception instance if raised, else None.
            tb (traceback): Traceback if exception was raised, else None.

        Example:
            async with SessionManager() as mgr:
                ...
            # Sessions are cleaned up here
        """
        await self.clear_all_sessions()

    async def clear_all_sessions(self) -> None:
        """
        Atomically clear all Deephaven sessions and their cache (async).

        This method:
        1. Acquires the async session cache lock for coroutine safety.
        2. Iterates through all cached sessions.
        3. Attempts to close each alive session (using await asyncio.to_thread).
        4. Clears the session cache after all sessions are processed.

        Args:
            None

        Returns:
            None

        Error Handling:
            - Any exceptions during session closure are logged but do not prevent other sessions from being closed.
            - The cache is always cleared regardless of errors.

        Async Safety:
            This method is coroutine-safe and uses an asyncio.Lock to prevent race conditions.

        Notes:
            Intended for both production and test cleanup. Should be preferred over forcibly clearing the cache to ensure all resources are released.
        """
        start_time = time.time()
        _LOGGER.info("Clearing Deephaven session cache...")
        _LOGGER.info(f"Current session cache size: {len(self._cache)}")

        async with self._lock:
            num_sessions = len(self._cache)
            _LOGGER.info(f"Processing {num_sessions} cached sessions...")
            for worker_key, session in list(self._cache.items()):
                await self._close_session_safely(worker_key, session)
            self._cache.clear()
            _LOGGER.info(
                f"Session cache cleared. Processed {num_sessions} sessions in {time.time() - start_time:.2f}s"
            )

    @staticmethod
    async def _close_session_safely(worker_key: str, session: Session) -> None:
        """
        Attempt to safely close a Deephaven session if it is alive.

        Used internally by clear_all_sessions for resource cleanup. If the session is alive, it is closed
        in a background thread using asyncio.to_thread. Any exceptions during closure are logged and do not prevent cleanup of other sessions.

        Args:
            worker_key (str): The cache key for the worker (used for logging).
            session (Session): The Deephaven Session object to close.

        Returns:
            None

        Error Handling:
            - Exceptions during session closure are logged but do not propagate.
            - Session state after error is logged for debugging.

        Example:
            await SessionManager._close_session_safely('worker1', session)
        """
        try:
            if session.is_alive:
                _LOGGER.info(f"Closing alive session for worker: {worker_key}")
                await asyncio.to_thread(session.close)
                _LOGGER.info(f"Successfully closed session for worker: {worker_key}")
            else:
                _LOGGER.debug(f"Session for worker '{worker_key}' is already closed")
        except Exception as e:
            _LOGGER.error(f"Failed to close session for worker '{worker_key}': {e}")
            _LOGGER.debug(
                f"Session state after error: is_alive={session.is_alive}",
                exc_info=True,
            )

    def _redact_sensitive_session_fields(self, config: Dict[str, Any], redact_binary_values: bool = True) -> Dict[str, Any]:
        """
        Return a copy of a session config dictionary with sensitive values redacted for safe logging.

        This method is used to sanitize session configuration dictionaries before logging, to prevent accidental leakage of secrets or binary credentials. It redacts authentication tokens and, by default, any sensitive fields that are binary data.

        Args:
            config (dict): The configuration dictionary to redact.
            redact_binary_values (bool, optional):
                If True (default), redact sensitive fields if their values are binary (bytes/bytearray); file paths are preserved if they are strings.
                If False, only redact always-sensitive values (e.g., auth_token); file paths are shown even if they point to secrets.

        Returns:
            dict: A shallow copy of the input config with sensitive values redacted as appropriate.

        Redacted Fields:
            - 'auth_token' (always redacted if present and non-empty)
            - 'tls_root_certs', 'client_cert_chain', 'client_private_key':
                - Redacted if value is bytes/bytearray and redact_binary_values is True
                - File paths (str) are preserved unless redact_binary_values is explicitly set to True and you want to hide them

        Security Rationale:
            Redacting these fields prevents accidental leakage of secrets and binary credentials in logs.

        Example:
            >>> cfg = {'auth_token': 'abc', 'client_private_key': b'secret'}
            >>> mgr._redact_sensitive_session_fields(cfg)
            {'auth_token': 'REDACTED', 'client_private_key': 'REDACTED'}
        """
        redacted = dict(config)
        sensitive_keys = ["auth_token", "tls_root_certs", "client_cert_chain", "client_private_key"]
        for key in sensitive_keys:
            if key in redacted and redacted[key]:
                # Redact if binary (bytes) or if always sensitive (auth_token)
                if key == "auth_token":
                    redacted[key] = "REDACTED"
                elif redact_binary_values and isinstance(redacted[key], (bytes, bytearray)):
                    redacted[key] = "REDACTED"
        return redacted

    async def _create_session(self, **kwargs: Any) -> Session:
        """
        Create and return a new Deephaven Session instance in a background thread.

        This helper is separated for testability and can be patched or mocked in unit tests.
        All session configuration parameters should be passed as keyword arguments. Sensitive values are redacted in logs.

        Args:
            **kwargs: All configuration options for pydeephaven.Session (host, port, auth_token, etc.)

        Returns:
            Session: A configured Deephaven Session instance.

        Error Handling:
            - Any exceptions during session creation will raise SessionCreationError with details.

        Raises:
            SessionCreationError: If the session could not be created for any reason.

        Example:
            session = await mgr._create_session(host='localhost', port=10000)
        """
        log_kwargs = self._redact_sensitive_session_fields(kwargs)
        _LOGGER.info(f"Creating new Deephaven Session with config: {log_kwargs}")

        try:
            session = await asyncio.to_thread(Session, **kwargs)
        except Exception as e:
            _LOGGER.error(f"Failed to create Deephaven Session with config: {log_kwargs}: {e}", exc_info=True)
            raise SessionCreationError(f"Failed to create Deephaven Session with config: {log_kwargs}: {e}") from e

        _LOGGER.info(f"Successfully created Deephaven Session: {session}")
        return session

    async def _get_session_parameters(self, worker_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare and return the configuration dictionary for Deephaven Session creation.

        This method loads certificate/key files as needed (using async I/O), redacts sensitive info for logging,
        and returns a dictionary of parameters ready to be passed to pydeephaven.Session. It is always used before
        session creation and ensures all configuration is normalized and safe for logging.

        Args:
            worker_cfg (dict): The worker configuration dictionary. May include:
                - host (str): Deephaven server host
                - port (int): Deephaven server port
                - auth_type (str): Authentication type
                - auth_token (str): Authentication token
                - never_timeout (bool): Disable session timeout
                - session_type (str): Session type (e.g., 'python')
                - use_tls (bool): Use TLS/SSL
                - tls_root_certs (str/bytes): Path or bytes for TLS root certs
                - client_cert_chain (str/bytes): Path or bytes for client cert chain
                - client_private_key (str/bytes): Path or bytes for client private key
                - worker_name (str): Optional, for logging context

        Returns:
            dict: Dictionary of parameters for Session creation, normalized and ready for pydeephaven.Session.

        Error Handling:
            - Any exceptions during file loading will propagate to the caller.
            - No required fields are enforced at this layer; missing fields are passed as None or omitted.

        Example:
            params = await mgr._get_session_parameters({'host': 'localhost', 'port': 10000})
        """
        log_cfg = self._redact_sensitive_session_fields(worker_cfg)
        _LOGGER.info(f"Session configuration: {log_cfg}")

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

        rst = {
            "host": host,
            "port": port,
            "auth_type": auth_type,
            "auth_token": auth_token,
            "never_timeout": never_timeout,
            "session_type": session_type,
            "use_tls": use_tls,
            "tls_root_certs": tls_root_certs,
            "client_cert_chain": client_cert_chain,
            "client_private_key": client_private_key,
        }

        # Log final prepared config (file paths may have been replaced by binary values)
        log_cfg = self._redact_sensitive_session_fields(rst)
        _LOGGER.info(f"Prepared Deephaven Session config: {log_cfg}")

        return rst

    async def get_or_create_session(self, worker_name: str) -> Session:
        """
        Retrieve a cached Deephaven session for the specified worker, or create and cache a new one if needed.

        This is the main entry point for obtaining a Deephaven Session for a given worker. Sessions are reused if possible;
        if the cached session is not alive, a new one is created and cached. All session creation and configuration is coroutine-safe.

        Args:
            worker_name (str): The name of the worker to retrieve a session for. This argument is required.

        Returns:
            Session: An alive Deephaven Session instance for the worker.

        Error Handling:
            - Any exceptions during session creation will raise SessionCreationError with details.
            - Any exceptions during config loading are logged and propagated to the caller.
            - If the cached session is not alive or liveness check fails, a new session is created.

        Raises:
            SessionCreationError: If the session could not be created for any reason.
            FileNotFoundError: If configuration or certificate files are missing.
            ValueError: If configuration is invalid.
            OSError: If there are file I/O errors when loading certificates/keys.
            RuntimeError: If configuration loading fails for other reasons.

        Usage:
            This method is coroutine-safe and can be used concurrently in async workflows.

        Example:
            session = await mgr.get_or_create_session('worker1')
        """
        start_time = time.time()
        _LOGGER.info(f"Getting or creating session for worker: {worker_name}")
        _LOGGER.info(f"Session cache size: {len(self._cache)}")

        async with self._lock:
            session = self._cache.get(worker_name)
            if session is not None:
                try:
                    if session.is_alive:
                        _LOGGER.info(
                            f"Found and returning cached session for worker: {worker_name}"
                        )
                        return session
                    else:
                        _LOGGER.info(
                            f"Cached session for worker '{worker_name}' is not alive. Recreating."
                        )
                except Exception as e:
                    _LOGGER.warning(
                        f"Error checking session liveness for worker '{worker_name}': {e}. Recreating session."
                    )

            # At this point, we need to create a new session and update the cache
            _LOGGER.info(f"Creating new session for worker: {worker_name}")
            worker_cfg = await self._config_manager.get_worker_config(worker_name)
            session_params = await self._get_session_parameters(worker_cfg)

            # Redact sensitive info for logging
            log_cfg = self._redact_sensitive_session_fields(session_params)
            log_cfg["worker_name"] = worker_name
            _LOGGER.info(f"Creating new Deephaven Session with config: (worker cache key: {worker_name}) {log_cfg}")

            session = await self._create_session(**session_params)

            _LOGGER.info(
                f"Successfully created session for worker: {worker_name}, adding to cache."
            )
            self._cache[worker_name] = session
            _LOGGER.info(
                f"Session cached for worker: {worker_name}. Returning session."
            )
            return session


async def _load_bytes(path: Optional[str]) -> Optional[bytes]:
    """
    Asynchronously load the contents of a binary file.

    This helper is used to read certificate and private key files for secure Deephaven session creation.
    It is designed to be coroutine-safe and leverages aiofiles for non-blocking I/O.

    Args:
        path (Optional[str]): Path to the file to load. If None, returns None.

    Returns:
        Optional[bytes]: The contents of the file as bytes, or None if the path is None.

    Raises:
        Exception: Propagates any exceptions encountered during file I/O (e.g., file not found, permission denied).

    Side Effects:
        - Logs the file path being loaded (info level).
        - Logs and re-raises any exceptions encountered (error level).

    Example:
        >>> cert_bytes = await _load_bytes('/path/to/cert.pem')
        >>> if cert_bytes is not None:
        ...     # Use cert_bytes for TLS configuration
    """
    _LOGGER.info(f"Loading binary file: {path}")
    if path is None:
        return None
    try:
        async with aiofiles.open(path, "rb") as f:
            return await f.read()
    except Exception as e:
        _LOGGER.error(f"Failed to load binary file: {path}: {e}")
        raise
