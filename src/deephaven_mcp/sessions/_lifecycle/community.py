"""
Async lifecycle operations for Deephaven Community (Core) sessions.

Provides coroutine-compatible helpers for creating, configuring, and instantiating Deephaven Community (Core) sessions.
Intended for use by session managers and orchestration logic for Deephaven Community (Core) clusters. All functions are async and raise
SessionCreationError on failure.
"""

import asyncio
import logging
import os
from typing import Any

from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp.config._community_session import redact_community_session_config
from deephaven_mcp.io import load_bytes
from deephaven_mcp.sessions._errors import SessionCreationError

_LOGGER = logging.getLogger(__name__)


async def _create_session_impl(**kwargs: Any) -> Session:
    """
    Asynchronously create and return a new Deephaven Community (Core) Session instance in a background thread.

    Sensitive fields in the config are redacted before logging. If session creation fails,
    a SessionCreationError is raised with details. This function is specific to Community (Core) sessions.

    Args:
        **kwargs: Keyword arguments passed to pydeephaven.Session for Community (Core) connection.

    Returns:
        Session: A new Deephaven Community (Core) Session instance.

    Raises:
        SessionCreationError: If session creation fails for any reason.
    """
    log_kwargs = redact_community_session_config(kwargs)
    _LOGGER.info(
        f"[Community] Creating new Deephaven Community (Core) Session with config: {log_kwargs}"
    )
    try:
        session = await asyncio.to_thread(Session, **kwargs)
    except Exception as e:
        _LOGGER.warning(
            f"[Community] Failed to create Deephaven Community (Core) Session with config: {log_kwargs}: {e}"
        )
        raise SessionCreationError(
            f"Failed to create Deephaven Community (Core) Session with config: {log_kwargs}: {e}"
        ) from e
    _LOGGER.info(
        f"[Community] Successfully created Deephaven Community (Core) Session: {session}"
    )
    return session


async def _get_session_parameters(worker_cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Prepare and return a configuration dictionary for Deephaven Community (Core) Session creation.

    Loads TLS certificate and key files asynchronously if specified in the community session config,
    redacts sensitive fields for logging, and returns a dictionary of parameters ready to be passed to pydeephaven.Session.

    If 'auth_token_env_var' is specified in the configuration, attempts to source the authentication token from the given environment variable.
    If the environment variable is not set, falls back to an empty authentication token and logs a warning. Otherwise, uses the direct 'auth_token' value or defaults to empty.

    Args:
        worker_cfg (dict): The worker's community session configuration.

    Returns:
        dict: Dictionary of ready-to-use session parameters for Community (Core) session creation.
    """
    log_cfg = redact_community_session_config(worker_cfg)
    _LOGGER.info(f"[Community] Community session configuration: {log_cfg}")
    host = worker_cfg.get("host", None)
    port = worker_cfg.get("port", None)
    auth_type = worker_cfg.get("auth_type", "Anonymous")
    auth_token = worker_cfg.get("auth_token")
    auth_token_env_var = worker_cfg.get("auth_token_env_var")
    if auth_token_env_var:
        _LOGGER.info(
            f"[Community] Attempting to read auth token from environment variable: {auth_token_env_var}"
        )
        token_from_env = os.getenv(auth_token_env_var)
        if token_from_env is not None:
            auth_token = token_from_env
            _LOGGER.info(
                f"[Community] Successfully read auth token from environment variable {auth_token_env_var}."
            )
        else:
            auth_token = ""
            _LOGGER.warning(
                f"[Community] Environment variable {auth_token_env_var} specified for auth_token but not found. Using empty token."
            )
    elif auth_token is None:
        auth_token = ""
    never_timeout = worker_cfg.get("never_timeout", False)
    session_type = worker_cfg.get("session_type", "python")
    use_tls = worker_cfg.get("use_tls", False)
    tls_root_certs = worker_cfg.get("tls_root_certs", None)
    client_cert_chain = worker_cfg.get("client_cert_chain", None)
    client_private_key = worker_cfg.get("client_private_key", None)
    if tls_root_certs:
        _LOGGER.info(
            f"[Community] Loading TLS root certs from: {worker_cfg.get('tls_root_certs')}"
        )
        tls_root_certs = await load_bytes(tls_root_certs)
        _LOGGER.info("[Community] Loaded TLS root certs successfully.")
    else:
        _LOGGER.debug("[Community] No TLS root certs provided for community session.")
    if client_cert_chain:
        _LOGGER.info(
            f"[Community] Loading client cert chain from: {worker_cfg.get('client_cert_chain')}"
        )
        client_cert_chain = await load_bytes(client_cert_chain)
        _LOGGER.info("[Community] Loaded client cert chain successfully.")
    else:
        _LOGGER.debug(
            "[Community] No client cert chain provided for community session."
        )
    if client_private_key:
        _LOGGER.info(
            f"[Community] Loading client private key from: {worker_cfg.get('client_private_key')}"
        )
        client_private_key = await load_bytes(client_private_key)
        _LOGGER.info("[Community] Loaded client private key successfully.")
    else:
        _LOGGER.debug(
            "[Community] No client private key provided for community session."
        )
    session_config = {
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
    log_cfg = redact_community_session_config(session_config)
    _LOGGER.info(
        f"[Community] Prepared Deephaven Community (Core) Session config: {log_cfg}"
    )
    return session_config


async def create_session(worker_cfg: dict[str, Any]) -> Session:
    """
    Asynchronously create and return a new Deephaven Community (Core) Session instance.

    Args:
        worker_cfg (dict): The worker's community session configuration.

    Returns:
        Session: A new Deephaven Community (Core) Session instance.

    Raises:
        SessionCreationError: If session creation fails.
    """
    session_params = await _get_session_parameters(worker_cfg)
    log_cfg = redact_community_session_config(session_params)
    _LOGGER.info(
        f"[Community] Creating new Deephaven Community (Core) Session with config: {log_cfg}"
    )
    session = await _create_session_impl(**session_params)
    _LOGGER.info(
        f"[Community] Successfully created Deephaven Community (Core) session for config: {log_cfg}"
    )
    return session

# TODO: is this used?  can it be deleted?
async def create_session_for_worker(
    config_manager: config.ConfigManager, session_name: str
) -> Session:
    """
    Asynchronously create and return a new Deephaven Community (Core) Session for the given worker/session name.

    This helper:
      - Looks up the worker configuration from the config manager (for Community sessions).
      - Gathers and logs (with redaction) the community session parameters.
      - Creates and returns a new Deephaven Community (Core) Session instance.

    Does not handle session caching; intended to be called by session managers for Community clusters.

    Args:
        config_manager (ConfigManager): The config manager to use for community config lookup.
        session_name (str): The name of the worker/session.

    Returns:
        Session: A new Deephaven Community (Core) Session instance.

    Raises:
        SessionCreationError: If session creation fails.
    """
    _LOGGER.info(
        f"[Community] Creating new Deephaven Community (Core) session: {session_name}"
    )
    full_config = await config_manager.get_config()
    worker_cfg = config.get_config_section(
        full_config, ["community", "sessions", session_name]
    )
    return await create_session(worker_cfg)
