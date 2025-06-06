"""
Validation logic for the 'enterprise_sessions' section of the Deephaven MCP configuration.
"""
import logging
from typing import Any

from . import McpConfigurationError

_LOGGER = logging.getLogger(__name__)

class EnterpriseSessionConfigurationError(McpConfigurationError):
    """Custom exception for errors in enterprise session configuration."""
    pass

def validate_enterprise_sessions_config(enterprise_sessions_map: Any | None) -> None:
    """
    Validates the 'enterprise_sessions' part of the MCP configuration.

    The 'enterprise_sessions' key in the config should map to a dictionary.
    Each key in this dictionary is a session name, and its value is the
    configuration object for that enterprise session.

    Args:
        enterprise_sessions_map: The value associated with the 'enterprise_sessions'
                                 key in the configuration. Expected to be a dict or None.

    Raises:
        EnterpriseSessionConfigurationError: If validation fails.
    """
    _LOGGER.debug(
        f"Validating enterprise_sessions configuration: {enterprise_sessions_map}"
    )

    if enterprise_sessions_map is None:
        _LOGGER.debug("'enterprise_sessions' key is not present, which is valid.")
        return

    if not isinstance(enterprise_sessions_map, dict):
        msg = f"'enterprise_sessions' must be a dictionary, but got type {type(enterprise_sessions_map).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSessionConfigurationError(msg)

    if not enterprise_sessions_map:
        _LOGGER.debug(
            "'enterprise_sessions' is an empty dictionary, which is valid (no enterprise sessions configured)."
        )
        return

    # Iterate over and validate each configured enterprise session
    for session_name, session_config in enterprise_sessions_map.items():
        if not isinstance(session_name, str):
            msg = f"Enterprise session name must be a string, but got {type(session_name).__name__}: {session_name!r}."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        _validate_single_enterprise_session(session_name, session_config)

    _LOGGER.info(
        f"Validation for 'enterprise_sessions' passed. Found {len(enterprise_sessions_map)} enterprise session(s)."
    )

def _validate_single_enterprise_session(session_name: str, config: Any) -> None:
    """Validates a single enterprise session's configuration object."""
    _LOGGER.debug(f"Validating enterprise session '{session_name}': {config}")

    if not isinstance(config, dict):
        msg = f"Configuration for enterprise session '{session_name}' must be a dictionary, but got {type(config).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSessionConfigurationError(msg)

    # Validate presence and type of fundamental required keys: connection_json_url and auth_type
    if "connection_json_url" not in config:
        msg = f"Enterprise session '{session_name}' is missing required key 'connection_json_url'."
        _LOGGER.error(msg)
        raise EnterpriseSessionConfigurationError(msg)
    if not isinstance(config["connection_json_url"], str):
        msg = f"'connection_json_url' for enterprise session '{session_name}' must be a string, but got {type(config['connection_json_url']).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSessionConfigurationError(msg)

    if "auth_type" not in config:
        msg = f"Enterprise session '{session_name}' is missing required key 'auth_type'."
        _LOGGER.error(msg)
        raise EnterpriseSessionConfigurationError(msg)

    auth_type = config["auth_type"]
    allowed_auth_types = {"api_key", "password", "private_key", "none"}
    if not isinstance(auth_type, str) or auth_type not in allowed_auth_types:
        msg = f"'auth_type' for enterprise session '{session_name}' must be one of {allowed_auth_types}, but got '{auth_type}'."
        _LOGGER.error(msg)
        raise EnterpriseSessionConfigurationError(msg)

    # Define base known keys and add auth-specific keys dynamically
    known_session_keys = {
        "connection_json_url", "auth_type"
    }
    if auth_type == "api_key":
        known_session_keys.update(["api_key", "api_key_env_var"])
    elif auth_type == "password":
        known_session_keys.update(["username", "password", "password_env_var"])
    elif auth_type == "private_key":
        known_session_keys.update(["private_key_path"])

    # Check for unknown keys first. This must happen after 'auth_type' is validated and 'known_session_keys' is built.
    for key_in_config in config:
        if key_in_config not in known_session_keys:
            _LOGGER.warning(f"Unknown key '{key_in_config}' in enterprise session '{session_name}' configuration (auth_type: {auth_type}).")

    # Now, validate conditional fields based on auth_type using the already identified known keys
    if auth_type == "api_key":
        if not ("api_key" in config or "api_key_env_var" in config):
            msg = f"Enterprise session '{session_name}' with auth_type 'api_key' requires 'api_key' or 'api_key_env_var'."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if "api_key" in config and not isinstance(config["api_key"], str):
            msg = f"'api_key' for enterprise session '{session_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if "api_key_env_var" in config and not isinstance(config["api_key_env_var"], str):
            msg = f"'api_key_env_var' for enterprise session '{session_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)

    elif auth_type == "password":
        if "username" not in config:
            msg = f"Enterprise session '{session_name}' with auth_type 'password' requires 'username'."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if not isinstance(config["username"], str):
            msg = f"'username' for enterprise session '{session_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if not ("password" in config or "password_env_var" in config):
            msg = f"Enterprise session '{session_name}' with auth_type 'password' requires 'password' or 'password_env_var'."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if "password" in config and not isinstance(config["password"], str):
            msg = f"'password' for enterprise session '{session_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if "password_env_var" in config and not isinstance(config["password_env_var"], str):
            msg = f"'password_env_var' for enterprise session '{session_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)

    elif auth_type == "private_key":
        if "private_key_path" not in config:
            msg = f"Enterprise session '{session_name}' with auth_type 'private_key' requires 'private_key_path'."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)
        if not isinstance(config["private_key_path"], str):
            msg = f"'private_key_path' for enterprise session '{session_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSessionConfigurationError(msg)

    _LOGGER.debug(f"Enterprise session '{session_name}' configuration validated successfully.")
