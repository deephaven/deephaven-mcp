"""
Validation logic for the 'enterprise_systems' section of the Deephaven MCP configuration.
"""
import logging
from typing import Any

from . import McpConfigurationError

_LOGGER = logging.getLogger(__name__)

class EnterpriseSystemConfigurationError(McpConfigurationError):
    """Custom exception for errors in enterprise system configuration."""
    pass

def validate_enterprise_systems_config(enterprise_systems_map: Any | None) -> None:
    """
    Validates the 'enterprise_systems' part of the MCP configuration.

    The 'enterprise_systems' key in the config should map to a dictionary.
    Each key in this dictionary is a system name, and its value is the
    configuration object for that enterprise system.

    Args:
        enterprise_systems_map: The value associated with the 'enterprise_systems'
                                 key in the configuration. Expected to be a dict or None.

    Raises:
        EnterpriseSystemConfigurationError: If validation fails.
    """
    _LOGGER.debug(
        f"Validating enterprise_systems configuration: {enterprise_systems_map}"
    )

    if enterprise_systems_map is None:
        _LOGGER.debug("'enterprise_systems' key is not present, which is valid.")
        return

    if not isinstance(enterprise_systems_map, dict):
        msg = f"'enterprise_systems' must be a dictionary, but got type {type(enterprise_systems_map).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    if not enterprise_systems_map:
        _LOGGER.debug(
            "'enterprise_systems' is an empty dictionary, which is valid (no enterprise systems configured)."
        )
        return

    # Iterate over and validate each configured enterprise system
    for system_name, system_config in enterprise_systems_map.items():
        if not isinstance(system_name, str):
            msg = f"Enterprise system name must be a string, but got {type(system_name).__name__}: {system_name!r}."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        _validate_single_enterprise_system(system_name, system_config)

    _LOGGER.info(
        f"Validation for 'enterprise_systems' passed. Found {len(enterprise_systems_map)} enterprise system(s)."
    )

def _validate_single_enterprise_system(system_name: str, config: Any) -> None:
    """Validates a single enterprise system's configuration object."""
    _LOGGER.debug(f"Validating enterprise system '{system_name}': {config}")

    if not isinstance(config, dict):
        msg = f"Configuration for enterprise system '{system_name}' must be a dictionary, but got {type(config).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    # Validate presence and type of fundamental required keys: connection_json_url and auth_type
    if "connection_json_url" not in config:
        msg = f"Enterprise system '{system_name}' is missing required key 'connection_json_url'."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)
    if not isinstance(config["connection_json_url"], str):
        msg = f"'connection_json_url' for enterprise system '{system_name}' must be a string, but got {type(config['connection_json_url']).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    if "auth_type" not in config:
        msg = f"Enterprise system '{system_name}' is missing required key 'auth_type'."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    auth_type = config["auth_type"]
    allowed_auth_types = {"api_key", "password", "private_key", "none"}
    if not isinstance(auth_type, str) or auth_type not in allowed_auth_types:
        msg = f"'auth_type' for enterprise system '{system_name}' must be one of {allowed_auth_types}, but got '{auth_type}'."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    # Define base known keys and add auth-specific keys dynamically
    known_system_keys = {
        "connection_json_url", "auth_type"
    }
    if auth_type == "api_key":
        known_system_keys.update(["api_key", "api_key_env_var"])
    elif auth_type == "password":
        known_system_keys.update(["username", "password", "password_env_var"])
    elif auth_type == "private_key":
        known_system_keys.update(["private_key_path"])

    # Check for unknown keys first. This must happen after 'auth_type' is validated and 'known_system_keys' is built.
    for key_in_config in config:
        if key_in_config not in known_system_keys:
            _LOGGER.warning(f"Unknown key '{key_in_config}' in enterprise system '{system_name}' configuration (auth_type: {auth_type}).")

    # Now, validate conditional fields based on auth_type using the already identified known keys
    if auth_type == "api_key":
        if not ("api_key" in config or "api_key_env_var" in config):
            msg = f"Enterprise system '{system_name}' with auth_type 'api_key' requires 'api_key' or 'api_key_env_var'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if "api_key" in config and not isinstance(config["api_key"], str):
            msg = f"'api_key' for enterprise system '{system_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if "api_key_env_var" in config and not isinstance(config["api_key_env_var"], str):
            msg = f"'api_key_env_var' for enterprise system '{system_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

    elif auth_type == "password":
        if "username" not in config:
            msg = f"Enterprise system '{system_name}' with auth_type 'password' requires 'username'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if not isinstance(config["username"], str):
            msg = f"'username' for enterprise system '{system_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if not ("password" in config or "password_env_var" in config):
            msg = f"Enterprise system '{system_name}' with auth_type 'password' requires 'password' or 'password_env_var'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if "password" in config and not isinstance(config["password"], str):
            msg = f"'password' for enterprise system '{system_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if "password_env_var" in config and not isinstance(config["password_env_var"], str):
            msg = f"'password_env_var' for enterprise system '{system_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

    elif auth_type == "private_key":
        if "private_key_path" not in config:
            msg = f"Enterprise system '{system_name}' with auth_type 'private_key' requires 'private_key_path'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if not isinstance(config["private_key_path"], str):
            msg = f"'private_key_path' for enterprise system '{system_name}' must be a string."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

    _LOGGER.debug(f"Enterprise system '{system_name}' configuration validated successfully.")
