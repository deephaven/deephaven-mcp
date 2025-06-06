"""
Validation logic for the 'enterprise_systems' section of the Deephaven MCP configuration.
"""
import logging
from typing import Any

from . import McpConfigurationError

_LOGGER = logging.getLogger(__name__)


_BASE_ENTERPRISE_SYSTEM_FIELDS: dict[str, type | tuple[type, ...]] = {
    "connection_json_url": str,
    "auth_type": str,
}
"""Defines the base fields and their expected types for any enterprise system configuration."""

_AUTH_SPECIFIC_FIELDS: dict[str, dict[str, type | tuple[type, ...]]] = {
    "password": {
        "username": str, # Required for this auth_type
        "password": str, # Type if present
        "password_env_var": str, # Type if present
    },
    "private_key": {
        "private_key": str, # Required for this auth_type
    }
}
"""
Defines auth-type specific fields and their expected types. 
Each key is an auth_type, and its value is a dictionary of fields specific to that auth_type.
"""


class EnterpriseSystemConfigurationError(McpConfigurationError):
    """Custom exception for errors in enterprise system configuration."""
    pass


def redact_enterprise_system_config(system_config: dict[str, Any]) -> dict[str, Any]:
    """Redacts sensitive fields from an enterprise system configuration dictionary.

    Creates a shallow copy of the input dictionary and redacts 'password' if it exists.

    Args:
        system_config (dict[str, Any]): The enterprise system configuration.

    Returns:
        dict[str, Any]: A new dictionary with sensitive fields redacted.
    """
    config_copy = system_config.copy()
    if "password" in config_copy:
        config_copy["password"] = "[REDACTED]"  # noqa: S105
    return config_copy


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
    # For logging purposes, create a redacted version of the map
    # We do this only if the map is a dictionary, otherwise log as is or let validation catch it
    logged_map_str = str(enterprise_systems_map) # Default to string representation
    if isinstance(enterprise_systems_map, dict):
        redacted_map_for_logging = {}
        for name, config_item in enterprise_systems_map.items():
            if isinstance(config_item, dict):
                redacted_map_for_logging[name] = redact_enterprise_system_config(config_item)
            else:
                redacted_map_for_logging[name] = config_item # Use as-is if not a dict
        logged_map_str = str(redacted_map_for_logging)

    _LOGGER.debug(
        f"Validating enterprise_systems configuration: {logged_map_str}"
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
    """Validate a single enterprise system's configuration dictionary.

    Args:
        system_name (str): The name of the enterprise system.
        config (Any): The configuration dictionary for the system.

    Raises:
        EnterpriseSystemConfigurationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        msg = f"Enterprise system '{system_name}' configuration must be a dictionary, but got {type(config).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    # Validate presence and type of base fields first
    for field_name, expected_type in _BASE_ENTERPRISE_SYSTEM_FIELDS.items():
        if field_name not in config:
            msg = f"Required field '{field_name}' missing in enterprise system '{system_name}'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        
        field_value = config[field_name]
        if isinstance(expected_type, tuple): # Handles (type, types.NoneType) for optional fields
            if not isinstance(field_value, expected_type):
                expected_type_names = ", ".join(t.__name__ for t in expected_type)
                msg = (
                    f"Field '{field_name}' for enterprise system '{system_name}' must be one of types "
                    f"({expected_type_names}), but got {type(field_value).__name__}."
                )
                _LOGGER.error(msg)
                raise EnterpriseSystemConfigurationError(msg)
        elif not isinstance(field_value, expected_type):
            msg = (
                f"Field '{field_name}' for enterprise system '{system_name}' must be of type "
                f"{expected_type.__name__}, but got {type(field_value).__name__}."
            )
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

    auth_type = config.get("auth_type") # Already validated to be a string by the loop above
    if auth_type not in _AUTH_SPECIFIC_FIELDS:
        allowed_types_str = sorted(list(_AUTH_SPECIFIC_FIELDS.keys()))
        msg = f"'auth_type' for enterprise system '{system_name}' must be one of {allowed_types_str}, but got '{auth_type}'."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    # Determine all allowed fields for this auth_type
    current_auth_specific_fields_schema = _AUTH_SPECIFIC_FIELDS.get(auth_type, {})
    all_allowed_fields_for_this_auth_type = {**_BASE_ENTERPRISE_SYSTEM_FIELDS, **current_auth_specific_fields_schema}

    # Validate types of auth-specific fields and check for unknown fields
    for field_name, field_value in config.items():
        if field_name in _BASE_ENTERPRISE_SYSTEM_FIELDS: # Base fields already type-checked
            continue

        if field_name not in all_allowed_fields_for_this_auth_type:
            # This will also catch TLS options if they are not in _KNOWN_TLS_OPTIONS and not part of the core schema.
            # If _KNOWN_TLS_OPTIONS are to be silently ignored (not warned), that logic needs to be explicit here.
            # Any field not in the defined schema (base + auth-specific) is warned as unknown.
            _LOGGER.warning(
                "Unknown field '%s' in enterprise system '%s' configuration. It will be ignored.",
                field_name,
                system_name,
            )
            continue # Skip type checking for unknown fields

        # Type check for auth-specific fields (already known to be in all_allowed_fields_for_this_auth_type)
        expected_type = all_allowed_fields_for_this_auth_type[field_name]
        if isinstance(expected_type, tuple):
            if not isinstance(field_value, expected_type):
                expected_type_names = ", ".join(t.__name__ for t in expected_type)
                msg = (
                    f"Field '{field_name}' for enterprise system '{system_name}' (auth_type: {auth_type}) "
                    f"must be one of types ({expected_type_names}), but got {type(field_value).__name__}."
                )
                _LOGGER.error(msg)
                raise EnterpriseSystemConfigurationError(msg)
        elif not isinstance(field_value, expected_type):
            msg = (
                f"Field '{field_name}' for enterprise system '{system_name}' (auth_type: {auth_type}) "
                f"must be of type {expected_type.__name__}, but got {type(field_value).__name__}."
            )
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

    # Specific validation logic based on auth_type (presence of required sub-fields and mutual exclusivity)

    if auth_type == "password":
        if "username" not in config: # This field is required for 'password' auth_type
             msg = f"Enterprise system '{system_name}' with auth_type 'password' must define 'username'."
             _LOGGER.error(msg)
             raise EnterpriseSystemConfigurationError(msg)
        # Type of 'username' is already checked by the loop above.

        password_present = "password" in config
        password_env_var_present = "password_env_var" in config
        if password_present and password_env_var_present:
            msg = f"Enterprise system '{system_name}' with auth_type 'password' must not define both 'password' and 'password_env_var'. Specify one."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        if not password_present and not password_env_var_present:
            msg = f"Enterprise system '{system_name}' with auth_type 'password' must define 'password' or 'password_env_var'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

    elif auth_type == "private_key":
        if "private_key" not in config: # This field is required for 'private_key' auth_type
            msg = f"Enterprise system '{system_name}' with auth_type 'private_key' must define 'private_key'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        # Type of 'private_key' is already checked by the loop above.
