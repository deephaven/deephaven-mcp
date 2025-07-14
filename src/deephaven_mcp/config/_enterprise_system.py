"""Validation logic for the 'enterprise_systems' section of the Deephaven MCP configuration."""

__all__ = [
    "validate_enterprise_systems_config",
    "validate_single_enterprise_system",
    "redact_enterprise_system_config",
    "redact_enterprise_systems_map",
]

import logging
from typing import Any

from deephaven_mcp._exceptions import EnterpriseSystemConfigurationError

_LOGGER = logging.getLogger(__name__)


_BASE_ENTERPRISE_SYSTEM_FIELDS: dict[str, type | tuple[type, ...]] = {
    "connection_json_url": str,
    "auth_type": str,
}
"""Defines the base fields and their expected types for any enterprise system configuration."""

_AUTH_SPECIFIC_FIELDS: dict[str, dict[str, type | tuple[type, ...]]] = {
    "password": {
        "username": str,  # Required for this auth_type
        "password": str,  # Type if present
        "password_env_var": str,  # Type if present
    },
    "private_key": {
        "private_key": str,  # Required for this auth_type
    },
}
"""
Defines auth-type specific fields and their expected types. 
Each key is an auth_type, and its value is a dictionary of fields specific to that auth_type.
"""


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


def redact_enterprise_systems_map(
    enterprise_systems_map: dict[str, Any],
) -> dict[str, Any]:
    """
    Redacts sensitive fields from an enterprise systems map dictionary.

    For each entry, if the value is a dict, redact sensitive fields. If not, include the value as-is (for robust logging).

    Args:
        enterprise_systems_map (dict[str, Any]): The enterprise systems map configuration.

    Returns:
        dict[str, Any]: A new dictionary with sensitive fields redacted where possible.
    """
    redacted_map = {}
    for system_name, system_config in enterprise_systems_map.items():
        if isinstance(system_config, dict):
            redacted_map[system_name] = redact_enterprise_system_config(system_config)
        else:
            redacted_map[system_name] = system_config  # log as-is for malformed
    return redacted_map


def validate_enterprise_systems_config(enterprise_systems_map: Any | None) -> None:
    """
    Validate the 'enterprise_systems' part of the MCP configuration.

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
    if isinstance(enterprise_systems_map, dict):
        logged_map_str = str(redact_enterprise_systems_map(enterprise_systems_map))
    else:
        logged_map_str = str(enterprise_systems_map)  # Default to string representation

    _LOGGER.debug(f"Validating enterprise_systems configuration: {logged_map_str}")

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
        validate_single_enterprise_system(system_name, system_config)

    _LOGGER.info(
        f"Validation for 'enterprise_systems' passed. Found {len(enterprise_systems_map)} enterprise system(s)."
    )


def validate_single_enterprise_system(system_name: str, config: Any) -> None:
    """
    Validate a single enterprise system's configuration dictionary.

    Args:
        system_name (str): The name of the enterprise system.
        config (Any): The configuration dictionary for the system.

    Raises:
        EnterpriseSystemConfigurationError: If the configuration is invalid.
    """
    _validate_enterprise_system_base_fields(system_name, config)
    auth_type, all_allowed_fields = _validate_and_get_auth_type(system_name, config)
    _validate_enterprise_system_auth_specific_fields(
        system_name, config, auth_type, all_allowed_fields
    )
    _validate_enterprise_system_auth_type_logic(system_name, config, auth_type)


def _validate_enterprise_system_base_fields(system_name: str, config: Any) -> None:
    """
    Validate that the enterprise system config is a dict and that all base fields are present and of correct type.

    Args:
        system_name (str): The name of the enterprise system.
        config (Any): The configuration dictionary for the system.

    Raises:
        EnterpriseSystemConfigurationError: If the config is not a dict, or if any base field is missing or of wrong type.
    """
    if not isinstance(config, dict):
        msg = f"Enterprise system '{system_name}' configuration must be a dictionary, but got {type(config).__name__}."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    for field_name, expected_type in _BASE_ENTERPRISE_SYSTEM_FIELDS.items():
        if field_name not in config:
            msg = f"Required field '{field_name}' missing in enterprise system '{system_name}'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
        field_value = config[field_name]

        if isinstance(expected_type, tuple):
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


def _validate_and_get_auth_type(
    system_name: str, config: dict[str, Any]
) -> tuple[str, dict[str, type | tuple[type, ...]]]:
    """
    Validate the 'auth_type' field and return it along with the allowed fields for that auth_type.

    Args:
        system_name (str): The name of the enterprise system.
        config (dict[str, Any]): The configuration dictionary for the system.

    Returns:
        Tuple[str, dict[str, type | tuple[type, ...]]]: The auth_type and a dict of all allowed fields for this auth_type.

    Raises:
        EnterpriseSystemConfigurationError: If 'auth_type' is missing or invalid.
    """
    auth_type = config.get("auth_type")
    if auth_type not in _AUTH_SPECIFIC_FIELDS:
        allowed_types_str = sorted(_AUTH_SPECIFIC_FIELDS.keys())
        msg = f"'auth_type' for enterprise system '{system_name}' must be one of {allowed_types_str}, but got '{auth_type}'."
        _LOGGER.error(msg)
        raise EnterpriseSystemConfigurationError(msg)

    current_auth_specific_fields_schema = _AUTH_SPECIFIC_FIELDS.get(auth_type, {})
    all_allowed_fields_for_this_auth_type = {
        **_BASE_ENTERPRISE_SYSTEM_FIELDS,
        **current_auth_specific_fields_schema,
    }
    return auth_type, all_allowed_fields_for_this_auth_type


def _validate_enterprise_system_auth_specific_fields(
    system_name: str,
    config: dict[str, Any],
    auth_type: str,
    all_allowed_fields_for_this_auth_type: dict[str, type | tuple[type, ...]],
) -> None:
    for field_name, field_value in config.items():
        if field_name in _BASE_ENTERPRISE_SYSTEM_FIELDS:
            continue

        if field_name not in all_allowed_fields_for_this_auth_type:
            _LOGGER.warning(
                "Unknown field '%s' in enterprise system '%s' configuration. It will be ignored.",
                field_name,
                system_name,
            )
            continue

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


def _validate_enterprise_system_auth_type_logic(
    system_name: str, config: dict[str, Any], auth_type: str
) -> None:
    """
    Perform additional validation logic specific to the given auth_type (e.g., required sub-fields, mutual exclusivity).

    Args:
        system_name (str): The name of the enterprise system.
        config (dict[str, Any]): The configuration dictionary for the system.
        auth_type (str): The authentication type for the system.

    Raises:
        EnterpriseSystemConfigurationError: If any auth-type-specific validation fails.
    """
    if auth_type == "password":
        if "username" not in config:
            msg = f"Enterprise system '{system_name}' with auth_type 'password' must define 'username'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)

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
        if "private_key" not in config:
            msg = f"Enterprise system '{system_name}' with auth_type 'private_key' must define 'private_key'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
