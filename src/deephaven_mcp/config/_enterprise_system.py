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
        "private_key_path": str,  # Required for this auth_type
    },
}
"""Authentication-specific field definitions and validation rules.

Maps each supported authentication type to its required and optional fields:
- 'password': Requires 'username' and either 'password' or 'password_env_var' (mutually exclusive)
- 'private_key': Requires 'private_key_path' field

Each field maps to its expected Python type for validation purposes.
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
    """Redact sensitive fields from an enterprise systems map dictionary.

    Creates a new dictionary where each enterprise system configuration has sensitive
    fields (like passwords) redacted for safe logging. If a system configuration is
    not a dictionary (malformed), it's included as-is to preserve error information.

    Args:
        enterprise_systems_map (dict[str, Any]): Dictionary mapping system names to their configurations.
            Expected format: {"system_name": {"connection_json_url": "...", ...}}

    Returns:
        dict[str, Any]: A new dictionary with the same structure but sensitive fields replaced with
        "[REDACTED]" placeholders. Non-dict values are preserved unchanged.
    """
    redacted_map = {}
    for system_name, system_config in enterprise_systems_map.items():
        if isinstance(system_config, dict):
            redacted_map[system_name] = redact_enterprise_system_config(system_config)
        else:
            redacted_map[system_name] = system_config  # log as-is for malformed
    return redacted_map


def validate_enterprise_systems_config(enterprise_systems_map: Any | None) -> None:
    """Validate the 'enterprise_systems' section of the MCP configuration.

    Validates the structure and content of enterprise system configurations. Each
    enterprise system must have valid base fields (connection_json_url, auth_type)
    and appropriate auth-specific fields based on the authentication type.

    Supported authentication types:
    - 'password': Requires 'username' and either 'password' or 'password_env_var'
    - 'private_key': Requires 'private_key_path'

    Args:
        enterprise_systems_map (Any | None): The value from the 'enterprise_systems' config key.
            Expected to be None (no enterprise systems) or a dictionary mapping
            system names to their configuration dictionaries.

    Raises:
        EnterpriseSystemConfigurationError: If the configuration is invalid,
            including missing required fields, incorrect types, or invalid
            authentication configurations.

    Example:
        Valid configuration structure:
        {
            "production": {
                "connection_json_url": "https://prod.example.com/iris/connection.json",
                "auth_type": "password",
                "username": "admin",
                "password_env_var": "DH_PROD_PASSWORD"
            },
            "staging": {
                "connection_json_url": "https://staging.example.com/iris/connection.json",
                "auth_type": "private_key",
                "private_key_path": "/path/to/staging.pem"
            }
        }
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
    """Validate a single enterprise system's configuration.

    Performs comprehensive validation including base fields, auth_type validation,
    auth-specific fields, and auth-type logic validation.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (Any): The configuration object for the system (expected to be a dict).

    Raises:
        EnterpriseSystemConfigurationError: If the configuration is invalid,
            including structural issues, missing fields, or invalid auth logic.
    """
    _validate_enterprise_system_base_fields(system_name, config)
    auth_type, all_allowed_fields = _validate_and_get_auth_type(system_name, config)
    _validate_enterprise_system_auth_specific_fields(
        system_name, config, auth_type, all_allowed_fields
    )
    _validate_enterprise_system_auth_type_logic(system_name, config, auth_type)


def _validate_enterprise_system_base_fields(system_name: str, config: Any) -> None:
    """Validate that the enterprise system config is a dict with all required base fields.

    Checks that the configuration is a dictionary and contains all required base fields
    (connection_json_url, auth_type) with the correct types.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (Any): The configuration object for the system (expected to be a dict).

    Raises:
        EnterpriseSystemConfigurationError: If the config is not a dictionary,
            if any required base field is missing, or if any field has the wrong type.
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
    """Validate the auth_type field and return allowed fields for that authentication type.

    Checks that the auth_type is supported and returns a combined dictionary of all
    allowed fields (base fields + auth-specific fields) with their expected types.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.

    Returns:
        tuple[str, dict[str, type | tuple[type, ...]]]: A tuple containing:
        - The validated auth_type string
        - Dictionary mapping all allowed field names to their expected types
          (combines base fields and auth-specific fields)

    Raises:
        EnterpriseSystemConfigurationError: If auth_type is missing, invalid,
            or not in the list of supported authentication types.
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
    """Validate authentication-specific fields in an enterprise system configuration.

    Validates all non-base fields (e.g., 'username', 'password', 'private_key_path') to ensure
    they are allowed for the given auth_type and have correct types. Base fields like
    'connection_json_url' and 'auth_type' are skipped as they're validated separately.
    Unknown fields generate warnings but don't cause validation failure.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.
        auth_type (str): The authentication type for the system ('password' or 'private_key').
        all_allowed_fields_for_this_auth_type (dict[str, type | tuple[type, ...]]): Dictionary mapping field names to their
            expected types for this auth_type (includes both base and auth-specific fields).

    Raises:
        EnterpriseSystemConfigurationError: If any field has an incorrect type.
    """
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
    """Perform auth-type-specific validation logic.

    Validates authentication-specific requirements such as required fields and
    mutual exclusivity rules. For 'password' auth: requires 'username' and either
    'password' or 'password_env_var' (but not both). For 'private_key' auth:
    requires 'private_key_path'.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.
        auth_type (str): The authentication type for the system.

    Raises:
        EnterpriseSystemConfigurationError: If any auth-type-specific validation
            fails, including missing required fields or mutual exclusivity violations.
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
        if "private_key_path" not in config:
            msg = f"Enterprise system '{system_name}' with auth_type 'private_key' must define 'private_key_path'."
            _LOGGER.error(msg)
            raise EnterpriseSystemConfigurationError(msg)
