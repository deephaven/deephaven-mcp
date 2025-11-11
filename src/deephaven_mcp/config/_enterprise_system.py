"""Validation logic for the 'enterprise_systems' section of the Deephaven MCP configuration.

This module provides comprehensive validation for enterprise system configurations,
including authentication type validation, field type checking, and security-focused
credential redaction for safe logging.

Supported Authentication Types:
    - 'password': Username/password authentication with optional environment variable support
    - 'private_key': Private key file authentication

Key Features:
    - Type-safe validation with detailed error messages
    - Credential redaction for secure logging
    - Comprehensive field validation (required, optional, and auth-specific)
    - Support for session creation configuration

Main Functions:
    - validate_enterprise_systems_config(): Validates entire enterprise systems configuration
    - validate_single_enterprise_system(): Validates individual system configuration
    - redact_enterprise_system_config(): Redacts sensitive fields for logging
    - redact_enterprise_systems_map(): Redacts entire enterprise systems map
"""

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

_OPTIONAL_ENTERPRISE_SYSTEM_FIELDS: dict[str, type | tuple[type, ...]] = {
    "session_creation": dict,  # Optional session creation configuration (max_concurrent_sessions, defaults)
}
"""Defines optional fields that can be included in enterprise system configurations."""

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
    """Redact sensitive fields from an enterprise system configuration dictionary.

    Creates a shallow copy of the input dictionary and redacts the 'password' field if present.
    This function is used for safe logging of configuration data without exposing
    sensitive credentials in logs or debug output.

    Args:
        system_config (dict[str, Any]): The enterprise system configuration dictionary containing
            fields like connection_json_url, auth_type, username, password, etc.

    Returns:
        dict[str, Any]: A new dictionary with the same structure but with the 'password' field
        replaced with '[REDACTED]' if it was present. All other fields are preserved unchanged.

    Example:
        >>> config = {"username": "admin", "password": "secret123"}
        >>> redacted = redact_enterprise_system_config(config)
        >>> print(redacted)
        {"username": "admin", "password": "[REDACTED]"}
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
    fields (like passwords) redacted for safe logging. This is particularly useful
    when logging configuration validation errors or debug information.

    Args:
        enterprise_systems_map (dict[str, Any]): Dictionary mapping system names to their configurations.
            Expected format: {"system_name": {"connection_json_url": "...", "password": "...", ...}}
            If a system configuration is not a dictionary (malformed), it's included as-is
            to preserve error information for debugging.

    Returns:
        dict[str, Any]: A new dictionary with the same structure but sensitive fields replaced with
        "[REDACTED]" placeholders. Non-dict values are preserved unchanged to help
        with debugging malformed configurations.

    Example:
        >>> systems = {
        ...     "prod": {"username": "admin", "password": "secret"},
        ...     "dev": {"username": "dev", "password": "dev123"}
        ... }
        >>> redacted = redact_enterprise_systems_map(systems)
        >>> print(redacted)
        {"prod": {"username": "admin", "password": "[REDACTED]"},
         "dev": {"username": "dev", "password": "[REDACTED]"}}
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

    Performs comprehensive validation of enterprise system configurations including
    structural validation, required field checking, authentication type validation,
    and session creation settings validation. This function is the main entry point
    for validating the entire 'enterprise_systems' configuration section.

    Validation Process:
        1. Structure validation (must be dict or None)
        2. System name validation (must be strings)
        3. Per-system validation via validate_single_enterprise_system()
        4. Logging with credential redaction for security

    Required Fields (per system):
        - connection_json_url (str): URL to Deephaven Core+ connection JSON
        - auth_type (str): Authentication method ('password' or 'private_key')

    Authentication-Specific Requirements:
        'password' auth_type:
            - username (str): Required username
            - password (str) XOR password_env_var (str): Mutually exclusive credential options

        'private_key' auth_type:
            - private_key_path (str): Path to private key file

    Optional Fields (per system):
        - session_creation (dict): Session creation defaults and limits
            - max_concurrent_sessions (int): Session limit (0 = disabled, >0 = limit)
            - defaults (dict): Default session parameters (heap_size_gb, server, etc.)

    Args:
        enterprise_systems_map (Any | None): The 'enterprise_systems' configuration value.
            - None: No enterprise systems configured (valid)
            - dict: Mapping of system_name -> system_config dictionaries
            - Other types: Invalid and will raise EnterpriseSystemConfigurationError

    Raises:
        EnterpriseSystemConfigurationError: Raised for any validation failure including:
            - Non-dict enterprise_systems_map (when not None)
            - Non-string system names
            - Individual system validation failures (missing fields, wrong types, etc.)
            - Invalid authentication configurations
            - Invalid session creation settings

    Example:
        Valid enterprise systems configuration:
        {
            "production": {
                "connection_json_url": "https://prod.deephaven.io/iris/connection.json",
                "auth_type": "password",
                "username": "prod_admin",
                "password_env_var": "DH_PROD_PASSWORD",
                "session_creation": {
                    "max_concurrent_sessions": 10,
                    "defaults": {
                        "heap_size_gb": 16.0,
                        "server": "prod-server-01"
                    }
                }
            },
            "development": {
                "connection_json_url": "https://dev.deephaven.io/iris/connection.json",
                "auth_type": "private_key",
                "private_key_path": "/opt/deephaven/keys/dev.pem"
            }
        }

    Note:
        Empty dictionary {} is valid (no enterprise systems configured).
        Credentials are automatically redacted in debug logging for security.
    """
    # For logging purposes, create a redacted version of the map
    # We do this only if the map is a dictionary, otherwise log as is or let validation catch it
    if isinstance(enterprise_systems_map, dict):
        logged_map_str = str(redact_enterprise_systems_map(enterprise_systems_map))
    else:
        logged_map_str = str(enterprise_systems_map)  # Default to string representation

    _LOGGER.debug(
        f"[config:validate_enterprise_systems_config] Validating enterprise_systems configuration: {logged_map_str}"
    )

    if enterprise_systems_map is None:
        _LOGGER.debug(
            "[config:validate_enterprise_systems_config] 'enterprise_systems' key is not present, which is valid."
        )
        return

    if not isinstance(enterprise_systems_map, dict):
        msg = f"'enterprise_systems' must be a dictionary, but got type {type(enterprise_systems_map).__name__}."
        _LOGGER.error(f"[config:validate_enterprise_systems_config] {msg}")
        raise EnterpriseSystemConfigurationError(msg)

    if not enterprise_systems_map:
        _LOGGER.debug(
            "[config:validate_enterprise_systems_config] 'enterprise_systems' is an empty dictionary, which is valid (no enterprise systems configured)."
        )
        return

    # Iterate over and validate each configured enterprise system
    for system_name, system_config in enterprise_systems_map.items():
        if not isinstance(system_name, str):
            msg = f"Enterprise system name must be a string, but got {type(system_name).__name__}: {system_name!r}."
            _LOGGER.error(f"[config:validate_enterprise_systems_config] {msg}")
            raise EnterpriseSystemConfigurationError(msg)
        validate_single_enterprise_system(system_name, system_config)

    _LOGGER.info(
        f"[config:validate_enterprise_systems_config] Validation passed. Found {len(enterprise_systems_map)} enterprise system(s)."
    )


def validate_single_enterprise_system(system_name: str, config: Any) -> None:
    """Validate a single enterprise system's configuration comprehensively.

    Performs multi-stage validation of an individual enterprise system configuration,
    ensuring all required fields are present with correct types and that authentication
    settings are logically consistent. This function orchestrates validation through
    multiple specialized helper functions.

    Validation Stages:
        1. Base field validation (structure, required fields, optional fields)
        2. Authentication type validation and field collection
        3. Authentication-specific field validation
        4. Authentication logic consistency validation
        5. Session creation configuration validation (if present)

    Required Base Fields:
        - connection_json_url (str): URL to Core+ connection.json endpoint
        - auth_type (str): Must be 'password' or 'private_key'

    Authentication Field Requirements:
        For 'password' auth_type:
            - username (str): Username for authentication
            - Exactly one of: password (str) OR password_env_var (str)

        For 'private_key' auth_type:
            - private_key_path (str): Filesystem path to private key file

    Optional Configuration:
        - session_creation (dict): Session management settings
            - max_concurrent_sessions (int ≥ 0): 0=disabled, >0=limit
            - defaults (dict): Default session parameters

    Args:
        system_name (str): The name/identifier of the enterprise system being validated.
            Used in error messages and logging to identify which system failed validation.
        config (Any): The configuration object for the system. Expected to be a dictionary,
            but accepts Any to provide clear error messages for incorrect types.

    Raises:
        EnterpriseSystemConfigurationError: Raised for any validation failure including:
            - config is not a dictionary
            - Missing required base fields (connection_json_url, auth_type)
            - Invalid auth_type value (not 'password' or 'private_key')
            - Missing authentication-specific fields (username, password info, key path)
            - Invalid authentication field combinations (both password and password_env_var)
            - Invalid session_creation configuration
            - Field type mismatches (wrong data types)

    Example:
        Valid password-based configuration:
        {
            "connection_json_url": "https://my-system.com/iris/connection.json",
            "auth_type": "password",
            "username": "service_account",
            "password_env_var": "DH_SERVICE_PASSWORD",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "heap_size_gb": 8.0,
                    "programming_language": "Python"
                }
            }
        }
    """
    _LOGGER.debug(
        f"[config:validate_single_enterprise_system] Validating enterprise system '{system_name}'"
    )

    _validate_enterprise_system_base_fields(system_name, config)
    auth_type, all_allowed_fields = _validate_and_get_auth_type(system_name, config)
    _validate_enterprise_system_auth_specific_fields(
        system_name, config, auth_type, all_allowed_fields
    )
    _validate_enterprise_system_auth_type_logic(system_name, config, auth_type)
    _validate_enterprise_system_session_creation(system_name, config)

    _LOGGER.debug(
        f"[config:validate_single_enterprise_system] Enterprise system '{system_name}' validation passed"
    )


def _validate_field_type(
    system_name: str,
    field_name: str,
    field_value: Any,
    expected_type: type | tuple[type, ...],
    is_optional: bool = False,
) -> None:
    """Validate that a configuration field has the correct type.

    Performs type checking for enterprise system configuration fields, supporting
    both single types and union types (multiple acceptable types). Generates
    clear error messages that distinguish between required and optional fields.

    Type Validation:
        - Single type: isinstance(field_value, expected_type)
        - Union types: isinstance(field_value, tuple_of_types)
        - Error messages include all acceptable type names

    Args:
        system_name (str): Name of the enterprise system being validated.
            Used in error messages to identify the problematic system configuration.
        field_name (str): Name of the configuration field being validated.
            Used in error messages to identify the specific problematic field.
        field_value (Any): The actual value of the field from the configuration.
            Can be any type - validation determines if it matches expected_type.
        expected_type (type | tuple[type, ...]): The expected type or types for the field.
            - Single type: e.g., str, int, dict
            - Union types: e.g., (str, int) for fields accepting multiple types
        is_optional (bool): Whether this field is optional. Defaults to False.
            Affects error message prefix: "Field" vs "Optional field" for clarity.

    Raises:
        EnterpriseSystemConfigurationError: Raised when field_value type doesn't match
            expected_type. Error message includes:
            - Field name and system name for context
            - Expected type(s) with human-readable names
            - Actual type received
            - Whether field is optional (for debugging)

    Examples:
        >>> # Valid single type
        >>> _validate_field_type("prod", "username", "admin", str, False)
        >>> # No exception raised

        >>> # Valid union type
        >>> _validate_field_type("prod", "timeout", 30.5, (int, float), False)
        >>> # No exception raised

        >>> # Invalid type - raises exception
        >>> _validate_field_type("prod", "username", 123, str, False)
        >>> # EnterpriseSystemConfigurationError: Field 'username' for enterprise
        >>> # system 'prod' must be of type str, but got int.
    """
    field_prefix = "Optional field" if is_optional else "Field"

    if isinstance(expected_type, tuple):
        if not isinstance(field_value, expected_type):
            expected_type_names = ", ".join(t.__name__ for t in expected_type)
            msg = (
                f"{field_prefix} '{field_name}' for enterprise system '{system_name}' must be one of types "
                f"({expected_type_names}), but got {type(field_value).__name__}."
            )
            _LOGGER.error(f"[config:_validate_field_type] {msg}")
            raise EnterpriseSystemConfigurationError(msg)
    elif not isinstance(field_value, expected_type):
        msg = (
            f"{field_prefix} '{field_name}' for enterprise system '{system_name}' must be of type "
            f"{expected_type.__name__}, but got {type(field_value).__name__}."
        )
        _LOGGER.error(f"[config:_validate_field_type] {msg}")
        raise EnterpriseSystemConfigurationError(msg)


def _validate_required_fields(system_name: str, config: dict[str, Any]) -> None:
    """Validate all required base fields.

    Args:
        system_name (str): Name of the enterprise system being validated
        config (dict[str, Any]): Configuration dictionary

    Raises:
        EnterpriseSystemConfigurationError: If required field is missing or has wrong type
    """
    for field_name, expected_type in _BASE_ENTERPRISE_SYSTEM_FIELDS.items():
        if field_name not in config:
            msg = f"Required field '{field_name}' missing in enterprise system '{system_name}'."
            _LOGGER.error(f"[config:_validate_required_fields] {msg}")
            raise EnterpriseSystemConfigurationError(msg)

        _validate_field_type(
            system_name,
            field_name,
            config[field_name],
            expected_type,
            is_optional=False,
        )


def _validate_optional_fields(system_name: str, config: dict[str, Any]) -> None:
    """Validate all optional fields if present.

    Args:
        system_name (str): Name of the enterprise system being validated
        config (dict[str, Any]): Configuration dictionary

    Raises:
        EnterpriseSystemConfigurationError: If optional field has wrong type
    """
    for field_name, expected_type in _OPTIONAL_ENTERPRISE_SYSTEM_FIELDS.items():
        if field_name not in config:
            continue  # Optional field not present - that's fine

        _validate_field_type(
            system_name, field_name, config[field_name], expected_type, is_optional=True
        )


def _validate_enterprise_system_base_fields(system_name: str, config: Any) -> None:
    """Validate base fields and optional fields in an enterprise system configuration.

    Validates that the configuration is a dictionary and contains all required base fields
    (connection_json_url, auth_type) with correct types. Also validates optional fields
    like session_creation if present.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (Any): The configuration object for the system (expected to be a dict).

    Raises:
        EnterpriseSystemConfigurationError: If the config is not a dictionary,
            if any required base field is missing, or if any field has the wrong type.
    """
    if not isinstance(config, dict):
        msg = f"Enterprise system '{system_name}' configuration must be a dictionary, but got {type(config).__name__}."
        _LOGGER.error(f"[config:_validate_enterprise_system_base_fields] {msg}")
        raise EnterpriseSystemConfigurationError(msg)

    _validate_required_fields(system_name, config)
    _validate_optional_fields(system_name, config)


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
        _LOGGER.error(f"[config:_validate_and_get_auth_type] {msg}")
        raise EnterpriseSystemConfigurationError(msg)

    current_auth_specific_fields_schema = _AUTH_SPECIFIC_FIELDS.get(auth_type, {})
    all_allowed_fields_for_this_auth_type = {
        **_BASE_ENTERPRISE_SYSTEM_FIELDS,
        **_OPTIONAL_ENTERPRISE_SYSTEM_FIELDS,
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
        if field_name in _OPTIONAL_ENTERPRISE_SYSTEM_FIELDS:
            continue  # Optional fields are validated separately

        if field_name not in all_allowed_fields_for_this_auth_type:
            _LOGGER.warning(
                "[config:_validate_enterprise_system_auth_specific_fields] Unknown field '%s' in enterprise system '%s' configuration. It will be ignored.",
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
                _LOGGER.error(
                    f"[config:_validate_enterprise_system_auth_specific_fields] {msg}"
                )
                raise EnterpriseSystemConfigurationError(msg)
        elif not isinstance(field_value, expected_type):
            msg = (
                f"Field '{field_name}' for enterprise system '{system_name}' (auth_type: {auth_type}) "
                f"must be of type {expected_type.__name__}, but got {type(field_value).__name__}."
            )
            _LOGGER.error(
                f"[config:_validate_enterprise_system_auth_specific_fields] {msg}"
            )
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
            _LOGGER.error(f"[config:_validate_enterprise_system_auth_type_logic] {msg}")
            raise EnterpriseSystemConfigurationError(msg)

        password_present = "password" in config
        password_env_var_present = "password_env_var" in config
        if password_present and password_env_var_present:
            msg = f"Enterprise system '{system_name}' with auth_type 'password' must not define both 'password' and 'password_env_var'. Specify one."
            _LOGGER.error(f"[config:_validate_enterprise_system_auth_type_logic] {msg}")
            raise EnterpriseSystemConfigurationError(msg)
        if not password_present and not password_env_var_present:
            msg = f"Enterprise system '{system_name}' with auth_type 'password' must define 'password' or 'password_env_var'."
            _LOGGER.error(f"[config:_validate_enterprise_system_auth_type_logic] {msg}")
            raise EnterpriseSystemConfigurationError(msg)
    elif auth_type == "private_key":
        if "private_key_path" not in config:
            msg = f"Enterprise system '{system_name}' with auth_type 'private_key' must define 'private_key_path'."
            _LOGGER.error(f"[config:_validate_enterprise_system_auth_type_logic] {msg}")
            raise EnterpriseSystemConfigurationError(msg)


def _validate_enterprise_system_session_creation(
    system_name: str, config: dict[str, Any]
) -> None:
    """Validate the optional session_creation configuration section.

    Validates the structure and content of the session_creation section if present.
    The entire section is optional, and if present, all fields within it are also optional.
    max_concurrent_sessions must be non-negative integer if specified.
    All default values within the defaults subsection are optional.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.

    Raises:
        EnterpriseSystemConfigurationError: If the session_creation configuration
            is invalid, including incorrect types or invalid values.
    """
    session_creation = config.get("session_creation")
    if session_creation is None:
        _LOGGER.debug(
            f"[config:_validate_enterprise_system_session_creation] Enterprise system '{system_name}' has no session_creation configuration (optional)."
        )
        return

    if not isinstance(session_creation, dict):
        msg = f"'session_creation' for enterprise system '{system_name}' must be a dictionary, but got {type(session_creation).__name__}."
        _LOGGER.error(f"[config:_validate_enterprise_system_session_creation] {msg}")
        raise EnterpriseSystemConfigurationError(msg)

    # max_concurrent_sessions is optional - validate if present
    if "max_concurrent_sessions" in session_creation:
        max_sessions = session_creation["max_concurrent_sessions"]
        if not isinstance(max_sessions, int) or max_sessions < 0:
            msg = f"'max_concurrent_sessions' for enterprise system '{system_name}' must be a non-negative integer, but got {max_sessions}."
            _LOGGER.error(
                f"[config:_validate_enterprise_system_session_creation] {msg}"
            )
            raise EnterpriseSystemConfigurationError(msg)

    # Validate defaults section if present (all fields optional)
    defaults = session_creation.get("defaults")
    if defaults is not None:
        if not isinstance(defaults, dict):
            msg = f"'defaults' in session_creation for enterprise system '{system_name}' must be a dictionary, but got {type(defaults).__name__}."
            _LOGGER.error(
                f"[config:_validate_enterprise_system_session_creation] {msg}"
            )
            raise EnterpriseSystemConfigurationError(msg)

        # Optional field validations
        _validate_optional_session_default(
            system_name, defaults, "heap_size_gb", int
        )
        _validate_optional_session_default(
            system_name, defaults, "auto_delete_timeout", int
        )
        _validate_optional_session_default(system_name, defaults, "server", str)
        _validate_optional_session_default(system_name, defaults, "engine", str)
        _validate_optional_session_default(
            system_name, defaults, "extra_jvm_args", list
        )
        _validate_optional_session_default(
            system_name, defaults, "extra_environment_vars", list
        )
        _validate_optional_session_default(system_name, defaults, "admin_groups", list)
        _validate_optional_session_default(system_name, defaults, "viewer_groups", list)
        _validate_optional_session_default(
            system_name, defaults, "timeout_seconds", (int, float)
        )
        _validate_optional_session_default(
            system_name, defaults, "session_arguments", dict
        )
        _validate_optional_session_default(
            system_name, defaults, "programming_language", str
        )

    _LOGGER.debug(
        f"[config:_validate_enterprise_system_session_creation] Session creation configuration for enterprise system '{system_name}' is valid."
    )


def _validate_optional_session_default(
    system_name: str,
    defaults: dict[str, Any],
    field_name: str,
    expected_type: type | tuple[type, ...],
) -> None:
    """Validate an optional session default field if present.

    Checks the type of a field within the session_creation.defaults section.
    If the field is not present, validation passes (all defaults are optional).
    If present, validates that the field value matches the expected type(s).

    Args:
        system_name (str): The name of the enterprise system being validated.
        defaults (dict[str, Any]): The session_creation.defaults dictionary.
        field_name (str): The name of the field to validate.
        expected_type (type | tuple[type, ...]): The expected type(s) for the field.

    Raises:
        EnterpriseSystemConfigurationError: If the field has an incorrect type.
    """
    if field_name not in defaults:
        return  # Field is optional

    field_value = defaults[field_name]
    if isinstance(expected_type, tuple):
        if not isinstance(field_value, expected_type):
            expected_type_names = ", ".join(t.__name__ for t in expected_type)
            msg = (
                f"Field '{field_name}' in session_creation defaults for enterprise system '{system_name}' "
                f"must be one of types ({expected_type_names}), but got {type(field_value).__name__}."
            )
            _LOGGER.error(f"[config:_validate_optional_session_default] {msg}")
            raise EnterpriseSystemConfigurationError(msg)
    elif not isinstance(field_value, expected_type):
        msg = (
            f"Field '{field_name}' in session_creation defaults for enterprise system '{system_name}' "
            f"must be of type {expected_type.__name__}, but got {type(field_value).__name__}."
        )
        _LOGGER.error(f"[config:_validate_optional_session_default] {msg}")
        raise EnterpriseSystemConfigurationError(msg)
