"""Validation logic for enterprise system configurations in the Deephaven MCP enterprise server.

This module provides comprehensive validation for flat enterprise system configurations
(the format used by the DHE MCP server), including authentication type validation,
field type checking, and security-focused credential redaction for safe logging.

Supported Authentication Types:
    - 'password': Username/password authentication with optional environment variable support
    - 'private_key': Private key file authentication

Key Features:
    - Type-safe validation with detailed error messages
    - Credential redaction for secure logging
    - Comprehensive field validation (required, optional, and auth-specific)
    - Support for session creation configuration

Module Constants:
    - DEFAULT_CONNECTION_TIMEOUT_SECONDS: Default connection timeout (10.0 seconds)

Main Exports:
    - EnterpriseServerConfigManager: Concrete ConfigManager subclass for the DHE MCP
      server; loads and caches a flat enterprise config file.
    - validate_enterprise_config(): Validates a flat enterprise system configuration.
    - redact_enterprise_system_config(): Redacts sensitive fields for logging.
"""

__all__ = [
    "EnterpriseServerConfigManager",
    "validate_enterprise_config",
    "redact_enterprise_system_config",
    "DEFAULT_CONNECTION_TIMEOUT_SECONDS",
]

import logging
from typing import Any

from deephaven_mcp._exceptions import EnterpriseSystemConfigurationError

from ._base import (
    ConfigManager,
    _get_config_path,
    _load_and_validate_config,
    _log_config_summary,
)
from ._validators import validate_optional_positive_number

_LOGGER = logging.getLogger(__name__)

# Default timeout for enterprise system connections
DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10.0
"""Default timeout in seconds for establishing connections to enterprise systems.

This value is used when 'connection_timeout' is not specified in the enterprise
system configuration. It provides a reasonable default that prevents indefinite
hanging while allowing sufficient time for typical connection establishment.
"""

_BASE_ENTERPRISE_SYSTEM_FIELDS: dict[str, type | tuple[type, ...]] = {
    "connection_json_url": str,
    "auth_type": str,
}
"""Required fields and their expected types for a flat enterprise system configuration."""

_OPTIONAL_ENTERPRISE_SYSTEM_FIELDS: dict[str, type | tuple[type, ...]] = {
    "system_name": str,  # Validated explicitly in validate_enterprise_config; kept here to suppress spurious "unknown field" warning
    "session_creation": dict,  # Optional; enables session creation when present. If present, defaults.heap_size_gb is required.
    "connection_timeout": (
        int,
        float,
    ),  # Optional timeout in seconds for initial connection
    "mcp_session_idle_timeout_seconds": (
        int,
        float,
    ),  # Optional idle timeout in seconds for MCP session Deephaven connections
}
"""Optional fields that can be included in enterprise system configurations."""

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


def validate_enterprise_config(config: Any) -> dict[str, Any]:
    """Validate a flat enterprise server configuration.

    Validates the DHE server config format where all fields (including system_name)
    sit at the top level.

    Required Fields:
        - system_name (str): Identifier for this enterprise system
        - connection_json_url (str): URL to Core+ connection.json endpoint
        - auth_type (str): Must be 'password' or 'private_key'

    Authentication Field Requirements:
        For 'password' auth_type:
            - username (str): Username for authentication
            - Exactly one of: password (str) OR password_env_var (str)

        For 'private_key' auth_type:
            - private_key_path (str): Filesystem path to private key file

    Optional Fields:
        - connection_timeout (int | float > 0, bool excluded): Timeout in seconds (default: 10.0)
        - session_creation (dict): Optional section that enables ``session_enterprise_create``.
            If absent, ``session_enterprise_create`` returns a "not configured" error.
            When present, ``defaults`` and ``defaults.heap_size_gb`` are required.
            - max_concurrent_sessions (int ≥ 0): 0=disabled, >0=limit (default: 5)
            - defaults (dict): **Required when section is present.** Default session parameters.
                - heap_size_gb (int | float): **Required.** JVM heap size in gigabytes.
                    The Deephaven API provides no server-side default for this value.
                - auto_delete_timeout (int): Auto-delete timeout in seconds (optional)
                - server (str): Target server name (optional)
                - engine (str): Execution engine name (optional)
                - extra_jvm_args (list): Additional JVM arguments (optional)
                - extra_environment_vars (list): Additional environment variables (optional)
                - admin_groups (list): Groups with admin access (optional)
                - viewer_groups (list): Groups with view-only access (optional)
                - timeout_seconds (int | float): Session timeout in seconds (optional)
                - session_arguments (dict): Additional session arguments (optional)
                - programming_language (str): Default programming language (optional)

    Args:
        config (Any): The configuration object. Expected to be a dictionary,
            but accepts Any to provide clear error messages for incorrect types.

    Returns:
        dict[str, Any]: The same input dictionary, returned unchanged after successful validation.

    Raises:
        EnterpriseSystemConfigurationError: For any validation failure.

    Example:
        >>> config = {
        ...     "system_name": "prod",
        ...     "connection_json_url": "https://my-system.com/iris/connection.json",
        ...     "auth_type": "password",
        ...     "username": "service_account",
        ...     "password_env_var": "DH_SERVICE_PASSWORD",
        ... }
        >>> result = validate_enterprise_config(config)
        >>> result is config  # Returns the same dict unchanged
        True
    """
    _LOGGER.debug(
        "[config:validate_enterprise_config] Validating enterprise server config"
    )
    if not isinstance(config, dict):
        msg = f"Enterprise system configuration must be a dictionary, but got {type(config).__name__}."
        _LOGGER.error(f"[config:validate_enterprise_config] {msg}")
        raise EnterpriseSystemConfigurationError(msg)
    system_name = config.get("system_name")
    if system_name is None:
        msg = "Required field 'system_name' is missing from enterprise system configuration."
        _LOGGER.error(f"[config:validate_enterprise_config] {msg}")
        raise EnterpriseSystemConfigurationError(msg)
    if not isinstance(system_name, str):
        msg = (
            f"Field 'system_name' in enterprise system configuration must be of type str, "
            f"but got {type(system_name).__name__}."
        )
        _LOGGER.error(f"[config:validate_enterprise_config] {msg}")
        raise EnterpriseSystemConfigurationError(msg)
    _validate_required_fields(system_name, config)
    _validate_optional_fields(system_name, config)
    auth_type, all_allowed_fields = _validate_and_get_auth_type(system_name, config)
    _validate_enterprise_system_auth_specific_fields(
        system_name, config, auth_type, all_allowed_fields
    )
    _validate_enterprise_system_auth_type_logic(system_name, config, auth_type)
    validate_optional_positive_number(config, "connection_timeout")
    validate_optional_positive_number(config, "mcp_session_idle_timeout_seconds")
    _validate_enterprise_system_session_creation(system_name, config)
    _LOGGER.debug(
        f"[config:validate_enterprise_config] Enterprise system '{system_name}' validation passed"
    )
    return config


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
    """Validate presence and type of all required base fields.

    Checks that every field in `_BASE_ENTERPRISE_SYSTEM_FIELDS` is present in
    the config and has the correct type. Raises on the first missing or
    incorrectly typed field encountered.

    Args:
        system_name (str): Name of the enterprise system being validated.
        config (dict[str, Any]): The enterprise system configuration dictionary.

    Raises:
        EnterpriseSystemConfigurationError: If a required field is absent or has the wrong type.
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
    """Validate type of all optional fields that are present in the config.

    Checks every field in `_OPTIONAL_ENTERPRISE_SYSTEM_FIELDS` against its
    expected type if the field appears in the config. Absent optional fields
    are silently skipped.

    Args:
        system_name (str): Name of the enterprise system being validated.
        config (dict[str, Any]): The enterprise system configuration dictionary.

    Raises:
        EnterpriseSystemConfigurationError: If a present optional field has the wrong type.
    """
    for field_name, expected_type in _OPTIONAL_ENTERPRISE_SYSTEM_FIELDS.items():
        if field_name not in config:
            continue  # Optional field not present - that's fine

        _validate_field_type(
            system_name, field_name, config[field_name], expected_type, is_optional=True
        )


def _validate_and_get_auth_type(
    system_name: str, config: dict[str, Any]
) -> tuple[str, dict[str, type | tuple[type, ...]]]:
    """Validate the auth_type field and return allowed fields for that authentication type.

    Checks that the auth_type is supported and returns a combined dictionary of all
    allowed fields — base fields, optional fields, and the auth-specific fields for
    the given auth_type — with their expected types.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.

    Returns:
        tuple[str, dict[str, type | tuple[type, ...]]]: A tuple containing:
            - The validated auth_type string
            - Dictionary mapping all allowed field names to their expected types
              (combines base fields, optional fields, and auth-specific fields)

    Raises:
        EnterpriseSystemConfigurationError: If auth_type is missing (None), unsupported,
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

    Validates all non-base, non-optional fields (e.g., 'username', 'password', 'private_key_path')
    to ensure they are allowed for the given auth_type and have correct types. Base fields
    ('connection_json_url', 'auth_type') and optional fields ('system_name', 'session_creation',
    'connection_timeout') are skipped as they are validated separately.
    Unknown fields generate warnings but don't cause validation failure.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.
        auth_type (str): The authentication type for the system ('password' or 'private_key').
        all_allowed_fields_for_this_auth_type (dict[str, type | tuple[type, ...]]): Dictionary
            mapping field names to their expected types for this auth_type (includes base,
            optional, and auth-specific fields).

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
                f"[config:_validate_enterprise_system_auth_specific_fields] Unknown field '{field_name}' in enterprise system '{system_name}' configuration. It will be ignored."
            )
            continue

        expected_type = all_allowed_fields_for_this_auth_type[field_name]
        _validate_field_type(system_name, field_name, field_value, expected_type)


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

    The section is optional; if absent, validation passes immediately. When present,
    defaults.heap_size_gb is required because the Deephaven API has no server-side
    default for it. max_concurrent_sessions must be a non-negative integer if specified.
    Note: bool values are technically accepted (since bool is a subclass of int), but are
    not intended for use. All other default values within the defaults subsection are
    optional.

    Args:
        system_name (str): The name of the enterprise system being validated.
        config (dict[str, Any]): The configuration dictionary for the system.

    Raises:
        EnterpriseSystemConfigurationError: If the section is present but invalid:
            not a dict, defaults is missing or not a dict, heap_size_gb is absent or
            the wrong type, or any optional field has an incorrect type.
    """
    session_creation = config.get("session_creation")
    if session_creation is None:
        _LOGGER.debug(
            f"[config:_validate_enterprise_system_session_creation] Enterprise system '{system_name}' has no session_creation configuration (optional)."
        )
        return

    # max_concurrent_sessions is optional - validate if present
    if "max_concurrent_sessions" in session_creation:
        max_sessions = session_creation["max_concurrent_sessions"]
        if not isinstance(max_sessions, int) or max_sessions < 0:
            msg = f"'max_concurrent_sessions' for enterprise system '{system_name}' must be a non-negative integer, but got {max_sessions}."
            _LOGGER.error(
                f"[config:_validate_enterprise_system_session_creation] {msg}"
            )
            raise EnterpriseSystemConfigurationError(msg)

    # Validate defaults section — required because heap_size_gb has no server-side default
    defaults = session_creation.get("defaults")
    if defaults is None:
        msg = f"'session_creation.defaults' is required for enterprise system '{system_name}' but is missing."
        _LOGGER.error(f"[config:_validate_enterprise_system_session_creation] {msg}")
        raise EnterpriseSystemConfigurationError(msg)

    if not isinstance(defaults, dict):
        msg = f"'defaults' in session_creation for enterprise system '{system_name}' must be a dictionary, but got {type(defaults).__name__}."
        _LOGGER.error(f"[config:_validate_enterprise_system_session_creation] {msg}")
        raise EnterpriseSystemConfigurationError(msg)

    # heap_size_gb is required — the Deephaven API provides no server-side default
    if "heap_size_gb" not in defaults:
        msg = f"'session_creation.defaults.heap_size_gb' is required for enterprise system '{system_name}' but is missing."
        _LOGGER.error(f"[config:_validate_enterprise_system_session_creation] {msg}")
        raise EnterpriseSystemConfigurationError(msg)
    _validate_field_type(
        system_name, "heap_size_gb", defaults["heap_size_gb"], (int, float)
    )

    # Optional field validations
    _validate_optional_session_default(
        system_name, defaults, "auto_delete_timeout", int
    )
    _validate_optional_session_default(system_name, defaults, "server", str)
    _validate_optional_session_default(system_name, defaults, "engine", str)
    _validate_optional_session_default(system_name, defaults, "extra_jvm_args", list)
    _validate_optional_session_default(
        system_name, defaults, "extra_environment_vars", list
    )
    _validate_optional_session_default(system_name, defaults, "admin_groups", list)
    _validate_optional_session_default(system_name, defaults, "viewer_groups", list)
    _validate_optional_session_default(
        system_name, defaults, "timeout_seconds", (int, float)
    )
    _validate_optional_session_default(system_name, defaults, "session_arguments", dict)
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
    _validate_field_type(
        system_name, field_name, defaults[field_name], expected_type, is_optional=True
    )


async def _load_and_validate_enterprise_config(config_path: str) -> dict[str, Any]:
    """Load, parse, and validate the flat enterprise configuration from a JSON/JSON5 file."""
    return await _load_and_validate_config(
        config_path, validate_enterprise_config, "_load_and_validate_enterprise_config"
    )


class EnterpriseServerConfigManager(ConfigManager):
    """ConfigManager for the DHE MCP server (``dh-mcp-enterprise-server``).

    Reads a *flat* enterprise config file where the system fields sit at the top level
    (no system-name nesting).  Validates the config as a single enterprise system and
    returns it directly.

    Config file format (flat)::

        {
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password_env_var": "DHE_PASSWORD",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4, "programming_language": "Python"}
            }
        }

    ``get_config()`` returns the flat config above directly — no wrapping.
    ``EnterpriseSessionRegistry`` uses the ``system_name`` field as the source name
    in all enterprise session identifiers (e.g. ``"enterprise:prod:my-pq"``).
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the flat enterprise config file.

        Returns:
            dict[str, Any]: The flat enterprise system config dict (fields at top level).

        Raises:
            RuntimeError: If no config path is provided and ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails enterprise validation.
        """
        _LOGGER.debug(
            "[EnterpriseServerConfigManager:get_config] Loading enterprise server configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[EnterpriseServerConfigManager:get_config] Using cached configuration."
                )
                return self._cache

            resolved_path = (
                self._config_path
                if self._config_path is not None
                else _get_config_path()
            )
            flat_config = await _load_and_validate_enterprise_config(resolved_path)
            self._cache = flat_config
            _log_config_summary(
                flat_config,
                label="EnterpriseServerConfigManager:get_config",
                redactor=redact_enterprise_system_config,
            )
            _LOGGER.info(
                "[EnterpriseServerConfigManager:get_config] Enterprise configuration loaded successfully."
            )
            return flat_config

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Set the in-memory configuration cache for testing (coroutine-safe).

        Validates ``config`` against the enterprise system schema before caching.
        Implements the abstract :meth:`ConfigManager._set_config_cache` method using
        enterprise validation rather than community validation.

        Args:
            config (dict[str, Any]): The flat enterprise configuration dictionary to cache.

        Raises:
            EnterpriseSystemConfigurationError: If the provided configuration is invalid.
        """
        async with self._lock:
            self._cache = validate_enterprise_config(config)
