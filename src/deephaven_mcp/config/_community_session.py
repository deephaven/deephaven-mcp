"""
Configuration handling specific to Deephaven Community Sessions.

This module provides validation and redaction functions for community session
configurations:

1. **Security Settings** (`security.community`):
   - Security configuration for community sessions (credential retrieval permissions)
   - Validated by `validate_security_community_config()`

2. **Static Community Sessions** (`community.sessions`):
   - Pre-configured connections to existing Deephaven Community servers
   - Validated by `validate_community_sessions_config()` and `validate_single_community_session_config()`
   - Redacted by `redact_community_session_config()`

3. **Dynamic Session Creation** (`community.session_creation`):
   - Configuration for on-demand creation of Deephaven Community sessions via Docker or python
   - Validated by `validate_community_session_creation_config()`
   - Redacted by `redact_community_session_creation_config()`

Key Features:
- Type validation for all configuration fields
- Mutual exclusivity checks (e.g., auth_token vs auth_token_env_var)
- Numeric range validation (positive values, non-negative counts)
- Content validation for lists and dicts
- Sensitive data redaction for secure logging
- Warnings for unknown auth_type values (custom authenticators are allowed)

All validation errors raise `CommunitySessionConfigurationError` with descriptive messages.
"""

__all__ = [
    "validate_security_community_config",
    "validate_community_sessions_config",
    "validate_single_community_session_config",
    "redact_community_session_config",
    "validate_community_session_creation_config",
    "redact_community_session_creation_config",
]

import copy
import logging
import types
from typing import Any

from deephaven_mcp._exceptions import CommunitySessionConfigurationError

_LOGGER = logging.getLogger(__name__)


# Known auth_type values from Deephaven Python client documentation
_KNOWN_AUTH_TYPES: set[str] = {
    "Anonymous",  # Default, no authentication required
    "Basic",  # Requires username:password format in auth_token
    "io.deephaven.authentication.psk.PskAuthenticationHandler",  # Requires auth_token
}
"""
Set of commonly known auth_type values for Deephaven Python client.
Custom authenticator strings are also valid but not listed here.
"""


_ALLOWED_COMMUNITY_SESSION_FIELDS: dict[str, type | tuple[type, ...]] = {
    "host": str,
    "port": int,
    "auth_type": str,
    "auth_token": str,  # Direct authentication token
    "auth_token_env_var": str,  # Environment variable for auth token
    "never_timeout": bool,
    "session_type": str,
    "use_tls": bool,
    "tls_root_certs": (str, types.NoneType),
    "client_cert_chain": (str, types.NoneType),
    "client_private_key": (str, types.NoneType),
}
"""
Dictionary of allowed community session configuration fields and their expected types.
Type: dict[str, type | tuple[type, ...]]
"""

_REQUIRED_FIELDS: list[str] = []
"""
list[str]: List of required fields for each community session configuration dictionary.
"""


def redact_community_session_config(
    session_config: dict[str, Any], redact_binary_values: bool = True
) -> dict[str, Any]:
    """
    Redacts sensitive fields from a community session configuration dictionary.

    Creates a shallow copy of the input dictionary and redacts all sensitive fields:
    - 'auth_token' (always redacted if present)
    - 'tls_root_certs', 'client_cert_chain', 'client_private_key' (redacted if value is binary and redact_binary_values is True)

    The original dictionary is not modified. Sensitive fields are replaced with the string "[REDACTED]".

    Args:
        session_config (dict[str, Any]): The community session configuration.
        redact_binary_values (bool): Whether to redact binary values for certain fields (default: True).
            If False, only auth_token is redacted; binary TLS fields are preserved.

    Returns:
        dict[str, Any]: A new dictionary with sensitive fields redacted.

    Example:
        >>> config = {"host": "localhost", "auth_token": "secret123", "port": 10000}
        >>> redacted = redact_community_session_config(config)
        >>> print(redacted)
        {'host': 'localhost', 'auth_token': '[REDACTED]', 'port': 10000}
        >>> print(config["auth_token"])  # Original unchanged
        'secret123'
    """
    config_copy = dict(session_config)
    sensitive_keys = [
        "auth_token",
        "tls_root_certs",
        "client_cert_chain",
        "client_private_key",
    ]
    for key in sensitive_keys:
        if key in config_copy and config_copy[key]:
            if key == "auth_token":
                config_copy[key] = "[REDACTED]"  # noqa: S105
            elif redact_binary_values and isinstance(
                config_copy[key], bytes | bytearray
            ):
                config_copy[key] = "[REDACTED]"
    return config_copy


# Valid values for credential_retrieval_mode
_VALID_CREDENTIAL_RETRIEVAL_MODES = {"none", "dynamic_only", "static_only", "all"}


def validate_security_community_config(security_community_config: Any | None) -> None:
    """
    Validate the 'security.community' configuration section.

    Validates security settings for community sessions from the top-level 'security' section.
    Currently validates the 'credential_retrieval_mode' field which controls which community
    session credentials can be retrieved via the session_community_credentials MCP tool.

    Valid credential_retrieval_mode values:
    - "none" (default): Credential retrieval disabled for all sessions (most secure)
    - "dynamic_only": Only auto-generated tokens (dynamic sessions) can be retrieved
    - "static_only": Only pre-configured tokens (static sessions) can be retrieved
    - "all": Both dynamic and static session credentials can be retrieved

    Args:
        security_community_config (dict[str, Any] | None): The security.community configuration dictionary.
            Can be None if the 'security.community' keys are absent.

    Raises:
        CommunitySessionConfigurationError: If the config is not a dict, or if
            credential_retrieval_mode is present but not a valid string enum value.
    """
    if security_community_config is None:
        return

    if not isinstance(security_community_config, dict):
        raise CommunitySessionConfigurationError(
            "'security.community' must be a dictionary in configuration"
        )

    # Validate credential_retrieval_mode if present
    if "credential_retrieval_mode" in security_community_config:
        value = security_community_config["credential_retrieval_mode"]
        if not isinstance(value, str):
            raise CommunitySessionConfigurationError(
                f"'security.community.credential_retrieval_mode' must be a string, got {type(value).__name__}"
            )
        if value not in _VALID_CREDENTIAL_RETRIEVAL_MODES:
            valid_modes = '", "'.join(sorted(_VALID_CREDENTIAL_RETRIEVAL_MODES))
            raise CommunitySessionConfigurationError(
                f'\'security.community.credential_retrieval_mode\' must be one of: "{valid_modes}", got "{value}"'
            )


def validate_community_sessions_config(
    community_sessions_map: Any | None,
) -> None:
    """
    Validate the overall 'community_sessions' part of the configuration, if present.

    If `community_sessions_map` is None (i.e., the 'community_sessions' key was absent
    from the main configuration), this function does nothing.
    If `community_sessions_map` is provided, this checks that it's a dictionary
    and that each individual session configuration within it is valid.
    An empty dictionary is allowed, signifying no sessions are configured under this key.

    Args:
        community_sessions_map (dict[str, Any] | None): The dictionary of community sessions
            (e.g., config.get('community_sessions')). Can be None if the key is absent.

    Raises:
        CommunitySessionConfigurationError: If `community_sessions_map` is provided and is not a dict,
            or if any individual session config is invalid (as determined by
            `validate_single_community_session_config`).
    """
    if community_sessions_map is None:
        # If 'community_sessions' key was absent from config, there's nothing to validate here.
        return

    if not isinstance(community_sessions_map, dict):
        _LOGGER.error(
            f"[config:validate_community_sessions_config] 'community_sessions' must be a dictionary in Deephaven community session config, got {type(community_sessions_map).__name__}"
        )
        raise CommunitySessionConfigurationError(
            "'community_sessions' must be a dictionary in Deephaven community session config"
        )

    for session_name, session_config_item in community_sessions_map.items():
        validate_single_community_session_config(session_name, session_config_item)


def _validate_field_types(session_name: str, config_item: dict[str, Any]) -> None:
    """Validate field types for a community session configuration.

    Checks that all fields in the configuration are known (present in _ALLOWED_COMMUNITY_SESSION_FIELDS)
    and that their values match the expected types. Handles both single types and tuple types (union types).

    Args:
        session_name (str): The name of the community session being validated.
        config_item (dict[str, Any]): The configuration dictionary to validate.

    Returns:
        None

    Raises:
        CommunitySessionConfigurationError: If unknown fields are present or field types don't match expected types.
    """
    for field_name, field_value in config_item.items():
        if field_name not in _ALLOWED_COMMUNITY_SESSION_FIELDS:
            raise CommunitySessionConfigurationError(
                f"Unknown field '{field_name}' in community session config for {session_name}"
            )

        allowed_types = _ALLOWED_COMMUNITY_SESSION_FIELDS[field_name]
        if isinstance(allowed_types, tuple):
            if not isinstance(field_value, allowed_types):
                expected_type_names = ", ".join(t.__name__ for t in allowed_types)
                raise CommunitySessionConfigurationError(
                    f"Field '{field_name}' in community session config for {session_name} "
                    f"must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
        elif not isinstance(field_value, allowed_types):
            raise CommunitySessionConfigurationError(
                f"Field '{field_name}' in community session config for {session_name} "
                f"must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )


def _validate_auth_configuration(
    session_name: str, config_item: dict[str, Any]
) -> None:
    """Validate authentication-related configuration for a community session.

    Checks mutual exclusivity of auth_token and auth_token_env_var, and logs warnings
    for unknown auth_type values (custom authenticators are allowed but generate a warning).

    Args:
        session_name (str): The name of the community session being validated.
        config_item (dict[str, Any]): The configuration dictionary to validate.

    Returns:
        None

    Raises:
        CommunitySessionConfigurationError: If both auth_token and auth_token_env_var are set.
    """
    # Check for mutual exclusivity of auth_token and auth_token_env_var
    if "auth_token" in config_item and "auth_token_env_var" in config_item:
        raise CommunitySessionConfigurationError(
            f"In community session config for '{session_name}', both 'auth_token' and 'auth_token_env_var' are set. "
            "Please use only one."
        )

    # Check auth_type value and log if it's not a known value
    if "auth_type" in config_item:
        auth_type_value = config_item["auth_type"]
        if auth_type_value not in _KNOWN_AUTH_TYPES:
            _LOGGER.warning(
                f"[config:validate_single_community_session_config] Community session config for '{session_name}' uses auth_type='{auth_type_value}' which is not a commonly known value. "
                f"Known values are: {', '.join(sorted(_KNOWN_AUTH_TYPES))}. Custom authenticators are also valid - if this is intentional, you can ignore this warning."
            )


def validate_single_community_session_config(
    session_name: str,
    config_item: dict[str, Any],
) -> None:
    """Validate a single community session's configuration.

    Performs comprehensive validation including type checking, mutual exclusivity checks,
    and required field validation. Currently _REQUIRED_FIELDS is empty, so no fields are
    required.

    Args:
        session_name (str): The name of the community session.
        config_item (dict[str, Any]): The configuration dictionary for the session.

    Returns:
        None

    Raises:
        CommunitySessionConfigurationError: If the configuration item is invalid (e.g., not a
            dictionary, unknown fields, wrong types, mutually exclusive fields like
            'auth_token' and 'auth_token_env_var' are both set, or missing required
            fields if any were defined in `_REQUIRED_FIELDS`).
    """
    if not isinstance(config_item, dict):
        raise CommunitySessionConfigurationError(
            f"Community session config for {session_name} must be a dictionary, got {type(config_item)}"
        )

    _validate_field_types(session_name, config_item)
    _validate_auth_configuration(session_name, config_item)

    for required_field in _REQUIRED_FIELDS:
        if required_field not in config_item:
            raise CommunitySessionConfigurationError(
                f"Missing required field '{required_field}' in community session config for {session_name}"
            )


# Session creation configuration constants
_ALLOWED_LAUNCH_METHODS: set[str] = {"docker", "python"}
"""Set of allowed launch methods for dynamic community session creation."""

_ALLOWED_SESSION_CREATION_FIELDS: dict[str, type | tuple[type, ...]] = {
    "max_concurrent_sessions": int,
    "defaults": dict,
}
"""Dictionary of allowed session_creation configuration fields and their expected types."""

_ALLOWED_SESSION_CREATION_DEFAULTS: dict[str, type | tuple[type, ...]] = {
    "launch_method": str,
    "auth_type": str,
    "auth_token": (str, types.NoneType),
    "auth_token_env_var": (str, types.NoneType),
    "docker_image": str,
    "docker_memory_limit_gb": (float, int, types.NoneType),
    "docker_cpu_limit": (float, int, types.NoneType),
    "docker_volumes": list,
    "python_venv_path": (str, types.NoneType),
    "heap_size_gb": int,
    "extra_jvm_args": list,
    "environment_vars": dict,
    "startup_timeout_seconds": (float, int),
    "startup_check_interval_seconds": (float, int),
    "startup_retries": int,
}
"""Dictionary of allowed session_creation.defaults fields and their expected types."""


def redact_community_session_creation_config(
    session_creation_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Redacts sensitive fields from a session_creation configuration dictionary.

    Creates a deep copy of the input dictionary and redacts sensitive fields in the defaults section:
    - 'auth_token' (always redacted if present in defaults)

    The original dictionary is not modified. Sensitive fields are replaced with the string "[REDACTED]".
    Note: auth_token_env_var is NOT redacted as it only contains the environment variable name,
    not the actual token value.

    Args:
        session_creation_config (dict[str, Any]): The session_creation configuration.

    Returns:
        dict[str, Any]: A new dictionary with sensitive fields redacted.

    Example:
        >>> config = {
        ...     "max_concurrent_sessions": 5,
        ...     "defaults": {"auth_token": "secret", "launch_method": "docker"}
        ... }
        >>> redacted = redact_community_session_creation_config(config)
        >>> print(redacted["defaults"]["auth_token"])
        '[REDACTED]'
        >>> print(config["defaults"]["auth_token"])  # Original unchanged
        'secret'
    """
    config_copy = copy.deepcopy(session_creation_config)
    if "defaults" in config_copy and isinstance(config_copy["defaults"], dict):
        if "auth_token" in config_copy["defaults"]:
            config_copy["defaults"]["auth_token"] = "[REDACTED]"  # noqa: S105
    return config_copy


def validate_community_session_creation_config(
    session_creation_config: Any | None,
) -> None:
    """
    Validate the 'session_creation' configuration for community sessions.

    This validates the configuration used for dynamically creating community sessions
    via Docker or python-based Deephaven. Performs comprehensive validation including:
    - Type checking for all fields
    - Validation that max_concurrent_sessions is non-negative
    - Validation of the 'defaults' section (if present) including:
      * launch_method must be 'docker' or 'python'
      * Mutual exclusivity of auth_token and auth_token_env_var
      * Positive values for sizes and timeouts
      * Non-negative value for startup_retries
      * String content validation for docker_volumes and extra_jvm_args lists
      * String key/value validation for environment_vars dict

    Args:
        session_creation_config (dict[str, Any] | None): The session_creation configuration.
            Can be None if the key is absent (in which case validation is skipped).

    Raises:
        CommunitySessionConfigurationError: If the configuration is invalid, including:
            - Not a dictionary when provided
            - Unknown fields present
            - Field types don't match expected types
            - Numeric values out of valid range
            - Invalid launch_method value
            - Both auth_token and auth_token_env_var set
            - List/dict items have wrong types
    """
    if session_creation_config is None:
        return

    if not isinstance(session_creation_config, dict):
        _LOGGER.error(
            f"[config:validate_community_session_creation_config] 'session_creation' must be a dictionary in community config, got {type(session_creation_config).__name__}"
        )
        raise CommunitySessionConfigurationError(
            "'session_creation' must be a dictionary in community config"
        )

    # Validate top-level fields
    for field_name, field_value in session_creation_config.items():
        if field_name not in _ALLOWED_SESSION_CREATION_FIELDS:
            raise CommunitySessionConfigurationError(
                f"Unknown field '{field_name}' in session_creation config"
            )

        allowed_types = _ALLOWED_SESSION_CREATION_FIELDS[field_name]
        # Note: Currently all types in _ALLOWED_SESSION_CREATION_FIELDS are single types (not tuples)
        # If tuple types are added in the future, add the tuple handling here
        if not isinstance(field_value, allowed_types):
            raise CommunitySessionConfigurationError(
                f"Field '{field_name}' in session_creation config "
                f"must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )

    # Validate max_concurrent_sessions if present
    if "max_concurrent_sessions" in session_creation_config:
        max_sessions = session_creation_config["max_concurrent_sessions"]
        if max_sessions < 0:
            raise CommunitySessionConfigurationError(
                f"'max_concurrent_sessions' must be non-negative, got {max_sessions}"
            )

    # Validate defaults section if present
    if "defaults" in session_creation_config:
        defaults = session_creation_config["defaults"]
        _validate_session_creation_defaults(defaults)


def _validate_defaults_field_types(defaults: dict[str, Any]) -> None:
    """Validate that all session creation defaults fields have correct types.

    Args:
        defaults: The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If field type is invalid.
    """
    for field_name, field_value in defaults.items():
        if field_name not in _ALLOWED_SESSION_CREATION_DEFAULTS:
            raise CommunitySessionConfigurationError(
                f"Unknown field '{field_name}' in session_creation.defaults config"
            )

        allowed_types = _ALLOWED_SESSION_CREATION_DEFAULTS[field_name]
        if isinstance(allowed_types, tuple):
            if not isinstance(field_value, allowed_types):
                expected_type_names = ", ".join(t.__name__ for t in allowed_types)
                raise CommunitySessionConfigurationError(
                    f"Field '{field_name}' in session_creation.defaults "
                    f"must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
        elif not isinstance(field_value, allowed_types):
            raise CommunitySessionConfigurationError(
                f"Field '{field_name}' in session_creation.defaults "
                f"must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )


def _validate_defaults_enum_fields(defaults: dict[str, Any]) -> None:
    """Validate enum-like defaults fields (launch_method, auth_type).

    Args:
        defaults: The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If enum value is invalid.
    """
    if "launch_method" in defaults:
        launch_method = defaults["launch_method"]
        if launch_method not in _ALLOWED_LAUNCH_METHODS:
            raise CommunitySessionConfigurationError(
                f"'launch_method' must be one of {_ALLOWED_LAUNCH_METHODS}, got '{launch_method}'"
            )

    if "auth_type" in defaults:
        auth_type = defaults["auth_type"]
        if auth_type not in _KNOWN_AUTH_TYPES:
            _LOGGER.warning(
                f"[config:_validate_enum_fields] session_creation.defaults uses auth_type='{auth_type}' which is not a commonly known value. "
                f"Known values are: {', '.join(sorted(_KNOWN_AUTH_TYPES))}. Custom authenticators are also valid - if this is intentional, you can ignore this warning."
            )

    if "auth_token" in defaults and "auth_token_env_var" in defaults:
        raise CommunitySessionConfigurationError(
            "In session_creation.defaults, both 'auth_token' and 'auth_token_env_var' are set. "
            "Please use only one."
        )


def _validate_positive_number(field_name: str, value: float | int) -> None:
    """Validate a numeric field is positive.

    Args:
        field_name: Name of the field being validated.
        value: Numeric value to check.

    Raises:
        CommunitySessionConfigurationError: If value is not positive.
    """
    if value <= 0:
        raise CommunitySessionConfigurationError(
            f"'{field_name}' must be positive, got {value}"
        )


def _validate_string_list(field_name: str, items: list) -> None:
    """Validate a list contains only strings.

    Args:
        field_name: Name of the field being validated.
        items: List to check.

    Raises:
        CommunitySessionConfigurationError: If list contains non-string items.
    """
    for i, item in enumerate(items):
        if not isinstance(item, str):
            raise CommunitySessionConfigurationError(
                f"'{field_name}[{i}]' must be a string, got {type(item).__name__}"
            )


def _validate_defaults_numeric_ranges(defaults: dict[str, Any]) -> None:
    """Validate numeric defaults fields are within valid ranges.

    Args:
        defaults: The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If numeric value is out of range.
    """
    if "heap_size_gb" in defaults:
        _validate_positive_number("heap_size_gb", defaults["heap_size_gb"])

    if (
        "docker_memory_limit_gb" in defaults
        and defaults["docker_memory_limit_gb"] is not None
    ):
        _validate_positive_number(
            "docker_memory_limit_gb", defaults["docker_memory_limit_gb"]
        )

    if "docker_cpu_limit" in defaults and defaults["docker_cpu_limit"] is not None:
        _validate_positive_number("docker_cpu_limit", defaults["docker_cpu_limit"])

    if "startup_timeout_seconds" in defaults:
        _validate_positive_number(
            "startup_timeout_seconds", defaults["startup_timeout_seconds"]
        )

    if "startup_check_interval_seconds" in defaults:
        _validate_positive_number(
            "startup_check_interval_seconds", defaults["startup_check_interval_seconds"]
        )

    if "startup_retries" in defaults:
        retries = defaults["startup_retries"]
        if retries < 0:
            raise CommunitySessionConfigurationError(
                f"'startup_retries' must be non-negative, got {retries}"
            )


def _validate_defaults_collection_contents(defaults: dict[str, Any]) -> None:
    """Validate list and dict defaults fields contain correct types.

    Args:
        defaults: The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If collection contents are invalid.
    """
    if "docker_volumes" in defaults:
        _validate_string_list("docker_volumes", defaults["docker_volumes"])

    if "extra_jvm_args" in defaults:
        _validate_string_list("extra_jvm_args", defaults["extra_jvm_args"])

    if "environment_vars" in defaults:
        env_vars = defaults["environment_vars"]
        for key, value in env_vars.items():
            if not isinstance(key, str):
                raise CommunitySessionConfigurationError(
                    f"'environment_vars' key must be a string, got {type(key).__name__}"
                )
            if not isinstance(value, str):
                raise CommunitySessionConfigurationError(
                    f"'environment_vars[{key}]' value must be a string, got {type(value).__name__}"
                )


def _validate_session_creation_defaults(defaults: dict[str, Any]) -> None:
    """Validate the defaults section of session_creation configuration.

    This orchestrates all validation steps using dedicated helper functions for each
    validation category: field types, enum values, numeric ranges, and collection contents.

    Args:
        defaults: The defaults dictionary from session_creation configuration.

    Raises:
        CommunitySessionConfigurationError: If any validation check fails.
    """
    _validate_defaults_field_types(defaults)
    _validate_defaults_enum_fields(defaults)
    _validate_defaults_numeric_ranges(defaults)
    _validate_defaults_collection_contents(defaults)
