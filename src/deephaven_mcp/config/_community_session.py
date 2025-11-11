"""
Configuration handling specific to Deephaven Community Sessions.

This module provides validation and redaction functions for two types of community
session configurations:

1. **Static Community Sessions** (`community.sessions`):
   - Pre-configured connections to existing Deephaven Community servers
   - Validated by `validate_community_sessions_config()` and `validate_single_community_session_config()`
   - Redacted by `redact_community_session_config()`

2. **Dynamic Session Creation** (`community.session_creation`):
   - Configuration for on-demand creation of Deephaven Community sessions via Docker or pip
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


_ALLOWED_COMMUNITY_SESSION_FIELDS: dict[str, type | tuple[type, type]] = {
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
            "'community_sessions' must be a dictionary in Deephaven community session config, got %s",
            type(community_sessions_map).__name__,
        )
        raise CommunitySessionConfigurationError(
            "'community_sessions' must be a dictionary in Deephaven community session config"
        )

    for session_name, session_config_item in community_sessions_map.items():
        validate_single_community_session_config(session_name, session_config_item)


def _validate_field_types(session_name: str, config_item: dict[str, Any]) -> None:
    """Validate field types for a community session configuration.
    
    Args:
        session_name (str): The name of the community session being validated.
        config_item (dict[str, Any]): The configuration dictionary to validate.
        
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
    for unknown auth_type values.
    
    Args:
        session_name (str): The name of the community session being validated.
        config_item (dict[str, Any]): The configuration dictionary to validate.
        
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
                "Community session config for '%s' uses auth_type='%s' which is not a commonly known value. "
                "Known values are: %s. Custom authenticators are also valid - if this is intentional, you can ignore this warning.",
                session_name,
                auth_type_value,
                ", ".join(sorted(_KNOWN_AUTH_TYPES)),
            )


def validate_single_community_session_config(
    session_name: str,
    config_item: dict[str, Any],
) -> None:
    """
    Validate a single community session's configuration.

    Args:
        session_name (str): The name of the community session.
        config_item (dict[str, Any]): The configuration dictionary for the session.

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
_ALLOWED_LAUNCH_METHODS: set[str] = {"docker", "pip"}
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
    "heap_size_gb": int,
    "extra_jvm_args": list,
    "environment_vars": dict,
    "startup_timeout_seconds": (float, int),
    "startup_check_interval_seconds": (float, int),
    "startup_retries": int,
}
"""Dictionary of allowed session_creation.defaults fields and their expected types."""


def redact_community_session_creation_config(
    session_creation_config: dict[str, Any]
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
    via Docker or pip-installed Deephaven. Performs comprehensive validation including:
    - Type checking for all fields
    - Validation that max_concurrent_sessions is non-negative
    - Validation of the 'defaults' section (if present) including:
      * launch_method must be 'docker' or 'pip'
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
            "'session_creation' must be a dictionary in community config, got %s",
            type(session_creation_config).__name__,
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


def _validate_session_creation_defaults(defaults: dict[str, Any]) -> None:
    """Validate the defaults section of session_creation configuration.
    
    Validates field types, launch_method values, auth_type values, mutual exclusivity
    of auth_token and auth_token_env_var, numeric ranges (positive values for sizes/timeouts,
    non-negative for retries), and content validation for lists and dicts (docker_volumes,
    extra_jvm_args, environment_vars must contain appropriate string types).
    
    Args:
        defaults (dict[str, Any]): The defaults dictionary from session_creation configuration.
        
    Raises:
        CommunitySessionConfigurationError: If any validation check fails.
    """
    # Validate field types
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

    # Validate launch_method value
    if "launch_method" in defaults:
        launch_method = defaults["launch_method"]
        if launch_method not in _ALLOWED_LAUNCH_METHODS:
            raise CommunitySessionConfigurationError(
                f"'launch_method' must be one of {_ALLOWED_LAUNCH_METHODS}, got '{launch_method}'"
            )

    # Validate auth_type value
    if "auth_type" in defaults:
        auth_type = defaults["auth_type"]
        if auth_type not in _KNOWN_AUTH_TYPES:
            _LOGGER.warning(
                "session_creation.defaults uses auth_type='%s' which is not a commonly known value. "
                "Known values are: %s. Custom authenticators are also valid - if this is intentional, you can ignore this warning.",
                auth_type,
                ", ".join(sorted(_KNOWN_AUTH_TYPES)),
            )

    # Validate mutual exclusivity of auth_token and auth_token_env_var
    if "auth_token" in defaults and "auth_token_env_var" in defaults:
        raise CommunitySessionConfigurationError(
            "In session_creation.defaults, both 'auth_token' and 'auth_token_env_var' are set. "
            "Please use only one."
        )

    # Validate numeric ranges
    if "heap_size_gb" in defaults:
        heap_size = defaults["heap_size_gb"]
        if heap_size <= 0:
            raise CommunitySessionConfigurationError(
                f"'heap_size_gb' must be positive, got {heap_size}"
            )

    if "docker_memory_limit_gb" in defaults and defaults["docker_memory_limit_gb"] is not None:
        memory_limit = defaults["docker_memory_limit_gb"]
        if memory_limit <= 0:
            raise CommunitySessionConfigurationError(
                f"'docker_memory_limit_gb' must be positive, got {memory_limit}"
            )

    if "docker_cpu_limit" in defaults and defaults["docker_cpu_limit"] is not None:
        cpu_limit = defaults["docker_cpu_limit"]
        if cpu_limit <= 0:
            raise CommunitySessionConfigurationError(
                f"'docker_cpu_limit' must be positive, got {cpu_limit}"
            )

    if "startup_timeout_seconds" in defaults:
        timeout = defaults["startup_timeout_seconds"]
        if timeout <= 0:
            raise CommunitySessionConfigurationError(
                f"'startup_timeout_seconds' must be positive, got {timeout}"
            )

    if "startup_check_interval_seconds" in defaults:
        interval = defaults["startup_check_interval_seconds"]
        if interval <= 0:
            raise CommunitySessionConfigurationError(
                f"'startup_check_interval_seconds' must be positive, got {interval}"
            )

    if "startup_retries" in defaults:
        retries = defaults["startup_retries"]
        if retries < 0:
            raise CommunitySessionConfigurationError(
                f"'startup_retries' must be non-negative, got {retries}"
            )

    # Validate docker_volumes is a list of strings
    if "docker_volumes" in defaults:
        volumes = defaults["docker_volumes"]
        for i, volume in enumerate(volumes):
            if not isinstance(volume, str):
                raise CommunitySessionConfigurationError(
                    f"'docker_volumes[{i}]' must be a string, got {type(volume).__name__}"
                )

    # Validate extra_jvm_args is a list of strings
    if "extra_jvm_args" in defaults:
        jvm_args = defaults["extra_jvm_args"]
        for i, arg in enumerate(jvm_args):
            if not isinstance(arg, str):
                raise CommunitySessionConfigurationError(
                    f"'extra_jvm_args[{i}]' must be a string, got {type(arg).__name__}"
                )

    # Validate environment_vars is a dict with string keys and values
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

