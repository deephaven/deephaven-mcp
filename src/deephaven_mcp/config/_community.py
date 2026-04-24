"""
Configuration handling specific to Deephaven Community Sessions.

This module provides validation and redaction functions for community session
configurations, as well as the top-level community config validation engine and the
:class:`CommunityServerConfigManager` for the DHC MCP server.

Community config file format (flat — all keys at top level)::

    {
        "security": {"credential_retrieval_mode": "dynamic_only"},
        "sessions": {
            "local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "..."}
        },
        "session_creation": {"defaults": {"launch_method": "python"}}
    }

Valid top-level keys: ``sessions``, ``session_creation``, ``security``.

1. **Security Settings** (``security``):
   - Security configuration for community sessions (credential retrieval permissions)
   - Validated by `validate_security_config()`

2. **Static Community Sessions** (``sessions``):
   - Pre-configured connections to existing Deephaven Community servers
   - Validated by `validate_community_sessions_config()` and `validate_single_community_session_config()`
   - Redacted by `redact_community_session_config()`

3. **Dynamic Session Creation** (``session_creation``):
   - Configuration for on-demand creation of Deephaven Community sessions via Docker or Python
   - Validated by `validate_community_session_creation_config()`
   - Redacted by `redact_community_session_creation_config()`

4. **Top-level Community Config Validation** (internal):
   - `_validate_community_config()` validates the full community config structure against the schema
   - `_apply_redaction_to_config()` redacts all sensitive fields for safe logging
   - `_get_config_section()` and `_get_all_config_names()` navigate the config tree

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
    "CommunityServerConfigManager",
    "validate_security_config",
    "validate_community_sessions_config",
    "validate_single_community_session_config",
    "redact_community_session_config",
    "validate_community_session_creation_config",
    "redact_community_session_creation_config",
]

import copy
import logging
import types
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    ConfigurationError,
)

from ._base import (
    ConfigManager,
    _get_config_path,
    _load_and_validate_config,
    _log_config_summary,
)

_LOGGER = logging.getLogger(__name__)


# Known auth_type values from Deephaven Python client documentation
_KNOWN_AUTH_TYPES: set[str] = {
    "PSK",  # Canonical shorthand for PSK authentication
    "Anonymous",  # Default, no authentication required
    "Basic",  # Requires username:password format in auth_token
    "io.deephaven.authentication.psk.PskAuthenticationHandler",  # Full PSK class name
}
"""
Set of commonly known auth_type values for Deephaven Python client.
Includes both canonical shorthand forms (PSK, Anonymous, Basic) and full class names.
Note: For config validation, these are case-sensitive canonical forms.
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

Maps field names to their allowed Python types. Fields with tuple values accept multiple
types (union types). Used for validating static community session configurations.

Type: dict[str, type | tuple[type, ...]]
"""

_REQUIRED_FIELDS: list[str] = []
"""
List of required fields for each community session configuration dictionary.

Currently empty - all fields are optional for static community sessions.
This allows maximum flexibility in configuration.

Type: list[str]
"""


def redact_community_session_config(
    session_config: dict[str, Any], redact_binary_values: bool = True
) -> dict[str, Any]:
    """Redact sensitive fields from a community session configuration dictionary.

    Creates a shallow copy of the input dictionary and redacts all sensitive fields:
    - 'auth_token': Redacted if present and truthy (non-empty)
    - 'tls_root_certs', 'client_cert_chain', 'client_private_key': Redacted only if value
      is truthy, binary (bytes/bytearray), and redact_binary_values is True

    Uses shallow copy for performance since nested structures are not expected in session configs.
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
"""
Valid values for the ``security.credential_retrieval_mode`` configuration field
(top-level ``security`` section; the schema is flat — there is no ``community``
wrapper). Controls which community session credentials can be retrieved via MCP
tools. If the field is absent the effective default used by consumers is
``"none"``.
"""


def validate_security_config(security_config: Any | None) -> None:
    """Validate the 'security' configuration section.

    Validates security settings for community sessions from the top-level 'security' section.
    Currently validates the 'credential_retrieval_mode' field which controls which community
    session credentials can be retrieved via the session_community_credentials MCP tool.

    Valid credential_retrieval_mode values:
    - "none" (default): Credential retrieval disabled for all sessions (most secure)
    - "dynamic_only": Only auto-generated tokens (dynamic sessions) can be retrieved
    - "static_only": Only pre-configured tokens (static sessions) can be retrieved
    - "all": Both dynamic and static session credentials can be retrieved

    Args:
        security_config (dict[str, Any] | None): The security configuration dictionary.
            Can be None if the 'security' key is absent.

    Raises:
        CommunitySessionConfigurationError: If the config is not a dict, or if
            credential_retrieval_mode is present but not a valid string enum value.
    """
    if security_config is None:
        return

    if not isinstance(security_config, dict):
        _LOGGER.error(
            f"[config:validate_security_config] 'security' must be a dictionary, got {type(security_config).__name__}"
        )
        raise CommunitySessionConfigurationError(
            "'security' must be a dictionary in configuration"
        )

    # Validate credential_retrieval_mode if present
    if "credential_retrieval_mode" in security_config:
        value = security_config["credential_retrieval_mode"]
        if not isinstance(value, str):
            _LOGGER.error(
                f"[config:validate_security_config] 'security.credential_retrieval_mode' must be a string, got {type(value).__name__}"
            )
            raise CommunitySessionConfigurationError(
                f"'security.credential_retrieval_mode' must be a string, got {type(value).__name__}"
            )
        if value not in _VALID_CREDENTIAL_RETRIEVAL_MODES:
            valid_modes = (
                '"' + '", "'.join(sorted(_VALID_CREDENTIAL_RETRIEVAL_MODES)) + '"'
            )
            _LOGGER.error(
                f"[config:validate_security_config] 'security.credential_retrieval_mode' must be one of: {valid_modes}, got '{value}'"
            )
            raise CommunitySessionConfigurationError(
                f'\'security.credential_retrieval_mode\' must be one of: "{valid_modes}", got "{value}"'
            )


def validate_community_sessions_config(
    community_sessions_map: Any | None,
) -> None:
    """Validate the overall 'sessions' configuration section, if present.

    This validates the dictionary of static community sessions defined in the configuration.
    If `community_sessions_map` is None (i.e., the 'sessions' key was absent
    from the main configuration), this function does nothing.

    If `community_sessions_map` is provided, this checks that it's a dictionary
    and that each individual session configuration within it is valid.
    An empty dictionary is allowed, signifying no sessions are configured under this key.

    Args:
        community_sessions_map (dict[str, Any] | None): The dictionary of static community sessions,
            where keys are session names and values are session config dicts.
            Can be None if the 'sessions' key is absent.

    Raises:
        CommunitySessionConfigurationError: If `community_sessions_map` is provided and is not a dict,
            or if any individual session config is invalid (as determined by
            `validate_single_community_session_config`).
    """
    if community_sessions_map is None:
        # If 'sessions' key was absent from config, there's nothing to validate here.
        return

    if not isinstance(community_sessions_map, dict):
        _LOGGER.error(
            f"[config:validate_community_sessions_config] 'sessions' must be a dictionary in Deephaven community session config, got {type(community_sessions_map).__name__}"
        )
        raise CommunitySessionConfigurationError(
            "'sessions' must be a dictionary in Deephaven community session config"
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

    Raises:
        CommunitySessionConfigurationError: If unknown fields are present or field types don't match expected types.
    """
    for field_name, field_value in config_item.items():
        if field_name not in _ALLOWED_COMMUNITY_SESSION_FIELDS:
            _LOGGER.error(
                f"[config:validate_single_community_session_config] Unknown field '{field_name}' in community session config for '{session_name}'"
            )
            raise CommunitySessionConfigurationError(
                f"Unknown field '{field_name}' in community session config for {session_name}"
            )

        allowed_types = _ALLOWED_COMMUNITY_SESSION_FIELDS[field_name]
        if isinstance(allowed_types, tuple):
            if not isinstance(field_value, allowed_types):
                expected_type_names = ", ".join(t.__name__ for t in allowed_types)
                _LOGGER.error(
                    f"[config:validate_single_community_session_config] Field '{field_name}' in community session config for '{session_name}' must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
                raise CommunitySessionConfigurationError(
                    f"Field '{field_name}' in community session config for {session_name} "
                    f"must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
        elif not isinstance(field_value, allowed_types):
            _LOGGER.error(
                f"[config:validate_single_community_session_config] Field '{field_name}' in community session config for '{session_name}' must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )
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

    Raises:
        CommunitySessionConfigurationError: If both auth_token and auth_token_env_var are set.
    """
    # Check for mutual exclusivity of auth_token and auth_token_env_var
    if "auth_token" in config_item and "auth_token_env_var" in config_item:
        _LOGGER.error(
            f"[config:validate_single_community_session_config] Community session config for '{session_name}' has both 'auth_token' and 'auth_token_env_var' set; only one is allowed."
        )
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

    Raises:
        CommunitySessionConfigurationError: If the configuration item is invalid (e.g., not a
            dictionary, unknown fields, wrong types, mutually exclusive fields like
            'auth_token' and 'auth_token_env_var' are both set, or missing required
            fields if any were defined in `_REQUIRED_FIELDS`).
    """
    if not isinstance(config_item, dict):
        _LOGGER.error(
            f"[config:validate_single_community_session_config] Community session config for '{session_name}' must be a dictionary, got {type(config_item).__name__}"
        )
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
"""
Set of allowed launch methods for dynamic community session creation.

- 'docker': Launch Deephaven in a Docker container
- 'python': Launch Deephaven using a Python subprocess with optional custom venv
"""

_ALLOWED_SESSION_CREATION_FIELDS: dict[str, type | tuple[type, ...]] = {
    "max_concurrent_sessions": int,
    "defaults": dict,
}
"""
Dictionary of allowed top-level session_creation configuration fields and their expected types.

Used for validating the structure of the 'community.session_creation' config section.
"""

_ALLOWED_SESSION_CREATION_DEFAULTS: dict[str, type | tuple[type, ...]] = {
    "launch_method": str,
    "auth_type": str,
    "auth_token": (str, types.NoneType),
    "auth_token_env_var": (str, types.NoneType),
    "programming_language": str,
    "docker_image": str,
    "docker_memory_limit_gb": (float, int, types.NoneType),
    "docker_cpu_limit": (float, int, types.NoneType),
    "docker_volumes": list,
    "python_venv_path": (str, types.NoneType),
    "heap_size_gb": (int, float),
    "extra_jvm_args": list,
    "environment_vars": dict,
    "startup_timeout_seconds": (float, int),
    "startup_check_interval_seconds": (float, int),
    "startup_retries": int,
}
"""
Dictionary of allowed session_creation.defaults fields and their expected types.

These are default parameters used when dynamically creating new community sessions.
All fields are optional - if not specified, system defaults are used.
"""


def redact_community_session_creation_config(
    session_creation_config: dict[str, Any],
) -> dict[str, Any]:
    """Redact sensitive fields from a session_creation configuration dictionary.

    Creates a deep copy of the input dictionary and redacts sensitive fields in the defaults section:
    - 'auth_token': Always redacted if present in defaults

    Uses deep copy because session_creation configs may contain nested structures (defaults dict).
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
    """Validate the 'session_creation' configuration section.

    This validates the configuration used for dynamically creating community sessions on demand
    via Docker or Python-based Deephaven. Performs comprehensive validation including:
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
        session_creation_config (dict[str, Any] | None): The session_creation configuration dictionary.
            Can be None if the 'session_creation' key is absent (validation is skipped).

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
            _LOGGER.error(
                f"[config:validate_community_session_creation_config] Unknown field '{field_name}' in session_creation config"
            )
            raise CommunitySessionConfigurationError(
                f"Unknown field '{field_name}' in session_creation config"
            )

        allowed_types = _ALLOWED_SESSION_CREATION_FIELDS[field_name]
        # Note: Currently all types in _ALLOWED_SESSION_CREATION_FIELDS are single types (not tuples)
        # If tuple types are added in the future, add the tuple handling here
        if not isinstance(field_value, allowed_types):
            type_name = (
                allowed_types.__name__
                if isinstance(allowed_types, type)
                else " | ".join(t.__name__ for t in allowed_types)
            )
            _LOGGER.error(
                f"[config:validate_community_session_creation_config] Field '{field_name}' in session_creation config must be of type {type_name}, got {type(field_value).__name__}"
            )
            raise CommunitySessionConfigurationError(
                f"Field '{field_name}' in session_creation config "
                f"must be of type {type_name}, got {type(field_value).__name__}"
            )

    # Validate max_concurrent_sessions if present
    if "max_concurrent_sessions" in session_creation_config:
        max_sessions = session_creation_config["max_concurrent_sessions"]
        if max_sessions < 0:
            _LOGGER.error(
                f"[config:validate_community_session_creation_config] 'max_concurrent_sessions' must be non-negative, got {max_sessions}"
            )
            raise CommunitySessionConfigurationError(
                f"'max_concurrent_sessions' must be non-negative, got {max_sessions}"
            )

    # Validate defaults section if present
    if "defaults" in session_creation_config:
        defaults = session_creation_config["defaults"]
        _validate_session_creation_defaults(defaults)


def _validate_defaults_field_types(defaults: dict[str, Any]) -> None:
    """Validate that all session creation defaults fields have correct types.

    Checks each field in the defaults dictionary against _ALLOWED_SESSION_CREATION_DEFAULTS
    to ensure the field is known and its value matches the expected type(s).

    Args:
        defaults (dict[str, Any]): The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If field type is invalid or field is unknown.
    """
    for field_name, field_value in defaults.items():
        if field_name not in _ALLOWED_SESSION_CREATION_DEFAULTS:
            _LOGGER.error(
                f"[config:_validate_defaults_field_types] Unknown field '{field_name}' in session_creation.defaults config"
            )
            raise CommunitySessionConfigurationError(
                f"Unknown field '{field_name}' in session_creation.defaults config"
            )

        allowed_types = _ALLOWED_SESSION_CREATION_DEFAULTS[field_name]
        if isinstance(allowed_types, tuple):
            if not isinstance(field_value, allowed_types):
                expected_type_names = ", ".join(t.__name__ for t in allowed_types)
                _LOGGER.error(
                    f"[config:_validate_defaults_field_types] Field '{field_name}' in session_creation.defaults must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
                raise CommunitySessionConfigurationError(
                    f"Field '{field_name}' in session_creation.defaults "
                    f"must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
        elif not isinstance(field_value, allowed_types):
            _LOGGER.error(
                f"[config:_validate_defaults_field_types] Field '{field_name}' in session_creation.defaults must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )
            raise CommunitySessionConfigurationError(
                f"Field '{field_name}' in session_creation.defaults "
                f"must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )


def _validate_defaults_enum_fields(defaults: dict[str, Any]) -> None:
    """Validate enum-like defaults fields (launch_method, auth_type).

    Checks that launch_method is one of the allowed values and auth_type is a known value
    (logs warning for custom authenticators). Also validates mutual exclusivity of
    auth_token and auth_token_env_var.

    Args:
        defaults (dict[str, Any]): The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If enum value is invalid or both auth_token
            and auth_token_env_var are set.
    """
    if "launch_method" in defaults:
        launch_method = defaults["launch_method"]
        if launch_method not in _ALLOWED_LAUNCH_METHODS:
            _LOGGER.error(
                f"[config:_validate_defaults_enum_fields] session_creation.defaults 'launch_method' must be one of {_ALLOWED_LAUNCH_METHODS}, got '{launch_method}'"
            )
            raise CommunitySessionConfigurationError(
                f"'launch_method' must be one of {_ALLOWED_LAUNCH_METHODS}, got '{launch_method}'"
            )

    if "auth_type" in defaults:
        auth_type = defaults["auth_type"]
        if auth_type not in _KNOWN_AUTH_TYPES:
            _LOGGER.warning(
                f"[config:_validate_defaults_enum_fields] session_creation.defaults uses auth_type='{auth_type}' which is not a commonly known value. "
                f"Known values are: {', '.join(sorted(_KNOWN_AUTH_TYPES))}. Custom authenticators are also valid - if this is intentional, you can ignore this warning."
            )

    if "auth_token" in defaults and "auth_token_env_var" in defaults:
        _LOGGER.error(
            "[config:_validate_defaults_enum_fields] session_creation.defaults has both 'auth_token' and 'auth_token_env_var' set; only one is allowed."
        )
        raise CommunitySessionConfigurationError(
            "In session_creation.defaults, both 'auth_token' and 'auth_token_env_var' are set. "
            "Please use only one."
        )


def _validate_positive_number(field_name: str, value: float | int) -> None:
    """Validate a numeric field is positive.

    Args:
        field_name (str): Name of the field being validated.
        value (float | int): Numeric value to check.

    Raises:
        CommunitySessionConfigurationError: If value is not positive (must be > 0).
    """
    if value <= 0:
        _LOGGER.error(
            f"[config:_validate_positive_number] '{field_name}' must be positive, got {value}"
        )
        raise CommunitySessionConfigurationError(
            f"'{field_name}' must be positive, got {value}"
        )


def _validate_string_list(field_name: str, items: list) -> None:
    """Validate a list contains only strings.

    Args:
        field_name (str): Name of the field being validated.
        items (list): List to check.

    Raises:
        CommunitySessionConfigurationError: If list contains non-string items.
    """
    for i, item in enumerate(items):
        if not isinstance(item, str):
            _LOGGER.error(
                f"[config:_validate_string_list] '{field_name}[{i}]' must be a string, got {type(item).__name__}"
            )
            raise CommunitySessionConfigurationError(
                f"'{field_name}[{i}]' must be a string, got {type(item).__name__}"
            )


def _validate_defaults_numeric_ranges(defaults: dict[str, Any]) -> None:
    """Validate numeric defaults fields are within valid ranges.

    Checks that heap_size_gb, docker_memory_limit_gb, docker_cpu_limit, and timeout fields
    are positive, and that startup_retries is non-negative.

    Args:
        defaults (dict[str, Any]): The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If numeric value is out of range (not positive
            for size/timeout fields, or negative for retry count).
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
            _LOGGER.error(
                f"[config:_validate_defaults_numeric_ranges] 'startup_retries' must be non-negative, got {retries}"
            )
            raise CommunitySessionConfigurationError(
                f"'startup_retries' must be non-negative, got {retries}"
            )


def _validate_defaults_collection_contents(defaults: dict[str, Any]) -> None:
    """Validate list and dict defaults fields contain correct types.

    Checks that docker_volumes and extra_jvm_args lists contain only strings, and that
    environment_vars dict has string keys and string values.

    Args:
        defaults (dict[str, Any]): The defaults dictionary to validate.

    Raises:
        CommunitySessionConfigurationError: If collection contents are invalid (non-string
            items in lists, or non-string keys/values in environment_vars dict).
    """
    if "docker_volumes" in defaults:
        _validate_string_list("docker_volumes", defaults["docker_volumes"])

    if "extra_jvm_args" in defaults:
        _validate_string_list("extra_jvm_args", defaults["extra_jvm_args"])

    if "environment_vars" in defaults:
        env_vars = defaults["environment_vars"]
        for key, value in env_vars.items():
            if not isinstance(key, str):
                _LOGGER.error(
                    f"[config:_validate_defaults_collection_contents] 'environment_vars' key must be a string, got {type(key).__name__}"
                )
                raise CommunitySessionConfigurationError(
                    f"'environment_vars' key must be a string, got {type(key).__name__}"
                )
            if not isinstance(value, str):
                _LOGGER.error(
                    f"[config:_validate_defaults_collection_contents] 'environment_vars[{key}]' value must be a string, got {type(value).__name__}"
                )
                raise CommunitySessionConfigurationError(
                    f"'environment_vars[{key}]' value must be a string, got {type(value).__name__}"
                )


def _validate_session_creation_defaults(defaults: dict[str, Any]) -> None:
    """Validate the defaults section of session_creation configuration.

    This orchestrates all validation steps using dedicated helper functions for each
    validation category: field types, enum values, numeric ranges, and collection contents.

    Args:
        defaults (dict[str, Any]): The defaults dictionary from session_creation configuration.

    Raises:
        CommunitySessionConfigurationError: If any validation check fails.
    """
    _validate_defaults_field_types(defaults)
    _validate_defaults_enum_fields(defaults)
    _validate_defaults_numeric_ranges(defaults)
    _validate_defaults_collection_contents(defaults)


# ---------------------------------------------------------------------------
# Community config validation engine (internal)
# ---------------------------------------------------------------------------


@dataclass
class _ConfigPathSpec:
    """Specification for a valid configuration path in the community config schema.

    Defines the validation and redaction rules for a specific configuration path.
    Used by the validation engine to ensure configuration correctness and security.

    Attributes:
        required (bool): Whether this configuration path must be present. If True and the
            path is missing, validation will fail with ConfigurationError.
        expected_type (type): The expected Python type for values at this path (e.g., dict, str, int).
            Type mismatches will cause validation to fail.
        validator (Callable[[Any], None] | None): Optional validation function for custom validation logic.
            Receives the value at this path and should raise CommunitySessionConfigurationError if
            validation fails (automatically wrapped as ConfigurationError).
            If None, only type validation is performed.
        redactor (Callable[[Any], Any] | None): Optional function to redact sensitive data for safe logging.
            Receives the config value and returns a redacted version (typically replacing sensitive
            strings with "[REDACTED]"). If None, no redaction is applied to this path.
    """

    required: bool
    expected_type: type
    validator: Callable[[Any], None] | None = None
    redactor: Callable[[Any], Any] | None = None


# Schema defining all valid community configuration paths (flat — all at depth 1)
_SCHEMA_PATHS: dict[tuple[str, ...], _ConfigPathSpec] = {
    ("security",): _ConfigPathSpec(
        required=False,
        expected_type=dict,
        validator=validate_security_config,
    ),
    ("sessions",): _ConfigPathSpec(
        required=False,
        expected_type=dict,
        validator=validate_community_sessions_config,
        redactor=lambda sessions_dict: (
            {
                name: redact_community_session_config(cfg)
                for name, cfg in sessions_dict.items()
            }
            if isinstance(sessions_dict, dict)
            else sessions_dict
        ),
    ),
    ("session_creation",): _ConfigPathSpec(
        required=False,
        expected_type=dict,
        validator=validate_community_session_creation_config,
        redactor=redact_community_session_creation_config,
    ),
}


def _get_config_section(
    config: dict[str, Any],
    section: Sequence[str],
) -> Any:
    """Navigate to and retrieve a nested configuration section by path.

    This helper function traverses the configuration dictionary using the provided path sequence,
    returning the value at the final key. Useful for accessing deeply nested configuration values.

    Args:
        config (dict[str, Any]): The root configuration dictionary to navigate.
        section (Sequence[str]): The path to the config section as a sequence of keys.
            For example, ``['sessions', 'local-dev']`` accesses
            ``config['sessions']['local-dev']``.

    Returns:
        Any: The configuration value at the specified path. Can be any type (dict, str, int, list, etc.)
            depending on what's stored at that location.

    Raises:
        KeyError: If any key in the section path does not exist or if any intermediate value is not a dictionary.
            The error message includes the full path for debugging.
    """
    _LOGGER.debug(
        f"[config:_get_config_section] Getting config section for path: {section}"
    )
    curr = config
    for key in section:
        if not isinstance(curr, dict) or key not in curr:
            raise KeyError(f"Section path {section} does not exist in configuration")
        curr = curr[key]
    return curr


def _get_all_config_names(
    config: dict[str, Any],
    section: Sequence[str],
) -> list[str]:
    """Retrieve all configuration names (keys) from a specific section path.

    This helper function is useful for discovering what sessions, systems, or other named entities
    are configured. Returns an empty list if the section doesn't exist or isn't a dictionary,
    making it safe to call without pre-checking.

    Args:
        config (dict[str, Any]): The root configuration dictionary to search within.
        section (Sequence[str]): The path to the config section (e.g., ``['sessions']``
            to get all static community session names).

    Returns:
        list[str]: A list of configuration names (dictionary keys) from the specified section.
            Returns an empty list in two cases:
            1. The section path doesn't exist (KeyError from _get_config_section)
            2. The section exists but is not a dictionary (e.g., it's a string or int)
    """
    _LOGGER.debug(
        f"[config:_get_all_config_names] Getting list of all names from config section path: {section}"
    )
    try:
        section_obj = _get_config_section(config, section)
    except KeyError:
        _LOGGER.warning(
            f"[config:_get_all_config_names] Section path {section} does not exist, returning empty list of names."
        )
        return []

    if not isinstance(section_obj, dict):
        _LOGGER.warning(
            f"[config:_get_all_config_names] Section at path {section} is not a dictionary, returning empty list of names."
        )
        return []

    names = list(section_obj.keys())
    _LOGGER.debug(
        f"[config:_get_all_config_names] Found {len(names)} names in section {section}: {names}"
    )
    return names


def _apply_redaction_to_config(config: dict[str, Any]) -> dict[str, Any]:
    """Apply redaction to sensitive configuration fields for safe logging.

    Creates a deep copy of the configuration and applies all redaction functions defined in
    _SCHEMA_PATHS. This ensures that sensitive data (auth tokens, passwords, private keys, etc.)
    is replaced with "[REDACTED]" before logging. The original configuration is never modified.

    Redaction is applied to each path that has a redactor function defined in _SCHEMA_PATHS.
    If a configuration section doesn't exist, its redaction is silently skipped (no error).

    Args:
        config (dict[str, Any]): The configuration dictionary to redact. This is NOT modified.

    Returns:
        dict[str, Any]: A new deep copy of the configuration with sensitive fields redacted.
            The structure remains identical, only sensitive values are replaced.
    """
    config_copy = copy.deepcopy(config)

    # Apply redaction functions for each configured path
    for path_tuple, spec in _SCHEMA_PATHS.items():
        if spec.redactor is not None:
            try:
                section = _get_config_section(config_copy, list(path_tuple))
                redacted_section = spec.redactor(section)

                # Navigate to the parent and set the redacted section
                current = config_copy
                for key in path_tuple[:-1]:
                    current = current[key]
                current[path_tuple[-1]] = redacted_section

            except KeyError:
                # Section doesn't exist, skip redaction
                continue

    return config_copy


def _validate_unknown_keys(
    data: dict[str, Any], path: tuple[str, ...], valid_keys: set[str]
) -> None:
    """Check for unknown keys at the current path level and raise ConfigurationError if found.

    This validation helper ensures that only known configuration keys are present at the
    specified path level. Any keys found in the data that are not in the valid_keys set
    will cause validation to fail with a detailed error message.

    Args:
        data (dict[str, Any]): The configuration dictionary section to validate.
        path (tuple[str, ...]): The current path tuple for error reporting context.
        valid_keys (set[str]): Set of allowed key names at this path level.

    Raises:
        ConfigurationError: If any unknown keys are found in the data.
    """
    unknown_keys = set(data.keys()) - valid_keys
    if unknown_keys:
        _LOGGER.error(
            f"[config:_validate_community_config] Unknown keys at config path {path}: {unknown_keys}"
        )
        raise ConfigurationError(f"Unknown keys at config path {path}: {unknown_keys}")


def _validate_required_keys(
    data: dict[str, Any], path: tuple[str, ...], required_keys: set[str]
) -> None:
    """Check for missing required keys at the current path level and raise ConfigurationError if any are missing.

    This validation helper ensures that all required configuration keys are present at the
    specified path level. Any required keys that are missing from the data will cause
    validation to fail with a detailed error message listing all missing keys.

    Args:
        data (dict[str, Any]): The configuration dictionary section to validate.
        path (tuple[str, ...]): The current path tuple for error reporting context.
        required_keys (set[str]): Set of key names that must be present at this path level.

    Raises:
        ConfigurationError: If any required keys are missing from the data.
    """
    missing_keys = required_keys - set(data.keys())
    if missing_keys:
        _LOGGER.error(
            f"[config:_validate_community_config] Missing required keys at config path {path}: {missing_keys}"
        )
        raise ConfigurationError(
            f"Missing required keys at config path {path}: {missing_keys}"
        )


def _validate_key_type_and_value(
    key: str, value: Any, spec: _ConfigPathSpec, path: tuple[str, ...]
) -> None:
    """Validate type and value for a single configuration key.

    Performs two types of validation:
    1. Type validation - ensures the value matches the expected type in the spec
    2. Specialized validation - if a validator is provided in the spec, runs it
       and wraps CommunitySessionConfigurationError as ConfigurationError

    Args:
        key (str): The configuration key being validated.
        value (Any): The value to validate.
        spec (_ConfigPathSpec): The configuration path specification containing type and validator.
        path (tuple[str, ...]): The parent path tuple (will be combined with key to form current_path).

    Raises:
        ConfigurationError: If validation fails for type or specialized validation.
    """
    current_path = path + (key,)

    # Type validation
    if not isinstance(value, spec.expected_type):
        _LOGGER.error(
            f"[config:_validate_community_config] Config path {current_path} must be of type {spec.expected_type.__name__}, got {type(value).__name__}"
        )
        raise ConfigurationError(
            f"Config path {current_path} must be of type {spec.expected_type.__name__}, got {type(value).__name__}"
        )

    # Specialized validation
    if spec.validator:
        try:
            spec.validator(value)
        except CommunitySessionConfigurationError as e:
            raise ConfigurationError(
                f"Invalid configuration for {'.'.join(current_path)}: {e}"
            ) from e


def _should_recurse_into_nested_dict(current_path: tuple[str, ...]) -> bool:
    """Check if there are nested schema paths for the current path.

    Determines if we should continue recursing into a dictionary by checking if any
    schema paths exist that are children of the current path (i.e., they start with
    the current path and have at least one more component). This is used during
    validation to decide whether to recursively validate nested dictionary sections.

    For example, if ``current_path`` is ``('security',)`` and ``_SCHEMA_PATHS``
    contained a hypothetical ``('security', 'sub_option')`` entry, this would return
    ``True``. With the current schema (all ``_SCHEMA_PATHS`` keys have length 1),
    there are no children of any path and this function therefore always returns
    ``False``; the helper is retained to support future nested schema extensions
    without changes to :func:`_validate_section`.

    Args:
        current_path (tuple[str, ...]): The current path tuple to check for children.

    Returns:
        bool: True if there are nested paths that extend beyond the current path,
              False if this is a leaf node in the schema tree.
    """
    return any(
        nested_path[: len(current_path)] == current_path
        and len(nested_path) > len(current_path)
        for nested_path in _SCHEMA_PATHS.keys()
    )


def _validate_section(data: dict[str, Any], path: tuple[str, ...]) -> None:
    """Validate a configuration section in a single pass.

    Performs comprehensive validation of a configuration section including:
    1. Checking for unknown keys not in the schema
    2. Checking for missing required keys
    3. Validating each key's type and value
    4. Recursively validating nested dictionary sections

    This is the core validation engine that processes each level of the configuration
    hierarchy according to the schema defined in _SCHEMA_PATHS. Recursion continues
    only if nested schema paths exist for the current path.

    Args:
        data (dict[str, Any]): The dictionary containing configuration data to validate.
        path (tuple[str, ...]): The current path tuple representing the location in the config.

    Raises:
        ConfigurationError: If validation fails for any reason (unknown keys, missing required keys,
                           type mismatches, or specialized validation failures).
    """
    # Get specs for the current path level
    current_specs = {
        nested_path[len(path)]: spec
        for nested_path, spec in _SCHEMA_PATHS.items()
        if len(nested_path) == len(path) + 1 and nested_path[: len(path)] == path
    }

    # Check for unknown keys
    valid_keys = set(current_specs.keys())
    _validate_unknown_keys(data, path, valid_keys)

    # Check for missing required keys
    required_keys = {key for key, spec in current_specs.items() if spec.required}
    _validate_required_keys(data, path, required_keys)

    # Validate each present key
    for key, value in data.items():
        if key in current_specs:
            spec = current_specs[key]
            current_path = path + (key,)

            _validate_key_type_and_value(key, value, spec, path)

            # Recurse into nested dictionaries
            if isinstance(value, dict) and _should_recurse_into_nested_dict(
                current_path
            ):
                _validate_section(value, current_path)


def _validate_community_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate the Deephaven MCP community configuration dictionary.

    This function ensures that the configuration dictionary conforms to the expected schema for
    Deephaven MCP community servers. The configuration may contain the following top-level keys
    (all optional):

      - 'security' (dict, optional):
            Security settings for community sessions (e.g., credential_retrieval_mode).
            If present, its value must be a dictionary (can be empty).

      - 'sessions' (dict, optional):
            A dictionary mapping session names to static community session configs.
            If present, its value must be a dictionary (can be empty).

      - 'session_creation' (dict, optional):
            Configuration for dynamically creating community sessions on demand.
            If present, its value must be a dictionary (can be empty).

    Validation Rules:
      - Only known top-level keys are allowed: 'security', 'sessions', 'session_creation'.
      - All present sections are validated according to their schema.
      - Unknown keys at the top level will cause validation to fail.
      - All field types must be correct if present.
      - Sensitive fields are redacted from logs.

    Args:
        config (dict[str, Any]): The configuration dictionary to validate.

    Returns:
        dict[str, Any]: The same input dictionary, returned unchanged after successful validation.

    Raises:
        ConfigurationError: If validation fails due to unknown keys, wrong types, or invalid nested
            configurations. Any unknown top-level key (e.g. ``"community"``, ``"enterprise"``) raises this error.

    Example:
        >>> validated_config = _validate_community_config({'sessions': {'local_session': {}}})
        >>> validated_config_empty = _validate_community_config({})  # Also valid
    """
    if not isinstance(config, dict):
        _LOGGER.error(
            f"[config:_validate_community_config] Configuration must be a dictionary, got {type(config).__name__}"
        )
        raise ConfigurationError("Configuration must be a dictionary")

    # Validate the entire configuration
    _validate_section(config, ())

    _LOGGER.info("[config:_validate_community_config] Configuration validation passed.")
    return config


async def _load_and_validate_community_config(config_path: str) -> dict[str, Any]:
    """Load, parse, and validate the community configuration from a JSON/JSON5 file."""
    return await _load_and_validate_config(
        config_path, _validate_community_config, "_load_and_validate_community_config"
    )


class CommunityServerConfigManager(ConfigManager):
    """ConfigManager for the DHC MCP server (``dh-mcp-community-server``).

    Reads a community config file. The format uses ``sessions``, ``session_creation``, and
    ``security`` as optional top-level keys; validation enforces the community schema defined in
    :mod:`deephaven_mcp.config._community`.

    Config file format::

        {
            "security": {"credential_retrieval_mode": "dynamic_only"},
            "sessions": {
                "local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "..."}
            },
            "session_creation": {"defaults": {"launch_method": "python"}}
        }
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the community config file (coroutine-safe).

        Returns:
            dict[str, Any]: The validated community configuration dictionary.

        Raises:
            RuntimeError: If no config path is provided and ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails community validation.
        """
        _LOGGER.debug(
            "[CommunityServerConfigManager:get_config] Loading Deephaven MCP application configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[CommunityServerConfigManager:get_config] Using cached Deephaven MCP application configuration."
                )
                return self._cache

            resolved_path = (
                self._config_path
                if self._config_path is not None
                else _get_config_path()
            )
            validated = await _load_and_validate_community_config(resolved_path)
            self._cache = validated
            _log_config_summary(
                validated,
                label="CommunityServerConfigManager:get_config",
                redactor=_apply_redaction_to_config,
            )
            _LOGGER.info(
                "[CommunityServerConfigManager:get_config] Community configuration loaded successfully."
            )
            return self._cache

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Inject a community configuration dictionary into the cache (coroutine-safe).

        Validates ``config`` against the community schema before caching.

        Args:
            config (dict[str, Any]): The community configuration dictionary to cache.

        Raises:
            ConfigurationError: If the provided configuration is invalid.
        """
        async with self._lock:
            self._cache = _validate_community_config(config)
