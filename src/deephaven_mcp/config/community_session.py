"""
Configuration handling specific to Deephaven Community Sessions.

This module includes:
- Validation logic for individual community session configurations.
- Definitions of allowed and required fields for community sessions.
- Utility functions like redacting sensitive data from session configurations.
- Custom exceptions related to community session configuration errors.
"""

import logging
import types
from typing import Any

_LOGGER = logging.getLogger(__name__)


class CommunitySessionConfigurationError(Exception):
    """Raised when a community session's configuration cannot be retrieved or is invalid."""

    pass


_ALLOWED_COMMUNITY_SESSION_FIELDS: dict[str, type | tuple[type, type]] = {
    "host": str,
    "port": int,
    "auth_type": str,
    "auth_token": str,
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


def validate_community_sessions_config(
    community_sessions_map: dict[str, Any] | None,
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
        ValueError: If community_sessions_map is provided and is not a dict, or if any
                    individual session config is invalid.
    """
    if community_sessions_map is None:
        # If 'community_sessions' key was absent from config, there's nothing to validate here.
        return

    if not isinstance(community_sessions_map, dict):
        _LOGGER.error(
            "'community_sessions' must be a dictionary in Deephaven community session config, got %s",
            type(community_sessions_map).__name__,
        )
        raise ValueError(
            "'community_sessions' must be a dictionary in Deephaven community session config"
        )

    for session_name, session_config_item in community_sessions_map.items():
        validate_single_community_session_config(session_name, session_config_item)


def redact_community_session_config(session_config: dict[str, Any]) -> dict[str, Any]:
    """Redacts sensitive fields from a community session configuration dictionary.

    Creates a shallow copy of the input dictionary and redacts 'auth_token'
    if it exists.

    Args:
        session_config (dict[str, Any]): The community session configuration.

    Returns:
        dict[str, Any]: A new dictionary with sensitive fields redacted.
    """
    config_copy = session_config.copy()
    if "auth_token" in config_copy:
        config_copy["auth_token"] = "[REDACTED]"  # noqa: S105
    return config_copy


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
        ValueError: If the configuration item is invalid (e.g., unknown fields, wrong types).
    """
    if not isinstance(config_item, dict):
        raise ValueError(
            f"Community session config for {session_name} must be a dictionary, got {type(config_item)}"
        )

    for field_name, field_value in config_item.items():
        if field_name not in _ALLOWED_COMMUNITY_SESSION_FIELDS:
            raise ValueError(
                f"Unknown field '{field_name}' in community session config for {session_name}"
            )

        allowed_types = _ALLOWED_COMMUNITY_SESSION_FIELDS[field_name]
        if isinstance(allowed_types, tuple):
            if not isinstance(field_value, allowed_types):
                expected_type_names = ", ".join(t.__name__ for t in allowed_types)
                raise ValueError(
                    f"Field '{field_name}' in community session config for {session_name} "
                    f"must be one of types ({expected_type_names}), got {type(field_value).__name__}"
                )
        elif not isinstance(field_value, allowed_types):
            raise ValueError(
                f"Field '{field_name}' in community session config for {session_name} "
                f"must be of type {allowed_types.__name__}, got {type(field_value).__name__}"
            )

    for required_field in _REQUIRED_FIELDS:
        if required_field not in config_item:
            raise ValueError(
                f"Missing required field '{required_field}' in community session config for {session_name}"
            )
