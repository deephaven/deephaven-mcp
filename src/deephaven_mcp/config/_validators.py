"""Shared field validation utilities for Deephaven MCP configuration.

These helpers contain the core validation logic shared between the community
and enterprise config modules. They raise :class:`ConfigurationError` so each
caller can propagate or convert the error to its own specific subtype.
"""

from typing import Any

from deephaven_mcp._exceptions import ConfigurationError


def validate_positive_number(field_name: str, value: Any) -> None:
    """Validate that value is a positive int or float (not bool).

    Shared by community and enterprise config validation. Raises
    :class:`ConfigurationError` so callers can propagate or wrap it as their
    own specific error subtype.

    Args:
        field_name: Name of the field, used in error messages.
        value: The value to validate.

    Raises:
        ConfigurationError: If value is a bool, not a number, or not positive.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(
            f"'{field_name}' must be a number (int or float), but got {type(value).__name__}."
        )
    if value <= 0:
        raise ConfigurationError(
            f"'{field_name}' must be positive, but got {value}."
        )


def validate_optional_positive_number(
    config: dict[str, Any], field_name: str
) -> None:
    """Validate that config[field_name] is a positive number if present and non-None.

    Silently passes when the field is absent or its value is None. Delegates to
    :func:`validate_positive_number` for the actual value check.

    Args:
        config: The configuration dictionary to inspect.
        field_name: Key to look up in config.

    Raises:
        ConfigurationError: If the field is present with a non-None value that is
            a bool, not a number, or not positive.
    """
    if field_name not in config:
        return
    value = config[field_name]
    if value is None:
        return
    validate_positive_number(field_name, value)


def validate_string_list(field_name: str, items: list) -> None:
    """Validate that a list contains only string items.

    Args:
        field_name: Name of the list field, used in error messages.
        items: The list to validate.

    Raises:
        ConfigurationError: If any item is not a string.
    """
    for i, item in enumerate(items):
        if not isinstance(item, str):
            raise ConfigurationError(
                f"'{field_name}[{i}]' must be a string, got {type(item).__name__}."
            )


def validate_optional_string_list(config: dict[str, Any], field_name: str) -> None:
    """Validate that config[field_name] is a list of strings if present.

    Silently passes when the field is absent. Delegates to
    :func:`validate_string_list` for the actual content check.

    Args:
        config: The configuration dictionary to inspect.
        field_name: Key to look up in config.

    Raises:
        ConfigurationError: If the field is present and contains a non-string item.
    """
    if field_name not in config:
        return
    validate_string_list(field_name, config[field_name])


def validate_string_dict(field_name: str, mapping: dict) -> None:
    """Validate that a dict has string keys and string values.

    Args:
        field_name: Name of the dict field, used in error messages.
        mapping: The dict to validate.

    Raises:
        ConfigurationError: If any key or value is not a string.
    """
    for key, value in mapping.items():
        if not isinstance(key, str):
            raise ConfigurationError(
                f"'{field_name}' key must be a string, got {type(key).__name__}."
            )
        if not isinstance(value, str):
            raise ConfigurationError(
                f"'{field_name}[{key}]' value must be a string, got {type(value).__name__}."
            )


def validate_optional_string_dict(config: dict[str, Any], field_name: str) -> None:
    """Validate that config[field_name] is a str→str dict if present.

    Silently passes when the field is absent. Delegates to
    :func:`validate_string_dict` for the actual content check.

    Args:
        config: The configuration dictionary to inspect.
        field_name: Key to look up in config.

    Raises:
        ConfigurationError: If the field is present and has a non-string key or value.
    """
    if field_name not in config:
        return
    validate_string_dict(field_name, config[field_name])
