"""Shared field validation utilities for Deephaven MCP configuration.

These helpers contain the core validation logic shared between the community
and enterprise config modules. Every function raises
:class:`~deephaven_mcp._exceptions.ConfigurationError` on failure and logs a
matching ``_LOGGER.error`` entry with the prefix ``[_validators:<func_name>]``
immediately before raising, to match the logging convention used elsewhere in
:mod:`deephaven_mcp.config`.
"""

import logging
from typing import Any

from deephaven_mcp._exceptions import ConfigurationError

_LOGGER = logging.getLogger(__name__)


def validate_field_type(
    context: str,
    field_name: str,
    value: Any,
    expected_type: type | tuple[type, ...],
    *,
    is_optional: bool = False,
) -> None:
    """Validate that a configuration field has the correct type.

    Supports single types and union types (tuples). Produces a consistent error
    message that includes the context (e.g., the containing system/session name
    or path), the field name, and the expected vs actual type.

    Args:
        context (str): Identifier of the containing section, used in error
            messages (e.g., ``"enterprise system 'prod'"`` or
            ``"community session 'local'"``).
        field_name (str): Name of the field being validated.
        value (Any): The value to type-check.
        expected_type (type | tuple[type, ...]): The expected Python type, or
            a tuple of acceptable types (acts as a union).
        is_optional (bool): If ``True``, error messages use ``"Optional field"``
            instead of ``"Field"``. Defaults to ``False``.

    Raises:
        ConfigurationError: If ``value`` does not match ``expected_type``.
    """
    prefix = "Optional field" if is_optional else "Field"
    if isinstance(expected_type, tuple):
        if not isinstance(value, expected_type):
            names = ", ".join(t.__name__ for t in expected_type)
            msg = (
                f"{prefix} '{field_name}' for {context} must be one of types "
                f"({names}), but got {type(value).__name__}."
            )
            _LOGGER.error(f"[_validators:validate_field_type] {msg}")
            raise ConfigurationError(msg)
    elif not isinstance(value, expected_type):
        msg = (
            f"{prefix} '{field_name}' for {context} must be of type "
            f"{expected_type.__name__}, but got {type(value).__name__}."
        )
        _LOGGER.error(f"[_validators:validate_field_type] {msg}")
        raise ConfigurationError(msg)


def validate_allowed_fields(
    context: str,
    data: dict[str, Any],
    allowed: dict[str, type | tuple[type, ...]],
    *,
    reject_unknown: bool = True,
) -> None:
    """Validate that ``data`` contains only ``allowed`` keys with correct types.

    For each key/value pair in ``data``:

    - If the key is in ``allowed``, type-check the value via :func:`validate_field_type`.
    - If the key is not in ``allowed``:

      * ``reject_unknown=True`` (default) → raise :class:`ConfigurationError`.
      * ``reject_unknown=False`` → log a warning and skip the value.

    Does not enforce presence of required keys (callers do that separately).

    Args:
        context (str): Identifier of the containing section, used in error
            messages.
        data (dict[str, Any]): The dictionary to validate.
        allowed (dict[str, type | tuple[type, ...]]): Map of allowed field
            name to expected type (single type or tuple of types).
        reject_unknown (bool): Policy for unknown fields. Defaults to ``True``
            (strict mode: raise on unknown). When ``False``, unknown fields
            are logged as warnings and skipped.

    Raises:
        ConfigurationError: If an unknown key is present and ``reject_unknown``
            is ``True``, or if any known field has an incorrect type.
    """
    for field_name, value in data.items():
        if field_name not in allowed:
            if reject_unknown:
                msg = (
                    f"Unknown field '{field_name}' for {context}. "
                    f"Allowed fields: {sorted(allowed.keys())}."
                )
                _LOGGER.error(f"[_validators:validate_allowed_fields] {msg}")
                raise ConfigurationError(msg)
            _LOGGER.warning(
                f"[_validators:validate_allowed_fields] Unknown field "
                f"'{field_name}' for {context} will be ignored."
            )
            continue
        validate_field_type(context, field_name, value, allowed[field_name])


def validate_mutually_exclusive(
    context: str, data: dict[str, Any], field_a: str, field_b: str
) -> None:
    """Raise if both ``field_a`` and ``field_b`` are present in ``data``.

    Args:
        context (str): Identifier of the containing section, used in error
            messages.
        data (dict[str, Any]): The dictionary to inspect.
        field_a (str): First field name.
        field_b (str): Second field name.

    Raises:
        ConfigurationError: If both fields are present.
    """
    if field_a in data and field_b in data:
        msg = (
            f"For {context}, '{field_a}' and '{field_b}' are mutually "
            f"exclusive; specify only one."
        )
        _LOGGER.error(f"[_validators:validate_mutually_exclusive] {msg}")
        raise ConfigurationError(msg)


def validate_non_negative_int(field_name: str, value: Any) -> None:
    """Validate that value is a non-negative int (not bool).

    Booleans are explicitly rejected even though ``bool`` is a subclass of
    ``int`` in Python (``isinstance(True, int)`` is ``True``); this prevents
    ``True``/``False`` from silently satisfying an integer field.

    Args:
        field_name (str): Name of the field, used in error messages.
        value (Any): The value to validate.

    Raises:
        ConfigurationError: If value is a bool, not an int, or negative.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"'{field_name}' must be an integer, but got {type(value).__name__}."
        _LOGGER.error(f"[_validators:validate_non_negative_int] {msg}")
        raise ConfigurationError(msg)
    if value < 0:
        msg = f"'{field_name}' must be non-negative, but got {value}."
        _LOGGER.error(f"[_validators:validate_non_negative_int] {msg}")
        raise ConfigurationError(msg)


def validate_positive_number(field_name: str, value: Any) -> None:
    """Validate that value is a positive int or float (not bool).

    Booleans are explicitly rejected even though ``bool`` is a subclass of
    ``int`` in Python (``isinstance(True, int)`` is ``True``); this prevents
    ``True``/``False`` from silently satisfying a numeric field.

    Args:
        field_name (str): Name of the field, used in error messages.
        value (Any): The value to validate.

    Raises:
        ConfigurationError: If value is a bool, not a number, or not positive.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = (
            f"'{field_name}' must be a number (int or float), but got "
            f"{type(value).__name__}."
        )
        _LOGGER.error(f"[_validators:validate_positive_number] {msg}")
        raise ConfigurationError(msg)
    if value <= 0:
        msg = f"'{field_name}' must be positive, but got {value}."
        _LOGGER.error(f"[_validators:validate_positive_number] {msg}")
        raise ConfigurationError(msg)


def validate_optional_positive_number(config: dict[str, Any], field_name: str) -> None:
    """Validate that config[field_name] is a positive number if present and non-None.

    Silently passes when the field is absent or its value is None. Delegates to
    :func:`validate_positive_number` for the actual value check.

    Args:
        config (dict[str, Any]): The configuration dictionary to inspect.
        field_name (str): Key to look up in ``config``.

    Raises:
        ConfigurationError: If the field is present with a non-None value that
            is a bool, not a number, or not positive.
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
        field_name (str): Name of the list field, used in error messages.
        items (list): The list to validate. Callers are expected to have
            already verified that the value is a list (e.g., via
            :func:`validate_field_type`); this function does not guard
            against non-iterable inputs.

    Raises:
        ConfigurationError: If any item is not a string.
    """
    for i, item in enumerate(items):
        if not isinstance(item, str):
            msg = (
                f"'{field_name}[{i}]' must be a string, got " f"{type(item).__name__}."
            )
            _LOGGER.error(f"[_validators:validate_string_list] {msg}")
            raise ConfigurationError(msg)


def validate_optional_string_list(config: dict[str, Any], field_name: str) -> None:
    """Validate that config[field_name] is a list of strings if present.

    Silently passes when the field is absent. Delegates to
    :func:`validate_string_list` for the actual content check.

    Args:
        config (dict[str, Any]): The configuration dictionary to inspect.
        field_name (str): Key to look up in ``config``.

    Raises:
        ConfigurationError: If the field is present and contains a non-string
            item.
    """
    if field_name not in config:
        return
    validate_string_list(field_name, config[field_name])


def validate_string_dict(field_name: str, mapping: dict) -> None:
    """Validate that a dict has string keys and string values.

    Args:
        field_name (str): Name of the dict field, used in error messages.
        mapping (dict): The dict to validate. Callers are expected to have
            already verified that the value is a dict (e.g., via
            :func:`validate_field_type`); this function does not guard
            against non-mapping inputs.

    Raises:
        ConfigurationError: If any key or value is not a string.
    """
    for key, value in mapping.items():
        if not isinstance(key, str):
            msg = f"'{field_name}' key must be a string, got " f"{type(key).__name__}."
            _LOGGER.error(f"[_validators:validate_string_dict] {msg}")
            raise ConfigurationError(msg)
        if not isinstance(value, str):
            msg = (
                f"'{field_name}[{key}]' value must be a string, got "
                f"{type(value).__name__}."
            )
            _LOGGER.error(f"[_validators:validate_string_dict] {msg}")
            raise ConfigurationError(msg)


def validate_optional_string_dict(config: dict[str, Any], field_name: str) -> None:
    """Validate that config[field_name] is a str→str dict if present.

    Silently passes when the field is absent. Delegates to
    :func:`validate_string_dict` for the actual content check.

    Args:
        config (dict[str, Any]): The configuration dictionary to inspect.
        field_name (str): Key to look up in ``config``.

    Raises:
        ConfigurationError: If the field is present and has a non-string key
            or value.
    """
    if field_name not in config:
        return
    validate_string_dict(field_name, config[field_name])
