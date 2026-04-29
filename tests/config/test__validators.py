"""Tests for deephaven_mcp.config._validators — shared validation utilities."""

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config._validators import (
    validate_optional_positive_number,
    validate_optional_string_dict,
    validate_optional_string_list,
    validate_positive_number,
    validate_string_dict,
    validate_string_list,
)


# ---------------------------------------------------------------------------
# validate_positive_number
# ---------------------------------------------------------------------------


def test_validate_positive_number_valid_int():
    """Integer > 0 passes."""
    validate_positive_number("timeout_seconds", 5)


def test_validate_positive_number_valid_float():
    """Float > 0 passes."""
    validate_positive_number("timeout_seconds", 1.5)


def test_validate_positive_number_zero_invalid():
    """Zero raises ConfigurationError naming the field."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be positive"):
        validate_positive_number("timeout_seconds", 0)


def test_validate_positive_number_negative_invalid():
    """Negative value raises ConfigurationError naming the field."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be positive"):
        validate_positive_number("timeout_seconds", -1)


def test_validate_positive_number_bool_invalid():
    """Bool raises ConfigurationError — bool is a subclass of int but not a valid number."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be a number"):
        validate_positive_number("timeout_seconds", True)


def test_validate_positive_number_string_invalid():
    """Non-numeric type raises ConfigurationError naming the field."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be a number"):
        validate_positive_number("timeout_seconds", "x")


# ---------------------------------------------------------------------------
# validate_optional_positive_number
# ---------------------------------------------------------------------------


def test_validate_optional_positive_number_absent():
    """Field absent from config passes silently."""
    validate_optional_positive_number({}, "timeout_seconds")


def test_validate_optional_positive_number_none_value():
    """Field present with None value passes silently."""
    validate_optional_positive_number({"timeout_seconds": None}, "timeout_seconds")


def test_validate_optional_positive_number_valid_int():
    """Field present with a positive int passes."""
    validate_optional_positive_number({"timeout_seconds": 5}, "timeout_seconds")


def test_validate_optional_positive_number_valid_float():
    """Field present with a positive float passes."""
    validate_optional_positive_number({"timeout_seconds": 1.5}, "timeout_seconds")


def test_validate_optional_positive_number_zero():
    """Field present with zero raises ConfigurationError."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be positive"):
        validate_optional_positive_number({"timeout_seconds": 0}, "timeout_seconds")


def test_validate_optional_positive_number_negative():
    """Field present with negative value raises ConfigurationError."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be positive"):
        validate_optional_positive_number({"timeout_seconds": -1}, "timeout_seconds")


def test_validate_optional_positive_number_bool():
    """Field present with bool raises ConfigurationError."""
    with pytest.raises(ConfigurationError, match=r"'timeout_seconds'.*must be a number"):
        validate_optional_positive_number({"timeout_seconds": True}, "timeout_seconds")


# ---------------------------------------------------------------------------
# validate_string_list
# ---------------------------------------------------------------------------


def test_validate_string_list_empty():
    """Empty list passes."""
    validate_string_list("volumes", [])


def test_validate_string_list_all_strings():
    """List of strings passes."""
    validate_string_list("volumes", ["a", "b", "c"])


def test_validate_string_list_int_item():
    """Non-string item raises ConfigurationError naming the index."""
    with pytest.raises(ConfigurationError, match=r"'volumes\[1\]'.*must be a string"):
        validate_string_list("volumes", ["a", 123])


def test_validate_string_list_none_item():
    """None item raises ConfigurationError naming the index."""
    with pytest.raises(ConfigurationError, match=r"'volumes\[0\]'.*must be a string"):
        validate_string_list("volumes", [None])


# ---------------------------------------------------------------------------
# validate_optional_string_list
# ---------------------------------------------------------------------------


def test_validate_optional_string_list_absent():
    """Field absent from config passes silently."""
    validate_optional_string_list({}, "volumes")


def test_validate_optional_string_list_empty():
    """Field present with empty list passes."""
    validate_optional_string_list({"volumes": []}, "volumes")


def test_validate_optional_string_list_valid():
    """Field present with all-string list passes."""
    validate_optional_string_list({"volumes": ["a", "b"]}, "volumes")


def test_validate_optional_string_list_int_item():
    """Non-string item raises ConfigurationError."""
    with pytest.raises(ConfigurationError, match=r"'volumes\[1\]'.*must be a string"):
        validate_optional_string_list({"volumes": ["a", 123]}, "volumes")


# ---------------------------------------------------------------------------
# validate_string_dict
# ---------------------------------------------------------------------------


def test_validate_string_dict_empty():
    """Empty dict passes."""
    validate_string_dict("env_vars", {})


def test_validate_string_dict_valid():
    """Dict with string keys and values passes."""
    validate_string_dict("env_vars", {"KEY": "value", "OTHER": "val"})


def test_validate_string_dict_int_key():
    """Non-string key raises ConfigurationError naming the field."""
    with pytest.raises(ConfigurationError, match=r"'env_vars' key must be a string"):
        validate_string_dict("env_vars", {123: "value"})


def test_validate_string_dict_int_value():
    """Non-string value raises ConfigurationError naming the key."""
    with pytest.raises(ConfigurationError, match=r"'env_vars\[KEY\]' value must be a string"):
        validate_string_dict("env_vars", {"KEY": 123})


# ---------------------------------------------------------------------------
# validate_optional_string_dict
# ---------------------------------------------------------------------------


def test_validate_optional_string_dict_absent():
    """Field absent from config passes silently."""
    validate_optional_string_dict({}, "env_vars")


def test_validate_optional_string_dict_valid():
    """Field present with valid str→str dict passes."""
    validate_optional_string_dict({"env_vars": {"K": "v"}}, "env_vars")


def test_validate_optional_string_dict_int_key():
    """Non-string key raises ConfigurationError."""
    with pytest.raises(ConfigurationError, match=r"'env_vars' key must be a string"):
        validate_optional_string_dict({"env_vars": {123: "v"}}, "env_vars")


def test_validate_optional_string_dict_int_value():
    """Non-string value raises ConfigurationError."""
    with pytest.raises(ConfigurationError, match=r"'env_vars\[K\]' value must be a string"):
        validate_optional_string_dict({"env_vars": {"K": 99}}, "env_vars")
