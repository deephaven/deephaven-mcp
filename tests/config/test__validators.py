"""Tests for deephaven_mcp.config._validators — shared validation utilities."""

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config._validators import (
    resolve_required_env_var,
    resolve_secret_field,
    validate_allowed_fields,
    validate_field_type,
    validate_mutually_exclusive,
    validate_non_negative_int,
    validate_optional_positive_number,
    validate_optional_string_dict,
    validate_optional_string_list,
    validate_positive_number,
    validate_string_dict,
    validate_string_list,
)

# ---------------------------------------------------------------------------
# validate_field_type
# ---------------------------------------------------------------------------


def test_validate_field_type_single_type_valid():
    validate_field_type("ctx", "port", 8080, int)


def test_validate_field_type_single_type_invalid():
    with pytest.raises(
        ConfigurationError, match=r"Field 'port' for ctx must be of type int"
    ):
        validate_field_type("ctx", "port", "8080", int)


def test_validate_field_type_tuple_valid():
    validate_field_type("ctx", "timeout", 1.5, (int, float))


def test_validate_field_type_tuple_invalid():
    with pytest.raises(
        ConfigurationError,
        match=r"Field 'timeout' for ctx must be one of types \(int, float\)",
    ):
        validate_field_type("ctx", "timeout", "x", (int, float))


def test_validate_field_type_is_optional_prefix():
    with pytest.raises(ConfigurationError, match=r"Optional field 'x'"):
        validate_field_type("ctx", "x", 1, str, is_optional=True)


# ---------------------------------------------------------------------------
# validate_allowed_fields
# ---------------------------------------------------------------------------


def test_validate_allowed_fields_valid():
    validate_allowed_fields("ctx", {"a": 1, "b": "x"}, {"a": int, "b": str})


def test_validate_allowed_fields_unknown_rejects_by_default():
    with pytest.raises(ConfigurationError, match=r"Unknown field 'c' for ctx"):
        validate_allowed_fields("ctx", {"a": 1, "c": 9}, {"a": int})


def test_validate_allowed_fields_unknown_warns_when_not_strict(caplog):
    caplog.set_level("WARNING")
    validate_allowed_fields("ctx", {"a": 1, "c": 9}, {"a": int}, reject_unknown=False)
    assert any("'c'" in rec.message for rec in caplog.records)


def test_validate_allowed_fields_bad_type():
    with pytest.raises(
        ConfigurationError, match=r"Field 'a' for ctx must be of type int"
    ):
        validate_allowed_fields("ctx", {"a": "nope"}, {"a": int})


# ---------------------------------------------------------------------------
# validate_mutually_exclusive
# ---------------------------------------------------------------------------


def test_validate_mutually_exclusive_neither():
    validate_mutually_exclusive("ctx", {}, "a", "b")


def test_validate_mutually_exclusive_only_a():
    validate_mutually_exclusive("ctx", {"a": 1}, "a", "b")


def test_validate_mutually_exclusive_only_b():
    validate_mutually_exclusive("ctx", {"b": 2}, "a", "b")


def test_validate_mutually_exclusive_both_raises():
    with pytest.raises(
        ConfigurationError,
        match=r"'a' and 'b' are mutually exclusive",
    ):
        validate_mutually_exclusive("ctx", {"a": 1, "b": 2}, "a", "b")


# ---------------------------------------------------------------------------
# validate_non_negative_int
# ---------------------------------------------------------------------------


def test_validate_non_negative_int_zero():
    validate_non_negative_int("retries", 0)


def test_validate_non_negative_int_positive():
    validate_non_negative_int("retries", 3)


def test_validate_non_negative_int_negative_raises():
    with pytest.raises(ConfigurationError, match=r"'retries'.*must be non-negative"):
        validate_non_negative_int("retries", -1)


def test_validate_non_negative_int_bool_raises():
    with pytest.raises(ConfigurationError, match=r"'retries'.*must be an integer"):
        validate_non_negative_int("retries", True)


def test_validate_non_negative_int_float_raises():
    with pytest.raises(ConfigurationError, match=r"'retries'.*must be an integer"):
        validate_non_negative_int("retries", 1.5)


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
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be positive"
    ):
        validate_positive_number("timeout_seconds", 0)


def test_validate_positive_number_negative_invalid():
    """Negative value raises ConfigurationError naming the field."""
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be positive"
    ):
        validate_positive_number("timeout_seconds", -1)


def test_validate_positive_number_bool_invalid():
    """Bool raises ConfigurationError — bool is a subclass of int but not a valid number."""
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be a number"
    ):
        validate_positive_number("timeout_seconds", True)


def test_validate_positive_number_string_invalid():
    """Non-numeric type raises ConfigurationError naming the field."""
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be a number"
    ):
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
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be positive"
    ):
        validate_optional_positive_number({"timeout_seconds": 0}, "timeout_seconds")


def test_validate_optional_positive_number_negative():
    """Field present with negative value raises ConfigurationError."""
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be positive"
    ):
        validate_optional_positive_number({"timeout_seconds": -1}, "timeout_seconds")


def test_validate_optional_positive_number_bool():
    """Field present with bool raises ConfigurationError."""
    with pytest.raises(
        ConfigurationError, match=r"'timeout_seconds'.*must be a number"
    ):
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
    with pytest.raises(
        ConfigurationError, match=r"'env_vars\[KEY\]' value must be a string"
    ):
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
    with pytest.raises(
        ConfigurationError, match=r"'env_vars\[K\]' value must be a string"
    ):
        validate_optional_string_dict({"env_vars": {"K": 99}}, "env_vars")


# ---------------------------------------------------------------------------
# resolve_required_env_var
# ---------------------------------------------------------------------------


def test_resolve_required_env_var_returns_value(monkeypatch):
    """A set, non-empty env var returns its value."""
    monkeypatch.setenv("DH_TEST_VAR", "secret-value")
    assert (
        resolve_required_env_var(
            field_name="auth.psk_env_var",
            env_var_name="DH_TEST_VAR",
            context="test ctx",
        )
        == "secret-value"
    )


def test_resolve_required_env_var_unset_raises(monkeypatch):
    """An unset env var raises ConfigurationError mentioning the field and var."""
    monkeypatch.delenv("DH_TEST_VAR", raising=False)
    with pytest.raises(
        ConfigurationError,
        match=r"test ctx: 'auth.psk_env_var' refers to environment variable 'DH_TEST_VAR'",
    ):
        resolve_required_env_var(
            field_name="auth.psk_env_var",
            env_var_name="DH_TEST_VAR",
            context="test ctx",
        )


def test_resolve_required_env_var_empty_raises(monkeypatch):
    """An env var set to empty string raises ConfigurationError."""
    monkeypatch.setenv("DH_TEST_VAR", "")
    with pytest.raises(ConfigurationError, match=r"unset or empty"):
        resolve_required_env_var(
            field_name="auth.psk_env_var",
            env_var_name="DH_TEST_VAR",
            context="test ctx",
        )


# ---------------------------------------------------------------------------
# resolve_secret_field
# ---------------------------------------------------------------------------


def test_resolve_secret_field_inline_returns_value():
    """Inline field set returns inline value."""
    config = {"psk": "inline-secret"}
    assert (
        resolve_secret_field(
            config=config,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        == "inline-secret"
    )


def test_resolve_secret_field_env_var_returns_value(monkeypatch):
    """Only env_var field set: resolves through the environment."""
    monkeypatch.setenv("DH_TEST_PSK", "env-secret")
    config = {"psk_env_var": "DH_TEST_PSK"}
    assert (
        resolve_secret_field(
            config=config,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        == "env-secret"
    )


def test_resolve_secret_field_neither_returns_none():
    """Neither field set returns None (caller decides what that means)."""
    assert (
        resolve_secret_field(
            config={},
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        is None
    )


def test_resolve_secret_field_env_var_unset_raises(monkeypatch):
    """env_var field names a variable that is unset → ConfigurationError."""
    monkeypatch.delenv("DH_TEST_PSK", raising=False)
    with pytest.raises(ConfigurationError, match=r"DH_TEST_PSK"):
        resolve_secret_field(
            config={"psk_env_var": "DH_TEST_PSK"},
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )


def test_resolve_secret_field_inline_takes_precedence(monkeypatch):
    """If both fields are present (validator failure), inline wins; env not consulted."""
    # If the env var were consulted, this would raise; assert it isn't.
    monkeypatch.delenv("DH_TEST_PSK", raising=False)
    config = {"psk": "inline-secret", "psk_env_var": "DH_TEST_PSK"}
    assert (
        resolve_secret_field(
            config=config,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        == "inline-secret"
    )


def test_resolve_secret_field_inline_empty_falls_through_to_env(monkeypatch):
    """Inline present but empty string is treated as absent; env is consulted."""
    monkeypatch.setenv("DH_TEST_PSK", "env-secret")
    config = {"psk": "", "psk_env_var": "DH_TEST_PSK"}
    assert (
        resolve_secret_field(
            config=config,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        == "env-secret"
    )


def test_resolve_secret_field_inline_non_string_falls_through(monkeypatch):
    """Non-string inline value (e.g. None) is treated as absent."""
    monkeypatch.setenv("DH_TEST_PSK", "env-secret")
    config = {"psk": None, "psk_env_var": "DH_TEST_PSK"}
    assert (
        resolve_secret_field(
            config=config,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        == "env-secret"
    )


def test_resolve_secret_field_env_var_non_string_returns_none():
    """Non-string env_var value (e.g. None) is treated as absent → None."""
    config = {"psk_env_var": None}
    assert (
        resolve_secret_field(
            config=config,
            inline_field="psk",
            env_var_field="psk_env_var",
            context="ctx",
        )
        is None
    )
