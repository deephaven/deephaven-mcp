"""Unit tests for :mod:`deephaven_mcp._env`."""

from __future__ import annotations

import pytest

from deephaven_mcp._env import (
    _TRUTHY_ENV_VALUES,
    env_bool,
    env_float,
    env_int,
    env_required,
    env_str,
)

# ---------------------------------------------------------------------------
# env_str
# ---------------------------------------------------------------------------


def test_env_str_unset_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DH_TEST_X", raising=False)
    assert env_str("DH_TEST_X") is None
    assert env_str("DH_TEST_X", "fallback") == "fallback"


def test_env_str_set_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DH_TEST_X", "hello")
    assert env_str("DH_TEST_X") == "hello"
    assert env_str("DH_TEST_X", "fallback") == "hello"


def test_env_str_empty_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """A set-to-empty variable returns ``""``, matching ``os.environ.get``."""
    monkeypatch.setenv("DH_TEST_X", "")
    assert env_str("DH_TEST_X", "fallback") == ""


# ---------------------------------------------------------------------------
# env_int
# ---------------------------------------------------------------------------


def test_env_int_unset_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DH_TEST_X", raising=False)
    assert env_int("DH_TEST_X", 42) == 42


def test_env_int_set_returns_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DH_TEST_X", "7")
    assert env_int("DH_TEST_X", 42) == 7


def test_env_int_negative_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DH_TEST_X", "-3")
    assert env_int("DH_TEST_X", 0) == -3


def test_env_int_invalid_raises_with_var_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DH_TEST_X", "abc")
    with pytest.raises(ValueError, match=r"DH_TEST_X='abc'"):
        env_int("DH_TEST_X", 0)


def test_env_int_empty_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty string is "set" but not parseable -> ValueError, not default."""
    monkeypatch.setenv("DH_TEST_X", "")
    with pytest.raises(ValueError, match=r"DH_TEST_X=''"):
        env_int("DH_TEST_X", 0)


# ---------------------------------------------------------------------------
# env_float
# ---------------------------------------------------------------------------


def test_env_float_unset_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DH_TEST_X", raising=False)
    assert env_float("DH_TEST_X", 1.5) == 1.5


def test_env_float_set_returns_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DH_TEST_X", "2.75")
    assert env_float("DH_TEST_X", 0.0) == 2.75


def test_env_float_integer_string_parses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DH_TEST_X", "3")
    assert env_float("DH_TEST_X", 0.0) == 3.0


def test_env_float_invalid_raises_with_var_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DH_TEST_X", "not-a-float")
    with pytest.raises(ValueError, match=r"DH_TEST_X='not-a-float'"):
        env_float("DH_TEST_X", 0.0)


# ---------------------------------------------------------------------------
# env_bool
# ---------------------------------------------------------------------------


def test_env_bool_unset_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DH_TEST_X", raising=False)
    assert env_bool("DH_TEST_X") is False
    assert env_bool("DH_TEST_X", default=True) is True


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True", "yes", "YES", "Yes"])
def test_env_bool_truthy_values(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("DH_TEST_X", raw)
    assert env_bool("DH_TEST_X") is True


def test_env_bool_truthy_with_whitespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DH_TEST_X", "  Yes  ")
    assert env_bool("DH_TEST_X") is True


@pytest.mark.parametrize(
    "raw", ["0", "false", "no", "off", "on", "y", "t", "", "garbage"]
)
def test_env_bool_falsy_values(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """Anything outside the truthy set is False, even when default=True."""
    monkeypatch.setenv("DH_TEST_X", raw)
    assert env_bool("DH_TEST_X") is False
    assert env_bool("DH_TEST_X", default=True) is False


def test_truthy_set_contents() -> None:
    """Lock the public truthy convention: only ``1``, ``true``, ``yes``."""
    assert _TRUTHY_ENV_VALUES == frozenset({"1", "true", "yes"})


# ---------------------------------------------------------------------------
# env_required
# ---------------------------------------------------------------------------


def test_env_required_set_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DH_TEST_X", "/path/to/config")
    assert env_required("DH_TEST_X") == "/path/to/config"


def test_env_required_unset_raises_default_msg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DH_TEST_X", raising=False)
    with pytest.raises(
        RuntimeError, match=r"Environment variable DH_TEST_X is not set\."
    ):
        env_required("DH_TEST_X")


def test_env_required_empty_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set-to-empty is treated as missing, mirroring existing callers."""
    monkeypatch.setenv("DH_TEST_X", "")
    with pytest.raises(RuntimeError, match=r"DH_TEST_X is not set"):
        env_required("DH_TEST_X")


def test_env_required_unset_raises_custom_msg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DH_TEST_X", raising=False)
    with pytest.raises(RuntimeError, match=r"please configure DH_TEST_X first"):
        env_required("DH_TEST_X", error_msg="please configure DH_TEST_X first")
