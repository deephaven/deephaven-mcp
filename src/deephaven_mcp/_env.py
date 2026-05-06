"""Typed helpers for reading process environment variables.

Centralizes the parse-with-default pattern that otherwise gets duplicated
across the codebase as ``int(os.environ.get(NAME, "60"))`` /
``float(os.environ.get(NAME, "1.0"))`` / ad-hoc truthy checks. Every
helper:

- Reads the variable lazily at call time (so tests can monkeypatch
  freely; nothing is captured at import time).
- Returns the documented default when the variable is unset.
- The parsing helpers (:func:`env_int`, :func:`env_float`) raise
  :class:`ValueError` whose message names the offending environment
  variable so operators can fix the misconfiguration without digging
  through stack frames.

The helpers preserve the externally visible semantics of the inline
expressions they replace:

- ``env_str(name, default)`` mirrors :py:func:`os.environ.get` (a set
  variable returns its value, even when empty).
- ``env_int`` / ``env_float`` cast via :py:func:`int` / :py:func:`float`
  but wrap the resulting :class:`ValueError` with an actionable message.
- ``env_bool`` follows the uvicorn convention: case-insensitive and
  whitespace-trimmed match against ``{"1", "true", "yes"}``; everything
  else is falsy.
- ``env_required`` raises :class:`RuntimeError` when the variable is
  missing or empty, matching how the few existing required-env-var
  callers behave today.
"""

from __future__ import annotations

import os
from typing import overload

__all__ = [
    "env_bool",
    "env_float",
    "env_int",
    "env_required",
    "env_str",
]

_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes"})
"""Case-insensitive truthy values for boolean environment variables.

Matches the convention used by uvicorn and most Python services: an env
var is "true" iff it is set to one of these tokens (after stripping
surrounding whitespace and lowercasing). Anything else (including the
common typos ``on``/``y``/``t``) is falsy. Using a small explicit set
avoids accidentally treating arbitrary non-empty strings as truthy.
"""


@overload
def env_str(name: str) -> str | None: ...


@overload
def env_str(name: str, default: str) -> str: ...


@overload
def env_str(name: str, default: None) -> str | None: ...


def env_str(name: str, default: str | None = None) -> str | None:
    """Return the environment variable ``name`` as a string.

    A set variable returns its value even when that value is the empty
    string. Callers that want "empty -> default" semantics should write
    ``env_str(name) or fallback``.

    Args:
        name (str): The environment variable name.
        default (str | None): Value to return when ``name`` is unset.
            Defaults to ``None``.

    Returns:
        str | None: The variable's value, or ``default`` if unset.
    """
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    """Return the environment variable ``name`` parsed as an ``int``.

    Args:
        name (str): The environment variable name.
        default (int): Value to return when ``name`` is unset.

    Returns:
        int: The parsed integer, or ``default`` if unset.

    Raises:
        ValueError: If the variable is set to a value that
            :py:func:`int` cannot parse. The message names the
            offending variable and includes the underlying error so
            operators can correct the misconfiguration.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name}={raw!r} is not a valid integer: {exc}"
        ) from exc


def env_float(name: str, default: float) -> float:
    """Return the environment variable ``name`` parsed as a ``float``.

    Args:
        name (str): The environment variable name.
        default (float): Value to return when ``name`` is unset.

    Returns:
        float: The parsed float, or ``default`` if unset.

    Raises:
        ValueError: If the variable is set to a value that
            :py:func:`float` cannot parse. The message names the
            offending variable and includes the underlying error.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name}={raw!r} is not a valid float: {exc}"
        ) from exc


def env_bool(name: str, *, default: bool = False) -> bool:
    """Return the environment variable ``name`` as a boolean.

    Args:
        name (str): The environment variable name.
        default (bool): Value to return when ``name`` is unset.
            Defaults to ``False`` (fail-closed).

    Returns:
        bool: ``True`` if the variable is set to a value in
        :data:`_TRUTHY_ENV_VALUES` (case-insensitive, whitespace
        trimmed); ``False`` if set to anything else; ``default`` if
        unset.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY_ENV_VALUES


def env_required(name: str, *, error_msg: str | None = None) -> str:
    """Return the environment variable ``name`` or raise.

    A variable is considered "missing" if it is unset OR set to the
    empty string; both cases raise. This is intentionally stricter than
    :py:func:`os.environ.get`'s notion of "set" so an empty value is
    treated as misconfiguration rather than silently accepted.

    Args:
        name (str): The environment variable name.
        error_msg (str | None): Custom message for the raised
            :class:`RuntimeError`. When ``None``, the default message
            ``f"Environment variable {name} is not set."`` is used.

    Returns:
        str: The variable's non-empty value.

    Raises:
        RuntimeError: When the variable is unset or empty.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(error_msg or f"Environment variable {name} is not set.")
    return value
