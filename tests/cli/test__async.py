"""Tests for ``deephaven_mcp.cli._async``."""

from __future__ import annotations

import functools
import inspect

from deephaven_mcp.cli._async import run_async


def test_coro_runs_async_function_and_returns_value() -> None:
    @run_async
    async def hello(name: str) -> str:
        """Say hi to ``name``."""
        return f"hi {name}"

    assert hello("world") == "hi world"


def test_coro_preserves_docstring_and_name() -> None:
    @run_async
    async def f() -> int:
        """Original docstring."""
        return 1

    assert f.__name__ == "f"
    assert f.__doc__ == "Original docstring."


def test_coro_preserves_wrapped_attribute_for_inspect() -> None:
    async def underlying(x: int) -> int:
        """Inner."""
        return x + 1

    wrapped = run_async(underlying)
    # ``functools.wraps`` sets ``__wrapped__`` pointing at the original.
    assert getattr(wrapped, "__wrapped__", None) is underlying
    # ``inspect.signature`` follows ``__wrapped__`` for parameter
    # introspection — this is what feeds click's ``--help`` rendering.
    sig = inspect.signature(wrapped)
    assert "x" in sig.parameters


def test_coro_propagates_exceptions() -> None:
    @run_async
    async def boom() -> None:
        raise ValueError("nope")

    try:
        boom()
    except ValueError as exc:
        assert str(exc) == "nope"
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError")


def test_coro_is_a_decorator_via_functools_wraps() -> None:
    """The decorator output should be a ``functools.wraps``-style wrapper."""

    async def src() -> None:
        """src docstring."""

    wrapped = run_async(src)
    # functools.wraps copies these attributes.
    for attr in functools.WRAPPER_ASSIGNMENTS:
        assert getattr(wrapped, attr) == getattr(src, attr)
