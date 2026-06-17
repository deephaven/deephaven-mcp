"""Tests for ``deephaven_mcp.cli._async``."""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging

import pytest

from deephaven_mcp.cli._async import _cli_loop_exception_handler, run_async


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


def test_coro_installs_loop_exception_handler() -> None:
    """The wrapper installs the CLI loop exception handler before running."""

    @run_async
    async def grab() -> object:
        return asyncio.get_running_loop().get_exception_handler()

    assert grab() is _cli_loop_exception_handler


def test_loop_exception_handler_logs_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Loop-level exceptions are logged at DEBUG, not ERROR."""
    with caplog.at_level(logging.DEBUG, logger="deephaven_mcp.cli._async"):
        _cli_loop_exception_handler(
            asyncio.new_event_loop(),
            {"message": "boom", "exception": ValueError("x")},
        )
    records = [r for r in caplog.records if r.name == "deephaven_mcp.cli._async"]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert "boom" in records[0].getMessage()


def test_loop_exception_handler_default_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A context without a message falls back to a default description."""
    with caplog.at_level(logging.DEBUG, logger="deephaven_mcp.cli._async"):
        _cli_loop_exception_handler(asyncio.new_event_loop(), {})
    records = [r for r in caplog.records if r.name == "deephaven_mcp.cli._async"]
    assert len(records) == 1
    assert "Unhandled exception in event loop" in records[0].getMessage()
