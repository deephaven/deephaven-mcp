"""Tests for ``deephaven_mcp._exception_utils``."""

from __future__ import annotations

from deephaven_mcp._exception_utils import (
    describe_exception,
    exception_summary,
    walk_exceptions,
)

# ---------------------------------------------------------------------------
# exception_summary
# ---------------------------------------------------------------------------


def test_summary_type_and_message() -> None:
    assert exception_summary(ValueError("bad input")) == "ValueError: bad input"


def test_summary_empty_message_falls_back_to_repr() -> None:
    assert exception_summary(ValueError()) == "ValueError: ValueError()"


# ---------------------------------------------------------------------------
# walk_exceptions
# ---------------------------------------------------------------------------


def test_walk_single_exception() -> None:
    exc = ValueError("v")
    assert list(walk_exceptions(exc)) == [exc]


def test_walk_group_yields_only_leaves() -> None:
    a, b = ValueError("a"), RuntimeError("b")
    group = ExceptionGroup("g", [a, b])
    assert list(walk_exceptions(group)) == [a, b]


def test_walk_nested_groups() -> None:
    a, b = ValueError("a"), RuntimeError("b")
    group = ExceptionGroup("outer", [ExceptionGroup("inner", [a]), b])
    assert list(walk_exceptions(group)) == [a, b]


def test_walk_follows_cause_by_default() -> None:
    cause = OSError("root")
    exc = RuntimeError("wrapper")
    exc.__cause__ = cause
    assert list(walk_exceptions(exc)) == [exc, cause]


def test_walk_cause_can_be_disabled() -> None:
    exc = RuntimeError("wrapper")
    exc.__cause__ = OSError("root")
    assert list(walk_exceptions(exc, follow_cause=False)) == [exc]


def test_walk_context_off_by_default() -> None:
    exc = RuntimeError("wrapper")
    exc.__context__ = OSError("suppressed")
    assert list(walk_exceptions(exc)) == [exc]


def test_walk_context_when_enabled() -> None:
    context = OSError("suppressed")
    exc = RuntimeError("wrapper")
    exc.__context__ = context
    assert list(walk_exceptions(exc, follow_context=True)) == [exc, context]


def test_walk_cause_inside_group_member() -> None:
    cause = OSError("root")
    member = RuntimeError("member")
    member.__cause__ = cause
    group = ExceptionGroup("g", [member])
    assert list(walk_exceptions(group)) == [member, cause]


def test_walk_group_as_cause() -> None:
    a = ValueError("a")
    exc = RuntimeError("wrapper")
    exc.__cause__ = ExceptionGroup("g", [a])
    assert list(walk_exceptions(exc)) == [exc, a]


def test_walk_group_node_cause_is_followed() -> None:
    cause = OSError("root")
    group = ExceptionGroup("g", [ValueError("a")])
    group.__cause__ = cause
    out = list(walk_exceptions(group))
    assert cause in out


def test_walk_is_cycle_safe() -> None:
    a = ValueError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert list(walk_exceptions(a)) == [a, b]


# ---------------------------------------------------------------------------
# describe_exception
# ---------------------------------------------------------------------------


def test_describe_single_exception() -> None:
    assert describe_exception(ValueError("v")) == "ValueError: v"


def test_describe_group_joins_members() -> None:
    group = ExceptionGroup("g", [ValueError("a"), RuntimeError("b")])
    assert describe_exception(group) == "ValueError: a; RuntimeError: b"


def test_describe_nested_groups() -> None:
    group = ExceptionGroup(
        "outer", [ExceptionGroup("inner", [ValueError("a")]), RuntimeError("b")]
    )
    assert describe_exception(group) == "ValueError: a; RuntimeError: b"


def test_describe_cause_chain_joined_with_arrow() -> None:
    cause = OSError("root")
    exc = RuntimeError("wrapper")
    exc.__cause__ = cause
    assert describe_exception(exc) == "RuntimeError: wrapper -> OSError: root"


def test_describe_dedupes_repeated_cause_message() -> None:
    """A cause whose text is already contained in the wrapper's is skipped."""
    cause = OSError("all connection attempts failed")
    exc = OSError("all connection attempts failed")
    exc.__cause__ = cause
    assert describe_exception(exc) == "OSError: all connection attempts failed"


def test_describe_context_not_followed() -> None:
    exc = RuntimeError("wrapper")
    exc.__context__ = OSError("suppressed")
    assert describe_exception(exc) == "RuntimeError: wrapper"


def test_describe_group_member_with_cause() -> None:
    cause = OSError("root")
    member = RuntimeError("member")
    member.__cause__ = cause
    group = ExceptionGroup("g", [member, ValueError("v")])
    assert (
        describe_exception(group)
        == "RuntimeError: member -> OSError: root; ValueError: v"
    )


def test_describe_group_as_cause() -> None:
    exc = RuntimeError("wrapper")
    exc.__cause__ = ExceptionGroup("g", [ValueError("a"), OSError("b")])
    assert (
        describe_exception(exc) == "RuntimeError: wrapper -> ValueError: a; OSError: b"
    )


def test_describe_custom_render_hook() -> None:
    def render(e: BaseException) -> str:
        return f"<{type(e).__name__}>"

    group = ExceptionGroup("g", [ValueError("a"), RuntimeError("b")])
    assert describe_exception(group, render=render) == "<ValueError>; <RuntimeError>"


def test_describe_is_cycle_safe() -> None:
    a = ValueError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert describe_exception(a) == "ValueError: a -> RuntimeError: b"


def test_describe_empty_group_then_cause() -> None:
    """An empty rendering (fully-cycled group) falls through to the cause text."""
    a = ValueError("a")
    group = ExceptionGroup("g", [a])
    outer = ExceptionGroup("outer", [a, group])
    # ``group`` renders empty (its only member was already seen), so the
    # outer join is just the member's text.
    assert describe_exception(outer) == "ValueError: a"


def test_describe_empty_text_uses_cause_text_alone() -> None:
    """A node that renders empty but has a cause yields just the cause text."""
    a = ValueError("a")
    inner = ExceptionGroup("g", [a])
    inner.__cause__ = OSError("root")
    outer = ExceptionGroup("outer", [a, inner])
    assert describe_exception(outer) == "ValueError: a; OSError: root"
