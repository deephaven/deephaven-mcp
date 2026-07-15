"""Exception utilities: rendering and traversal of exceptions, groups, and cause chains."""

from __future__ import annotations

from collections.abc import Callable, Iterator

__all__ = ["describe_exception", "exception_summary", "walk_exceptions"]


def exception_summary(exc: BaseException) -> str:
    """Render an exception as the canonical ``TypeName: message`` form.

    This is the project-wide format for exceptions embedded in
    user-facing strings (MCP payload ``error`` fields, CLI error
    messages, recorded init errors). It is also a parsed contract:
    ``_short_reason`` in ``mcp_systems_server/_tools/session_enterprise.py``
    extracts the type name back out of it.

    Args:
        exc (BaseException): The exception to render.

    Returns:
        str: ``"TypeName: message"``; when the exception's message is
            empty, its ``repr`` stands in for the message.
    """
    return f"{type(exc).__name__}: {str(exc) or repr(exc)}"


def walk_exceptions(
    exc: BaseException,
    *,
    follow_cause: bool = True,
    follow_context: bool = False,
) -> Iterator[BaseException]:
    """Iterate depth-first over an exception, its group members, and its links.

    Descends into :class:`BaseExceptionGroup` members and, per the
    flags, follows ``__cause__`` (``raise ... from ...``) and
    ``__context__`` (implicit chaining) links. Group nodes are
    containers, not failures, so only non-group exceptions are
    yielded. Traversal is cycle-safe; each exception is yielded at
    most once.

    Args:
        exc (BaseException): The root exception to walk.
        follow_cause (bool): Follow explicit ``__cause__`` links.
        follow_context (bool): Follow implicit ``__context__`` links.
            Off by default so unrelated suppressed exceptions do not
            leak into the result.

    Yields:
        BaseException: Each non-group exception reachable from ``exc``.
    """
    seen: set[int] = set()

    def _walk(e: BaseException) -> Iterator[BaseException]:
        if id(e) in seen:
            return
        seen.add(id(e))
        if isinstance(e, BaseExceptionGroup):
            for sub in e.exceptions:
                yield from _walk(sub)
        else:
            yield e
        if follow_cause and e.__cause__ is not None:
            yield from _walk(e.__cause__)
        if follow_context and e.__context__ is not None:
            yield from _walk(e.__context__)

    return _walk(exc)


def describe_exception(
    exc: BaseException,
    *,
    render: Callable[[BaseException], str] = exception_summary,
) -> str:
    """Render an exception tree into a single readable line.

    Handles both composition axes together: members of a
    :class:`BaseExceptionGroup` are joined with ``"; "`` and
    ``__cause__`` chains are joined with ``" -> "``, recursively — a
    group member may carry a cause chain and a cause may itself be a
    group. A cause's text is skipped when it is already contained in
    the text it would follow, so wrapper exceptions that repeat the
    underlying message do not produce duplicate fragments. Implicit
    ``__context__`` links are not followed. Traversal is cycle-safe.

    Args:
        exc (BaseException): The exception to describe.
        render (Callable[[BaseException], str]): Renderer for each
            non-group exception; defaults to :func:`exception_summary`.
            Pass a custom renderer to enrich specific leaf types (e.g.
            gRPC calls with status details).

    Returns:
        str: The joined one-line description.
    """
    seen: set[int] = set()

    def _describe(e: BaseException) -> str:
        if id(e) in seen:
            return ""
        seen.add(id(e))
        if isinstance(e, BaseExceptionGroup):
            parts = [p for p in (_describe(sub) for sub in e.exceptions) if p]
            text = "; ".join(parts)
        else:
            text = render(e)
        if e.__cause__ is not None:
            cause_text = _describe(e.__cause__)
            if cause_text and cause_text not in text:
                text = f"{text} -> {cause_text}" if text else cause_text
        return text

    return _describe(exc)
