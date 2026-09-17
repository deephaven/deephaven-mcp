"""Guard against reintroducing the abandoned-worker bug.

``asyncio.wait_for(asyncio.to_thread(...))`` looks like a bounded call but is
not: the timeout cancels the awaiting coroutine while the vendor call keeps a
shared default-executor worker forever. Enough of those and unrelated work
starves, because every ``asyncio.to_thread`` in the process shares one pool.

:func:`deephaven_mcp._blocking.run_blocking` is the bounded alternative — same
deadline, but on a private daemon thread, so an abandoned call costs one thread
instead of a shared worker.

The allowlist below records the sites that predate that helper. It may shrink,
never grow: a new entry means a new instance of the bug.
"""

import ast
import pathlib

import pytest

# Project-wide convention enforcement over every module in the package, not a
# mirror of one source file (``ref-python-coding-practices`` rule 5).
pytestmark = pytest.mark.guardrail

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "deephaven_mcp"

LEGACY_WAIT_FOR_TO_THREAD: frozenset[tuple[str, int]] = frozenset(
    {
        ("client/_auth_client.py", 164),
        ("client/_controller_client.py", 264),
        ("client/_controller_client.py", 374),
        ("client/_controller_client.py", 857),
        ("client/_controller_client.py", 1207),
        ("client/_controller_client.py", 1303),
        ("client/_session.py", 1273),
        ("client/_session_factory.py", 292),
        ("client/_session_factory.py", 397),
        ("client/_session_factory.py", 822),
        ("client/_session_factory.py", 996),
        ("client/_session_factory.py", 1117),
        ("client/_session_factory.py", 1226),
        ("client/_session_factory.py", 1320),
        ("client/_session_factory.py", 1429),
        ("client/_session_factory.py", 1544),
        ("client/_session_factory.py", 1649),
    }
)
"""Sites that wrap ``to_thread`` in ``wait_for``, from before ``run_blocking``.

Converting one means deleting its entry. Line numbers are matched loosely (the
test compares counts per file) so unrelated edits above a site do not fail it.
"""


def _wait_for_to_thread_sites() -> list[tuple[str, int]]:
    """Return every ``asyncio.wait_for(asyncio.to_thread(...))`` under ``src``."""
    found: list[tuple[str, int]] = []
    for path in sorted(SRC.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "wait_for"
                and node.args
            ):
                continue
            inner = node.args[0]
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr == "to_thread"
            ):
                found.append((path.relative_to(SRC).as_posix(), node.lineno))
    return found


def test_no_new_wait_for_around_to_thread() -> None:
    """A timeout around ``to_thread`` abandons a shared worker; use run_blocking."""
    by_file: dict[str, int] = {}
    for rel, _ in _wait_for_to_thread_sites():
        by_file[rel] = by_file.get(rel, 0) + 1

    allowed: dict[str, int] = {}
    for rel, _ in LEGACY_WAIT_FOR_TO_THREAD:
        allowed[rel] = allowed.get(rel, 0) + 1

    new = {f: n for f, n in by_file.items() if n > allowed.get(f, 0)}
    assert not new, (
        "New asyncio.wait_for(asyncio.to_thread(...)) site(s) in "
        f"{sorted(new)}. The timeout cancels the coroutine but strands a shared "
        "default-executor worker. Use deephaven_mcp._blocking.run_blocking, "
        "which bounds the call on a private daemon thread."
    )


def test_legacy_allowlist_does_not_grow_stale() -> None:
    """Converting a legacy site must also shrink the allowlist."""
    by_file: dict[str, int] = {}
    for rel, _ in _wait_for_to_thread_sites():
        by_file[rel] = by_file.get(rel, 0) + 1

    allowed: dict[str, int] = {}
    for rel, _ in LEGACY_WAIT_FOR_TO_THREAD:
        allowed[rel] = allowed.get(rel, 0) + 1

    stale = {
        f: (n, by_file.get(f, 0)) for f, n in allowed.items() if by_file.get(f, 0) < n
    }
    assert not stale, (
        f"Allowlist over-counts {sorted(stale)} (allowed, actual). Remove the "
        "converted entries from LEGACY_WAIT_FOR_TO_THREAD."
    )
