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

LEGACY_WAIT_FOR_TO_THREAD: frozenset[tuple[str, str]] = frozenset(
    {
        ("client/_auth_client.py", "CorePlusAuthClient.get_token"),
        ("client/_controller_client.py", "CorePlusControllerClient.add_query"),
        ("client/_controller_client.py", "CorePlusControllerClient.delete_query"),
        ("client/_controller_client.py", "CorePlusControllerClient.modify_query"),
        ("client/_controller_client.py", "CorePlusControllerClient.ping"),
        ("client/_controller_client.py", "CorePlusControllerClient.subscribe"),
        ("client/_session.py", "CoreSession.from_credentials"),
        ("client/_session_factory.py", "CorePlusSessionFactory.connect_to_new_worker"),
        (
            "client/_session_factory.py",
            "CorePlusSessionFactory.connect_to_persistent_query",
        ),
        ("client/_session_factory.py", "CorePlusSessionFactory.delete_key"),
        ("client/_session_factory.py", "CorePlusSessionFactory.from_credentials"),
        ("client/_session_factory.py", "CorePlusSessionFactory.from_url"),
        ("client/_session_factory.py", "CorePlusSessionFactory.password"),
        ("client/_session_factory.py", "CorePlusSessionFactory.ping"),
        ("client/_session_factory.py", "CorePlusSessionFactory.private_key"),
        ("client/_session_factory.py", "CorePlusSessionFactory.saml"),
        ("client/_session_factory.py", "CorePlusSessionFactory.upload_key"),
    }
)
"""Sites that wrap ``to_thread`` in ``wait_for``, from before ``run_blocking``.

Converting one means deleting its entry. A site is identified by its file and
enclosing qualified function, which survives edits that move its line.
"""


def _enclosing(tree: ast.Module) -> dict[ast.AST, str]:
    """Map every node to the qualified name of the function enclosing it.

    Args:
        tree (ast.Module): Parsed module to walk.

    Returns:
        dict[ast.AST, str]: Node to dotted qualified name, ``"<module>"`` at
            module scope.
    """
    scopes: dict[ast.AST, str] = {}

    def walk(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            scopes[child] = ".".join(scope) or "<module>"
            nested = scope
            if isinstance(child, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                nested = (*scope, child.name)
            walk(child, nested)

    walk(tree, ())
    return scopes


def _wait_for_to_thread_sites() -> list[tuple[str, str]]:
    """Return every ``asyncio.wait_for(asyncio.to_thread(...))`` under ``src``.

    Returns:
        list[tuple[str, str]]: One entry per site, as the module path relative
            to ``src/deephaven_mcp`` and its enclosing qualified function.
    """
    found: list[tuple[str, str]] = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text())
        scopes = _enclosing(tree)
        for node in ast.walk(tree):
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
                found.append((path.relative_to(SRC).as_posix(), scopes[node]))
    return found


def test_no_new_wait_for_around_to_thread() -> None:
    """A timeout around ``to_thread`` abandons a shared worker; use run_blocking."""
    sites = _wait_for_to_thread_sites()
    new = sorted(set(sites) - LEGACY_WAIT_FOR_TO_THREAD)
    assert not new, (
        f"New asyncio.wait_for(asyncio.to_thread(...)) site(s): {new}. "
        "The timeout cancels the coroutine but strands a shared "
        "default-executor worker. Use deephaven_mcp._blocking.run_blocking, "
        "which bounds the call on a private daemon thread."
    )
    # A second site inside an already-listed function shares its identity, so
    # the totals have to agree as well.
    assert len(sites) == len(LEGACY_WAIT_FOR_TO_THREAD), (
        f"{len(sites)} site(s) found but {len(LEGACY_WAIT_FOR_TO_THREAD)} "
        "allowlisted; a listed function gained another one."
    )


def test_legacy_allowlist_does_not_grow_stale() -> None:
    """Converting a legacy site must also shrink the allowlist."""
    stale = sorted(LEGACY_WAIT_FOR_TO_THREAD - set(_wait_for_to_thread_sites()))
    assert not stale, (
        f"Allowlist names site(s) that no longer exist: {stale}. Remove the "
        "converted entries from LEGACY_WAIT_FOR_TO_THREAD."
    )
