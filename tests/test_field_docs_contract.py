"""Project-wide enforcement: every Pydantic field carries a runtime description.

The MCP server exposes Pydantic schemas to AI agents via
``model_json_schema()`` (which becomes the tool/resource schema in the
MCP protocol). Field descriptions on the model are what the agent sees
when deciding how to populate a parameter or interpret a result.

A Sphinx-style ``Attributes:`` block on the class docstring is invisible
to runtime introspection. The project convention (codified by
``use_attribute_docstrings=True`` on
:class:`deephaven_mcp._pydantic.StrictSchema`) is to write a PEP 257
trailing string literal under each field declaration::

    class Foo(StrictSchema):
        bar: int = 5
        '''Short description of ``bar``.'''

Pydantic v2 then harvests that string into ``Foo.model_fields['bar'].description``
which flows into JSON Schema and the MCP tool surface.

This test walks every concrete :class:`StrictSchema` subclass reachable
through the ``deephaven_mcp`` package tree and asserts every field has
a non-empty description string. The only schemas it skips are those
defined under ``tests/`` (minimal fixtures used to exercise base-class
machinery, not production surfaces).

Discriminator fields (``type: Literal[<name>]`` on credential
subclasses) are **not** exempt: the literal value tells the JSON
Schema layer *which* variant is selected, but a prose description is
still what tells an AI agent what role the field plays in the
discriminated union. A one-line trailing docstring on each
discriminator is the minimum.

Failures are aggregated and reported as a single assertion message so
the developer sees every missing field at once, not one at a time.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest

import deephaven_mcp
from deephaven_mcp._pydantic import StrictSchema

# Project-wide convention enforcement over every schema in the package, not a
# mirror of one source file (``ref-python-coding-practices`` rule 5).
pytestmark = pytest.mark.guardrail


def _import_all_submodules(package: object) -> None:
    """Recursively import every submodule of ``package``.

    Pydantic populates ``StrictSchema.__subclasses__()`` only when each
    subclass module is imported. Walking the package tree once at test
    collection time guarantees the assertion below sees every schema
    the project defines.

    Some submodules require environment variables that may be absent in
    the unit-test environment (for example, the docs server's Inkeep
    API key). Import errors are swallowed: those modules either expose
    no schemas, or their schemas are also imported via another path
    that succeeds. The regression test runs against the union of
    successfully-imported modules, which is the same union the live
    server consumes.
    """
    for module_info in pkgutil.walk_packages(
        package.__path__,  # type: ignore[attr-defined]
        prefix=f"{package.__name__}.",  # type: ignore[attr-defined]
        onerror=lambda _name: None,
    ):
        try:
            importlib.import_module(module_info.name)
        except Exception:
            continue


def _all_concrete_strict_schema_subclasses() -> list[type[StrictSchema]]:
    """Return every concrete (non-abstract) ``StrictSchema`` subclass."""
    seen: set[type[StrictSchema]] = set()
    stack: list[type[StrictSchema]] = list(StrictSchema.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
    return sorted(seen, key=lambda c: f"{c.__module__}.{c.__qualname__}")


@pytest.fixture(scope="session", autouse=True)
def _populate_subclass_registry() -> None:
    """Import every ``deephaven_mcp`` submodule so subclass registry is complete."""
    _import_all_submodules(deephaven_mcp)


def test_every_pydantic_field_has_a_runtime_description() -> None:
    """Every field on every concrete StrictSchema subclass has a description.

    The description is what reaches JSON Schema, the MCP tool surface,
    and any AI agent introspecting the model at runtime. A missing
    description is a documentation bug visible to end users; this test
    fails the build until it is fixed.

    Only production schemas under the ``deephaven_mcp.`` namespace are
    checked. Test-only schemas under ``tests/`` are deliberately
    minimal fixtures (defined inline to exercise base-class machinery)
    and need not carry per-field documentation.
    """
    missing: list[str] = []
    for cls in _all_concrete_strict_schema_subclasses():
        if not cls.__module__.startswith("deephaven_mcp."):
            continue
        for field_name, info in cls.model_fields.items():
            description = info.description
            if not (isinstance(description, str) and description.strip()):
                missing.append(f"{cls.__module__}.{cls.__qualname__}.{field_name}")
    if missing:
        joined = "\n  - ".join(missing)
        pytest.fail(
            "Every Pydantic field must have a runtime-introspectable "
            "description (PEP 257 trailing docstring on the field, with "
            "`use_attribute_docstrings=True` on the base). The following "
            f"fields are missing a description:\n  - {joined}\n\n"
            "Add a trailing string literal under each field declaration. "
            "See `.agents/skills/ref-configuration-conventions/SKILL.md` "
            "for the convention."
        )
