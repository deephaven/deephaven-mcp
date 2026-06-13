"""Schema-drift guard for ``dh-mcp`` MCP-tool wrappers.

Each runtime CLI command that fronts an MCP tool declares the binding
via ``wraps_tool`` / ``wraps_tools`` on
:class:`~deephaven_mcp.cli._help.HelpfulCommand`. This test reads those
bindings off the live click tree, builds every tool's JSON input schema
in-process from the ``register_tools`` functions, and fails when a
wrapper and its tool have drifted:

- **Drift:** a tool requires a parameter the wrapper neither surfaces as
  a flag/argument nor lists in ``intentionally_unsupported``.
- **Phantom:** a wrapper declares a flag that is not a parameter of any
  tool it wraps (and is not a declared ``router_params`` flag) — the
  call would fail at runtime.

When this test fails, update the wrapper (add the flag, or add the
parameter to ``intentionally_unsupported`` / ``router_params``) in the
same change that altered the tool signature. See
``docs/design/CLI_TOOL_WRAPPING.md``.

Scope note: the guard inspects a command's *declared* click params, not
the argument dict its body builds, and joins them to tool params by
**name only** — it does not check that a flag's type/cardinality matches
the tool param (a scalar flag bound to a ``list`` param passes here).
A handler that forwards a literal key the tool doesn't accept (e.g.
``arguments["max_row"] = max_rows``) and type/cardinality mismatches are
outside this test's reach — the per-command unit tests, which assert the
exact argument dict passed to ``call_tool``, cover that.
"""

from __future__ import annotations

import asyncio
from functools import cache

import click
import pytest
from mcp.server.fastmcp import FastMCP

from deephaven_mcp.cli._commands._wrapping import wrapper_error_codes
from deephaven_mcp.cli._help import HelpfulCommand
from deephaven_mcp.cli._main import cli
from deephaven_mcp.mcp_systems_server._tools import (
    catalog,
    pq,
    script,
    session,
    session_community,
    session_enterprise,
    table,
)

# Every tool module whose tools the CLI may wrap. Registering all of
# them on one server (no config gating) yields the full schema set.
_TOOL_MODULES = (
    session,
    table,
    script,
    catalog,
    session_community,
    session_enterprise,
    pq,
)

# Click adds a help option to every command; it is never a tool arg.
_NON_TOOL_PARAMS = frozenset({"help"})


@cache
def _tool_schemas() -> dict[str, dict]:
    """Return ``{tool_name: input_json_schema}`` for every registered tool.

    Computed lazily and cached so a tool-registration import failure surfaces
    as a failure of the tests that read it, not a collection-time error that
    aborts the whole suite.
    """
    server: FastMCP = FastMCP("drift")
    for module in _TOOL_MODULES:
        module.register_tools(server)
    tools = asyncio.run(server.list_tools())
    return {t.name: t.inputSchema for t in tools}


def _wrapped_commands(
    cmd: click.Command, path: tuple[str, ...] = ()
) -> list[tuple[str, HelpfulCommand]]:
    """Collect ``(command_path, command)`` for every wrapper in the tree."""
    found: list[tuple[str, HelpfulCommand]] = []
    here = (*path, cmd.name) if cmd.name else path
    if isinstance(cmd, HelpfulCommand) and (cmd.wraps_tool or cmd.wraps_tools):
        found.append((" ".join(here), cmd))
    if isinstance(cmd, click.Group):
        for sub in cmd.commands.values():
            found.extend(_wrapped_commands(sub, here))
    return found


_WRAPPERS = _wrapped_commands(cli)


def test_at_least_one_wrapper_is_discovered() -> None:
    """Guard the guard: a tree with no wrappers would vacuously pass."""
    assert _WRAPPERS, "no wraps_tool/wraps_tools bindings found on the CLI tree"


@pytest.mark.parametrize("path, cmd", _WRAPPERS, ids=[p for p, _ in _WRAPPERS])
def test_wrapper_matches_tool_schema(path: str, cmd: HelpfulCommand) -> None:
    """A wrapper surfaces every required tool param and no phantom flags."""
    schemas = _tool_schemas()
    tools = sorted({*cmd.wraps_tools, *([cmd.wraps_tool] if cmd.wraps_tool else [])})
    declared = {p.name for p in cmd.params if p.name and p.name not in _NON_TOOL_PARAMS}

    for tool in tools:
        assert tool in schemas, f"{path}: wraps unknown tool {tool!r}"
        schema = schemas[tool]
        required = set(schema.get("required", []) or [])
        # Drift: every required tool param must be surfaced or allowlisted.
        missing = required - declared - cmd.intentionally_unsupported
        assert not missing, (
            f"{path}: tool {tool!r} requires {sorted(missing)} which the wrapper "
            f"neither exposes as a flag nor lists in intentionally_unsupported"
        )

    # Phantom: every declared flag (minus router + client-only flags)
    # must be a real parameter of at least one wrapped tool.
    props_union: set[str] = set()
    for tool in tools:
        props_union |= set(schemas[tool].get("properties", {}))
    phantom = declared - props_union - cmd.router_params - cmd.client_only_params
    assert not phantom, (
        f"{path}: declares {sorted(phantom)} which are not parameters of "
        f"{tools} (and not declared router_params/client_only_params)"
    )

    # Exemptions must reference real tool parameters, so a stale/typo'd
    # entry fails the build rather than silently absorbing a future
    # required param of the same name. ``client_only_params`` is the
    # explicit escape hatch for flags that are deliberately *not* tool
    # parameters (e.g. ``print_only``), so it is not checked here.
    stale_router = cmd.router_params - props_union
    assert not stale_router, (
        f"{path}: router_params {sorted(stale_router)} are not parameters of "
        f"{tools}; move client-side-only flags to client_only_params"
    )
    stale_unsupported = cmd.intentionally_unsupported - props_union
    assert not stale_unsupported, (
        f"{path}: intentionally_unsupported {sorted(stale_unsupported)} are not "
        f"parameters of {tools} (stale exemption)"
    )

    # ``client_only_params`` must genuinely NOT be tool parameters — that is
    # their definition. A flag mislabeled client-only that is actually a
    # (required) tool param escapes both the drift and phantom checks (it sits
    # in ``declared``) yet is never forwarded by the wrapper body, so the tool
    # call would fail at runtime while this guard stayed green.
    mislabeled_client = cmd.client_only_params & props_union
    assert not mislabeled_client, (
        f"{path}: client_only_params {sorted(mislabeled_client)} ARE parameters "
        f"of {tools}; declare them (so the body forwards them) or use "
        f"router_params, not client-only"
    )


@pytest.mark.parametrize("path, cmd", _WRAPPERS, ids=[p for p, _ in _WRAPPERS])
def test_wrapper_help_lists_acquire_error_codes(path: str, cmd: HelpfulCommand) -> None:
    """Every tool-wrapping command surfaces the shared acquire error codes in help.

    Each wrapper routes through ``_wrapping.acquire`` + ``call_tool``, so its
    help must list the codes that flow can raise. This pins each command's
    ``error_codes`` to the single-sourced ``wrapper_error_codes`` set and fails
    a new wrapper that forgets to splice it in.
    """
    help_text = cmd.help or ""
    missing = [
        ec.value
        for ec in wrapper_error_codes(tool_error=False)
        if ec.value not in help_text
    ]
    assert not missing, (
        f"{path}: help omits acquire error codes {missing}; "
        f"splice *wrapper_error_codes() into the command's error_codes"
    )
