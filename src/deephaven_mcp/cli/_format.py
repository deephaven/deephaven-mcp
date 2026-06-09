"""Output rendering for ``dh-mcp`` subcommand results.

Subcommand handlers produce one of these things:

- A list of MCP tool descriptors (``dh-mcp tool list``).
- A single ``CallToolResult`` (``dh-mcp tool call <name>``).
- A diagnostic record (``dh-mcp daemon status``).

Each is rendered through :func:`format_output`, which dispatches on
the requested ``output`` mode (``"human"`` / ``"json"`` / ``"yaml"``)
and the runtime value type. Human output is intentionally minimal: a
flat two-column listing for tool catalogs, ``TextContent``
concatenation for tool results, and ``key: value`` lines for
diagnostic dicts. ``json`` and ``yaml`` modes emit deterministically
sorted documents suitable for piping into ``jq`` / ``yq`` or for
programmatic consumption by AI agents.
"""

from __future__ import annotations

__all__ = ["OUTPUT_MODES", "OutputMode", "format_output"]

import json
import shutil
from typing import Any, Literal, assert_never, get_args

import yaml
from mcp.types import CallToolResult, TextContent, Tool
from pydantic import BaseModel

OutputMode = Literal["human", "json", "yaml"]
"""Static type of the ``-o/--output`` flag: one of ``human``/``json``/``yaml``."""

OUTPUT_MODES: tuple[OutputMode, ...] = get_args(OutputMode)
"""Runtime tuple of accepted ``-o/--output`` values, derived from :data:`OutputMode`."""


# ``Any``: renders heterogeneous CLI return values (pydantic models,
# redacted dicts, primitive scalars) recursively into JSON-safe data.
def _coerce_jsonable(value: Any) -> Any:
    """Best-effort conversion of pydantic models / dataclasses to plain JSON.

    Most of the values rendered by the CLI come from the ``mcp``
    package, which exposes ``BaseModel`` instances. Falling back to
    ``str()`` ensures we never crash on an unexpected type.
    """
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list | tuple):
        return [_coerce_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _coerce_jsonable(v) for k, v in value.items()}
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value


# ``Any``: ``value`` is any subcommand return value (tool results,
# redacted-config dicts, scalars); the render path dispatches on type.
def format_output(value: Any, *, output: OutputMode) -> str:
    """Render ``value`` according to the requested CLI output mode.

    Args:
        value (Any): The value to render. Supported types include
            :class:`mcp.types.CallToolResult`, :class:`mcp.types.Tool`
            lists, plain dicts, and primitive scalars.
        output (OutputMode): One of :data:`OUTPUT_MODES`. ``"human"``
            for terminal-friendly output; ``"json"`` for a single
            deterministically-formatted JSON document; ``"yaml"`` for
            a deterministically-formatted YAML document.

    Returns:
        str: The rendered output, *without* a trailing newline.
    """
    match output:
        case "human":
            return _format_human(value)
        case "json":
            return json.dumps(_coerce_jsonable(value), indent=2, sort_keys=True)
        case "yaml":
            dumped: str = yaml.safe_dump(
                _coerce_jsonable(value), sort_keys=True, default_flow_style=False
            )
            return dumped.rstrip("\n")
        case _ as unexpected:
            # Statically unreachable thanks to the ``OutputMode``
            # ``Literal``: mypy will flag any caller that passes an
            # un-annotated ``str``. The runtime ``assert_never`` is
            # the safety net for callers that bypassed type checking
            # (e.g. by reading a string from JSON config).
            assert_never(unexpected)


def _format_human(value: Any) -> str:
    """Render ``value`` for a human reader on a terminal.

    Unrecognized types render via ``repr`` as a best-effort fallback.
    """
    if isinstance(value, CallToolResult):
        return _format_tool_result_human(value)
    if isinstance(value, list) and all(isinstance(v, Tool) for v in value):
        # An empty list still routes here so :func:`_format_tool_list`
        # can emit the canonical "(no tools registered)" message.
        return _format_tool_list(value)
    if isinstance(value, dict):
        return "\n".join(f"{k}: {v}" for k, v in value.items())
    if isinstance(value, str):
        return value
    return repr(value)


def _format_tool_result_human(result: CallToolResult) -> str:
    """Concatenate any text content blocks; surface errors loudly."""
    parts: list[str] = []
    for block in result.content:
        if isinstance(block, TextContent):
            parts.append(block.text)
        else:
            # Non-text blocks (image, resource) are uncommon in the
            # systems server; render them as JSON so the operator at
            # least sees something concrete.
            parts.append(json.dumps(_coerce_jsonable(block), indent=2))
    body = "\n".join(parts) if parts else "(empty result)"
    if result.isError:
        return f"ERROR: {body}"
    return body


_MIN_TERMINAL_WIDTH = 40
"""Floor for description-column width math.

Narrow terminals (or unknown widths reported as <40) get a sensible
minimum so the truncation slice index in :func:`_format_tool_list`
(``truncate_at``, floored at 1) stays valid.
"""


def _format_tool_list(tools: list[Tool]) -> str:
    """Render an MCP tool list as a flat human-readable table.

    The layout is two columns (name + description), with the
    description truncated to fit on a single line scaled to the
    current terminal width, falling back to 80 columns when no TTY
    is detected. The operator can request ``-o json`` for the full
    payload.
    """
    if not tools:
        return "(no tools registered)"
    name_width = max(len(t.name) for t in tools)
    # ``shutil.get_terminal_size`` falls back to ``(80, 24)`` when
    # stdout is not a TTY (pipes, redirected output, tests). The
    # floor protects against pathological narrow widths and prevents
    # the desc-budget math below from going negative.
    terminal_width = max(shutil.get_terminal_size().columns, _MIN_TERMINAL_WIDTH)
    # ``name_width + 2`` accounts for the two-space gutter between
    # columns; ``- 3`` reserves room for the "..." ellipsis on the
    # truncate path.
    desc_budget = terminal_width - name_width - 2
    truncate_at = max(desc_budget - 3, 1)
    lines: list[str] = []
    for tool in tools:
        desc = (tool.description or "").splitlines()[0] if tool.description else ""
        if len(desc) > desc_budget:
            desc = desc[:truncate_at] + "..."
        lines.append(f"{tool.name:<{name_width}}  {desc}")
    return "\n".join(lines)
