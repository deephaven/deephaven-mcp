"""Output-mode vocabulary and rendering for ``dh-mcp`` subcommand results.

This module owns the output-mode vocabulary every command shares — the
``OutputMode`` type, its valid values (``OUTPUT_MODES``), and the
environment variable that supplies it (``OUTPUT_ENV_VAR``) — and renders
any subcommand return value in the selected mode via :func:`format_output`.

:func:`format_output` dispatches on the requested ``output`` mode
(``"human"`` / ``"json"`` / ``"yaml"``) and the runtime value type. Human
output is terminal-friendly: a flat two-column listing for tool catalogs,
``TextContent`` concatenation for tool results, an aligned table for a list
of row dicts (and for a ``data`` block nested in a dict), ``key: value``
lines for other dicts, and a best-effort string for scalars. ``json`` and
``yaml`` modes emit deterministically sorted documents suitable for piping
into ``jq`` / ``yq`` or for programmatic consumption by AI agents.
"""

from __future__ import annotations

__all__ = ["OUTPUT_ENV_VAR", "OUTPUT_MODES", "OutputMode", "format_output"]

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

OUTPUT_ENV_VAR = "DH_MCP_OUTPUT"
"""Environment variable backing the ``-o/--output`` flag."""


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
def format_output(
    value: Any, *, output: OutputMode, empty_message: str = "(none)"
) -> str:
    """Render ``value`` according to the requested CLI output mode.

    Args:
        value (Any): The value to render. Supported types include
            :class:`mcp.types.CallToolResult`, :class:`mcp.types.Tool`
            lists, plain dicts, lists of row dicts, and primitive scalars.
        output (OutputMode): One of :data:`OUTPUT_MODES`. ``"human"``
            for terminal-friendly output; ``"json"`` for a single
            deterministically-formatted JSON document; ``"yaml"`` for
            a deterministically-formatted YAML document.
        empty_message (str): Human-mode text for an empty list. Defaults
            to ``"(none)"``; ``json``/``yaml`` modes ignore it and emit
            ``[]``.

    Returns:
        str: The rendered output, *without* a trailing newline.
    """
    match output:
        case "human":
            return _format_human(value, empty_message=empty_message)
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


# ``Any``: ``value`` is any subcommand return value (tool results,
# redacted-config dicts, scalars); the render path dispatches on type.
def _is_row_list(value: Any) -> bool:
    """Return whether ``value`` is a non-empty list whose items are all dicts.

    Such a value renders as an aligned :func:`_format_table` (a tabular tool
    result or a ``data`` block); this is the single test that selects that path.
    """
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, dict) for item in value)
    )


# ``Any``: ``value`` is any subcommand return value (tool results,
# redacted-config dicts, scalars); the render path dispatches on type.
def _format_human(value: Any, *, empty_message: str = "(none)") -> str:
    """Render ``value`` for a human reader on a terminal.

    Unrecognized types render via ``repr`` as a best-effort fallback.
    """
    if isinstance(value, CallToolResult):
        return _format_tool_result_human(value)
    if isinstance(value, list):
        if not value:
            return empty_message
        if all(isinstance(v, Tool) for v in value):
            return _format_tool_list(value)
        if _is_row_list(value):
            return _format_row_list(value)
        return "\n".join(str(v) for v in value)
    if isinstance(value, dict):
        return _format_dict(value)
    if isinstance(value, str):
        return value
    return repr(value)


def _format_dict(value: dict[str, Any]) -> str:
    """Render a dict as an indented ``key: value`` tree.

    Nested structures expand under their key instead of collapsing to a
    one-line ``str()`` repr: a non-empty list of dicts (e.g. the ``data`` block
    of a tabular tool result) renders as an indented :func:`_format_row_list`; a
    non-empty nested dict renders as an indented sub-tree; a non-empty list of
    scalars renders as indented ``- item`` bullets. Scalars and empty
    containers render inline as ``key: value``.
    """
    lines: list[str] = []
    for k, v in value.items():
        if _is_row_list(v):
            lines.append(f"{k}:")
            lines.append(_indent(_format_row_list(v)))
        elif isinstance(v, dict) and v:
            lines.append(f"{k}:")
            lines.append(_indent(_format_dict(v)))
        elif isinstance(v, list) and v:
            lines.append(f"{k}:")
            lines.append(_indent("\n".join(f"- {item}" for item in v)))
        else:
            lines.append(f"{k}: {v}")
    return "\n".join(lines)


def _row_has_complex_cell(row: dict[str, Any]) -> bool:
    """Return whether any cell in ``row`` is a non-empty dict or list."""
    return any(
        (isinstance(v, dict) and v) or (isinstance(v, list) and v) for v in row.values()
    )


def _format_row_list(rows: list[dict[str, Any]]) -> str:
    """Render a list of row dicts as an aligned table or stacked blocks.

    When every cell across all rows is scalar, renders the aligned
    :func:`_format_table`. When any row carries a nested dict or non-empty list,
    each row instead renders as a ``- key: value`` block via
    :func:`_format_dict`, with nested structures expanding under their key.
    """
    if any(_row_has_complex_cell(row) for row in rows):
        blocks: list[str] = []
        for row in rows:
            rendered = _format_dict(row)
            head, *rest = rendered.splitlines() or [""]
            blocks.append("\n".join(["- " + head, *(_indent(line) for line in rest)]))
        return "\n".join(blocks)
    return _format_table(rows)


def _indent(text: str, prefix: str = "  ") -> str:
    """Prefix every line of ``text`` with ``prefix``."""
    return "\n".join(prefix + line for line in text.splitlines())


def _format_table(rows: list[dict[str, Any]]) -> str:
    """Render a list of row dicts as an aligned, header-topped text table.

    Callers pass a non-empty list (:func:`_is_row_list` gates this path); the
    column-width math assumes at least one row. Columns are ordered by first
    appearance across the rows (the first row's keys, then any keys that only
    later rows introduce). Cell values render via ``str``; missing cells render
    empty. Columns are not truncated.
    """
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    cells = [[str(row.get(col, "")) for col in columns] for row in rows]
    widths = [
        max(len(col), max((len(row[i]) for row in cells), default=0))
        for i, col in enumerate(columns)
    ]
    header = "  ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
    body = [
        "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in cells
    ]
    return "\n".join([header, *body])


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
    payload. Callers pass a non-empty list; :func:`_format_human`
    handles the empty case via its ``empty_message``.
    """
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
