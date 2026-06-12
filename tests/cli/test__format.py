"""Tests for ``deephaven_mcp.cli._format``."""

from __future__ import annotations

import json

import pytest
from mcp.types import CallToolResult, TextContent, Tool

from deephaven_mcp.cli._format import (
    OUTPUT_ENV_VAR,
    OUTPUT_MODES,
    _format_tool_list,
    format_output,
)


def test_output_env_var_is_the_public_contract_name() -> None:
    """Pin the env-var name agents and users rely on to select the output mode.

    This is the external wire contract (``DH_MCP_OUTPUT``); changing it is a
    breaking change, so it is asserted directly rather than left to behavior.
    """
    assert OUTPUT_ENV_VAR == "DH_MCP_OUTPUT"


def test_output_modes_are_the_three_supported_values() -> None:
    """The runtime mode tuple matches the documented choices."""
    assert OUTPUT_MODES == ("human", "json", "yaml")


def _tool(name: str, desc: str | None = None) -> Tool:
    return Tool(name=name, description=desc, inputSchema={"type": "object"})


# ---------------------------------------------------------------------------
# format_output: dispatch
# ---------------------------------------------------------------------------


def test_unknown_output_mode_hits_assert_never() -> None:
    """An out-of-band mode trips the runtime safety net.

    Statically unreachable thanks to the ``OutputMode`` ``Literal``;
    we deliberately bypass type checking to confirm the runtime
    ``assert_never`` branch is covered, rather than silently
    falling through to a default rendering.
    """
    with pytest.raises(AssertionError):
        format_output({}, output="xml")  # type: ignore[arg-type]
    # Suppression justified: deliberately feeding an off-``Literal``
    # value so the runtime safety net is exercised. The bracketed
    # ``arg-type`` code names what is being silenced; mypy still
    # flags any *unintentional* misuse at real call sites.


# ---------------------------------------------------------------------------
# JSON format
# ---------------------------------------------------------------------------


def test_json_format_dict() -> None:
    out = format_output({"b": 1, "a": 2}, output="json")
    parsed = json.loads(out)
    assert parsed == {"a": 2, "b": 1}
    # Sorted keys per the format contract.
    assert out.index('"a"') < out.index('"b"')


def test_json_format_pydantic_model() -> None:
    tool = _tool("foo", "bar")
    out = format_output(tool, output="json")
    parsed = json.loads(out)
    assert parsed["name"] == "foo"
    assert parsed["description"] == "bar"


def test_json_format_list_of_models_recurses() -> None:
    """A top-level list of pydantic models is coerced element-wise.

    Exercises the ``_coerce_jsonable`` list branch that the ``tool
    list`` subcommand drives in production (``list[Tool]`` under
    ``-o json``).
    """
    out = format_output([_tool("a", "first"), _tool("b", "second")], output="json")
    parsed = json.loads(out)
    assert isinstance(parsed, list)
    assert [t["name"] for t in parsed] == ["a", "b"]
    assert parsed[0]["description"] == "first"


def test_json_format_unserializable_falls_back_to_str() -> None:
    class Opaque:
        def __repr__(self) -> str:
            return "<opaque>"

    out = format_output({"x": Opaque()}, output="json")
    assert json.loads(out) == {"x": "<opaque>"}


# ---------------------------------------------------------------------------
# Human format
# ---------------------------------------------------------------------------


def test_human_format_call_tool_result_text() -> None:
    result = CallToolResult(content=[TextContent(type="text", text="hello")])
    assert format_output(result, output="human") == "hello"


def test_human_format_call_tool_result_error() -> None:
    result = CallToolResult(
        content=[TextContent(type="text", text="boom")], isError=True
    )
    out = format_output(result, output="human")
    assert out.startswith("ERROR:")
    assert "boom" in out


def test_human_format_non_text_content_block_falls_back_to_json() -> None:
    """A non-TextContent block (e.g. image) is rendered as inline JSON."""
    from mcp.types import EmbeddedResource, TextResourceContents

    block = EmbeddedResource(
        type="resource",
        resource=TextResourceContents(
            uri="file:///tmp/x", mimeType="text/plain", text="payload"
        ),
    )
    result = CallToolResult(content=[block])
    out = format_output(result, output="human")
    assert "payload" in out


def test_human_format_empty_call_tool_result() -> None:
    result = CallToolResult(content=[])
    assert format_output(result, output="human") == "(empty result)"


def test_human_format_dict() -> None:
    out = format_output({"k": "v", "n": 1}, output="human")
    assert "k: v" in out
    assert "n: 1" in out


def test_human_format_list_of_dicts_renders_aligned_table() -> None:
    rows = [
        {"Namespace": "Mkt", "Table": "Trades"},
        {"Namespace": "Mkt", "Table": "Quotes"},
    ]
    out = format_output(rows, output="human")
    lines = out.splitlines()
    assert lines[0].split() == ["Namespace", "Table"]
    assert "Trades" in lines[1]
    # Columns align: the second column starts at the same offset on the
    # header and the data row.
    assert lines[1].index("Trades") == lines[0].index("Table")


def test_human_format_list_of_dicts_handles_ragged_rows() -> None:
    """A key missing from some rows renders an empty cell, not a crash."""
    rows = [{"a": 1, "b": 2}, {"a": 3}]
    out = format_output(rows, output="human")
    lines = out.splitlines()
    assert lines[0].split() == ["a", "b"]
    assert lines[2].startswith("3")


def test_human_format_dict_renders_data_block_as_table() -> None:
    """The tabular tool envelope: metadata as key:value, ``data`` as a table."""
    payload = {
        "row_count": 2,
        "is_complete": True,
        "data": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
    }
    out = format_output(payload, output="human")
    lines = out.splitlines()
    assert "row_count: 2" in out
    assert "is_complete: True" in out
    assert "data:" in out
    # The data block is a rendered table, not a Python repr.
    assert "[{" not in out
    assert "x" in out and "y" in out
    # The nested table is indented two spaces under the ``data:`` key.
    data_idx = lines.index("data:")
    assert lines[data_idx + 1].startswith("  ")
    assert lines[data_idx + 1].lstrip().startswith("x")


def test_human_format_list_of_scalars_newline_joined() -> None:
    assert format_output(["trades", "quotes"], output="human") == "trades\nquotes"


def test_human_format_string() -> None:
    assert format_output("hello", output="human") == "hello"


def test_human_format_other_repr() -> None:
    assert format_output(42, output="human") == "42"


# ---------------------------------------------------------------------------
# YAML format
# ---------------------------------------------------------------------------


def test_yaml_format_dict() -> None:
    import yaml

    out = format_output({"b": 1, "a": 2}, output="yaml")
    parsed = yaml.safe_load(out)
    assert parsed == {"a": 2, "b": 1}
    # Sorted keys per the contract.
    assert out.index("a:") < out.index("b:")


def test_yaml_format_pydantic_model() -> None:
    import yaml

    parsed = yaml.safe_load(format_output(_tool("foo", "bar"), output="yaml"))
    assert parsed["name"] == "foo"
    assert parsed["description"] == "bar"


def test_yaml_format_no_trailing_newline() -> None:
    out = format_output({"a": 1}, output="yaml")
    assert not out.endswith("\n")


# ---------------------------------------------------------------------------
# _format_tool_list
# ---------------------------------------------------------------------------


def test_human_format_empty_list_uses_default_message() -> None:
    """An empty list renders the generic placeholder, not ``"[]"`` or a repr."""
    assert format_output([], output="human") == "(none)"


def test_human_format_empty_list_uses_custom_empty_message() -> None:
    """A caller supplies its own empty-list wording (e.g. ``tool list``).

    Regression for the shape-guess bug: an empty list of *anything* (rows,
    systems, sessions) used to render ``"(no tools registered)"`` because
    ``all()`` over ``[]`` is vacuously ``True``. The wording is now the
    caller's choice via ``empty_message``.
    """
    assert (
        format_output([], output="human", empty_message="(no tools registered)")
        == "(no tools registered)"
    )


def test_format_tool_list_aligns_names() -> None:
    out = _format_tool_list([_tool("short"), _tool("muchlonger", "desc")])
    lines = out.splitlines()
    # The shorter name is padded to align with the longer one.
    assert lines[0].startswith("short     ")
    assert lines[1].startswith("muchlonger")


def test_format_tool_list_truncates_long_descriptions() -> None:
    long = "x" * 500
    out = _format_tool_list([_tool("t", long)])
    assert "..." in out
    assert len(out.splitlines()[0]) <= 100


def test_format_tool_list_handles_missing_description() -> None:
    out = _format_tool_list([_tool("t", None)])
    assert out.startswith("t")


def test_human_format_dispatches_to_tool_list() -> None:
    out = format_output([_tool("foo")], output="human")
    assert out.startswith("foo")
