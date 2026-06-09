"""Tests for ``deephaven_mcp.cli._format``."""

from __future__ import annotations

import json

import pytest
from mcp.types import CallToolResult, TextContent, Tool

from deephaven_mcp.cli._format import _format_tool_list, format_output


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


def test_format_tool_list_empty() -> None:
    assert _format_tool_list([]) == "(no tools registered)"


def test_human_format_routes_empty_tool_list_to_format_tool_list() -> None:
    """A typed-empty Tool list under human format renders the canonical message.

    Regression for S2-3: previously ``format_output([], output="human")``
    fell through to ``repr`` and emitted ``"[]"``. The ``tool list``
    subcommand always returns ``list[Tool]`` (filtered), so an empty
    result must route to :func:`_format_tool_list`.
    """
    assert format_output([], output="human") == "(no tools registered)"


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
