"""Tests for ``deephaven_mcp.cli._command``.

Blank-parameter rejection is exercised by ``test__params.py``, which owns
that guard; this file covers the runtime-load hook, the ``--agents``
injection, and the metadata the two classes carry.
"""

from __future__ import annotations

from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from deephaven_mcp.cli import _runtime as runtime_mod
from deephaven_mcp.cli._command import HelpfulCommand, HelpfulGroup
from deephaven_mcp.cli._help import HelpSpec, OutputField, OutputSpec
from deephaven_mcp.cli._runtime import Runtime, RuntimeSpec

# ---------------------------------------------------------------------------
# HelpfulCommand / HelpfulGroup — the output-spec + wrapper-binding metadata
# ---------------------------------------------------------------------------


def test_helpful_command_defaults_are_empty() -> None:
    """A bare command carries no output spec and no wrapper binding."""
    cmd = HelpfulCommand("c")
    assert cmd.output_spec is None
    assert cmd.wraps_tool is None
    assert cmd.wraps_tools == ()
    assert cmd.intentionally_unsupported == frozenset()
    assert cmd.router_params == frozenset()
    assert cmd.client_only_params == frozenset()
    assert cmd.needs_runtime is True


def test_helpful_command_needs_runtime_opt_out_is_stored() -> None:
    """``needs_runtime=False`` (the agents verbs' declaration) is stored."""
    assert HelpfulCommand("c", needs_runtime=False).needs_runtime is False


def test_invoke_materializes_runtime_from_spec() -> None:
    """``invoke`` swaps a ``RuntimeSpec`` obj for the resolved ``Runtime``."""
    from deephaven_mcp.cli._runtime import RuntimeSpec

    sentinel = object()
    seen: list[object] = []

    @click.command(cls=HelpfulCommand)
    @click.pass_obj
    def c(obj: object) -> None:
        seen.append(obj)

    spec = RuntimeSpec()
    with patch.object(RuntimeSpec, "resolve", return_value=sentinel):
        result = CliRunner().invoke(c, [], obj=spec, standalone_mode=False)
    assert result.exit_code == 0
    assert seen == [sentinel]


def test_invoke_skips_load_when_needs_runtime_false() -> None:
    """A ``needs_runtime=False`` command receives the spec untouched."""
    from deephaven_mcp.cli._runtime import RuntimeSpec

    seen: list[object] = []

    @click.command(cls=HelpfulCommand, needs_runtime=False)
    @click.pass_obj
    def c(obj: object) -> None:
        seen.append(obj)

    spec = RuntimeSpec()
    with patch.object(
        RuntimeSpec,
        "resolve",
        side_effect=AssertionError("resolve must not be called"),
    ):
        result = CliRunner().invoke(c, [], obj=spec, standalone_mode=False)
    assert result.exit_code == 0
    assert seen == [spec]


def test_invoke_leaves_prebuilt_runtime_untouched() -> None:
    """A non-spec obj (e.g. a prebuilt ``Runtime`` in tests) passes through."""
    prebuilt = object()
    seen: list[object] = []

    @click.command(cls=HelpfulCommand)
    @click.pass_obj
    def c(obj: object) -> None:
        seen.append(obj)

    result = CliRunner().invoke(c, [], obj=prebuilt, standalone_mode=False)
    assert result.exit_code == 0
    assert seen == [prebuilt]


def test_helpful_command_stores_output_spec_and_wrapper_binding() -> None:
    """Every metadata field is stored verbatim for ``agents`` to read."""
    spec = OutputSpec("object", (OutputField("f", "string", "field"),))
    cmd = HelpfulCommand(
        "c",
        output_spec=spec,
        wraps_tool="t",
        wraps_tools=("a", "b"),
        intentionally_unsupported=frozenset({"x"}),
        router_params=frozenset({"system"}),
        client_only_params=frozenset({"print_only"}),
    )
    assert cmd.output_spec is spec
    assert cmd.wraps_tool == "t"
    assert cmd.wraps_tools == ("a", "b")
    assert cmd.intentionally_unsupported == frozenset({"x"})
    assert cmd.router_params == frozenset({"system"})
    assert cmd.client_only_params == frozenset({"print_only"})


def test_helpful_command_derives_output_spec_from_help_spec() -> None:
    """A command with a help_spec always exposes the spec's output."""
    output = OutputSpec("object", (OutputField("f", "string", "field"),))
    cmd = HelpfulCommand("c", help_spec=HelpSpec(summary="S.", output=output))
    assert cmd.output_spec is output


def test_helpful_command_rejects_output_spec_alongside_help_spec() -> None:
    """help_spec is the single source; a separate output_spec is rejected."""
    with pytest.raises(ValueError, match="help_spec.output"):
        HelpfulCommand(
            "c",
            help_spec=HelpSpec(summary="S."),
            output_spec=OutputSpec("text"),
        )


def test_helpful_group_leaves_default_to_helpful_command() -> None:
    """A ``HelpfulGroup``'s verbs are ``HelpfulCommand`` so they carry metadata."""
    assert HelpfulGroup.command_class is HelpfulCommand
    group = HelpfulGroup("g")

    @group.command("v", wraps_tool="some_tool")
    def _verb() -> None: ...

    leaf = group.commands["v"]
    assert isinstance(leaf, HelpfulCommand)
    assert leaf.wraps_tool == "some_tool"


def test_helpful_group_stores_help_spec() -> None:
    """HelpfulGroup renders help from a spec and stores it for the manifest."""
    group = HelpfulGroup("g", help_spec=HelpSpec(summary="Group summary."))
    assert group.help_spec is not None
    assert group.help_spec.summary == "Group summary."
    assert group.help is not None and "Group summary." in group.help
