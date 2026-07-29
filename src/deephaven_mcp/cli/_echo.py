"""Printing a ``dhcli`` payload to stdout in the active output mode.

Rendering itself belongs to :func:`~deephaven_mcp.cli._format.format_output`;
this module owns the one decision that sits above it — *where the output
mode comes from* — because the answer depends on when the command runs
relative to the leaf-boundary configuration load:

- **After the load** (the ordinary case): :func:`echo_payload` reads
  ``runtime.config.cli.output.format``, which
  :func:`~deephaven_mcp.cli._runtime.load_runtime` has already merged the
  root ``-o/--output`` flag into. Config, environment, and flag are all
  accounted for in that one value.
- **Before the load**: :func:`echo_payload_no_runtime` reads the root
  ``-o`` parameter directly, because there is no ``Runtime`` to consult.
  Two kinds of caller are in this position — a command declared
  ``needs_runtime=False`` (the ``dhcli config`` authoring verbs, which
  must work on a tree too broken to load) and the eager ``--agents``
  callback (which exits during argument parsing, before any load). Such a
  caller cannot honor ``cli.json``'s ``output.format``; ``-o`` and
  ``DHCLI_OUTPUT`` still work, since click resolves both into the root
  parameter.

The command's ``needs_runtime`` declaration selects between them:
``False`` means :func:`echo_payload_no_runtime`, anything else means
:func:`echo_payload`.
"""

from __future__ import annotations

__all__ = ["echo_payload", "echo_payload_no_runtime"]

from collections.abc import Collection
from typing import Any

import click

from deephaven_mcp.cli._format import (
    DEFAULT_OUTPUT_MODE,
    OutputMode,
    format_output,
)
from deephaven_mcp.cli._runtime import Runtime


# ``Any``: ``value`` is any subcommand result (tool payload dict, shaped
# list, scalar); ``format_output`` dispatches on type when rendering.
def echo_payload(
    runtime: Runtime,
    value: Any,
    *,
    empty_message: str = "(none)",
    sort_keys: bool = True,
    human_exclude: Collection[str] = (),
) -> None:
    """Render ``value`` in the runtime's output mode and print it.

    The ordinary emitter, for a command whose body runs after the
    configuration load. Presentation is owned by
    :func:`~deephaven_mcp.cli._format.format_output`; this is where most
    commands read ``runtime.config.cli.output``.

    Args:
        runtime (Runtime): The active CLI runtime, for the output mode.
        value (Any): The value to render (a payload dict, a shaped list, etc.).
        empty_message (str): Human-mode text for an empty list, forwarded to
            :func:`~deephaven_mcp.cli._format.format_output`.
        sort_keys (bool): Whether ``json``/``yaml`` modes sort object keys
            alphabetically. Defaults to ``True``. Pass ``False`` for payloads
            whose key order is meaningful, forwarded to
            :func:`~deephaven_mcp.cli._format.format_output`.
        human_exclude (Collection[str]): Keys dropped from a dict ``value`` in
            ``human`` mode only, for fields that are noise to a terminal reader
            but meaningful to machine consumers. Ignored in ``json``/``yaml``
            and for non-dict values. Defaults to ``()`` (drop nothing).
    """
    output = runtime.config.cli.output.format
    if human_exclude and output == "human" and isinstance(value, dict):
        value = {k: v for k, v in value.items() if k not in human_exclude}
    click.echo(
        format_output(
            value,
            output=output,
            empty_message=empty_message,
            sort_keys=sort_keys,
        )
    )


# ``Any``: ``payload`` is any JSON-safe value a pre-load command emits
# (raw configuration subtree, manifest, summary tree, command node,
# error-code registry); ``format_output`` dispatches on type.
def echo_payload_no_runtime(ctx: click.Context, payload: Any) -> None:
    """Render ``payload`` in the root ``-o/--output`` mode and print it.

    The emitter for a caller that has no :class:`Runtime` — a
    ``needs_runtime=False`` command, or the eager ``--agents`` callback.
    Mode comes from the root ``-o/--output`` flag or ``DHCLI_OUTPUT``,
    falling back to
    :data:`~deephaven_mcp.cli._format.DEFAULT_OUTPUT_MODE` (``json``,
    compact). ``cli.json``'s ``output.format`` is *not* consulted: these
    callers run before the configuration is loaded, so use ``-o`` (or set
    ``DHCLI_OUTPUT``) to opt into ``json-pretty``/``human``/``yaml``.

    Args:
        ctx (click.Context): The invoking command's context; the root
            context supplies the output mode.
        payload (Any): The JSON-safe value to render.
    """
    output: OutputMode = ctx.find_root().params.get("output") or DEFAULT_OUTPUT_MODE
    click.echo(format_output(payload, output=output))
