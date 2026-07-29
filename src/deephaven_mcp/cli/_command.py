"""The ``dhcli`` click command and group classes.

Every command in the tree is one of these two, so each inherits the
declared help and manifest metadata from
:class:`~deephaven_mcp.cli._help.HelpfulMeta` and each enforces the
project-wide parameter and configuration contracts:

- **Blank parameters are rejected** before any body runs, by both classes
  (see :func:`~deephaven_mcp.cli._params.reject_blank_values`). A group
  needs its own hook because it is not a :class:`HelpfulCommand`, so the
  root's own options would otherwise escape the rule.
- **The configuration tree is loaded at the leaf boundary**, by
  :meth:`HelpfulCommand.invoke`, so a malformed file fails fast with
  ``config_invalid`` before a subcommand body executes, while ``--help``
  and ``--agents`` exit during parsing and never read configuration.
- **The universal ``--agents`` flag** is injected via ``get_params``,
  like click's own ``--help``. Both classes override ``get_params`` and
  both append the same :func:`~deephaven_mcp.cli._manifest.agents_option`.
"""

from __future__ import annotations

__all__ = ["HelpfulCommand", "HelpfulGroup"]

from typing import Any

import click

from deephaven_mcp.cli._help import HelpfulMeta
from deephaven_mcp.cli._manifest import agents_option
from deephaven_mcp.cli._params import reject_blank_values
from deephaven_mcp.cli._runtime import RuntimeSpec


class HelpfulCommand(HelpfulMeta):
    """A leaf command: :class:`HelpfulMeta` plus the runtime-load hook."""

    def invoke(self, ctx: click.Context) -> Any:
        """Reject blank parameters, then materialize the ``Runtime``.

        The root callback stores a cheap
        :class:`~deephaven_mcp.cli._runtime.RuntimeSpec` (the load
        recipe) on ``ctx.obj``; this hook swaps it for the real
        :class:`~deephaven_mcp.cli._runtime.Runtime` so the body's
        ``@click.pass_obj`` receives a fully-validated runtime. Running
        the load here — after click has parsed this command's arguments —
        means ``--help`` and ``--agents`` never touch configuration,
        because their eager callbacks have already exited. Commands
        declared ``needs_runtime=False`` skip the swap entirely, as does
        an ``obj`` that is already a :class:`Runtime` rather than a
        :class:`RuntimeSpec`.

        Blank parameters are rejected *before* the load, so an obviously
        malformed command line is reported as such rather than as
        ``CONFIG_INVALID`` from an unrelated broken configuration file.

        Args:
            ctx (click.Context): The context being invoked.

        Returns:
            Any: The command callback's return value.

        Raises:
            CliError: With ``MISSING_ARGUMENT`` when a parameter was
                supplied blank, or ``CONFIG_INVALID`` when the
                configuration tree fails to load — either way before the
                command body runs.
        """
        reject_blank_values(ctx)
        if self.needs_runtime and isinstance(ctx.obj, RuntimeSpec):
            ctx.obj = ctx.obj.resolve()
        return super().invoke(ctx)

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Append the ``--agents`` option, like click's ``--help``.

        The option appears only here, never in ``self.params``, so the
        option-lifter and the agents manifest do not see it. Returns a
        new list; ``self.params`` is never mutated.
        """
        # A new list, not an in-place append: click's ``get_params``
        # returns ``self.params`` itself when a command disables its help
        # option, so appending would grow the list on every call.
        return [*super().get_params(ctx), agents_option()]


class HelpfulGroup(HelpfulMeta, click.Group):
    """A noun group: :class:`HelpfulMeta` whose leaves default to commands."""

    command_class = HelpfulCommand

    def invoke(self, ctx: click.Context) -> Any:
        """Reject blank group-level parameters, then dispatch as a group.

        Examines only this context's own parameters — group-level options
        such as the root ``--config-dir``. A subcommand's parameters are
        checked by its own ``invoke``.

        Unlike :meth:`HelpfulCommand.invoke`, this loads no
        configuration: a group dispatches, and the leaf owns the load.

        Args:
            ctx (click.Context): The context being invoked.

        Returns:
            Any: The result of the group's dispatch.

        Raises:
            CliError: With ``MISSING_ARGUMENT`` when a group-level
                parameter was supplied blank, before the group callback
                or any subcommand runs.
        """
        reject_blank_values(ctx)
        return super().invoke(ctx)

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Append the ``--agents`` option; see :meth:`HelpfulCommand.get_params`.

        Applies to the root ``dhcli`` group and every noun group, so
        ``--agents`` is accepted at every depth of the tree.
        """
        return [*super().get_params(ctx), agents_option()]
