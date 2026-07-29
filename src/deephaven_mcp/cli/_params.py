"""Parameter hygiene for the dhcli CLI: the blank-value rule.

No ``dhcli`` parameter has a meaningful blank value — every positional
names a resource and every string option names a system, path, script,
group, or credential — so a blank is always a mistake, and often a
dangerous one. Two guards enforce that, at different stages, because one
stage cannot cover both cases:

- :func:`reject_blank_values` runs *after* click parses a command's
  arguments, from ``HelpfulCommand.invoke`` and ``HelpfulGroup.invoke``.
  It catches every plain-string parameter, including each element of a
  repeatable or variadic one.
- :class:`NonBlankPath` runs *during* parsing. A path must be checked
  before conversion because ``click.Path`` turns ``''`` into
  ``Path('.')``, which is indistinguishable from an explicit ``.`` and so
  invisible to the post-parse guard.

Both raise ``CliError`` with ``MISSING_ARGUMENT`` and share one wording
(:func:`_blank_message`), so the two stages cannot drift apart. Neither
affects a ``KEY=VALUE`` option: the parameter is the whole ``'DEBUG='``
string, which is not blank, so setting an empty environment-variable
value still works.

:func:`param_label` supplies the user-facing name both guards put in
their messages. It is public because any command that names a flag in an
error of its own reads the spelling from the option declaration through
it, rather than repeating that spelling in a literal.
"""

from __future__ import annotations

__all__ = ["NonBlankPath", "param_label", "reject_blank_values"]

from collections.abc import Sequence
from typing import Any

import click

from deephaven_mcp.cli._errors import CliError, ErrorCode


def _has_blank(value: Any) -> bool:
    """Whether ``value`` is a blank string, or a sequence containing one.

    Args:
        value (Any): A parsed click parameter value. Non-string scalars
            (ints, floats, ``Path``, flags, ``None``) are never blank;
            ``click.Choice`` already rejects a blank of its own accord.

    Returns:
        bool: ``True`` when the value is a whitespace-only string, or a
            sequence containing one at any depth of nesting — a
            ``multiple=True`` option declared ``nargs=2`` yields a tuple
            of tuples.
    """
    # The string branch must come first: ``str`` is itself a ``Sequence``,
    # so leading with it is what stops the recursion descending into
    # individual characters.
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, Sequence):
        return any(_has_blank(item) for item in value)
    return False


def param_label(param: click.Parameter) -> str:
    """Name ``param`` the way the user spells it on the command line.

    Args:
        param (click.Parameter): The offending parameter.

    Returns:
        str: The long flag for an option (``--system``), falling back to
            its only spelling when it has no long form (``-o``); the
            upper-cased argument name for a positional (``ID``).
    """
    # Not click's ``get_error_hint``, which renders the usage metavar:
    # an optional argument becomes '[ID]' and a variadic one '[REST]...',
    # where the brackets mean "optional" and the ellipsis "repeatable".
    if isinstance(param, click.Option):
        return next((opt for opt in param.opts if opt.startswith("--")), param.opts[0])
    return param.human_readable_name


def _blank_message(label: str) -> str:
    """Render the single wording used for every blank-parameter rejection.

    Args:
        label (str): The parameter's user-facing name, from
            :func:`param_label`.

    Returns:
        str: The complete error message.
    """
    return (
        f"{label} cannot be blank. Pass a real value — a blank string is "
        "never a valid id, name, or path, and is not the same as leaving "
        "the parameter out."
    )


class NonBlankPath(click.Path):
    """A :class:`click.Path` that refuses a blank value.

    The raw string is rejected before :meth:`click.Path.convert` runs,
    which keeps an explicit ``--config-dir .`` legal: after conversion
    ``Path("")`` and ``Path(".")`` are both ``PosixPath('.')``, so a
    post-parse check could not tell one from the other.
    """

    def convert(
        self,
        value: Any,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> Any:
        """Reject a blank raw value, then convert as :class:`click.Path`.

        Args:
            value (Any): The raw value from the command line, normally a
                string before conversion.
            param (click.Parameter | None): The parameter being
                converted. When ``None``, the message names the value
                generically as ``PATH``.
            ctx (click.Context | None): The active click context, if any.

        Returns:
            Any: The converted path, per :class:`click.Path`.

        Raises:
            CliError: With ``MISSING_ARGUMENT`` when ``value`` is an empty
                or whitespace-only string.
        """
        if isinstance(value, str) and not value.strip():
            label = "PATH" if param is None else param_label(param)
            raise CliError(_blank_message(label), code=ErrorCode.MISSING_ARGUMENT)
        return super().convert(value, param, ctx)


def reject_blank_values(ctx: click.Context) -> None:
    """Reject any parameter of ``ctx.command`` supplied as a blank string.

    Called from both ``HelpfulCommand.invoke`` and
    ``HelpfulGroup.invoke``. Only ``ctx``'s own command's parameters are
    examined, so the group-level and leaf-level checks never overlap.

    Args:
        ctx (click.Context): The context being invoked, read for both
            ``params`` and the command's parameter declarations.

    Raises:
        CliError: With ``MISSING_ARGUMENT`` when any parameter, or any
            element of a repeatable one, is whitespace-only.
    """
    # Keyed by parameter name; every key of ``ctx.params`` is one of
    # these, so the lookup below always hits.
    labels = {param.name: param_label(param) for param in ctx.command.get_params(ctx)}
    for name, value in ctx.params.items():
        if _has_blank(value):
            raise CliError(
                _blank_message(labels[name]), code=ErrorCode.MISSING_ARGUMENT
            )
