"""Interactive prompting for the ``dhcli`` configuration commands.

Implements the CLI's hybrid interaction policy: a command's inputs
are always available as flags, and a *human* at a terminal may omit
them and be prompted instead. Prompts are written to **stderr** so
stdout stays machine-parseable, and they fire only when

- stdin is a TTY, and
- the root ``--no-input`` flag was not given.

Outside those conditions a missing value raises
:class:`~deephaven_mcp.cli._errors.CliError` with
:attr:`~deephaven_mcp.cli._errors.ErrorCode.MISSING_REQUIRED_OPTION`
naming the exact flag to supply — an AI agent never sees a hanging
prompt.

:func:`prompt_text` and :func:`confirm` are the unconditional
primitives: they refuse to run when prompting is not permitted,
raising :class:`~deephaven_mcp._exceptions.InternalError` instead of
prompting in violation of the policy. Callers that resolve a value
conditionally use the higher-level entry points —
:func:`require_value`, :func:`require_choice`, :func:`prompt_optional`,
:func:`prompt_optional_int`, and :func:`require_confirmation` — which
decide whether prompting is permitted before delegating.

Closed-set options follow the same division of labor as typed numeric
options: click owns *domain* validation on both paths (``click.Choice``
on the flag, ``click.prompt(type=click.Choice(...))`` on the prompt) and
the resolver owns *presence*. :func:`require_choice` bridges click's
``str`` result to the caller's ``Literal`` type by returning the matched
member of ``choices``; because click validates both paths, a
non-member reaching the bridge is a wiring bug (a closed-set flag
declared without a matching ``click.Choice``) and raises
:class:`~deephaven_mcp._exceptions.InternalError`.
"""

from __future__ import annotations

__all__ = [
    "can_prompt",
    "confirm",
    "prompt_optional",
    "prompt_optional_int",
    "prompt_text",
    "require_choice",
    "require_confirmation",
    "require_value",
]

import logging
import sys

import click

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.cli._errors import CliError, ErrorCode

_LOGGER = logging.getLogger(__name__)


def can_prompt(*, no_input: bool) -> bool:
    """Return whether interactive prompting is permitted.

    Args:
        no_input (bool): The root ``--no-input`` flag value.

    Returns:
        bool: ``True`` when ``no_input`` is unset and stdin is a TTY.
    """
    return not no_input and sys.stdin.isatty()


def _require_prompt_permitted(*, no_input: bool, label: str) -> None:
    """Raise :class:`InternalError` when prompting is not permitted.

    Args:
        no_input (bool): The root ``--no-input`` flag value.
        label (str): Prompt label, included in the error for context.

    Raises:
        InternalError: When :func:`can_prompt` returns ``False``.
    """
    if not can_prompt(no_input=no_input):
        raise InternalError(
            f"Prompt {label!r} attempted while interactive prompting is "
            "unavailable (stdin is not a TTY or --no-input was given). "
            "Callers must gate on can_prompt before invoking a prompt primitive."
        )


def prompt_text(
    label: str,
    *,
    no_input: bool,
    default: str | None = None,
    hide: bool = False,
    choices: tuple[str, ...] | None = None,
) -> str:
    """Prompt for one text value on stderr.

    Args:
        label (str): Prompt label shown to the user.
        no_input (bool): The root ``--no-input`` flag value, checked
            to refuse prompting when the policy forbids it.
        default (str | None): Value returned when the user presses
            Enter; ``None`` keeps prompting until non-empty input.
        hide (bool): Hide the typed input (secrets).
        choices (tuple[str, ...] | None): When given, restrict input
            to these values (shown in the prompt).

    Returns:
        str: The entered (or defaulted) value.

    Raises:
        InternalError: When prompting is not permitted (stdin is not
            a TTY or ``--no-input`` was given).
    """
    _require_prompt_permitted(no_input=no_input, label=label)
    _LOGGER.debug(
        f"[_prompt:prompt_text] Prompting on stderr: label={label!r}, hide={hide}"
    )
    value: str = click.prompt(
        label,
        default=default,
        hide_input=hide,
        type=click.Choice(choices) if choices else None,
        err=True,
        show_choices=True,
    )
    return value


def confirm(label: str, *, no_input: bool, default: bool = False) -> bool:
    """Ask a yes/no question on stderr.

    Args:
        label (str): Question shown to the user.
        no_input (bool): The root ``--no-input`` flag value, checked
            to refuse prompting when the policy forbids it.
        default (bool): Value returned when the user presses Enter.

    Returns:
        bool: The user's answer.

    Raises:
        InternalError: When prompting is not permitted (stdin is not
            a TTY or ``--no-input`` was given).
    """
    _require_prompt_permitted(no_input=no_input, label=label)
    _LOGGER.debug(f"[_prompt:confirm] Confirming on stderr: label={label!r}")
    return bool(click.confirm(label, default=default, err=True))


def prompt_optional(
    value: str | None,
    *,
    label: str,
    no_input: bool,
    default: str | None = None,
) -> str | None:
    """Return ``value``, prompting only when it is absent and permitted.

    A missing value that cannot be prompted for is returned as
    ``None``, unlike :func:`require_value` which raises.

    Args:
        value (str | None): The flag value (``None`` when omitted).
        label (str): Prompt label for interactive use.
        no_input (bool): The root ``--no-input`` flag value.
        default (str | None): Interactive default.

    Returns:
        str | None: The supplied or prompted value, or ``None`` when
            absent and prompting is unavailable.
    """
    if value is not None:
        return value
    if can_prompt(no_input=no_input):
        return prompt_text(label, no_input=no_input, default=default)
    return None


def prompt_optional_int(
    value: int | None,
    *,
    label: str,
    no_input: bool,
    default: int | None = None,
) -> int | None:
    """Return ``value``, prompting for an integer when absent and permitted.

    The interactive prompt is typed, so ``click`` re-prompts on
    non-numeric input rather than letting a ``ValueError`` escape.

    Args:
        value (int | None): The flag value (``None`` when omitted).
        label (str): Prompt label for interactive use.
        no_input (bool): The root ``--no-input`` flag value.
        default (int | None): Interactive default.

    Returns:
        int | None: The supplied or prompted integer, or ``None``
            when absent and prompting is unavailable.
    """
    if value is not None:
        return value
    if not can_prompt(no_input=no_input):
        return None
    result: int = click.prompt(label, default=default, type=int, err=True)
    return result


def require_confirmation(label: str, *, yes: bool, no_input: bool) -> None:
    """Confirm a destructive action, or fail when it cannot be confirmed.

    Args:
        label (str): Confirmation question shown on a TTY.
        yes (bool): The ``--yes`` flag; skips the prompt when set.
        no_input (bool): The root ``--no-input`` flag value.

    Raises:
        CliError: With
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.MISSING_REQUIRED_OPTION`
            when ``--yes`` was not given and prompting is unavailable,
            or with
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.OPERATION_CANCELED`
            when the user answers no. The latter is a deliberate
            decline that exits 2, distinct from a Ctrl-C interruption
            (exit 130).
    """
    if yes:
        return
    if not can_prompt(no_input=no_input):
        _LOGGER.debug(
            f"[_prompt:require_confirmation] Refusing: label={label!r}, "
            "prompting unavailable and --yes not given"
        )
        raise CliError(
            "Refusing to proceed without confirmation; pass --yes.",
            code=ErrorCode.MISSING_REQUIRED_OPTION,
        )
    if not confirm(label, no_input=no_input):
        raise CliError(
            "Canceled: confirmation declined.",
            code=ErrorCode.OPERATION_CANCELED,
        )


def require_value(
    value: str | None,
    *,
    flag: str,
    label: str,
    no_input: bool,
    default: str | None = None,
    hide: bool = False,
) -> str:
    """Return a required free-text ``value``, prompting when absent and permitted.

    The entry point implementing the hybrid flags-plus-prompts policy
    for open-ended text: a supplied flag value is returned as-is; a
    missing value is prompted for on a TTY (stderr) or raises a
    structured error otherwise. Use :func:`require_choice` for
    closed-set options.

    Args:
        value (str | None): The flag value as parsed by click
            (``None`` when the flag was omitted).
        flag (str): Flag spelling for the error message (e.g.
            ``"--host"``).
        label (str): Prompt label for interactive use.
        no_input (bool): The root ``--no-input`` flag value.
        default (str | None): Interactive default; also returned
            without prompting when prompting is unavailable.
        hide (bool): Hide typed input (secrets).

    Returns:
        str: The resolved value.

    Raises:
        CliError: With
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.MISSING_REQUIRED_OPTION`
            when the value is absent, prompting is unavailable, and
            no ``default`` applies.
    """
    if value is not None:
        return value
    if can_prompt(no_input=no_input):
        return prompt_text(label, no_input=no_input, default=default, hide=hide)
    if default is not None:
        _LOGGER.debug(
            f"[_prompt:require_value] Using default for {flag}: prompting unavailable"
        )
        return default
    _LOGGER.debug(f"[_prompt:require_value] Missing {flag}: prompting unavailable")
    raise CliError(
        f"Missing required option {flag} (interactive prompting is "
        "unavailable: stdin is not a TTY or --no-input was given).",
        code=ErrorCode.MISSING_REQUIRED_OPTION,
    )


def require_choice[T: str](
    value: str | None,
    *,
    flag: str,
    label: str,
    no_input: bool,
    choices: tuple[T, ...],
) -> T:
    """Return a required closed-set ``value`` narrowed to its ``Literal`` type.

    The closed-set counterpart to :func:`require_value`. click owns
    domain validation on both paths — ``click.Choice(choices)`` on the
    flag and on the interactive prompt — so every value that reaches
    this resolver is already a member of ``choices``. The resolver
    bridges click's ``str`` result to the caller's ``Literal`` type by
    returning the matched member, which types as ``T`` without a cast.

    Args:
        value (str | None): The flag value as parsed by click
            (``None`` when the flag was omitted).
        flag (str): Flag spelling for the error message (e.g.
            ``"--auth"``).
        label (str): Prompt label for interactive use.
        no_input (bool): The root ``--no-input`` flag value.
        choices (tuple[T, ...]): The permitted values, typically
            ``get_args`` of the caller's ``Literal``. Must match the
            flag's own ``click.Choice`` tuple.

    Returns:
        T: The resolved value, typed as the ``Literal`` member of
            ``choices`` it matched.

    Raises:
        CliError: With
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.MISSING_REQUIRED_OPTION`
            when the value is absent and prompting is unavailable.
        InternalError: When a resolved value is not in ``choices`` — a
            wiring bug: a closed-set flag declared without a matching
            ``click.Choice``, so click never validated the flag path.
    """
    if value is None and can_prompt(no_input=no_input):
        value = prompt_text(label, no_input=no_input, choices=choices)
    if value is None:
        _LOGGER.debug(f"[_prompt:require_choice] Missing {flag}: prompting unavailable")
        raise CliError(
            f"Missing required option {flag} (interactive prompting is "
            "unavailable: stdin is not a TTY or --no-input was given).",
            code=ErrorCode.MISSING_REQUIRED_OPTION,
        )
    for choice in choices:
        if value == choice:
            return choice
    raise InternalError(
        f"require_choice received {flag} value {value!r}, which is not one "
        f"of {choices!r}. Closed-set flags must be declared with click.Choice "
        "so click validates the flag before it reaches require_choice."
    )
