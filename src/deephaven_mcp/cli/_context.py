"""Sticky CLI context: persistent default session/system/PQ for ``dhcli``.

Many commands take a fully qualified session or PQ id, or an
Enterprise system name, as their first positional argument. Typing
the same id on every invocation is tedious for humans and error-prone
for AI agents, which have been observed picking an arbitrary
(possibly unrelated, possibly production) id when one is not obvious.
The sticky context lets a command like ``session create`` establish a
default that later commands (``session exec``, ``table list``, ...)
fall back to when the id is omitted.

Three keys are tracked (:class:`ContextKey`): ``session``, ``system``,
and ``pq``. Under ``dhcli context set`` they are independent: setting
one never implies or overrides another, even though an Enterprise PQ
and its running session share the same id — managing a PQ by hand
should never silently redirect session/table commands, and vice versa.
The ``create`` and ``delete`` verbs deliberately do relate them,
because there the keys describe one resource: an Enterprise ``session
create`` sets ``session``, ``system``, and ``pq`` together (its id *is*
the PQ's), ``pq create`` sets ``pq`` and ``system``, and both delete
verbs clear ``session`` and ``pq`` when either pointed at the deleted
id. Pass ``--no-set-context`` to suppress the automatic set.

Storage: ``<runtime_dir>/context.json`` (:class:`ContextStore`),
mutable per-user state written atomically via
:func:`~deephaven_mcp._platform.fsutil.atomic_write_private`, next to
the daemon registry. A missing or corrupt file is treated as an empty
context (with a logged warning for the corrupt case) — never a hard
failure.

Resolution order (:func:`resolve_context_value`): explicit CLI
argument, then ``context.json``. The file step is skipped when the
caller passes ``enabled=False`` (wired to the root ``--no-context``
flag / ``cli.json``'s ``context.enabled=false``), which is the
deterministic mode a scripted caller can opt into.

:func:`require_context_value` is the convenience a command body
calls directly: it resolves and raises
``CliError(ErrorCode.CONTEXT_NOT_SET)`` when nothing supplies a value.

Context values are never validated by this module against what the
daemon actually knows about — that is ``dhcli context set``'s job
(each key validates against its own tool: ``session_details`` for
``session``, ``pq_details`` for ``pq``, the configured Enterprise
systems for ``system``). A context value that later turns out to be
stale (the session was deleted outside the CLI) surfaces as whatever
error the downstream tool call raises, which names the resolved id.
"""

from __future__ import annotations

__all__ = [
    "CONTEXT_HINT",
    "CONTEXT_RISK_DESTRUCTIVE",
    "CONTEXT_RISK_STATEFUL",
    "CliContext",
    "ContextKey",
    "ContextProvenance",
    "ContextStore",
    "ResolvedContext",
    "clear_matching",
    "require_context_target",
    "require_context_value",
    "resolve_context_value",
    "resolve_for_runtime",
]

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, assert_never

from pydantic import ValidationError

from deephaven_mcp._platform.dir_permissions import harden_private_dir
from deephaven_mcp._platform.fsutil import atomic_write_private
from deephaven_mcp._pydantic import StrictSchema
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._prompt import can_prompt, confirm

if TYPE_CHECKING:
    from deephaven_mcp.cli._runtime import Runtime

_LOGGER = logging.getLogger(__name__)

_CONTEXT_FILENAME = "context.json"
"""Filename of the sticky-context file under ``<runtime_dir>/``."""

CONTEXT_HINT = "See 'dhcli context show'."
"""Discovery pointer appended to every context-defaultable argument's help."""

_CONTEXT_RISK_TEMPLATE = (
    "Safety: with the id omitted this acts on the sticky context target, "
    "which may not be the one you expect — an unintended context {consequence}. "
    "Run 'dhcli context show' first if you are unsure what the current "
    "context is."
)
"""Shared wording for the hazard warning, specialized per risk tier."""

CONTEXT_RISK_DESTRUCTIVE = _CONTEXT_RISK_TEMPLATE.format(
    consequence="executes or destroys in the wrong worker or system"
)
"""Hazard warning for verbs that execute, destroy, or disrupt."""

CONTEXT_RISK_STATEFUL = _CONTEXT_RISK_TEMPLATE.format(
    consequence="leaves state on the wrong system"
)
"""Hazard warning for verbs that create or start resources on a shared system."""


class ContextKey(StrEnum):
    """The sticky-context slots ``dhcli`` maintains.

    Independent under ``dhcli context set``: a PQ and its associated
    running session share the same id, but setting one by hand never
    implies or overrides another. The ``create``/``delete`` verbs are
    the deliberate exception — see the module docstring for which keys
    each of them writes or clears together.

    Each member carries its user-facing wording intrinsically via the
    ``(value, label, descriptor)`` tuple, so a new member without it
    fails at class-construction time. :attr:`value` is the wire form
    (the ``context.json`` key and the ``dhcli context set`` argument),
    :attr:`label` the prose spelling, and :attr:`descriptor` the noun
    phrase naming what the key holds.
    """

    SESSION = ("session", "session", "session id")
    """Fully qualified id of the sticky default session."""

    SYSTEM = ("system", "system", "system name")
    """Sticky default Enterprise system name (or ``'community'``)."""

    PQ = ("pq", "PQ", "PQ id")
    """Fully qualified id of the sticky default Persistent Query."""

    label: str
    descriptor: str

    def __new__(cls, value: str, label: str, descriptor: str) -> ContextKey:
        """Bind the wire value, prose label, and noun phrase together.

        The three differ per member: ``PQ`` is an acronym in prose, and
        ``system`` holds a name where the others hold ids.
        """
        member = str.__new__(cls, value)
        member._value_ = value
        member.label = label
        member.descriptor = descriptor
        return member

    @classmethod
    def from_value(cls, value: str) -> ContextKey:
        """Return the member whose :attr:`value` is ``value``.

        Use this rather than ``ContextKey(value)``, which does not
        type-check: members bind their metadata through :meth:`__new__`,
        so the class takes three arguments.

        Args:
            value (str): The wire value to look up (``'session'``,
                ``'system'``, or ``'pq'``).

        Returns:
            ContextKey: The matching member.

        Raises:
            KeyError: When ``value`` is not any member's value.
        """
        for key in cls:
            if key.value == value:
                return key
        raise KeyError(value)


class CliContext(StrictSchema):
    """Validated contents of ``context.json``.

    Every field is optional and independently nullable; a missing file
    is equivalent to ``CliContext()`` (all keys unset).
    """

    session: str | None = None
    """Fully qualified id of the sticky default session."""

    system: str | None = None
    """Sticky default Enterprise system name (or ``'community'``)."""

    pq: str | None = None
    """Fully qualified id of the sticky default Persistent Query."""

    def get(self, key: ContextKey) -> str | None:
        """Return the value stored for ``key``, or ``None`` when unset.

        Args:
            key (ContextKey): The key to read.

        Returns:
            str | None: The stored value, or ``None`` when that key has
                no value.
        """
        match key:
            case ContextKey.SESSION:
                return self.session
            case ContextKey.SYSTEM:
                return self.system
            case ContextKey.PQ:
                return self.pq
            case _ as unexpected:
                assert_never(unexpected)


class ContextProvenance(StrEnum):
    """Where a resolved context value came from, for ``dhcli context show``."""

    ARGUMENT = "argument"
    """Supplied explicitly on the command line.

    Produced by :func:`resolve_context_value` for every command that
    takes an id, but never reported by ``dhcli context show``, which
    inspects stored state and passes no argument.
    """

    FILE = "file"
    """Read from ``context.json`` and in effect."""

    DISABLED = "disabled"
    """Fallback is switched off, so ``context.json`` was not consulted.

    Reported by ``dhcli context show`` when ``--no-context`` was given
    or ``cli.json``'s ``context.enabled`` is false, for *every* key —
    whether or not one holds a value. Distinguishing this from
    :attr:`UNSET` is the difference between "the fallback is off" and
    "nothing is stored", which imply opposite remedies: re-enable the
    fallback, versus run ``dhcli context set``.
    """

    UNSET = "unset"
    """No value was available from any source."""


@dataclass(frozen=True, slots=True)
class ResolvedContext:
    """The outcome of resolving one context key."""

    value: str | None
    """The resolved value, or ``None`` when nothing supplied one."""

    provenance: ContextProvenance
    """Which resolution step produced :attr:`value`."""


@dataclass(frozen=True, slots=True)
class ContextStore:
    """Typed handle to one ``<runtime_dir>/context.json`` file.

    A stateless wrapper around a path, safe to construct fresh on every
    access: no file handles or locks are held, and writes are single
    atomic renames.
    """

    path: Path
    """Full path to ``context.json``."""

    @classmethod
    def for_runtime_dir(cls, runtime_dir: Path) -> ContextStore:
        """Construct a :class:`ContextStore` rooted at ``runtime_dir``."""
        return cls(path=runtime_dir / _CONTEXT_FILENAME)

    def read(self) -> CliContext:
        """Read and validate ``context.json``.

        Returns:
            CliContext: The parsed contents. A missing file returns
                ``CliContext()`` silently (this is the common case: no
                context has been set yet). A file that exists but
                cannot be parsed or fails validation also returns
                ``CliContext()``, after logging a warning — a
                corrupted sticky-context file must never block an
                otherwise-valid command; the fix is ``dhcli context
                unset --all`` or simply setting fresh values.
        """
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return CliContext()
        except OSError as exc:
            _LOGGER.warning(
                f"[_context:ContextStore.read] Cannot read {self.path}: {exc}; "
                "treating context as empty."
            )
            return CliContext()
        try:
            return CliContext.model_validate_json(raw)
        except ValidationError as exc:
            _LOGGER.warning(
                f"[_context:ContextStore.read] {self.path} is corrupt or "
                f"invalid ({exc}); treating context as empty. Run 'dhcli "
                "context unset --all' to clear it, or 'dhcli context set' "
                "to overwrite it."
            )
            return CliContext()

    def write(self, context: CliContext) -> None:
        """Atomically publish ``context`` to ``context.json``.

        Args:
            context (CliContext): The full context to persist. Unset
                (``None``) fields are omitted from the written JSON.
        """
        harden_private_dir(self.path.parent)
        data = context.model_dump_json(exclude_none=True).encode("utf-8")
        atomic_write_private(self.path, data)
        _LOGGER.debug(
            f"[_context:ContextStore.write] Wrote sticky context to {self.path}: "
            f"{context.model_dump(exclude_none=True)}"
        )

    def _apply(self, updates: Mapping[str, str | None]) -> CliContext:
        """Merge ``updates`` into the stored context, validate, and persist.

        The single write path: every mutation goes through here, so a
        value that :meth:`read` would reject cannot reach the file.
        Validating rather than :meth:`~pydantic.BaseModel.model_copy`
        matters because a rejected file is read as *empty*, which would
        turn one bad value into the loss of every key.

        Args:
            updates (Mapping[str, str | None]): Field-name-keyed values
                to merge over the current contents. ``None`` clears a
                field.

        Returns:
            CliContext: The full context after the update.

        Raises:
            ValidationError: When a merged value is not a string or
                ``None``. This means a caller passed something a tool
                payload should never contain; failing here keeps the
                cause next to the bad write.
        """
        updated = CliContext.model_validate({**self.read().model_dump(), **updates})
        self.write(updated)
        return updated

    def set(self, key: ContextKey, value: str) -> CliContext:
        """Set one key to ``value``, preserving the others, and persist.

        Returns:
            CliContext: The full context after the update.
        """
        return self.set_many({key: value})

    def set_many(self, updates: Mapping[ContextKey, str]) -> CliContext:
        """Set several keys at once, preserving the others, and persist.

        Used by verbs that auto-set the sticky context on success (e.g.
        ``session create`` setting ``session`` alone, or an Enterprise
        ``pq create`` setting ``pq`` and ``system`` together).

        Args:
            updates (Mapping[ContextKey, str]): The keys to set.

        Returns:
            CliContext: The full context after the update.
        """
        _LOGGER.debug(
            "[_context:ContextStore.set_many] Setting sticky context: "
            f"{ {k.value: v for k, v in updates.items()} }"
        )
        return self._apply({k.value: v for k, v in updates.items()})

    def unset(self, keys: tuple[ContextKey, ...] | None = None) -> CliContext:
        """Clear ``keys`` (or every key when ``None``) and persist.

        Args:
            keys (tuple[ContextKey, ...] | None): The keys to clear.
                ``None`` clears every :class:`ContextKey`.

        Returns:
            CliContext: The full context after the update.
        """
        target = keys if keys is not None else tuple(ContextKey)
        _LOGGER.debug(
            "[_context:ContextStore.unset] Clearing sticky context keys: "
            f"{[k.value for k in target]}"
        )
        return self._apply({k.value: None for k in target})


def clear_matching(
    store: ContextStore, values: frozenset[str], keys: tuple[ContextKey, ...]
) -> None:
    """Clear each of ``keys`` from ``store`` if it currently equals any of ``values``.

    Called after a delete verb succeeds, so the sticky context never
    keeps pointing at an id that no longer exists. A no-op for any key
    whose current value is not in ``values`` (including an unset key),
    so deleting one session never disturbs an unrelated sticky default.
    Clears every matching key in a single read/write.

    Args:
        store (ContextStore): The runtime's context file handle.
        values (frozenset[str]): The ids that were just deleted — only
            those that actually succeeded, since clearing on a failed
            delete would discard a pointer to a resource that still
            exists. An empty set is a no-op. Must be a ``frozenset``,
            never a bare ``str``: a string is itself a collection of
            characters and would match substrings of the stored value.
        keys (tuple[ContextKey, ...]): The keys to check and clear.
    """
    if not values:
        return
    current = store.read()
    to_clear = tuple(k for k in keys if current.get(k) in values)
    if to_clear:
        _LOGGER.debug(
            f"[_context:clear_matching] Clearing sticky context keys pointing at "
            f"deleted {sorted(values)}: {[k.value for k in to_clear]}"
        )
        store.unset(to_clear)


def resolve_context_value(
    key: ContextKey,
    explicit: str | None,
    *,
    store: ContextStore,
    enabled: bool,
) -> ResolvedContext:
    """Resolve one context key: explicit argument, then ``context.json``.

    Args:
        key (ContextKey): The key being resolved.
        explicit (str | None): The value supplied directly on the
            command line, or ``None`` when the argument was omitted.
            Only ``None`` counts as omitted. A blank string cannot
            reach here: ``HelpfulCommand.invoke`` rejects any
            blank-valued parameter before the command body runs, so a
            non-``None`` ``explicit`` is always a real value rather
            than something that would silently retarget the command.
        store (ContextStore): The runtime's context file handle.
        enabled (bool): Whether context fallback is active. When
            ``False`` (``--no-context`` / ``cli.json``'s
            ``context.enabled=false``), only ``explicit`` is
            considered; the ``context.json`` step is skipped
            entirely.

    Returns:
        ResolvedContext: The resolved value and its provenance. The
            ``context.json`` step is deliberately *not* symmetric with
            ``explicit``: a blank stored value counts as unset and
            yields :attr:`ContextProvenance.UNSET`, because a blank is
            reachable there only by hand-editing the file (``dhcli
            context set`` validates the value first) and must never
            become a command's target.
    """
    if explicit is not None:
        return ResolvedContext(explicit, ContextProvenance.ARGUMENT)
    if not enabled:
        return ResolvedContext(None, ContextProvenance.UNSET)
    file_value = store.read().get(key)
    if file_value:
        return ResolvedContext(file_value, ContextProvenance.FILE)
    return ResolvedContext(None, ContextProvenance.UNSET)


def resolve_for_runtime(
    runtime: Runtime, key: ContextKey, explicit: str | None
) -> ResolvedContext:
    """Resolve ``key`` using ``runtime``'s context store and settings.

    Owns the wiring from a :class:`~deephaven_mcp.cli._runtime.Runtime`
    to :func:`resolve_context_value` -- the context store and the
    ``context.enabled`` setting -- so no caller needs to know where
    either lives. Use this for a soft fallback that tolerates an unset
    key; use :func:`require_context_value` or
    :func:`require_context_target` when an unset key is an error.

    Args:
        runtime (Runtime): The active CLI runtime.
        key (ContextKey): The key to resolve.
        explicit (str | None): The value supplied directly on the
            command line, or ``None`` when the argument was omitted.

    Returns:
        ResolvedContext: The resolved value and its provenance.
    """
    return resolve_context_value(
        key,
        explicit,
        store=runtime.context_store,
        enabled=runtime.config.cli.context.enabled,
    )


def _resolve_required(
    runtime: Runtime, key: ContextKey, explicit: str | None
) -> tuple[str, ContextProvenance]:
    """Resolve ``key`` via ``runtime``, raising when nothing supplies a value.

    Returns the provenance alongside the value so a caller can treat a
    context-supplied target differently from an explicit one.

    The raised message's suggested remedy depends on whether
    ``context.enabled`` is set: re-enabling the fallback when it is off,
    ``dhcli context set`` when it is on but nothing is stored.

    Args:
        runtime (Runtime): The active CLI runtime.
        key (ContextKey): The key to resolve.
        explicit (str | None): The value supplied directly on the
            command line, or ``None`` when the argument was omitted.

    Returns:
        tuple[str, ContextProvenance]: The resolved value and the step
            that produced it.

    Raises:
        CliError: With :attr:`~deephaven_mcp.cli._errors.ErrorCode.CONTEXT_NOT_SET`
            when ``explicit`` is ``None`` and no fallback supplies a value.
    """
    resolved = resolve_for_runtime(runtime, key, explicit)
    if resolved.value is None:
        enabled = runtime.config.cli.context.enabled
        remedy = (
            f"Pass it explicitly, or run 'dhcli context set {key.value} <VALUE>'."
            if enabled
            else (
                "Pass it explicitly. The sticky context cannot supply it: the "
                "fallback is disabled by --no-context or cli.json's "
                "context.enabled=false."
            )
        )
        raise CliError(
            f"No {key.descriptor} was given, and no sticky context "
            f"{key.value} is set. {remedy}",
            code=ErrorCode.CONTEXT_NOT_SET,
        )
    return resolved.value, resolved.provenance


def require_context_value(
    runtime: Runtime, key: ContextKey, explicit: str | None
) -> str:
    """Resolve ``key`` via ``runtime``, raising when nothing supplies a value.

    The convenience a command body calls directly: combines
    :func:`resolve_for_runtime` with the ``CONTEXT_NOT_SET`` error a
    leaf command needs when the argument was optional. Use
    :func:`require_context_target` instead for a verb that destroys,
    executes, or disrupts.

    Args:
        runtime (Runtime): The active CLI runtime.
        key (ContextKey): The key to resolve.
        explicit (str | None): The value supplied directly on the
            command line, or ``None`` when the argument was omitted.

    Returns:
        str: The resolved value.

    Raises:
        CliError: With :attr:`~deephaven_mcp.cli._errors.ErrorCode.CONTEXT_NOT_SET`
            when ``explicit`` is ``None`` and no fallback supplies a value.
    """
    value, _ = _resolve_required(runtime, key, explicit)
    return value


def require_context_target(
    runtime: Runtime,
    key: ContextKey,
    explicit: str | None,
    *,
    action: str,
    yes: bool,
) -> str:
    """Resolve ``key`` for a destructive verb, confirming a context-supplied value.

    Behaves like :func:`require_context_value`, and additionally asks
    for confirmation when the value came from ``context.json`` rather
    than the command line and ``cli.json``'s
    ``context.confirm_destructive`` is enabled. An explicit id is never
    confirmed: naming the target is already the statement of intent.

    The prompt is skipped, and the value returned, when prompting is
    unavailable (stdin is not a TTY, or ``--no-input`` was given); a
    non-interactive caller is never refused for lack of confirmation.

    Args:
        runtime (Runtime): The active CLI runtime.
        key (ContextKey): The key to resolve.
        explicit (str | None): The value supplied directly on the
            command line, or ``None`` when the argument was omitted.
        action (str): Imperative phrase naming what the verb is about
            to do, used to build the prompt (e.g. ``"Delete"`` renders
            ``"Delete session 'x' (from sticky context)?"``).
        yes (bool): The verb's ``--yes`` flag; skips the prompt.

    Returns:
        str: The resolved value.

    Raises:
        CliError: With :attr:`~deephaven_mcp.cli._errors.ErrorCode.CONTEXT_NOT_SET`
            when nothing supplies a value, or with
            :attr:`~deephaven_mcp.cli._errors.ErrorCode.OPERATION_CANCELED`
            when the user declines the confirmation.
    """
    value, provenance = _resolve_required(runtime, key, explicit)
    if provenance is not ContextProvenance.FILE:
        return value
    _LOGGER.debug(
        f"[_context:require_context_target] Target came from the sticky "
        f"context: {key.value}={value!r} action={action!r}"
    )
    if yes or not runtime.config.cli.context.confirm_destructive:
        return value
    if not can_prompt(no_input=runtime.no_input):
        _LOGGER.debug(
            "[_context:require_context_target] Skipping confirmation: "
            "prompting unavailable (stdin is not a TTY or --no-input was given)"
        )
        return value
    if not confirm(
        f"{action} {key.label} '{value}' (from sticky context)?",
        no_input=runtime.no_input,
    ):
        raise CliError(
            "Canceled: confirmation declined.",
            code=ErrorCode.OPERATION_CANCELED,
        )
    return value
