r"""JSON-value templating engine for Deephaven MCP configuration.

This module owns the single mechanism by which a configuration JSON file
pulls in values from the process environment or from files on disk. It is
invoked by :func:`deephaven_mcp.config._file_loader.load_config_from_file` after
the JSON5 parser has produced an in-memory tree and *before* that tree is
handed to a Pydantic model for validation. The Pydantic schemas therefore
see only fully-resolved string values and never carry parallel
``<field>`` / ``<field>_env_var`` or ``<field>`` / ``<field>_path`` shadow
fields.

Syntax
------

Placeholders use a ``${kind:argument}`` form and may appear anywhere inside
a JSON string value, including inside an otherwise-literal string
(substring expansion). The three recognized kinds are:

- ``${env:VAR}`` --- resolves to the value of environment variable
  ``VAR``. Raises :class:`~deephaven_mcp._exceptions.ConfigurationError`
  if ``VAR`` is unset or empty.
- ``${env:VAR:-default}`` --- resolves to the value of ``VAR`` when set
  and non-empty, otherwise the literal text after ``:-`` (which may
  itself be empty, e.g. ``${env:FOO:-}``).
- ``${file:PATH}`` --- resolves to the UTF-8 text contents of the file
  at ``PATH``, returned verbatim (trailing newlines preserved). A
  leading ``~`` in ``PATH`` is expanded via :meth:`Path.expanduser`.
  Raises :class:`~deephaven_mcp._exceptions.ConfigurationError` if the
  file is missing, unreadable, or not valid UTF-8. **No fallback form**:
  ``${file:PATH:-default}`` is intentionally not supported; file paths
  are required when written.

Nesting is intentionally **not** supported. The placeholder grammar is a
single-pass match of ``\\$\\{[^}]+\\}``; a ``}`` inside the argument
closes the placeholder. Documented limitation.

Substring expansion
-------------------

A string value may contain zero or more placeholders interleaved with
literal text. The result is the string-concatenation of the resolved
parts. Numeric coercion happens later, at Pydantic validation time --- an
``"${env:DH_MCP_PORT}"`` that resolves to ``"10000"`` is accepted as an
``int`` field because Pydantic parses it. Authors of multi-placeholder
strings must therefore ensure the final string is a valid representation
of the field's declared type.

Recursive walk
--------------

:func:`expand_tree` recursively visits every value in the parsed JSON
tree (dicts, lists, scalars). Only string values are scanned for
placeholders; non-string scalars pass through unchanged. Dict keys are
**not** expanded.

Error reporting
---------------

Every :class:`~deephaven_mcp._exceptions.ConfigurationError` raised by
this module carries the source filename and the JSON-path of the
offending value, e.g.::

    In community/sessions/local.json at auth.credentials.token:
    env var DH_MCP_PSK is not set

This is the format consumed by
:func:`deephaven_mcp._pydantic.format_validation_error` so configuration
errors surface with the same shape regardless of whether they originate
in templating or in Pydantic validation.
"""

from __future__ import annotations

__all__ = [
    "JsonLoc",
    "LenientExpansion",
    "expand_string",
    "expand_tree",
    "expand_tree_lenient",
    "is_single_placeholder",
]

import os
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Final

from deephaven_mcp._exceptions import ConfigurationError, TemplateResolutionError


class JsonLoc(tuple[str | int, ...]):
    """Structural location of one value within a parsed JSON document.

    An immutable sequence of the dict keys and list indices leading to
    the value from the document root, in order (empty for the root
    itself, :attr:`ROOT`). The same shape as Pydantic's
    :meth:`~pydantic.ValidationError.errors` ``loc`` field; tuple
    equality and hashing are inherited, so a ``JsonLoc`` compares equal
    to the corresponding plain tuple.

    Distinct from :class:`~deephaven_mcp.config._field_path.FieldPath`,
    which addresses the *logical configuration document* with
    string-only, quotable segments; a ``JsonLoc`` addresses one parsed
    JSON value and may contain list indices.

    ``str(loc)`` renders dotted display text with bracketed list
    indices (``sessions[0].token``), or ``"<root>"`` when empty.
    """

    __slots__ = ()

    ROOT: ClassVar[JsonLoc]
    """The empty location — the document root, and the canonical way
    to write ``JsonLoc()``."""

    def __new__(cls, segments: Iterable[str | int] = ()) -> JsonLoc:
        # A bare `str` is structurally an `Iterable[str]` (its
        # characters); left unguarded, `super().__new__` would silently
        # decompose it into one segment per character.
        if isinstance(segments, str):
            raise TypeError(
                "JsonLoc does not accept a bare str; use JsonLoc((text,)) "
                "for one literal segment"
            )
        return super().__new__(cls, segments)

    def child(self, segment: str | int) -> JsonLoc:
        """Return this location extended by one dict key or list index.

        Args:
            segment (str | int): The key or index to append.

        Returns:
            JsonLoc: A new location one level deeper.
        """
        return JsonLoc((*self, segment))

    def render(self) -> str:
        """Render this location as display text for error messages.

        Returns:
            str: Dotted JSON-path text with bracketed list indices
                (e.g. ``"sessions[0].token"``), or ``"<root>"`` for
                the empty location.
        """
        if not self:
            return "<root>"
        out = ""
        for segment in self:
            if isinstance(segment, int):
                out += f"[{segment}]"
            else:
                out += f".{segment}" if out else segment
        return out

    def __str__(self) -> str:
        return self.render()

    def __repr__(self) -> str:
        return f"JsonLoc({tuple(self)!r})"


JsonLoc.ROOT = JsonLoc()

type _OnUnresolved = Callable[[TemplateResolutionError, str, JsonLoc], str]
"""Lenient-walk callback: receives the resolution failure, the original
string, and its location; returns the string to place in the output."""

# Match ``${kind:argument}`` where argument is anything not containing ``}``.
# Non-greedy ``}``-termination intentionally rejects nesting: ``${a:${b:c}}``
# matches ``${a:${b:c}`` (then trailing ``}`` is literal), which fails parsing.
_PLACEHOLDER_RE = re.compile(r"\$\{([^}]+)\}")

_MAX_FILE_TEMPLATE_BYTES: Final[int] = 1024 * 1024
"""Maximum number of bytes a ``${file:PATH}`` placeholder may pull in.

Anything larger raises :class:`ConfigurationError`. The cap is sized
for real PEM material: the largest legitimate input is a system CA
trust bundle (e.g. ``/etc/ssl/cert.pem`` on macOS is ~333 KiB; the
Mozilla curated bundle is ~190 KiB). 1 MiB is roughly 3x the largest
observed real-world bundle, with headroom for future growth, while
still refusing accidental gigabyte reads (log files, ``/dev/zero``,
attacker-pinned files).
"""


def _validate_placeholder_syntax(
    match: re.Match[str], *, source: str, path: str
) -> None:
    """Raise :class:`ConfigurationError` when one placeholder is malformed.

    Checks only *syntax* (kind, separator, argument shape); performs no
    resolution and no I/O. Sole source of truth for the placeholder
    grammar, run over every match before any resolution so a malformed
    placeholder is always fatal — even in lenient mode and even when an
    earlier placeholder in the same string is unresolvable.

    Args:
        match (re.Match[str]): One ``_PLACEHOLDER_RE`` match.
        source (str): Human-readable label for the originating file.
        path (str): Dotted JSON-path to the value, for error messages.

    Raises:
        ConfigurationError: When the placeholder has no kind separator,
            an unknown kind, an empty env-var name, an empty file path,
            or a ``:-`` fallback on a ``file`` placeholder.
    """
    body = match.group(1)
    kind, sep, argument = body.partition(":")
    if not sep:
        raise ConfigurationError(
            f"In {source} at {path}: malformed placeholder "
            f"{match.group(0)!r}: expected '${{env:...}}' or '${{file:...}}'"
        )
    if kind == "env":
        if not argument.partition(":-")[0]:
            raise ConfigurationError(
                f"In {source} at {path}: empty env-var name in "
                f"'${{env:{argument}}}'"
            )
    elif kind == "file":
        if not argument:
            raise ConfigurationError(
                f"In {source} at {path}: empty file path in '${{file:}}'"
            )
        if ":-" in argument:
            raise ConfigurationError(
                f"In {source} at {path}: '${{file:...}}' does not support "
                f"':-default' fallback syntax; file paths are always required"
            )
    else:
        raise ConfigurationError(
            f"In {source} at {path}: unknown placeholder kind {kind!r} in "
            f"{match.group(0)!r}; expected 'env' or 'file'"
        )


def is_single_placeholder(value: str) -> bool:
    """Whether ``value`` is exactly one ``${...}`` placeholder and nothing else.

    Distinguishes a value that merely *points* at a secret from one that
    *is* a secret. ``"${env:DH_PSK}"`` names an environment variable and
    discloses nothing; ``"s3cret"`` in the same field is the secret
    itself. Callers that redact secret-bearing fields use this to leave
    the pointer legible, which is what makes a redacted view still
    useful for diagnosing configuration.

    A string with a placeholder *plus* literal text (``"tok-${env:X}"``)
    is deliberately **not** a single placeholder: its literal part may
    itself be sensitive, so it is reported as a value, not a reference.

    Args:
        value (str): The raw string value as written in the file.

    Returns:
        bool: ``True`` when ``value`` consists of one placeholder and
            no surrounding text. Syntactic only — no attempt is made
            to resolve the placeholder or to validate its kind, so a
            malformed ``${bogus:x}`` still counts (it fails later, at
            load time, with a proper error).
    """
    return _PLACEHOLDER_RE.fullmatch(value) is not None


def expand_string(
    template: str,
    *,
    source: str,
    path: str,
    config_dir: Path | None = None,
) -> str:
    """Resolve every ``${...}`` placeholder inside ``template``.

    Args:
        template: The JSON string value to expand. May contain zero or
            more placeholders interleaved with literal text.
        source: Human-readable label for the originating JSON file
            (e.g. ``"community/sessions/local.json"``). Included in
            every error message.
        path: Dotted JSON-path to the value within the source file
            (e.g. ``"credentials.token"``). Included in every error
            message.
        config_dir: Optional base directory for resolving *relative*
            ``${file:PATH}`` arguments. A relative path is resolved
            against this directory; an absolute path (including one
            produced by expanding a leading ``~``) is used as-is.
            When ``None``, a relative path is resolved against the
            process working directory.

    Returns:
        The fully-resolved string with every placeholder replaced by
        its resolved value.

    Raises:
        ConfigurationError: If any placeholder is syntactically
            malformed (unknown kind, missing kind separator, empty
            env-var name, ``:-`` fallback on a ``file`` placeholder).
        TemplateResolutionError: If a syntactically valid placeholder
            cannot be resolved: a required environment variable is
            unset/empty, or a referenced file is missing, unreadable,
            not UTF-8, or exceeds :data:`_MAX_FILE_TEMPLATE_BYTES`.
            A subclass of :class:`ConfigurationError`.
    """
    # Validate every placeholder's syntax before resolving any, so a
    # malformed placeholder is fatal even when an earlier one is
    # unresolvable (lenient mode stops resolving at the first failure).
    for match in _PLACEHOLDER_RE.finditer(template):
        _validate_placeholder_syntax(match, source=source, path=path)

    def _replace(match: re.Match[str]) -> str:
        # Syntax (kind, separator, argument shape) is already validated
        # above, so ``kind`` is guaranteed to be 'env' or 'file' here.
        kind, _sep, argument = match.group(1).partition(":")
        if kind == "env":
            return _resolve_env(argument, source=source, path=path)
        return _resolve_file(argument, source=source, path=path, config_dir=config_dir)

    return _PLACEHOLDER_RE.sub(_replace, template)


def expand_tree(
    node: Any,
    *,
    source: str,
    config_dir: Path | None = None,
) -> Any:
    """Recursively expand every string value in a parsed JSON tree.

    Walks ``node`` depth-first. Strings are passed to
    :func:`expand_string`; dicts and lists are recursed into; all other
    scalars (``int``, ``float``, ``bool``, ``None``) pass through.

    Args:
        node: The parsed JSON value. Typically a ``dict`` at the top
            level but the function is total over the JSON value space.
        source: Human-readable label for the originating JSON file.
        config_dir: Optional base directory for resolving relative
            ``${file:PATH}`` arguments. Forwarded to
            :func:`expand_string`; see that function for the exact
            semantics.

    Returns:
        A new value of the same shape as ``node`` with every string
        leaf fully resolved. Non-string scalars are returned as-is.
        Dicts and lists are returned as freshly-constructed objects;
        the input is not mutated.

    Raises:
        ConfigurationError: Propagated from :func:`expand_string` when
            a placeholder is syntactically malformed. The error
            message includes ``source`` and the JSON-path of the
            offending value.
        TemplateResolutionError: Propagated from :func:`expand_string`
            when a syntactically valid placeholder cannot be resolved
            (a subclass of :class:`ConfigurationError`).
    """
    return _walk_tree(
        node, source=source, loc=JsonLoc.ROOT, config_dir=config_dir, on_unresolved=None
    )


@dataclass(frozen=True, slots=True)
class LenientExpansion:
    """Result of one :func:`expand_tree_lenient` walk."""

    value: Any
    """The expanded tree; strings holding an unresolvable placeholder
    pass through verbatim."""

    warnings: list[str] = field(default_factory=list)
    """Resolution-failure messages collected during the walk, in
    encounter order (possibly empty)."""

    unresolved_locations: frozenset[JsonLoc] = frozenset()
    """The structural locations of string values left verbatim in
    :attr:`value` because a placeholder within them could not be
    resolved."""


def expand_tree_lenient(
    node: Any,
    *,
    source: str,
    config_dir: Path | None = None,
) -> LenientExpansion:
    """Expand every string value, downgrading resolution failures to warnings.

    Identical to :func:`expand_tree` except that a placeholder which is
    syntactically valid but cannot be *resolved* in this environment
    (unset env var, missing ``${file:...}`` target) is left verbatim in
    the output instead of raising, and the failure message is collected
    into the returned warnings list. Placeholder *syntax* errors still
    raise :class:`ConfigurationError`.

    Used by :meth:`deephaven_mcp.config._store.ConfigStore.validate`: a
    ``${env:VAR}`` unset in the CLI's shell is not a file defect --- the
    daemon's environment may differ at load time.

    Args:
        node: The parsed JSON value to walk.
        source: Human-readable label for the originating JSON file.
        config_dir: Optional base directory for resolving relative
            ``${file:PATH}`` arguments.

    Returns:
        LenientExpansion: The expanded value, the resolution-failure
            messages, and the locations of strings left verbatim.

    Raises:
        ConfigurationError: When a placeholder is syntactically
            malformed.
    """
    warnings: list[str] = []
    unresolved: set[JsonLoc] = set()

    def _on_unresolved(
        exc: TemplateResolutionError, original: str, loc: JsonLoc
    ) -> str:
        warnings.append(str(exc))
        unresolved.add(loc)
        return original

    expanded = _walk_tree(
        node,
        source=source,
        loc=JsonLoc.ROOT,
        config_dir=config_dir,
        on_unresolved=_on_unresolved,
    )
    return LenientExpansion(
        value=expanded, warnings=warnings, unresolved_locations=frozenset(unresolved)
    )


def _walk_tree(
    node: Any,
    *,
    source: str,
    loc: JsonLoc,
    config_dir: Path | None,
    on_unresolved: _OnUnresolved | None,
) -> Any:
    """Shared recursive walk behind :func:`expand_tree` / :func:`expand_tree_lenient`.

    Args:
        node: The parsed JSON value to walk.
        source: Human-readable label for the originating JSON file.
        loc: Structural location (dict keys and list indices)
            accumulated during recursion; rendered via ``str()`` for
            error messages.
        config_dir: Optional base directory for resolving relative
            ``${file:PATH}`` arguments.
        on_unresolved: When ``None``, a :class:`TemplateResolutionError`
            propagates (the :func:`expand_tree` contract). Otherwise
            called with the exception, the original string, and its
            location; its return value replaces the unresolved string
            (the :func:`expand_tree_lenient` contract).

    Returns:
        A new value of the same shape as ``node`` with every string
        leaf resolved (or, under the lenient contract, left verbatim
        where unresolvable).

    Raises:
        ConfigurationError: Propagated from :func:`expand_string` when
            a placeholder is syntactically malformed.
        TemplateResolutionError: Propagated from :func:`expand_string`
            when a placeholder cannot be resolved and ``on_unresolved``
            is ``None``.
    """
    if isinstance(node, str):
        try:
            return expand_string(
                node, source=source, path=str(loc), config_dir=config_dir
            )
        except TemplateResolutionError as exc:
            if on_unresolved is None:
                raise
            return on_unresolved(exc, node, loc)
    if isinstance(node, dict):
        return {
            key: _walk_tree(
                value,
                source=source,
                loc=loc.child(key),
                config_dir=config_dir,
                on_unresolved=on_unresolved,
            )
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [
            _walk_tree(
                item,
                source=source,
                loc=loc.child(idx),
                config_dir=config_dir,
                on_unresolved=on_unresolved,
            )
            for idx, item in enumerate(node)
        ]
    return node


def _resolve_env(argument: str, *, source: str, path: str) -> str:
    """Resolve a single ``${env:...}`` placeholder argument.

    Assumes ``argument`` has already passed
    :func:`_validate_placeholder_syntax` (non-empty env-var name).
    """
    name, sep, default = argument.partition(":-")
    value = os.environ.get(name, "")
    if value:
        return value
    if sep:
        # ``${env:NAME:-default}`` --- ``default`` may be empty.
        return default
    raise TemplateResolutionError(f"In {source} at {path}: env var {name!r} is not set")


def _resolve_file(
    argument: str,
    *,
    source: str,
    path: str,
    config_dir: Path | None,
) -> str:
    """Resolve a single ``${file:...}`` placeholder argument.

    Reads up to :data:`_MAX_FILE_TEMPLATE_BYTES` from the file at
    ``argument`` and returns its UTF-8 decoded contents. A leading
    ``~`` is expanded via :meth:`Path.expanduser`. A relative
    ``argument`` is resolved against ``config_dir`` when supplied
    (otherwise against the process working directory); an absolute
    ``argument`` is used as-is. Symlinks are followed (common for
    system CA bundles such as ``/etc/ssl/cert.pem``).

    Assumes ``argument`` has already passed
    :func:`_validate_placeholder_syntax` (non-empty path, no ``:-``
    fallback).
    """
    file_path = Path(argument).expanduser()
    if config_dir is not None and not file_path.is_absolute():
        file_path = config_dir / file_path

    try:
        with open(file_path, "rb") as handle:
            data = handle.read(_MAX_FILE_TEMPLATE_BYTES + 1)
    except FileNotFoundError as exc:
        raise TemplateResolutionError(
            f"In {source} at {path}: file {argument!r} does not exist"
        ) from exc
    except PermissionError as exc:
        raise TemplateResolutionError(
            f"In {source} at {path}: file {argument!r} cannot be read "
            f"(permission denied)"
        ) from exc
    except OSError as exc:
        raise TemplateResolutionError(
            f"In {source} at {path}: cannot read file {argument!r}: {exc}"
        ) from exc

    if len(data) > _MAX_FILE_TEMPLATE_BYTES:
        raise TemplateResolutionError(
            f"In {source} at {path}: file {argument!r} exceeds the "
            f"{_MAX_FILE_TEMPLATE_BYTES}-byte limit for "
            f"'${{file:...}}' placeholders"
        )

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TemplateResolutionError(
            f"In {source} at {path}: file {argument!r} is not valid UTF-8"
        ) from exc
