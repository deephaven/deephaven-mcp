"""``FieldPath``: one dotted path, as a sequence of segments.

A :class:`FieldPath` is a dot-separated address into the logical
configuration document — see
:mod:`deephaven_mcp.config._logical_paths` for that document's shape
and how a path resolves to a file. A path can address any depth
(``cli.output.format``, ``community.settings.session_creation``,
``enterprise.systems.prod.auth.credentials``), or be empty
(:attr:`FieldPath.ROOT`), addressing the whole document. It always
addresses the **wire format** (the JSON as written in the files, e.g.
``auth.credentials``), never the post-validation model shape.

A path segment may be double-quoted to contain a literal dot (TOML
dotted-key grammar, e.g. ``defaults.session_arguments."my.key"``).
Resource names themselves can never contain dots
(:mod:`deephaven_mcp._names`), so an unquoted path is always
unambiguous.

Every other module in this package that needs to carry a dotted path
or a bare sequence of path segments — file-kind prefixes
(:class:`~deephaven_mcp.config._file_kinds.ConfigFileKind`), resolved
targets (:class:`~deephaven_mcp.config._logical_paths.ConfigFieldLocation`),
schema-derived key listings
(:class:`~deephaven_mcp.config._settable_fields.SettableField`), and the field-access
helpers in :mod:`deephaven_mcp.config._fields` — uses this one type,
so the concept has exactly one home.
"""

from __future__ import annotations

__all__ = [
    "FieldPath",
]

from collections.abc import Iterable
from typing import ClassVar

from deephaven_mcp._exceptions import ConfigurationPathError


def _parse_quoted_segment(text: str, i: int) -> tuple[str, int]:
    """Parse a double-quoted segment starting at ``text[i] == '"'``.

    Returns:
        tuple[str, int]: The segment text and the index just past the
            closing quote.

    Raises:
        ConfigurationPathError: When the quote is unterminated or the
            quoted segment is empty.
    """
    end = text.find('"', i + 1)
    if end < 0:
        raise ConfigurationPathError(
            f"unterminated quote in configuration path {text!r}"
        )
    segment = text[i + 1 : end]
    if not segment:
        raise ConfigurationPathError(
            f"empty quoted segment in configuration path {text!r}"
        )
    return segment, end + 1


def _parse_unquoted_segment(text: str, i: int) -> tuple[str, int]:
    """Parse an unquoted segment starting at index ``i``.

    Returns:
        tuple[str, int]: The segment text and the index of the
            terminating ``.`` (or ``len(text)``).

    Raises:
        ConfigurationPathError: When the segment is empty, or a stray quote
            appears inside it.
    """
    n = len(text)
    start = i
    while i < n and text[i] not in '."':
        i += 1
    if i < n and text[i] == '"':
        raise ConfigurationPathError(
            f"stray quote inside segment in configuration path "
            f'{text!r}; quote the whole segment: "seg.ment"'  # codespell:ignore ment
        )
    segment = text[start:i]
    if not segment:
        raise ConfigurationPathError(f"empty segment in configuration path {text!r}")
    return segment, i


class FieldPath(tuple[str, ...]):
    """An immutable sequence of dotted-path segments.

    Addresses a location in the logical configuration document, at
    any depth: a whole file (``FieldPath(("cli",))``), a field within
    one (``FieldPath(("cli", "output", "format"))``), or the document
    root (:attr:`ROOT`, empty). Behaves like a tuple of strings for
    iteration, ``len``, falsiness when empty, hashing, and equality
    (including against a plain ``tuple``); concatenable with ``+``
    (a bare ``str`` operand is appended as one segment, never
    decomposed into characters). :attr:`first`, :attr:`last`,
    :attr:`parent`, :meth:`has_prefix`, and :meth:`remove_prefix` are
    the path-specific API — there is no generic indexing or slicing.

    ``str(path)`` renders it back to dotted text — a segment
    containing a literal dot is double-quoted (TOML dotted-key style,
    e.g. ``defaults.session_arguments."my.key"``) — and
    :meth:`FieldPath.parse` is the inverse. The two round-trip for
    every ``FieldPath``.
    """

    __slots__ = ()

    ROOT: ClassVar[FieldPath]
    """The empty path — the configuration document's root, and the
    canonical way to write ``FieldPath()``. Always use this in a
    default-argument position; a bare ``FieldPath()`` call there also
    trips ruff's B008 (no-call-in-default-argument) rule."""

    def __new__(cls, segments: Iterable[str] = ()) -> FieldPath:
        # A bare `str` is structurally an `Iterable[str]` (its
        # characters), so this is not redundant with the type hint:
        # mypy accepts `FieldPath("cli.output")` as well-typed, and
        # the hint is unchecked at runtime regardless. Left
        # unguarded, `super().__new__` would silently decompose the
        # string into one segment per character.
        if isinstance(segments, str):
            raise TypeError(
                "FieldPath does not accept a bare str; use FieldPath.parse(text) "
                "for dotted text or FieldPath((text,)) for one literal segment"
            )
        return super().__new__(cls, segments)

    @classmethod
    def parse(cls, text: str) -> FieldPath:
        """Parse dot-separated path text into a :class:`FieldPath`.

        Grammar: segments are separated by ``.``; a segment is either
        an unquoted run of characters containing neither ``.`` nor
        ``"``, or a double-quoted string that may contain dots (TOML
        dotted-key style). Quotes cannot appear inside a segment
        body.

        Args:
            text (str): The path text, e.g.
                ``community.sessions.local_dev.port`` or
                ``defaults.session_arguments."my.key"``.

        Returns:
            FieldPath: The parsed path.

        Raises:
            ConfigurationPathError: When ``text`` is empty, has an empty
                segment (leading/trailing/double dot), an unterminated
                quote, a stray quote inside an unquoted segment, or a
                quoted segment not followed by ``.`` or end of input.
        """
        if not text:
            raise ConfigurationPathError("configuration path must not be empty")
        segments: list[str] = []
        i = 0
        n = len(text)
        while True:
            if i < n and text[i] == '"':
                segment, i = _parse_quoted_segment(text, i)
            else:
                segment, i = _parse_unquoted_segment(text, i)
            segments.append(segment)
            if i == n:
                return cls(segments)
            if text[i] != ".":
                raise ConfigurationPathError(
                    "expected '.' after quoted segment in configuration path "
                    f"{text!r}"
                )
            i += 1
            if i == n:
                raise ConfigurationPathError(
                    f"trailing dot in configuration path {text!r}"
                )

    def render(self) -> str:
        """Render this path back into canonical dot-separated text.

        The inverse of :meth:`parse`: a segment containing a dot is
        double-quoted, all others are emitted bare, so the result
        always re-parses to this same path.

        Returns:
            str: The canonical path text (empty string for an empty
                path).

        Raises:
            ConfigurationPathError: When a segment is empty or contains
                ``"`` (such a key is not addressable as a path; assign
                the parent object instead).
        """
        parts: list[str] = []
        for segment in self:
            if not segment or '"' in segment:
                raise ConfigurationPathError(
                    f"segment {segment!r} cannot be rendered as a "
                    "configuration path segment"
                )
            parts.append(f'"{segment}"' if "." in segment else segment)
        return ".".join(parts)

    def __str__(self) -> str:
        return self.render()

    def __repr__(self) -> str:
        return f"FieldPath({tuple(self)!r})"

    # Deliberately narrower than tuple.__add__'s generic overload: a
    # FieldPath only ever concatenates with another sequence of
    # segments (or a bare str, treated as exactly one segment — never
    # a heterogeneous tuple, and never decomposed into characters).
    def __add__(self, other: FieldPath | str) -> FieldPath:  # type: ignore[override]
        if isinstance(other, str):
            return FieldPath((*self, other))
        return FieldPath((*self, *other))

    def has_prefix(self, prefix: FieldPath) -> bool:
        """Whether this path begins with every segment of ``prefix``.

        Args:
            prefix (FieldPath): The candidate leading segments.

        Returns:
            bool: ``True`` when this path's leading segments equal
                ``prefix`` exactly (including when ``prefix`` is
                longer than this path, which is always ``False``
                unless ``prefix`` is also empty).
        """
        return self[: len(prefix)] == prefix

    def remove_prefix(self, prefix: FieldPath) -> FieldPath:
        """Return this path with ``prefix`` stripped from the front.

        Mirrors :meth:`pathlib.PurePath.relative_to` rather than
        ``str.removeprefix``: ``FieldPath`` is a structured,
        hierarchical path, not opaque text, so a ``prefix`` that
        does not actually apply indicates a caller error rather than
        an optional decoration to tolerate. Check :meth:`has_prefix`
        first if a mismatch is a legitimate possibility.

        Args:
            prefix (FieldPath): The leading segments to remove.

        Returns:
            FieldPath: The remaining segments after ``prefix``.

        Raises:
            ValueError: When this path does not start with
                ``prefix``.
        """
        if not self.has_prefix(prefix):
            raise ValueError(f"{self} does not start with {prefix}")
        return FieldPath(self[len(prefix) :])

    @property
    def first(self) -> str:
        """The first segment.

        Raises:
            IndexError: When this path is empty.
        """
        return self[0]

    @property
    def last(self) -> str:
        """The last segment.

        Raises:
            IndexError: When this path is empty.
        """
        return self[-1]

    @property
    def parent(self) -> FieldPath:
        """Every segment but the last.

        Returns:
            FieldPath: ``FieldPath.ROOT`` when this path has 0 or 1
                segments.
        """
        return FieldPath(self[:-1])


FieldPath.ROOT = FieldPath()
