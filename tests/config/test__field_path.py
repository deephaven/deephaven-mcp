"""Tests for :mod:`deephaven_mcp.config._field_path`.

Covers:

- :meth:`FieldPath.parse` / ``str()`` grammar, including quoted
  segments and every rejection path.
- :class:`FieldPath`'s tuple-like behavior: equality/hash against a
  plain tuple, falsiness, ``len``, iteration, and the bare-``str``
  construction guard.
- The path-specific API: :attr:`FieldPath.first`, :attr:`FieldPath.last`,
  :attr:`FieldPath.parent`, :meth:`FieldPath.has_prefix`,
  :meth:`FieldPath.remove_prefix`, concatenation with ``+`` (including
  the bare-``str``-as-one-segment branch), and :attr:`FieldPath.ROOT`.
"""

from __future__ import annotations

import pytest

from deephaven_mcp._exceptions import ConfigurationPathError
from deephaven_mcp.config._field_path import FieldPath

# ---------------------------------------------------------------------------
# FieldPath.parse
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("cli", ("cli",)),
        ("cli.output.format", ("cli", "output", "format")),
        (
            "community.sessions.local_dev.port",
            ("community", "sessions", "local_dev", "port"),
        ),
        ('a."b.c".d', ("a", "b.c", "d")),
        ('"b.c"', ("b.c",)),
        ('a."b.c"', ("a", "b.c")),
        ('"x".y', ("x", "y")),
    ],
)
def test_parse_valid(text: str, expected: tuple[str, ...]) -> None:
    parsed = FieldPath.parse(text)
    assert parsed == expected
    assert isinstance(parsed, FieldPath)


@pytest.mark.parametrize(
    "text,match",
    [
        ("", "must not be empty"),
        ("a..b", "empty segment"),
        (".a", "empty segment"),
        ("a.", "trailing dot"),
        ('a."unterminated', "unterminated quote"),
        ('a.b"c', "stray quote"),
        ('a."q"x', "expected '.'"),
        ('""', "empty quoted segment"),
    ],
)
def test_parse_invalid(text: str, match: str) -> None:
    with pytest.raises(ConfigurationPathError, match=match):
        FieldPath.parse(text)


# ---------------------------------------------------------------------------
# render / str()
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "segments,expected",
    [
        (("cli",), "cli"),
        (("cli", "output", "format"), "cli.output.format"),
        (("a", "b.c", "d"), 'a."b.c".d'),
        ((), ""),
    ],
)
def test_render(segments: tuple[str, ...], expected: str) -> None:
    path = FieldPath(segments)
    assert path.render() == expected
    assert str(path) == expected


@pytest.mark.parametrize(
    "text",
    ["cli", "cli.output.format", 'a."b.c".d'],
)
def test_render_parse_round_trip(text: str) -> None:
    assert str(FieldPath.parse(text)) == text


def test_render_rejects_quote_in_segment() -> None:
    with pytest.raises(ConfigurationPathError, match="cannot be rendered"):
        FieldPath(("a", 'has"quote')).render()


def test_render_rejects_empty_segment() -> None:
    with pytest.raises(ConfigurationPathError, match="cannot be rendered"):
        FieldPath(("a", "")).render()


# ---------------------------------------------------------------------------
# tuple-like behavior
# ---------------------------------------------------------------------------


def test_empty_default() -> None:
    assert FieldPath() == ()
    assert not FieldPath()
    assert bool(FieldPath(("a",)))


def test_equality_and_hash_against_plain_tuple() -> None:
    path = FieldPath(("a", "b"))
    assert path == ("a", "b")
    assert hash(path) == hash(("a", "b"))


def test_len_and_iteration() -> None:
    path = FieldPath(("a", "b", "c"))
    assert len(path) == 3
    assert list(path) == ["a", "b", "c"]


def test_repr() -> None:
    assert repr(FieldPath(("a", "b"))) == "FieldPath(('a', 'b'))"


def test_bare_str_rejected() -> None:
    with pytest.raises(TypeError, match="bare str"):
        FieldPath("cli")


# ---------------------------------------------------------------------------
# __add__
# ---------------------------------------------------------------------------


def test_add_field_path_stays_field_path() -> None:
    combined = FieldPath(("a",)) + FieldPath(("b", "c"))
    assert combined == ("a", "b", "c")
    assert isinstance(combined, FieldPath)


def test_add_accepts_plain_tuple() -> None:
    combined = FieldPath(("a",)) + ("b", "c")
    assert combined == ("a", "b", "c")
    assert isinstance(combined, FieldPath)


def test_add_bare_str_appends_one_segment() -> None:
    combined = FieldPath(("a",)) + "bc"
    assert combined == ("a", "bc")
    assert isinstance(combined, FieldPath)


def test_add_bare_str_not_decomposed_into_characters() -> None:
    combined = FieldPath() + "xyz"
    assert combined == ("xyz",)
    assert combined != ("x", "y", "z")


# ---------------------------------------------------------------------------
# first / last / parent
# ---------------------------------------------------------------------------


def test_first_and_last() -> None:
    path = FieldPath(("a", "b", "c"))
    assert path.first == "a"
    assert path.last == "c"


def test_first_and_last_single_segment() -> None:
    path = FieldPath(("a",))
    assert path.first == "a"
    assert path.last == "a"


@pytest.mark.parametrize("accessor", ["first", "last"])
def test_first_and_last_raise_on_empty(accessor: str) -> None:
    with pytest.raises(IndexError):
        getattr(FieldPath(), accessor)


def test_parent_drops_last_segment() -> None:
    path = FieldPath(("a", "b", "c"))
    assert path.parent == ("a", "b")
    assert isinstance(path.parent, FieldPath)


def test_parent_of_single_segment_is_root() -> None:
    assert FieldPath(("a",)).parent == FieldPath.ROOT


def test_parent_of_empty_is_root() -> None:
    assert FieldPath().parent == FieldPath.ROOT


# ---------------------------------------------------------------------------
# has_prefix / remove_prefix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path,prefix,expected",
    [
        (("a", "b", "c"), ("a", "b"), True),
        (("a", "b", "c"), ("a", "b", "c"), True),
        (("a", "b", "c"), (), True),
        (("a", "b"), ("a", "b", "c"), False),
        (("a", "b", "c"), ("x",), False),
        ((), (), True),
    ],
)
def test_has_prefix(
    path: tuple[str, ...], prefix: tuple[str, ...], expected: bool
) -> None:
    assert FieldPath(path).has_prefix(FieldPath(prefix)) is expected


def test_remove_prefix_strips_matching_prefix() -> None:
    path = FieldPath(("a", "b", "c"))
    assert path.remove_prefix(FieldPath(("a", "b"))) == ("c",)


def test_remove_prefix_raises_when_not_matching() -> None:
    path = FieldPath(("a", "b"))
    with pytest.raises(ValueError, match="does not start with"):
        path.remove_prefix(FieldPath(("x",)))


def test_remove_prefix_empty_prefix_is_no_op() -> None:
    path = FieldPath(("a", "b"))
    assert path.remove_prefix(FieldPath.ROOT) == path


# ---------------------------------------------------------------------------
# ROOT
# ---------------------------------------------------------------------------


def test_root_is_empty() -> None:
    assert FieldPath.ROOT == FieldPath()
    assert not FieldPath.ROOT
