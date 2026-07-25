"""Tests for :mod:`deephaven_mcp.config._logical_paths`.

Covers:

- :class:`ConfigFieldLocation` logical/physical path derivation.
- :func:`resolve_path` classification for every file kind, the
  section results, and every error path (unknown path, invalid name,
  reserved enterprise name).
- :class:`ConfigSection` construction invariant, kind filtering, and
  file enumeration.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from deephaven_mcp._exceptions import ConfigurationPathError
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._logical_paths import (
    ConfigFieldLocation,
    ConfigSection,
    resolve_path,
)

# ---------------------------------------------------------------------------
# ConfigFieldLocation
# ---------------------------------------------------------------------------


def test_logical_path_unnamed() -> None:
    target = ConfigFieldLocation(
        kind=ConfigFileKind.COMMUNITY_SETTINGS,
        name=None,
        field_path=FieldPath(("session_creation", "defaults")),
    )
    assert target.logical_path == ("community", "settings")
    assert target.relative_file_path == Path("community/settings.json")


def test_logical_path_named() -> None:
    target = ConfigFieldLocation(
        kind=ConfigFileKind.ENTERPRISE_SYSTEM,
        name="prod",
        field_path=FieldPath(("auth", "credentials", "token")),
    )
    assert target.logical_path == ("enterprise", "systems", "prod")
    assert target.relative_file_path == Path("enterprise/systems/prod.json")


def test_field_path_is_required() -> None:
    """``field_path`` has no default: whole-file targets say so explicitly."""
    with pytest.raises(TypeError):
        ConfigFieldLocation(kind=ConfigFileKind.CLI, name=None)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# resolve_path
# ---------------------------------------------------------------------------


def test_resolve_root_is_a_section() -> None:
    resolved = resolve_path(FieldPath.ROOT)
    assert resolved == ConfigSection(FieldPath.ROOT)


@pytest.mark.parametrize(
    "segments",
    [
        ("community",),
        ("enterprise",),
        ("community", "sessions"),
        ("enterprise", "systems"),
    ],
)
def test_resolve_above_file_level_is_a_section(segments: tuple[str, ...]) -> None:
    resolved = resolve_path(FieldPath(segments))
    assert isinstance(resolved, ConfigSection)
    assert resolved.prefix == segments


@pytest.mark.parametrize(
    "segments,kind,name,field_path",
    [
        (("cli",), ConfigFileKind.CLI, None, ()),
        (("cli", "output", "format"), ConfigFileKind.CLI, None, ("output", "format")),
        (("server", "psk"), ConfigFileKind.SERVER, None, ("psk",)),
        (
            ("community", "settings", "session_creation", "defaults", "heap_size_gb"),
            ConfigFileKind.COMMUNITY_SETTINGS,
            None,
            ("session_creation", "defaults", "heap_size_gb"),
        ),
        (
            ("community", "sessions", "local_dev"),
            ConfigFileKind.COMMUNITY_SESSION,
            "local_dev",
            (),
        ),
        (
            ("community", "sessions", "local_dev", "port"),
            ConfigFileKind.COMMUNITY_SESSION,
            "local_dev",
            ("port",),
        ),
        (
            ("enterprise", "settings", "timeouts"),
            ConfigFileKind.ENTERPRISE_SETTINGS,
            None,
            ("timeouts",),
        ),
        (
            ("enterprise", "systems", "prod", "auth", "credentials", "token"),
            ConfigFileKind.ENTERPRISE_SYSTEM,
            "prod",
            ("auth", "credentials", "token"),
        ),
    ],
)
def test_resolve_to_file_targets(
    segments: tuple[str, ...],
    kind: ConfigFileKind,
    name: str | None,
    field_path: tuple[str, ...],
) -> None:
    target = resolve_path(FieldPath(segments))
    assert isinstance(target, ConfigFieldLocation)
    assert target.kind is kind
    assert target.name == name
    assert target.field_path == field_path


def test_resolve_unknown_root_lists_suggestions() -> None:
    with pytest.raises(ConfigurationPathError) as exc:
        resolve_path(FieldPath(("bogus",)))
    msg = str(exc.value)
    assert "unknown configuration path 'bogus'" in msg
    assert "cli" in msg
    assert "enterprise.systems.<name>" in msg


def test_resolve_unknown_section_child_lists_scoped_suggestions() -> None:
    with pytest.raises(ConfigurationPathError) as exc:
        resolve_path(FieldPath(("community", "foo")))
    msg = str(exc.value)
    assert "community.settings" in msg
    assert "community.sessions.<name>" in msg
    # Unrelated sections are not suggested.
    assert "enterprise" not in msg


def test_resolve_rejects_invalid_entity_name() -> None:
    # A dotted name can only arrive via a quoted segment; the resource
    # name rule rejects it.
    with pytest.raises(ConfigurationPathError, match="illegal character"):
        resolve_path(FieldPath(("community", "sessions", "bad.name")))


def test_resolve_rejects_reserved_enterprise_name() -> None:
    with pytest.raises(ConfigurationPathError, match="reserved"):
        resolve_path(FieldPath(("enterprise", "systems", "community")))


def test_resolve_allows_community_as_session_name() -> None:
    target = resolve_path(FieldPath(("community", "sessions", "community")))
    assert isinstance(target, ConfigFieldLocation)
    assert target.name == "community"


# ---------------------------------------------------------------------------
# ConfigSection
# ---------------------------------------------------------------------------


def _touch_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


@pytest.mark.parametrize(
    "prefix",
    [
        ("cli",),  # exactly at an unnamed kind's file boundary
        ("cli", "output", "format"),  # below an unnamed kind's boundary
        ("community", "sessions", "local_dev"),  # exactly at a named kind's file
        ("community", "sessions", "x", "port"),  # below a named kind's file
    ],
)
def test_section_rejects_prefix_at_or_below_file_level(
    prefix: tuple[str, ...],
) -> None:
    """The construction invariant: a ``ConfigSection`` can never name a
    file or a field — that is :class:`ConfigFieldLocation`'s domain."""
    with pytest.raises(ConfigurationPathError, match="names a configuration file"):
        ConfigSection(FieldPath(prefix))


def test_section_rejects_unknown_prefix() -> None:
    with pytest.raises(ConfigurationPathError, match="unknown configuration path"):
        ConfigSection(FieldPath(("bogus",)))


def test_section_kinds_root_lists_every_kind() -> None:
    assert ConfigSection(FieldPath.ROOT).kinds() == list(ConfigFileKind)


def test_section_kinds_filters_by_prefix() -> None:
    assert ConfigSection(FieldPath(("community",))).kinds() == [
        ConfigFileKind.COMMUNITY_SETTINGS,
        ConfigFileKind.COMMUNITY_SESSION,
    ]
    assert ConfigSection(FieldPath(("enterprise", "systems"))).kinds() == [
        ConfigFileKind.ENTERPRISE_SYSTEM,
    ]


def test_section_files_empty_dir(tmp_path: Path) -> None:
    targets = ConfigSection(FieldPath.ROOT).files(tmp_path)
    assert [t.kind for t in targets] == [
        ConfigFileKind.CLI,
        ConfigFileKind.SERVER,
        ConfigFileKind.COMMUNITY_SETTINGS,
        ConfigFileKind.ENTERPRISE_SETTINGS,
    ]
    assert all(t.field_path == FieldPath.ROOT for t in targets)


def test_section_files_with_named_entries(tmp_path: Path) -> None:
    _touch_json(tmp_path / "community" / "sessions" / "b.json", {})
    _touch_json(tmp_path / "community" / "sessions" / "a.json", {})
    _touch_json(tmp_path / "enterprise" / "systems" / "prod.json", {})
    targets = ConfigSection(FieldPath.ROOT).files(tmp_path)
    named = [(t.kind, t.name) for t in targets if t.name is not None]
    assert named == [
        (ConfigFileKind.COMMUNITY_SESSION, "a"),
        (ConfigFileKind.COMMUNITY_SESSION, "b"),
        (ConfigFileKind.ENTERPRISE_SYSTEM, "prod"),
    ]


def test_section_files_prefix_section(tmp_path: Path) -> None:
    _touch_json(tmp_path / "community" / "sessions" / "x.json", {})
    targets = ConfigSection(FieldPath(("community",))).files(tmp_path)
    assert [(t.kind, t.name) for t in targets] == [
        (ConfigFileKind.COMMUNITY_SETTINGS, None),
        (ConfigFileKind.COMMUNITY_SESSION, "x"),
    ]


def test_section_files_prefix_collection(tmp_path: Path) -> None:
    _touch_json(tmp_path / "community" / "sessions" / "x.json", {})
    targets = ConfigSection(FieldPath(("community", "sessions"))).files(tmp_path)
    assert [(t.kind, t.name) for t in targets] == [
        (ConfigFileKind.COMMUNITY_SESSION, "x"),
    ]
