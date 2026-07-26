"""Tests for :mod:`deephaven_mcp.config._file_kinds`.

Covers:

- :meth:`ConfigFileKind.relative_file_path` for every kind and its
  error paths.
- Each kind's schema accepting a minimal wire-format payload.
- Registry <-> :class:`ConfigTreeLoader` drift: a directory containing
  one file of every kind loads cleanly and
  :meth:`~deephaven_mcp.config._logical_paths.ConfigSection.files`
  enumerates exactly those files.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from deephaven_mcp._exceptions import ConfigurationPathError
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._logical_paths import ConfigSection
from deephaven_mcp.config.tree import ConfigTreeLoader

# ---------------------------------------------------------------------------
# ConfigFileKind.relative_file_path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind,name,expected",
    [
        (ConfigFileKind.CLI, None, "cli.json"),
        (ConfigFileKind.SERVER, None, "server.json"),
        (ConfigFileKind.COMMUNITY_SETTINGS, None, "community/settings.json"),
        (ConfigFileKind.COMMUNITY_SESSION, "local", "community/sessions/local.json"),
        (ConfigFileKind.ENTERPRISE_SETTINGS, None, "enterprise/settings.json"),
        (ConfigFileKind.ENTERPRISE_SYSTEM, "prod", "enterprise/systems/prod.json"),
    ],
)
def test_relative_file_path(
    kind: ConfigFileKind, name: str | None, expected: str
) -> None:
    assert kind.relative_file_path(name) == Path(expected)


def test_relative_file_path_named_requires_name() -> None:
    with pytest.raises(ConfigurationPathError, match="requires an entity name"):
        ConfigFileKind.COMMUNITY_SESSION.relative_file_path(None)


def test_relative_file_path_unnamed_rejects_name() -> None:
    with pytest.raises(ConfigurationPathError, match="does not take an entity name"):
        ConfigFileKind.CLI.relative_file_path("oops")


# ---------------------------------------------------------------------------
# Schema validation of the wire shape
# ---------------------------------------------------------------------------


def test_registry_schemas_validate_wire_shapes() -> None:
    """Each kind's schema accepts a minimal wire-format payload.

    Named kinds take the filename stem injected as ``name`` — the same
    contract ``load_named_json_with_stem`` applies.
    """
    ConfigFileKind.CLI.schema.model_validate({})
    ConfigFileKind.SERVER.schema.model_validate({})
    ConfigFileKind.COMMUNITY_SETTINGS.schema.model_validate({})
    ConfigFileKind.ENTERPRISE_SETTINGS.schema.model_validate({})
    ConfigFileKind.COMMUNITY_SESSION.schema.model_validate(
        {"name": "x", "auth": {"credentials": {"type": "anonymous"}}}
    )
    ConfigFileKind.ENTERPRISE_SYSTEM.schema.model_validate(
        {
            "name": "prod",
            "connection_json_url": "https://x/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        }
    )


# ---------------------------------------------------------------------------
# Registry <-> ConfigTreeLoader drift
# ---------------------------------------------------------------------------


def _touch_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _chmod_tree(config_dir: Path) -> None:
    """Apply user-only POSIX modes to ``config_dir`` and everything below it."""
    if sys.platform == "win32":
        return
    config_dir.chmod(0o700)
    for child in config_dir.rglob("*"):
        child.chmod(0o700 if child.is_dir() else 0o600)


@pytest.mark.asyncio
async def test_registry_matches_config_tree_loader(tmp_path: Path) -> None:
    """One file of every registry kind loads cleanly via ConfigTreeLoader.

    Pins the registry to the loader's real layout: if the loader gains,
    renames, or moves a section, this test fails until ConfigFileKind is
    updated (and vice versa).
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _touch_json(config_dir / "cli.json", {})
    _touch_json(config_dir / "server.json", {})
    _touch_json(config_dir / "community" / "settings.json", {})
    _touch_json(
        config_dir / "community" / "sessions" / "local.json",
        {"host": "localhost", "auth": {"credentials": {"type": "anonymous"}}},
    )
    _touch_json(config_dir / "enterprise" / "settings.json", {})
    _touch_json(
        config_dir / "enterprise" / "systems" / "prod.json",
        {
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    _chmod_tree(config_dir)

    tree = await ConfigTreeLoader(config_dir=config_dir).initialize()

    # Every registry kind was consumed by the loader.
    assert tree.cli is not None
    assert tree.server is not None
    assert tree.community is not None
    assert tree.community.settings is not None
    assert list(tree.community.sessions) == ["local"]
    assert tree.enterprise is not None
    assert tree.enterprise.settings is not None
    assert list(tree.enterprise.systems) == ["prod"]

    # The registry enumerates exactly the files on disk.
    targets = ConfigSection(FieldPath.ROOT).files(config_dir)
    on_disk = sorted(str(p.relative_to(config_dir)) for p in config_dir.rglob("*.json"))
    enumerated = sorted(str(t.relative_file_path) for t in targets)
    assert enumerated == on_disk
