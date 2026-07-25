"""Registry of configuration file kinds.

The configuration directory holds six kinds of file: two singletons
(``cli.json``, ``server.json``), two per-umbrella settings files
(``community/settings.json``, ``enterprise/settings.json``), and two
per-name entity collections (``community/sessions/<name>.json``,
``enterprise/systems/<name>.json``). :class:`ConfigFileKind` is the
single source of truth binding each kind to its logical-path prefix,
whether it is a per-name file, and the Pydantic schema that validates
it (wire format; named kinds validate with the filename stem injected
as ``name``).
"""

from __future__ import annotations

__all__ = [
    "ConfigFileKind",
]

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from deephaven_mcp._exceptions import ConfigurationPathError
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config.schema import (
    CliConfig,
    CommunitySettings,
    EnterpriseSettings,
    ServerConfig,
)
from deephaven_mcp.sessions import CommunitySessionConfig, EnterpriseSystemConfig


class ConfigFileKind(StrEnum):
    """One of the six configuration file kinds under the config directory."""

    prefix: FieldPath
    """The path addressing this kind. For a named kind the prefix is
    the collection (``FieldPath(("community", "sessions"))``) and the
    entity name is the next segment."""

    named: bool
    """Whether the kind is a per-name file (one file per session or
    system, the filename stem being the name)."""

    schema: type[BaseModel]
    """Pydantic model validating one file of this kind (wire format;
    named kinds are validated with the filename stem injected as
    ``name``)."""

    def __new__(
        cls,
        value: str,
        prefix: FieldPath,
        named: bool,
        schema: type[BaseModel],
    ) -> ConfigFileKind:
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.prefix = prefix
        obj.named = named
        obj.schema = schema
        return obj

    CLI = ("cli", FieldPath(("cli",)), False, CliConfig)
    """``cli.json`` — dhcli CLI user defaults."""

    SERVER = ("server", FieldPath(("server",)), False, ServerConfig)
    """``server.json`` — server-process tunables."""

    COMMUNITY_SETTINGS = (
        "community_settings",
        FieldPath(("community", "settings")),
        False,
        CommunitySettings,
    )
    """``community/settings.json`` — community-wide globals."""

    COMMUNITY_SESSION = (
        "community_session",
        FieldPath(("community", "sessions")),
        True,
        CommunitySessionConfig,
    )
    """``community/sessions/<name>.json`` — one static community session."""

    ENTERPRISE_SETTINGS = (
        "enterprise_settings",
        FieldPath(("enterprise", "settings")),
        False,
        EnterpriseSettings,
    )
    """``enterprise/settings.json`` — enterprise-wide globals."""

    ENTERPRISE_SYSTEM = (
        "enterprise_system",
        FieldPath(("enterprise", "systems")),
        True,
        EnterpriseSystemConfig,
    )
    """``enterprise/systems/<name>.json`` — one enterprise system."""

    def relative_file_path(self, name: str | None = None) -> Path:
        """Return the file path relative to the configuration directory.

        Args:
            name (str | None): Entity name for a named kind (the
                filename stem). Must be ``None`` for unnamed kinds and
                a non-empty string for named kinds.

        Returns:
            Path: Relative path such as ``cli.json`` or
                ``community/sessions/<name>.json``.

        Raises:
            ConfigurationPathError: When ``name`` is missing for a
                named kind or supplied for an unnamed kind.
        """
        if self.named:
            if not name:
                raise ConfigurationPathError(
                    f"file kind {self.value!r} requires an entity name"
                )
            return Path(*self.prefix) / f"{name}.json"
        if name is not None:
            raise ConfigurationPathError(
                f"file kind {self.value!r} does not take an entity name"
            )
        return Path(*self.prefix.parent, f"{self.prefix.last}.json")
