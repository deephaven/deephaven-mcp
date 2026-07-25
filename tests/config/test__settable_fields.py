"""Tests for :mod:`deephaven_mcp.config._settable_fields`."""

from __future__ import annotations

from enum import IntEnum
from typing import Annotated

from pydantic import BaseModel, Field, SecretStr

from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._settable_fields import (
    _contains_secret,
    _json_type_name,
    _model_settable_fields,
    _to_plain_text,
    settable_fields,
)


class _SampleIntEnum(IntEnum):
    """IntEnum sample; no such field exists in the real schemas today."""

    ONE = 1


def test_json_type_name_detects_int_enum() -> None:
    assert _json_type_name(_SampleIntEnum) == "integer"


def test_json_type_name_falls_back_to_object() -> None:
    """A non-optional multi-member union has no single JSON type."""
    assert _json_type_name(str | int) == "object"


def test_json_type_name_unwraps_annotated() -> None:
    """An ``Annotated[int, ...] | None`` annotation is an integer.

    Pydantic normally hoists ``Annotated`` metadata into the
    ``FieldInfo`` before the walk sees it; this pins the defensive
    unwrap for any annotation that arrives still wrapped.
    """
    assert _json_type_name(Annotated[int, Field(gt=0)]) == "integer"
    assert _json_type_name(Annotated[int, Field(gt=0)] | None) == "integer"


def test_community_session_port_reported_as_integer() -> None:
    """The constrained ``port`` field (Annotated int in the schema
    source) must surface as ``integer``, not ``object``."""
    fields = settable_fields(ConfigFileKind.COMMUNITY_SESSION)
    port = next(f for f in fields if f.path == FieldPath(("port",)))
    assert port.json_type == "integer"


def test_contains_secret_checks_every_generic_argument() -> None:
    """A SecretStr nested as a dict *value* type must still be detected.

    ``dict[str, SecretStr]`` has generic args ``(str, SecretStr)``; the
    secret type is the second argument, not the first. No current
    schema field has this shape, but the check must not silently miss
    it if one is added.
    """
    assert _contains_secret(dict[str, SecretStr]) is True
    assert _contains_secret(dict[str, str]) is False
    assert _contains_secret(list[SecretStr]) is True
    assert _contains_secret(SecretStr | None) is True
    assert _contains_secret(str | None) is False


class _SecretLeaf(BaseModel):
    token: SecretStr


class _PlainLeaf(BaseModel):
    value: str


def test_contains_secret_recurses_into_model_fields() -> None:
    """A SecretStr inside a model class (e.g. a credentials union
    variant) must be detected even though ``get_args`` on the class
    returns nothing."""
    assert _contains_secret(_SecretLeaf) is True
    assert _contains_secret(_PlainLeaf) is False
    assert _contains_secret(_SecretLeaf | None) is True


def test_plain_text_none_passes_through() -> None:
    assert _to_plain_text(None) is None


def test_plain_text_strips_rst_literals_and_collapses_whitespace() -> None:
    raw = "Uses ``foo`` when set.\nOtherwise ``None``   falls back."
    assert _to_plain_text(raw) == "Uses foo when set. Otherwise None falls back."


class _RequiredChild(BaseModel):
    needed: str


class _OptionalBlockParent(BaseModel):
    block: _RequiredChild | None = None
    strict_block: _RequiredChild


def test_required_demoted_under_optional_block() -> None:
    """A required child inside an optional block is not itself required."""
    by_path = {f.path: f for f in _model_settable_fields(_OptionalBlockParent)}
    assert by_path[("block", "needed")].required is False
    assert by_path[("strict_block", "needed")].required is True


def _by_path(kind: ConfigFileKind) -> dict[FieldPath, object]:
    return {f.path: f for f in settable_fields(kind)}


def test_cli_leaf_fields() -> None:
    fields = _by_path(ConfigFileKind.CLI)
    fmt = fields[("output", "format")]
    assert fmt.json_type == "string"
    assert fmt.default == "json"
    assert fmt.required is False
    assert fmt.secret is False


def test_cli_enum_field_reported_as_string() -> None:
    fields = _by_path(ConfigFileKind.CLI)
    field = fields[("daemon", "reuse", "version")]
    assert field.json_type == "string"
    assert field.default == "refuse"


def test_server_secret_field() -> None:
    fields = _by_path(ConfigFileKind.SERVER)
    psk = fields[("psk",)]
    assert psk.json_type == "string"
    assert psk.secret is True
    assert psk.default is None  # SecretStr default is never surfaced as scalar


def test_community_settings_recurses_nested_models() -> None:
    fields = _by_path(ConfigFileKind.COMMUNITY_SETTINGS)
    assert ("session_creation", "defaults", "heap_size_gb") in fields
    heap = fields[("session_creation", "defaults", "heap_size_gb")]
    assert heap.json_type == "number"
    assert heap.default == 4.0


def test_community_settings_nested_auth_credentials_wire_path() -> None:
    """Regression: the defaults block's credentials live at the nested
    wire path ``session_creation.defaults.auth.credentials``, and the
    flattened model-era path must not be reported."""
    fields = _by_path(ConfigFileKind.COMMUNITY_SETTINGS)
    creds = fields[("session_creation", "defaults", "auth", "credentials")]
    assert creds.json_type == "object"
    assert creds.secret is True
    assert creds.required is False  # the auth block itself is optional
    assert ("session_creation", "defaults", "credentials") not in fields


def test_community_session_auth_credentials() -> None:
    fields = _by_path(ConfigFileKind.COMMUNITY_SESSION)
    creds = fields[("auth", "credentials")]
    assert creds.required is True
    assert creds.json_type == "object"
    assert creds.secret is True
    # The loader-injected 'name' field is not a settable wire path.
    assert not any(path[0] == "name" for path in fields)
    assert ("credentials",) not in fields


def test_enterprise_system_required_fields() -> None:
    fields = _by_path(ConfigFileKind.ENTERPRISE_SYSTEM)
    assert fields[("connection_json_url",)].required is True
    creds = fields[("auth", "credentials")]
    assert creds.required is True
    assert creds.secret is True
    assert not any(path[0] == "name" for path in fields)
    assert ("credentials",) not in fields


def test_open_dict_field_reported_as_object() -> None:
    fields = _by_path(ConfigFileKind.ENTERPRISE_SYSTEM)
    env_vars = fields[("session_creation", "defaults", "environment_vars")]
    assert env_vars.json_type == "object"
    assert env_vars.required is False


def test_list_field_reported_as_array() -> None:
    fields = _by_path(ConfigFileKind.COMMUNITY_SETTINGS)
    volumes = fields[("session_creation", "defaults", "docker", "volumes")]
    assert volumes.json_type == "array"


def test_nested_model_block_emitted_as_object_entry() -> None:
    """A nested block is itself a 'config set' target and precedes its children."""
    fields = settable_fields(ConfigFileKind.CLI)
    paths = [f.path for f in fields]
    block = next(f for f in fields if f.path == FieldPath(("output",)))
    assert block.json_type == "object"
    assert paths.index(FieldPath(("output",))) < paths.index(
        FieldPath(("output", "format"))
    )


def test_nested_model_block_entry_flags() -> None:
    """The block entry carries the field's own required/secret status."""
    fields = _by_path(ConfigFileKind.COMMUNITY_SESSION)
    auth = fields[("auth",)]
    assert auth.json_type == "object"
    assert auth.required is True
    assert auth.secret is True
    assert auth.default is None


def test_every_kind_has_fields() -> None:
    for kind in ConfigFileKind:
        assert settable_fields(kind), f"{kind} produced no fields"


def test_descriptions_present_for_documented_fields() -> None:
    fields = _by_path(ConfigFileKind.SERVER)
    assert fields[("host",)].description
