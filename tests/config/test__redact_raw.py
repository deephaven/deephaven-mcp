"""Tests for schema-guided redaction of raw configuration data.

The redaction points come from the same schema walk that powers
``dhcli config keys``, so these tests exercise the real schemas rather
than synthetic models: a drift between the two surfaces should fail
here.
"""

from __future__ import annotations

from typing import Any, get_args

import pytest

from deephaven_mcp.auth.credentials import CredentialsUnion
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._redact_raw import (
    _DISCRIMINATOR,
    _redaction_points,
    redact_raw,
)

_CREDENTIALS = FieldPath(("auth", "credentials"))
_PRIVATE_KEY = FieldPath(("tls", "client_certificate", "private_key"))


def _session_file() -> dict[str, Any]:
    """Return a static community session file with both secret shapes."""
    return {
        "host": "dh.example.com",
        "port": 10000,
        "tls": {
            "root_certs": "${file:/etc/ssl/ca.pem}",
            "client_certificate": {
                "cert_chain": "-----BEGIN CERTIFICATE-----",
                "private_key": "-----BEGIN PRIVATE KEY-----",
            },
        },
        "auth": {"credentials": {"type": "psk", "token": "literal-token"}},
    }


# ---------------------------------------------------------------------------
# redaction points
# ---------------------------------------------------------------------------


def test_points_are_the_deepest_secret_fields() -> None:
    """``auth`` and ``tls`` are flagged secret because they *contain* a
    secret; redacting there would hide non-secret structure, so only the
    deepest flagged paths are redaction points."""
    points = _redaction_points(ConfigFileKind.COMMUNITY_SESSION)
    assert points == {_CREDENTIALS, _PRIVATE_KEY}
    assert FieldPath(("auth",)) not in points
    assert FieldPath(("tls",)) not in points
    assert FieldPath(("tls", "client_certificate")) not in points


def test_server_psk_is_a_point() -> None:
    assert FieldPath(("psk",)) in _redaction_points(ConfigFileKind.SERVER)


def test_points_exclude_non_secret_fields() -> None:
    points = _redaction_points(ConfigFileKind.COMMUNITY_SESSION)
    assert FieldPath(("host",)) not in points
    assert FieldPath(("port",)) not in points
    assert FieldPath(("tls", "client_certificate", "cert_chain")) not in points


_EXPECTED_POINTS: dict[ConfigFileKind, set[FieldPath]] = {
    ConfigFileKind.CLI: set(),
    ConfigFileKind.SERVER: {FieldPath(("psk",))},
    ConfigFileKind.COMMUNITY_SETTINGS: {
        FieldPath(("session_creation", "defaults", "auth", "credentials"))
    },
    ConfigFileKind.COMMUNITY_SESSION: {_CREDENTIALS, _PRIVATE_KEY},
    ConfigFileKind.ENTERPRISE_SETTINGS: set(),
    ConfigFileKind.ENTERPRISE_SYSTEM: {_CREDENTIALS},
}


@pytest.mark.parametrize("kind", list(ConfigFileKind), ids=lambda k: k.value)
def test_points_are_pinned_for_every_file_kind(kind: ConfigFileKind) -> None:
    """``config get`` with no PATH spans all six kinds, so each one's
    redaction points are pinned exactly. A schema change that adds or
    moves a secret anywhere fails here instead of silently leaking."""
    assert _redaction_points(kind) == _EXPECTED_POINTS[kind]


def test_every_file_kind_is_pinned() -> None:
    """Guards the table above against a newly added file kind slipping in
    unpinned, which parametrization alone would not catch."""
    assert set(_EXPECTED_POINTS) == set(ConfigFileKind)


def test_discriminator_matches_the_schema_declaration() -> None:
    """``_DISCRIMINATOR`` duplicates the ``Field(discriminator=...)`` on
    the credentials union. Renaming the tag there without updating the
    constant would make ``config get`` start redacting it -- a silent
    loss of diagnostic value, with nothing else failing.

    ``CredentialsUnion`` is an ``Annotated[...]`` alias, so ``get_args``
    yields the union first and its metadata after; the discriminator
    lives on the ``FieldInfo`` in that metadata.
    """
    declared = [
        getattr(meta, "discriminator", None) for meta in get_args(CredentialsUnion)[1:]
    ]
    assert _DISCRIMINATOR in declared, (
        f"credentials union declares {declared}, but _redact_raw preserves "
        f"{_DISCRIMINATOR!r}"
    )


# ---------------------------------------------------------------------------
# whole-file redaction
# ---------------------------------------------------------------------------


def test_secret_block_keeps_its_keys_and_discriminator() -> None:
    """The credential value is replaced, but the block's shape and the
    ``type`` tag — which names the auth kind, not the secret — survive."""
    outcome = redact_raw(ConfigFileKind.COMMUNITY_SESSION, _session_file())
    assert outcome.value["auth"] == {
        "credentials": {"type": "psk", "token": "[REDACTED]"}
    }
    assert outcome.value["host"] == "dh.example.com"
    assert outcome.value["port"] == 10000
    assert (
        outcome.value["tls"]["client_certificate"]["cert_chain"]
        == "-----BEGIN CERTIFICATE-----"
    )


def test_count_totals_every_redacted_value() -> None:
    """Both the credentials block and the literal private key count."""
    outcome = redact_raw(ConfigFileKind.COMMUNITY_SESSION, _session_file())
    assert outcome.count == 2


def test_scalar_secret_is_redacted() -> None:
    outcome = redact_raw(ConfigFileKind.COMMUNITY_SESSION, _session_file())
    assert outcome.value["tls"]["client_certificate"]["private_key"] == "[REDACTED]"


def test_input_is_not_mutated() -> None:
    data = _session_file()
    redact_raw(ConfigFileKind.COMMUNITY_SESSION, data)
    assert data["auth"]["credentials"]["token"] == "literal-token"
    assert (
        data["tls"]["client_certificate"]["private_key"]
        == "-----BEGIN PRIVATE KEY-----"
    )


def test_key_order_is_preserved() -> None:
    outcome = redact_raw(ConfigFileKind.COMMUNITY_SESSION, _session_file())
    assert list(outcome.value) == ["host", "port", "tls", "auth"]


# ---------------------------------------------------------------------------
# templating references
# ---------------------------------------------------------------------------


def test_templating_ref_at_a_scalar_secret_survives() -> None:
    """``${env:VAR}`` names the secret rather than being it, so it is
    shown as written and not counted."""
    outcome = redact_raw(
        ConfigFileKind.SERVER, {"psk": "${env:DH_PSK}"}, at=FieldPath.ROOT
    )
    assert outcome.value == {"psk": "${env:DH_PSK}"}
    assert outcome.count == 0


def test_literal_at_a_scalar_secret_is_redacted() -> None:
    outcome = redact_raw(ConfigFileKind.SERVER, {"psk": "hunter2"})
    assert outcome.value == {"psk": "[REDACTED]"}
    assert outcome.count == 1


def test_ref_embedded_in_a_larger_string_is_redacted() -> None:
    """Only a *lone* placeholder discloses nothing; concatenated text
    may carry literal secret material alongside it."""
    outcome = redact_raw(ConfigFileKind.SERVER, {"psk": "prefix-${env:DH_PSK}"})
    assert outcome.value == {"psk": "[REDACTED]"}
    assert outcome.count == 1


def test_defaulted_ref_has_its_fallback_redacted() -> None:
    """A ``:-`` fallback is a literal secret living inside an otherwise
    safe reference. The variable name stays legible; the literal does
    not, and it counts so --reveal-secrets reports it."""
    outcome = redact_raw(ConfigFileKind.SERVER, {"psk": "${env:DH_PSK:-hunter2}"})
    assert outcome.value == {"psk": "${env:DH_PSK:-[REDACTED]}"}
    assert outcome.count == 1
    assert "hunter2" not in str(outcome.value)


def test_defaulted_ref_with_an_empty_fallback_survives() -> None:
    """An empty fallback resolves to the empty string, so there is no
    literal to hide and nothing to count."""
    outcome = redact_raw(ConfigFileKind.SERVER, {"psk": "${env:DH_PSK:-}"})
    assert outcome.value == {"psk": "${env:DH_PSK:-}"}
    assert outcome.count == 0


def test_defaulted_ref_inside_a_secret_block_is_redacted() -> None:
    """The fallback rule applies inside a credentials block too."""
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {"auth": {"credentials": {"type": "psk", "token": "${env:T:-s3cret}"}}},
    )
    assert outcome.value["auth"]["credentials"] == {
        "type": "psk",
        "token": "${env:T:-[REDACTED]}",
    }
    assert outcome.count == 1


def test_ref_inside_a_secret_block_survives() -> None:
    """The placeholder rule applies inside a block too, so a properly
    externalized credential stays fully readable."""
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {"auth": {"credentials": {"type": "psk", "token": "${env:DH_PSK}"}}},
    )
    assert outcome.value["auth"]["credentials"] == {
        "type": "psk",
        "token": "${env:DH_PSK}",
    }
    assert outcome.count == 0


def test_anonymous_block_is_untouched_and_uncounted() -> None:
    """An anonymous credentials block holds no secret, so redacting it
    would hide a diagnostic and make --reveal-secrets cry wolf."""
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {"auth": {"credentials": {"type": "anonymous"}}},
    )
    assert outcome.value["auth"]["credentials"] == {"type": "anonymous"}
    assert outcome.count == 0


def test_username_beside_a_password_is_redacted() -> None:
    """Only ``type`` is exempt; every other value in the block goes."""
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {
            "auth": {
                "credentials": {
                    "type": "basic",
                    "username": "admin",
                    "password": "hunter2",
                }
            }
        },
    )
    assert outcome.value["auth"]["credentials"] == {
        "type": "basic",
        "username": "[REDACTED]",
        "password": "[REDACTED]",
    }
    assert outcome.count == 2


# ---------------------------------------------------------------------------
# subtree addressing
# ---------------------------------------------------------------------------


def test_subtree_redaction_uses_the_at_path() -> None:
    """``config get community.sessions.x.auth`` hands the redactor a
    subtree, which only resolves correctly with ``at`` applied."""
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {"credentials": {"type": "psk", "token": "literal"}},
        at=FieldPath(("auth",)),
    )
    assert outcome.value == {"credentials": {"type": "psk", "token": "[REDACTED]"}}
    assert outcome.count == 1


def test_at_the_secret_itself_scrubs_its_values() -> None:
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {"type": "psk", "token": "literal"},
        at=_CREDENTIALS,
    )
    assert outcome.value == {"type": "psk", "token": "[REDACTED]"}
    assert outcome.count == 1


def test_list_inside_a_secret_field_is_scrubbed_elementwise() -> None:
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION,
        {"type": "custom", "tokens": ["a", "${env:B}"]},
        at=_CREDENTIALS,
    )
    assert outcome.value == {"type": "custom", "tokens": ["[REDACTED]", "${env:B}"]}
    assert outcome.count == 1


def test_at_a_path_below_a_secret_point_is_still_redacted() -> None:
    """``config get ...auth.credentials.token`` addresses *inside* a
    secret block; it must not escape redaction by being deeper than the
    point.
    """
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION, "literal", at=_CREDENTIALS + "token"
    )
    assert outcome.value == "[REDACTED]"
    assert outcome.count == 1


def test_at_a_path_below_a_secret_point_keeps_a_ref_legible() -> None:
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION, "${env:DH_PSK}", at=_CREDENTIALS + "token"
    )
    assert outcome.value == "${env:DH_PSK}"
    assert outcome.count == 0


def test_at_a_scalar_secret_redacts_the_bare_value() -> None:
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION, "-----BEGIN PRIVATE KEY-----", at=_PRIVATE_KEY
    )
    assert outcome.value == "[REDACTED]"
    assert outcome.count == 1


def test_at_a_non_secret_path_changes_nothing() -> None:
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION, "dh.example.com", at=FieldPath(("host",))
    )
    assert outcome.value == "dh.example.com"
    assert outcome.count == 0


# ---------------------------------------------------------------------------
# scope of the discriminator exemption
# ---------------------------------------------------------------------------


def test_nested_type_key_inside_a_secret_block_is_scrubbed() -> None:
    """The exemption covers the flagged field's own mapping, not every
    mapping beneath it. This runs on unvalidated data, where a deeper
    key named ``type`` need not be a discriminator -- exempting it
    everywhere printed a literal secret verbatim.
    """
    outcome = redact_raw(
        ConfigFileKind.ENTERPRISE_SYSTEM,
        {
            "auth": {
                "credentials": {
                    "type": "custom",
                    "extra": {"type": "literal-secret"},
                }
            }
        },
    )
    credentials = outcome.value["auth"]["credentials"]
    assert credentials["type"] == "custom"
    assert credentials["extra"] == {"type": "[REDACTED]"}
    assert outcome.count == 1


def test_nested_type_key_is_scrubbed_inside_a_list() -> None:
    """A list at the point carries the point's own values, so its
    elements keep the exemption while their nested mappings do not."""
    outcome = redact_raw(
        ConfigFileKind.ENTERPRISE_SYSTEM,
        {"auth": {"credentials": [{"type": "psk", "inner": {"type": "s3cret"}}]}},
    )
    entry = outcome.value["auth"]["credentials"][0]
    assert entry["type"] == "psk"
    assert entry["inner"] == {"type": "[REDACTED]"}
    assert outcome.count == 1


def test_directly_addressed_discriminator_is_not_redacted() -> None:
    """``config get ...credentials.type`` must agree with the value the
    whole-block view shows, and must not warn of a disclosure."""
    outcome = redact_raw(
        ConfigFileKind.ENTERPRISE_SYSTEM,
        "psk",
        at=_CREDENTIALS + _DISCRIMINATOR,
    )
    assert outcome.value == "psk"
    assert outcome.count == 0


def test_directly_addressed_discriminator_agrees_with_the_block_view() -> None:
    """The two routes to the same field are the contract; pin them
    together so neither can drift."""
    block = redact_raw(
        ConfigFileKind.ENTERPRISE_SYSTEM,
        {"type": "psk", "token": "literal"},
        at=_CREDENTIALS,
    )
    direct = redact_raw(
        ConfigFileKind.ENTERPRISE_SYSTEM, "psk", at=_CREDENTIALS + _DISCRIMINATOR
    )
    assert direct.value == block.value["type"]


def test_a_sibling_of_the_discriminator_is_still_redacted() -> None:
    """The exemption is keyed to the discriminator name, not to being
    addressed directly -- a real secret leaf stays covered."""
    outcome = redact_raw(
        ConfigFileKind.ENTERPRISE_SYSTEM, "literal", at=_CREDENTIALS + "token"
    )
    assert outcome.value == "[REDACTED]"
    assert outcome.count == 1


# ---------------------------------------------------------------------------
# values the schema does not describe
# ---------------------------------------------------------------------------


def test_undeclared_keys_are_left_verbatim() -> None:
    """An unknown key cannot be a schema secret, and surfacing it is
    exactly why someone runs ``config get`` on a broken tree."""
    outcome = redact_raw(
        ConfigFileKind.COMMUNITY_SESSION, {"typo_host": "x", "nested": {"deep": 1}}
    )
    assert outcome.value == {"typo_host": "x", "nested": {"deep": 1}}
    assert outcome.count == 0


def test_non_string_at_a_secret_point_is_redacted() -> None:
    """Invalid data still discloses whatever it holds."""
    outcome = redact_raw(ConfigFileKind.SERVER, {"psk": 12345})
    assert outcome.value == {"psk": "[REDACTED]"}
    assert outcome.count == 1


def test_null_at_a_secret_point_is_redacted() -> None:
    outcome = redact_raw(ConfigFileKind.SERVER, {"psk": None})
    assert outcome.value == {"psk": "[REDACTED]"}
    assert outcome.count == 1


def test_list_values_pass_through() -> None:
    """A list is opaque to the field inventory, so any secret inside one
    is flagged at the list's own path rather than within it."""
    outcome = redact_raw(
        ConfigFileKind.SERVER, {"server_name": "s", "unknown_list": [{"a": 1}, "b"]}
    )
    assert outcome.value == {"server_name": "s", "unknown_list": [{"a": 1}, "b"]}
    assert outcome.count == 0


def test_scalar_root_passes_through() -> None:
    outcome = redact_raw(ConfigFileKind.SERVER, "not-a-mapping")
    assert outcome.value == "not-a-mapping"
    assert outcome.count == 0


def test_empty_mapping_passes_through() -> None:
    outcome = redact_raw(ConfigFileKind.SERVER, {})
    assert outcome.value == {}
    assert outcome.count == 0
