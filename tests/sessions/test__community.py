"""Tests for ``deephaven_mcp.config._community_sessions``.

Covers Pydantic-based validation, redaction, and runtime model
behavior for ``community/sessions/<name>.json`` files. Auth-specific
shapes are covered in ``tests/auth/credentials/test__credentials.py``;
here we exercise the per-session wrapper, ``session_name`` cross-check,
and round-trip behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.credentials import (
    AnonymousCredentials,
    PSKCredentials,
)
from deephaven_mcp.sessions import CommunitySessionConfig

_ANON_AUTH = {"credentials": {"type": "anonymous"}}


def _session_payload(**overrides) -> dict:
    payload: dict = {"name": "local", "auth": dict(_ANON_AUTH)}
    payload.update(overrides)
    return payload


# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------


def test_non_dict_input_passes_through_to_pydantic():
    with pytest.raises(ValidationError):
        CommunitySessionConfig.model_validate("not-a-dict")


def test_minimal_anonymous_session_construct():
    cfg = CommunitySessionConfig.model_validate(_session_payload())
    assert cfg.name == "local"
    assert isinstance(cfg.credentials, AnonymousCredentials)
    assert cfg.tls is None
    assert cfg.host is None
    assert cfg.port is None
    assert cfg.programming_language is None
    assert cfg.never_timeout is None


def test_construct_psk_session_with_literal_token():
    # Env-var indirection is handled by the templating engine at
    # file-load time; the model itself receives the literal value.
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(
            auth={"credentials": {"type": "psk", "token": "from-env"}},
            host="localhost",
            port=10000,
            programming_language="Python",
            never_timeout=True,
        )
    )
    assert isinstance(cfg.credentials, PSKCredentials)
    assert cfg.credentials.token.get_secret_value() == "from-env"
    assert cfg.host == "localhost"
    assert cfg.port == 10000


def test_programming_language_lowercase_normalized():
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(programming_language="python")
    )
    assert cfg.programming_language == "Python"


def test_programming_language_uppercase_normalized():
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(programming_language="GROOVY")
    )
    assert cfg.programming_language == "Groovy"


def test_programming_language_invalid_value_rejected():
    with pytest.raises(ValidationError):
        CommunitySessionConfig.model_validate(
            _session_payload(programming_language="scala")
        )


def test_legacy_session_type_rejected():
    """The old ``session_type`` field name is now an unknown field."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySessionConfig.model_validate(_session_payload(session_type="python"))


def test_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySessionConfig.model_validate(_session_payload(bad_field=1))


def test_requires_auth_block():
    with pytest.raises(ValidationError, match="'auth' is required"):
        CommunitySessionConfig.model_validate({"name": "local", "host": "localhost"})


def test_session_name_mismatch_rejected():
    with pytest.raises(ValidationError, match="does not match"):
        CommunitySessionConfig.model_validate(
            _session_payload(session_name="other-name")
        )


def test_session_name_match_accepted():
    cfg = CommunitySessionConfig.model_validate(_session_payload(session_name="local"))
    assert cfg.name == "local"


def test_requires_name():
    with pytest.raises(ValidationError, match="'name'"):
        CommunitySessionConfig.model_validate({"auth": _ANON_AUTH})


def test_rejects_unknown_credential_type():
    with pytest.raises(ValidationError):
        CommunitySessionConfig.model_validate(
            _session_payload(auth={"credentials": {"type": "bogus"}})
        )


def test_rejects_auth_with_extra_keys():
    with pytest.raises(ValidationError, match="only a 'credentials'"):
        CommunitySessionConfig.model_validate(
            _session_payload(auth={"credentials": {"type": "anonymous"}, "extra": 1})
        )


def test_rejects_auth_missing_credentials():
    with pytest.raises(ValidationError, match="'auth.credentials' is required"):
        CommunitySessionConfig.model_validate(_session_payload(auth={}))


def test_rejects_both_auth_and_top_level_credentials():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        CommunitySessionConfig.model_validate(
            {
                "name": "local",
                "auth": {"credentials": {"type": "anonymous"}},
                "credentials": {"type": "anonymous"},
            }
        )


def test_accepts_top_level_credentials_round_trip():
    """Round-trip via model_dump produces a top-level ``credentials`` key."""
    original = CommunitySessionConfig.model_validate(_session_payload())
    dumped = original.model_dump(mode="json", context={"reveal": True})
    # Reconstruct from the dumped shape (which has no ``auth`` wrapper).
    rebuilt = CommunitySessionConfig.model_validate(dumped)
    assert rebuilt == original


def test_legacy_tls_root_certs_rejected():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySessionConfig.model_validate(
            _session_payload(tls_root_certs="/etc/ssl/ca.pem")
        )


def test_legacy_use_tls_rejected():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySessionConfig.model_validate(_session_payload(use_tls=True))


# ---------------------------------------------------------------------------
# TLS sub-block
# ---------------------------------------------------------------------------


def test_accepts_empty_tls_block():
    cfg = CommunitySessionConfig.model_validate(_session_payload(tls={}))
    assert cfg.tls is not None
    assert cfg.tls.root_certs is None
    assert cfg.tls.client_certificate is None


def test_accepts_full_tls_block():
    # File indirection lives in the JSON as ``"${file:/path}"`` and is
    # resolved at file-load time; the typed model receives literal PEM
    # text.
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(
            tls={
                "root_certs": "CA",
                "client_certificate": {
                    "cert_chain": "CHAIN",
                    "private_key": "KEY",
                },
            }
        )
    )
    assert cfg.tls is not None
    assert cfg.tls.root_certs == "CA"
    assert cfg.tls.client_certificate is not None
    assert cfg.tls.client_certificate.cert_chain == "CHAIN"
    assert cfg.tls.client_certificate.private_key.get_secret_value() == "KEY"


def test_rejects_partial_client_certificate():
    """``client_certificate`` requires both halves."""
    with pytest.raises(ValidationError):
        CommunitySessionConfig.model_validate(
            _session_payload(tls={"client_certificate": {"cert_chain": "CHAIN"}})
        )


# ---------------------------------------------------------------------------
# Redaction & repr
# ---------------------------------------------------------------------------


def test_redacted_dump_replaces_secret_token():
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(auth={"credentials": {"type": "psk", "token": "shh"}})
    )
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["credentials"]["token"] == REDACTED
    assert "shh" not in str(out)


def test_redacted_dump_no_auth_block_present():
    """Round-trip dump uses the unwrapped ``credentials`` key, not ``auth``."""
    cfg = CommunitySessionConfig.model_validate(_session_payload())
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert "auth" not in out
    assert out["credentials"] == {"type": "anonymous"}


def test_redacted_dump_keeps_tls_root_certs_visible():
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(tls={"root_certs": "CA"})
    )
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["tls"]["root_certs"] == "CA"


def test_repr_masks_secret_token():
    cfg = CommunitySessionConfig.model_validate(
        _session_payload(auth={"credentials": {"type": "psk", "token": "shh"}})
    )
    assert "shh" not in repr(cfg)
