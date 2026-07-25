"""Tests for ``deephaven_mcp.sessions._auth``."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.credentials import (
    AnonymousCredentials,
    PSKCredentials,
)
from deephaven_mcp.sessions import AuthConfig


def test_dispatches_credentials_union_on_type():
    cfg = AuthConfig.model_validate({"credentials": {"type": "anonymous"}})
    assert isinstance(cfg.credentials, AnonymousCredentials)
    cfg = AuthConfig.model_validate({"credentials": {"type": "psk", "token": "t"}})
    assert isinstance(cfg.credentials, PSKCredentials)


def test_credentials_required():
    with pytest.raises(ValidationError, match="credentials"):
        AuthConfig.model_validate({})


def test_rejects_unknown_keys():
    with pytest.raises(ValidationError, match="Extra inputs"):
        AuthConfig.model_validate({"credentials": {"type": "anonymous"}, "extra": 1})


def test_rejects_unknown_credential_type():
    with pytest.raises(ValidationError):
        AuthConfig.model_validate({"credentials": {"type": "bogus"}})


def test_redacted_dump_masks_secret():
    cfg = AuthConfig.model_validate({"credentials": {"type": "psk", "token": "shh"}})
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["credentials"]["token"] == REDACTED
    assert "shh" not in str(out)


def test_model_dump_round_trip():
    original = AuthConfig.model_validate({"credentials": {"type": "psk", "token": "t"}})
    dumped = original.model_dump(mode="json", context={"reveal": True})
    assert AuthConfig.model_validate(dumped) == original
