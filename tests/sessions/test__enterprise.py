"""Tests for ``deephaven_mcp.sessions._enterprise``.

Covers the Pydantic schema and runtime models for the enterprise
system declaration value type: top-level field validation, optional
``session_creation`` sub-schema, redaction, and the default-filled
timer fields. The :class:`EnterpriseConfig` umbrella is tested
separately in ``tests/config/test__enterprise.py``.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.credentials import PasswordCredentials
from deephaven_mcp.sessions import (
    EnterpriseSessionCreationDefaults,
    EnterpriseSystemConfig,
)

_BASE_CREDENTIALS = {
    "type": "password",
    "username": "alice",
    "password": "shh",
}


def _password_payload(**overrides) -> dict:
    payload = {
        "name": "prod",
        "system_name": "prod",
        "connection_json_url": "https://example.com/iris/connection.json",
        "auth": {"credentials": dict(_BASE_CREDENTIALS)},
    }
    payload.update(overrides)
    return payload


# ---------------------------------------------------------------------------
# Top-level validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("invalid", ["python", "GROOVY", "rust"])
def test_defaults_programming_language_rejects_non_members(invalid: str):
    """The vocabulary is exact-case: wrong case and unknown values both fail."""
    with pytest.raises(ValidationError):
        EnterpriseSessionCreationDefaults.model_validate(
            {"programming_language": invalid}
        )


def test_non_dict_input_passes_through_to_pydantic():
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate("not-a-dict")


def test_construct_minimal_password_config():
    cfg = EnterpriseSystemConfig.model_validate(_password_payload())
    assert cfg.name == "prod"
    assert cfg.connection_json_url.startswith("https://")
    assert isinstance(cfg.auth.credentials, PasswordCredentials)
    assert cfg.auth.credentials.username == "alice"
    assert cfg.auth.credentials.password.get_secret_value() == "shh"


def test_rejects_missing_connection_json_url():
    payload = _password_payload()
    del payload["connection_json_url"]
    with pytest.raises(ValidationError, match="connection_json_url"):
        EnterpriseSystemConfig.model_validate(payload)


def test_rejects_unknown_top_level_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSystemConfig.model_validate(_password_payload(weird=1))


def test_system_name_mismatch_rejected():
    with pytest.raises(ValidationError, match="does not match"):
        EnterpriseSystemConfig.model_validate(_password_payload(system_name="other"))


def test_system_name_optional_when_matching():
    payload = _password_payload()
    del payload["system_name"]
    cfg = EnterpriseSystemConfig.model_validate(payload)
    assert cfg.name == "prod"


def test_optional_positive_timer_rejected():
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate(
            _password_payload(connection_timeout_seconds=0)
        )


def test_rejects_top_level_tls_block():
    """Enterprise has no per-system TLS knob; ``tls`` is a hard error."""
    with pytest.raises(ValidationError, match="'tls' is not supported"):
        EnterpriseSystemConfig.model_validate(
            _password_payload(tls={"root_certs": "CA"})
        )


def test_requires_name():
    payload = _password_payload()
    del payload["name"]
    with pytest.raises(ValidationError, match="'name'"):
        EnterpriseSystemConfig.model_validate(payload)


# ---------------------------------------------------------------------------
# Auth delegation
# ---------------------------------------------------------------------------


def test_auth_requires_credentials():
    payload = _password_payload()
    payload["auth"] = {}
    with pytest.raises(ValidationError, match="credentials"):
        EnterpriseSystemConfig.model_validate(payload)


def test_auth_block_required():
    payload = _password_payload()
    del payload["auth"]
    with pytest.raises(ValidationError, match="auth"):
        EnterpriseSystemConfig.model_validate(payload)


def test_auth_rejects_extra_keys():
    payload = _password_payload()
    payload["auth"]["extra"] = 1
    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSystemConfig.model_validate(payload)


def test_auth_credentials_rejects_unknown_type():
    payload = _password_payload()
    payload["auth"]["credentials"]["type"] = "magic"
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate(payload)


def test_model_dump_round_trip():
    """model_dump emits the wire shape, which re-validates to an equal model."""
    original = EnterpriseSystemConfig.model_validate(_password_payload())
    dumped = original.model_dump(mode="json", context={"reveal": True})
    rebuilt = EnterpriseSystemConfig.model_validate(dumped)
    assert rebuilt == original


def test_rejects_top_level_credentials():
    """Credentials live only inside the ``auth`` block."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSystemConfig.model_validate(
            {
                "name": "prod",
                "connection_json_url": "https://x/",
                "auth": {"credentials": dict(_BASE_CREDENTIALS)},
                "credentials": dict(_BASE_CREDENTIALS),
            }
        )


# ---------------------------------------------------------------------------
# session_creation
# ---------------------------------------------------------------------------


def test_session_creation_rejects_unknown_field():
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate(
            _password_payload(session_creation={"weird": 1})
        )


def test_session_creation_defaults_block_is_optional():
    """``session_creation`` accepts an empty body; ``defaults`` is now
    default-constructed and ``heap_size_gb`` carries a schema default."""
    cfg = EnterpriseSystemConfig.model_validate(
        _password_payload(session_creation={"max_concurrent_sessions": 5})
    )
    assert cfg.session_creation is not None
    assert cfg.session_creation.defaults.heap_size_gb == 4.0


def test_session_creation_defaults_have_default_heap_size_gb():
    """``heap_size_gb`` defaults to ``4.0`` (mirrors the community side)."""
    cfg = EnterpriseSystemConfig.model_validate(
        _password_payload(session_creation={"defaults": {}})
    )
    assert cfg.session_creation is not None
    assert cfg.session_creation.defaults.heap_size_gb == 4.0


def test_session_creation_defaults_reject_unknown_field():
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate(
            _password_payload(
                session_creation={"defaults": {"heap_size_gb": 4, "weird_field": 1}}
            )
        )


def test_session_creation_max_must_be_positive():
    """``max_concurrent_sessions`` must be ≥1 when set; ``None`` disables."""
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate(
            _password_payload(
                session_creation={
                    "max_concurrent_sessions": 0,
                    "defaults": {"heap_size_gb": 4},
                }
            )
        )
    with pytest.raises(ValidationError):
        EnterpriseSystemConfig.model_validate(
            _password_payload(
                session_creation={
                    "max_concurrent_sessions": -1,
                    "defaults": {"heap_size_gb": 4},
                }
            )
        )


def test_session_creation_max_accepts_null():
    """``None`` disables the cap (unbounded)."""
    cfg = EnterpriseSystemConfig.model_validate(
        _password_payload(
            session_creation={
                "max_concurrent_sessions": None,
                "defaults": {"heap_size_gb": 4},
            }
        )
    )
    assert cfg.session_creation is not None
    assert cfg.session_creation.max_concurrent_sessions is None


def test_session_creation_accepts_full_valid_block():
    cfg = EnterpriseSystemConfig.model_validate(
        _password_payload(
            session_creation={
                "max_concurrent_sessions": 5,
                "defaults": {
                    "heap_size_gb": 4,
                    "auto_delete_timeout": 600,
                    "server": "primary",
                    "engine": "DeephavenEnterprise",
                    "extra_jvm_args": ["-Xmx2g"],
                    "environment_vars": {"X": "1"},
                    "admin_groups": ["admins"],
                    "viewer_groups": ["viewers"],
                    "session_arguments": {"k": "v"},
                    "programming_language": "Python",
                },
            }
        )
    )
    assert cfg.session_creation is not None
    assert cfg.session_creation.max_concurrent_sessions == 5
    assert cfg.session_creation.defaults.heap_size_gb == 4
    assert cfg.session_creation.defaults.environment_vars == {"X": "1"}
    assert cfg.session_creation.defaults.engine == "DeephavenEnterprise"


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_redacted_dump_replaces_password():
    cfg = EnterpriseSystemConfig.model_validate(_password_payload())
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["auth"]["credentials"]["password"] == REDACTED
    assert "shh" not in str(out)


def test_redacted_dump_keeps_username():
    cfg = EnterpriseSystemConfig.model_validate(_password_payload())
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["auth"]["credentials"]["username"] == "alice"


def test_redacted_dump_password_is_redacted():
    # Env-var indirection is handled by the templating engine at
    # file-load time; the typed model carries the already-resolved
    # password literal which is then redacted on dump.
    cfg = EnterpriseSystemConfig.model_validate(
        _password_payload(
            auth={
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "from-env",
                }
            }
        )
    )
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["auth"]["credentials"]["password"] == REDACTED
    assert "from-env" not in str(out)


# ---------------------------------------------------------------------------
# Timer helpers
# ---------------------------------------------------------------------------


def test_legacy_per_system_connection_timeout_rejected():
    """``connection_timeout_seconds`` was retired; the global
    ``timeouts.session_connect_timeout_seconds`` on
    ``enterprise/settings.json`` is the only knob."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSystemConfig.model_validate(
            _password_payload(connection_timeout_seconds=42)
        )


def test_legacy_per_system_session_idle_timeout_rejected():
    """``session_idle_timeout_seconds`` is now system-wide on EnterpriseSettings."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSystemConfig.model_validate(
            _password_payload(session_idle_timeout_seconds=60)
        )


def test_legacy_per_system_session_idle_sweep_rejected():
    """``session_idle_sweep_interval_seconds`` is now system-wide."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSystemConfig.model_validate(
            _password_payload(session_idle_sweep_interval_seconds=5)
        )


# Note: ``EnterpriseConfig`` umbrella tests live in
# ``tests/config/test__enterprise.py`` alongside its community sibling;
# this file is dedicated to the ``EnterpriseSystemConfig`` declaration
# (and its nested session-creation sub-models) that the umbrella
# aggregates.
