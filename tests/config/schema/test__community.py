"""Tests for :mod:`deephaven_mcp.config.schema._community`.

Covers both the :class:`CommunitySettings` Pydantic schema for
``community/settings.json`` (with its nested ``security`` and
``session_creation`` sub-models) and the :class:`CommunityConfig`
umbrella that aggregates settings + per-session declarations.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.credentials import AnonymousCredentials, PSKCredentials
from deephaven_mcp.config.schema import (
    CommunityConfig,
    CommunitySettings,
)
from deephaven_mcp.sessions import CommunitySessionConfig


def _settings_with_defaults(defaults: dict) -> dict:
    return {"session_creation": {"defaults": defaults}}


# ---------------------------------------------------------------------------
# Top-level CommunitySettings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("invalid", ["python", "GROOVY", "rust"])
def test_defaults_programming_language_rejects_non_members(invalid: str):
    """The vocabulary is exact-case: wrong case and unknown values both fail."""
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            _settings_with_defaults({"programming_language": invalid})
        )


def test_defaults_non_dict_input_passes_through_to_pydantic():
    """The mode='before' validator on defaults returns non-dicts unchanged."""
    with pytest.raises(ValidationError):
        # Force the defaults model to see a non-dict input.
        CommunitySettings.model_validate(
            {"session_creation": {"defaults": "not-a-dict"}}
        )


def test_settings_empty_dict_is_valid():
    cfg = CommunitySettings.model_validate({})
    assert cfg.security is None
    assert cfg.session_creation is None
    # Timer fields carry schema defaults when JSON omits them.
    assert cfg.timeouts.eviction.session_idle_timeout_seconds == 3600.0
    assert cfg.timeouts.eviction.sweep_interval_seconds == 60.0


def test_settings_rejects_unknown_top_level_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate({"unknown_key": 1})


def test_settings_idle_timer_must_be_positive():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            {"timeouts": {"eviction": {"session_idle_timeout_seconds": 0}}}
        )


def test_settings_sweep_interval_must_be_positive():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            {"timeouts": {"eviction": {"sweep_interval_seconds": -1}}}
        )


def test_settings_naked_idle_timeout_at_top_level_rejected():
    """Pre-reorganization shape (naked top-level field) must surface a clear error."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate({"session_idle_timeout_seconds": 1800})


def test_settings_accepts_full_valid_block():
    cfg = CommunitySettings.model_validate(
        {
            "security": {"credential_retrieval_mode": "dynamic_only"},
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "launch_method": "docker",
                    "auth": {"credentials": {"type": "anonymous"}},
                    "docker": {
                        "images": {
                            "python": "deephaven/server:latest",
                            "groovy": "deephaven/server-slim:latest",
                        },
                        "memory_limit_gb": 8,
                        "cpu_limit": 2.5,
                        "volumes": ["/a:/b"],
                    },
                    "python": {"venv_path": "/opt/venvs/dh"},
                    "extra_jvm_args": ["-Xmx2g"],
                    "environment_vars": {"JAVA_OPTS": "-Xmx2g"},
                    "heap_size_gb": 4,
                    "startup_timeout_seconds": 30,
                    "startup_check_interval_seconds": 1,
                    "startup_retries": 3,
                },
            },
            "timeouts": {
                "eviction": {
                    "session_idle_timeout_seconds": 1800,
                    "sweep_interval_seconds": 30,
                },
            },
        }
    )
    assert cfg.timeouts.eviction.session_idle_timeout_seconds == 1800
    assert cfg.session_creation is not None
    defaults = cfg.session_creation.defaults
    assert defaults is not None
    assert defaults.heap_size_gb == 4
    assert defaults.docker.images.python == "deephaven/server:latest"
    assert defaults.docker.images.groovy == "deephaven/server-slim:latest"
    assert defaults.docker.memory_limit_gb == 8
    assert defaults.docker.cpu_limit == 2.5
    assert defaults.docker.volumes == ["/a:/b"]
    assert defaults.python.venv_path == "/opt/venvs/dh"


def test_defaults_default_constructed_blocks_have_default_images():
    """Operator omits the docker block entirely; per-language defaults still apply."""
    cfg = CommunitySettings.model_validate(
        {"session_creation": {"defaults": {"launch_method": "docker"}}}
    )
    assert cfg.session_creation is not None
    defaults = cfg.session_creation.defaults
    assert defaults.docker.images.python == "ghcr.io/deephaven/server:latest"
    assert defaults.docker.images.groovy == "ghcr.io/deephaven/server-slim:latest"
    assert defaults.docker.memory_limit_gb is None
    assert defaults.python.venv_path is None


def test_defaults_partial_images_override_keeps_other_default():
    """Setting only images.python preserves the schema default for groovy."""
    cfg = CommunitySettings.model_validate(
        {
            "session_creation": {
                "defaults": {
                    "docker": {"images": {"python": "myreg/dh:1.2.3"}},
                }
            }
        }
    )
    assert cfg.session_creation is not None
    defaults = cfg.session_creation.defaults
    assert defaults.docker.images.python == "myreg/dh:1.2.3"
    assert defaults.docker.images.groovy == "ghcr.io/deephaven/server-slim:latest"


def test_defaults_legacy_flat_docker_image_rejected():
    """The old flat ``docker_image`` field is now an unknown key."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate(
            {
                "session_creation": {
                    "defaults": {"docker_image": "deephaven/server:latest"}
                }
            }
        )


def test_defaults_legacy_flat_python_venv_path_rejected():
    """The old flat ``python_venv_path`` field is now an unknown key."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate(
            {"session_creation": {"defaults": {"python_venv_path": "/x"}}}
        )


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


def test_security_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate({"security": {"weird": 1}})


def test_security_rejects_bad_mode():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            {"security": {"credential_retrieval_mode": "nope"}}
        )


def test_security_accepts_valid_mode():
    cfg = CommunitySettings.model_validate(
        {"security": {"credential_retrieval_mode": "all"}}
    )
    assert cfg.security is not None
    assert cfg.security.credential_retrieval_mode == "all"


def test_security_mode_defaults_to_none_string():
    """``credential_retrieval_mode`` defaults to ``"none"`` when the
    ``security`` block is empty; the whole block can also be omitted,
    in which case ``security`` itself is ``None``.
    """
    cfg = CommunitySettings.model_validate({"security": {}})
    assert cfg.security is not None
    assert cfg.security.credential_retrieval_mode == "none"


def test_security_mode_rejects_null():
    """``credential_retrieval_mode`` no longer accepts ``None``; the
    field must be one of the literal strings, or the whole block
    omitted.
    """
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            {"security": {"credential_retrieval_mode": None}}
        )


# ---------------------------------------------------------------------------
# session_creation
# ---------------------------------------------------------------------------


def test_session_creation_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate({"session_creation": {"weird_field": True}})


def test_session_creation_max_must_be_non_negative():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            {"session_creation": {"max_concurrent_sessions": -1}}
        )


# ---------------------------------------------------------------------------
# session_creation.defaults
# ---------------------------------------------------------------------------


def test_defaults_reject_unknown_field():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(_settings_with_defaults({"weird": 1}))


def test_defaults_launch_method_rejected_if_unknown():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            _settings_with_defaults({"launch_method": "kubernetes"})
        )


def test_defaults_launch_method_accepts_known_value():
    cfg = CommunitySettings.model_validate(
        _settings_with_defaults({"launch_method": "python"})
    )
    assert cfg.session_creation is not None
    assert cfg.session_creation.defaults is not None
    assert cfg.session_creation.defaults.launch_method == "python"


def test_defaults_auth_block_dispatches_to_credentials_union():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            _settings_with_defaults({"auth": {"credentials": {"type": "bogus"}}})
        )


def test_defaults_auth_carries_typed_credentials():
    cfg = CommunitySettings.model_validate(
        _settings_with_defaults({"auth": {"credentials": {"type": "anonymous"}}})
    )
    assert cfg.session_creation is not None
    assert cfg.session_creation.defaults is not None
    assert cfg.session_creation.defaults.auth is not None
    assert isinstance(
        cfg.session_creation.defaults.auth.credentials, AnonymousCredentials
    )


def test_defaults_auth_accepts_literal_psk_token():
    # Env-var indirection is handled by the templating engine at
    # file-load time; the model receives the already-resolved token.
    cfg = CommunitySettings.model_validate(
        _settings_with_defaults(
            {
                "auth": {
                    "credentials": {
                        "type": "psk",
                        "token": "from-env",
                    }
                }
            }
        )
    )
    creds = cfg.session_creation.defaults.auth.credentials  # type: ignore[union-attr]
    assert isinstance(creds, PSKCredentials)
    assert creds.token.get_secret_value() == "from-env"


def test_defaults_positive_number_rules():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(_settings_with_defaults({"heap_size_gb": 0}))


def test_defaults_startup_retries_must_be_non_negative():
    with pytest.raises(ValidationError):
        CommunitySettings.model_validate(
            _settings_with_defaults({"startup_retries": -1})
        )


def test_defaults_rejects_auth_with_extra_keys():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunitySettings.model_validate(
            _settings_with_defaults(
                {"auth": {"credentials": {"type": "anonymous"}, "extra": 1}}
            )
        )


def test_defaults_rejects_auth_missing_credentials():
    with pytest.raises(ValidationError, match="credentials"):
        CommunitySettings.model_validate(_settings_with_defaults({"auth": {}}))


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_redacted_dump_replaces_defaults_credentials_token():
    cfg = CommunitySettings.model_validate(
        _settings_with_defaults(
            {"auth": {"credentials": {"type": "psk", "token": "shh"}}}
        )
    )
    out = cfg.model_dump(mode="json", context={"redact": True})
    creds = out["session_creation"]["defaults"]["auth"]["credentials"]
    assert creds["token"] == REDACTED
    assert "shh" not in str(out)


# ---------------------------------------------------------------------------
# CommunityConfig umbrella
# ---------------------------------------------------------------------------


def test_community_config_timer_fields_use_defaults_when_unset() -> None:
    """With no JSON overrides, the umbrella's settings carry defaults."""
    cfg = CommunityConfig(settings=CommunitySettings(), sessions={})
    assert cfg.settings.timeouts.eviction.session_idle_timeout_seconds == 3600.0
    assert cfg.settings.timeouts.eviction.sweep_interval_seconds == 60.0


def test_community_config_timer_fields_use_configured_values() -> None:
    """Explicit JSON values override the project-wide defaults."""
    cfg = CommunityConfig(
        settings=CommunitySettings.model_validate(
            {
                "timeouts": {
                    "eviction": {
                        "session_idle_timeout_seconds": 60,
                        "sweep_interval_seconds": 5,
                    },
                },
            }
        ),
        sessions={},
    )
    assert cfg.settings.timeouts.eviction.session_idle_timeout_seconds == 60.0
    assert cfg.settings.timeouts.eviction.sweep_interval_seconds == 5.0


def test_community_config_holds_sessions() -> None:
    """The umbrella exposes per-session declarations via ``sessions``."""
    session = CommunitySessionConfig.model_validate(
        {"name": "local", "auth": {"credentials": {"type": "anonymous"}}}
    )
    cfg = CommunityConfig(settings=CommunitySettings(), sessions={"local": session})
    assert cfg.sessions["local"] is session
