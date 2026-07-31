"""Tests for :mod:`deephaven_mcp.config.schema._enterprise`.

Covers the (currently empty) :class:`EnterpriseSettings` placeholder
schema and the :class:`EnterpriseConfig` umbrella that aggregates
settings + per-system declarations.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp.config.schema import EnterpriseConfig, EnterpriseSettings
from deephaven_mcp.sessions import EnterpriseSystemConfig

# ---------------------------------------------------------------------------
# EnterpriseSettings (reserved placeholder)
# ---------------------------------------------------------------------------


def test_enterprise_settings_accepts_empty_dict():
    cfg = EnterpriseSettings.model_validate({})
    assert isinstance(cfg, EnterpriseSettings)


def test_enterprise_settings_rejects_any_keys():
    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseSettings.model_validate({"future_feature": True})


def test_enterprise_settings_is_frozen():
    cfg = EnterpriseSettings.model_validate({})
    # Has no fields, so just verify the class is a frozen model.
    assert cfg.model_config["frozen"] is True


# ---------------------------------------------------------------------------
# EnterpriseConfig umbrella
# ---------------------------------------------------------------------------


def _system() -> EnterpriseSystemConfig:
    return EnterpriseSystemConfig.model_validate(
        {
            "name": "prod",
            "system_name": "prod",
            "connection_json_url": "https://dhe.example.com/iris/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "shh",
                }
            },
        }
    )


def test_enterprise_config_holds_systems() -> None:
    """The umbrella exposes per-system declarations via ``systems``."""
    sys_cfg = _system()
    cfg = EnterpriseConfig(settings=EnterpriseSettings(), systems={"prod": sys_cfg})
    assert cfg.systems["prod"] is sys_cfg
