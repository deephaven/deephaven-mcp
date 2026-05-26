"""Tests for :mod:`deephaven_mcp.client._timeouts`.

The client module owns only ``CommunityClientTimeouts`` and
``EnterpriseClientTimeouts``. The umbrella ``CommunityTimeouts`` /
``EnterpriseTimeouts`` schemas live in
:mod:`deephaven_mcp.mcp_systems_server.config` (tested in
``tests/mcp_systems_server/config/``) and the eviction sub-block lives
with its consumer in :mod:`deephaven_mcp.resource_manager._evictor`
(tested in ``tests/resource_manager/test__evictor.py``).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp.client._timeouts import (
    CommunityClientTimeouts,
    EnterpriseClientTimeouts,
)

# ---------------------------------------------------------------------------
# CommunityClientTimeouts
# ---------------------------------------------------------------------------


def test_community_client_default_values():
    """The community client schema has one field with a schema-level default."""
    cfg = CommunityClientTimeouts()
    assert cfg.session_connect_timeout_seconds == 60.0


def test_community_client_model_fields_set():
    assert set(CommunityClientTimeouts.model_fields) == {
        "session_connect_timeout_seconds"
    }


def test_community_client_validate_block():
    cfg = CommunityClientTimeouts.model_validate(
        {"session_connect_timeout_seconds": 1.0}
    )
    assert cfg.session_connect_timeout_seconds == 1.0


def test_community_client_rejects_zero():
    with pytest.raises(ValidationError):
        CommunityClientTimeouts.model_validate({"session_connect_timeout_seconds": 0})


def test_community_client_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunityClientTimeouts.model_validate({"bogus": 1})


def test_community_client_rejects_enterprise_field():
    """Enterprise-only fields must not be accepted by the community client schema."""
    with pytest.raises(ValidationError, match="Extra inputs"):
        CommunityClientTimeouts.model_validate(
            {"worker_creation_timeout_seconds": 30.0}
        )


def test_community_client_frozen():
    cfg = CommunityClientTimeouts()
    with pytest.raises(ValidationError):
        cfg.session_connect_timeout_seconds = 99.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# EnterpriseClientTimeouts
# ---------------------------------------------------------------------------


def test_enterprise_client_default_values():
    """Every field has a schema-level default; the empty model is valid."""
    cfg = EnterpriseClientTimeouts()
    assert cfg.session_connect_timeout_seconds == 60.0
    assert cfg.subscribe_timeout_seconds == 30.0
    assert cfg.pq_connection_timeout_seconds == 60.0
    assert cfg.worker_creation_timeout_seconds == 60.0
    assert cfg.auth_timeout_seconds == 60.0
    assert cfg.saml_auth_timeout_seconds == 120.0
    assert cfg.pq_management_timeout_seconds == 60.0
    assert cfg.quick_operation_timeout_seconds == 5.0
    assert cfg.pq_state_change_timeout_seconds == 120
    assert cfg.no_wait_seconds == 0.0


def test_enterprise_client_model_fields_set():
    assert set(EnterpriseClientTimeouts.model_fields) == {
        "session_connect_timeout_seconds",
        "worker_creation_timeout_seconds",
        "pq_connection_timeout_seconds",
        "auth_timeout_seconds",
        "saml_auth_timeout_seconds",
        "quick_operation_timeout_seconds",
        "subscribe_timeout_seconds",
        "pq_management_timeout_seconds",
        "pq_state_change_timeout_seconds",
        "no_wait_seconds",
    }


def test_enterprise_client_validate_full_block():
    cfg = EnterpriseClientTimeouts.model_validate(
        {
            "session_connect_timeout_seconds": 1.0,
            "subscribe_timeout_seconds": 2.0,
            "pq_connection_timeout_seconds": 3.0,
            "worker_creation_timeout_seconds": 4.0,
            "auth_timeout_seconds": 5.0,
            "saml_auth_timeout_seconds": 6.0,
            "pq_management_timeout_seconds": 7.0,
            "quick_operation_timeout_seconds": 8.0,
            "pq_state_change_timeout_seconds": 9,
            "no_wait_seconds": 0.0,
        }
    )
    assert cfg.session_connect_timeout_seconds == 1.0
    assert cfg.pq_state_change_timeout_seconds == 9


def test_enterprise_client_rejects_zero_for_positive_timeouts():
    with pytest.raises(ValidationError):
        EnterpriseClientTimeouts.model_validate({"session_connect_timeout_seconds": 0})


def test_enterprise_client_rejects_negative_no_wait_seconds():
    with pytest.raises(ValidationError):
        EnterpriseClientTimeouts.model_validate({"no_wait_seconds": -1.0})


def test_enterprise_client_pq_state_change_timeout_seconds_rejects_float():
    """The field is declared as ``int``; Pydantic coerces but rejects fractional floats."""
    with pytest.raises(ValidationError):
        EnterpriseClientTimeouts.model_validate(
            {"pq_state_change_timeout_seconds": 120.5}
        )


def test_enterprise_client_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        EnterpriseClientTimeouts.model_validate({"bogus": 1})


def test_enterprise_client_frozen():
    cfg = EnterpriseClientTimeouts()
    with pytest.raises(ValidationError):
        cfg.session_connect_timeout_seconds = 99.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Cross-schema invariants
# ---------------------------------------------------------------------------


def test_independent_client_schemas():
    """The two client schemas are independent (not in an inheritance relationship)."""
    assert not issubclass(CommunityClientTimeouts, EnterpriseClientTimeouts)
    assert not issubclass(EnterpriseClientTimeouts, CommunityClientTimeouts)
