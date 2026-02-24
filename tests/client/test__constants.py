"""Unit tests for _constants.py timeout constants."""

import importlib

import pytest

import deephaven_mcp.client._constants as _constants_module
from deephaven_mcp.client._constants import (
    AUTH_TIMEOUT_SECONDS,
    NO_WAIT_SECONDS,
    PQ_CONNECTION_TIMEOUT_SECONDS,
    PQ_MANAGEMENT_TIMEOUT_SECONDS,
    PQ_STATE_CHANGE_TIMEOUT_SECONDS,
    QUICK_OPERATION_TIMEOUT_SECONDS,
    SAML_AUTH_TIMEOUT_SECONDS,
    SESSION_CONNECT_TIMEOUT_SECONDS,
    SUBSCRIBE_TIMEOUT_SECONDS,
    WORKER_CREATION_TIMEOUT_SECONDS,
)


class TestTimeoutConstants:
    """Test that timeout constants are defined with correct types and reasonable values."""

    def test_connection_timeout_seconds_is_positive_float(self):
        """SESSION_CONNECT_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(SESSION_CONNECT_TIMEOUT_SECONDS, float)
        assert SESSION_CONNECT_TIMEOUT_SECONDS > 0

    def test_subscribe_timeout_seconds_is_positive_float(self):
        """SUBSCRIBE_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(SUBSCRIBE_TIMEOUT_SECONDS, float)
        assert SUBSCRIBE_TIMEOUT_SECONDS > 0

    def test_pq_connection_timeout_seconds_is_positive_float(self):
        """PQ_CONNECTION_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(PQ_CONNECTION_TIMEOUT_SECONDS, float)
        assert PQ_CONNECTION_TIMEOUT_SECONDS > 0

    def test_worker_creation_timeout_seconds_is_positive_float(self):
        """WORKER_CREATION_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(WORKER_CREATION_TIMEOUT_SECONDS, float)
        assert WORKER_CREATION_TIMEOUT_SECONDS > 0

    def test_auth_timeout_seconds_is_positive_float(self):
        """AUTH_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(AUTH_TIMEOUT_SECONDS, float)
        assert AUTH_TIMEOUT_SECONDS > 0

    def test_saml_auth_timeout_seconds_is_positive_float(self):
        """SAML_AUTH_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(SAML_AUTH_TIMEOUT_SECONDS, float)
        assert SAML_AUTH_TIMEOUT_SECONDS > 0

    def test_saml_timeout_longer_than_standard_auth(self):
        """SAML timeout should be longer than standard auth due to browser interaction."""
        assert SAML_AUTH_TIMEOUT_SECONDS > AUTH_TIMEOUT_SECONDS

    def test_pq_operation_timeout_seconds_is_positive_float(self):
        """PQ_MANAGEMENT_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(PQ_MANAGEMENT_TIMEOUT_SECONDS, float)
        assert PQ_MANAGEMENT_TIMEOUT_SECONDS > 0

    def test_pq_wait_timeout_seconds_is_positive_float(self):
        """PQ_STATE_CHANGE_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(PQ_STATE_CHANGE_TIMEOUT_SECONDS, float)
        assert PQ_STATE_CHANGE_TIMEOUT_SECONDS > 0

    def test_pq_wait_timeout_longer_than_operation_timeout(self):
        """PQ wait timeout should be longer since it waits for state changes."""
        assert PQ_STATE_CHANGE_TIMEOUT_SECONDS >= PQ_MANAGEMENT_TIMEOUT_SECONDS

    def test_quick_operation_timeout_seconds_is_positive_float(self):
        """QUICK_OPERATION_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(QUICK_OPERATION_TIMEOUT_SECONDS, float)
        assert QUICK_OPERATION_TIMEOUT_SECONDS > 0

    def test_quick_operation_timeout_shorter_than_connection_timeout(self):
        """Quick operation timeout should be shorter than connection timeout."""
        assert QUICK_OPERATION_TIMEOUT_SECONDS <= SESSION_CONNECT_TIMEOUT_SECONDS

    def test_no_wait_seconds_is_zero(self):
        """NO_WAIT_SECONDS should be zero (meaning return immediately in SDK)."""
        assert isinstance(NO_WAIT_SECONDS, float)
        assert NO_WAIT_SECONDS == 0.0


class TestTimeoutConstantsEnvVarOverrides:
    """Test that each constant can be overridden via its environment variable."""

    def _reload(self):
        importlib.reload(_constants_module)

    def test_connection_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS overrides SESSION_CONNECT_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS", "99.5")
        self._reload()
        assert _constants_module.SESSION_CONNECT_TIMEOUT_SECONDS == 99.5

    def test_subscribe_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS overrides SUBSCRIBE_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS", "15.0")
        self._reload()
        assert _constants_module.SUBSCRIBE_TIMEOUT_SECONDS == 15.0

    def test_pq_connection_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS overrides PQ_CONNECTION_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS", "45.0")
        self._reload()
        assert _constants_module.PQ_CONNECTION_TIMEOUT_SECONDS == 45.0

    def test_worker_creation_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS overrides WORKER_CREATION_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS", "120.0")
        self._reload()
        assert _constants_module.WORKER_CREATION_TIMEOUT_SECONDS == 120.0

    def test_auth_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_AUTH_TIMEOUT_SECONDS overrides AUTH_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_AUTH_TIMEOUT_SECONDS", "30.0")
        self._reload()
        assert _constants_module.AUTH_TIMEOUT_SECONDS == 30.0

    def test_saml_auth_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_SAML_AUTH_TIMEOUT_SECONDS overrides SAML_AUTH_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_SAML_AUTH_TIMEOUT_SECONDS", "300.0")
        self._reload()
        assert _constants_module.SAML_AUTH_TIMEOUT_SECONDS == 300.0

    def test_pq_operation_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS overrides PQ_MANAGEMENT_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS", "90.0")
        self._reload()
        assert _constants_module.PQ_MANAGEMENT_TIMEOUT_SECONDS == 90.0

    def test_quick_operation_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS overrides QUICK_OPERATION_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS", "10.0")
        self._reload()
        assert _constants_module.QUICK_OPERATION_TIMEOUT_SECONDS == 10.0

    def test_pq_wait_timeout_seconds_env_override(self, monkeypatch):
        """DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS overrides PQ_STATE_CHANGE_TIMEOUT_SECONDS."""
        monkeypatch.setenv("DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS", "240.0")
        self._reload()
        assert _constants_module.PQ_STATE_CHANGE_TIMEOUT_SECONDS == 240.0

    def test_no_wait_seconds_env_override(self, monkeypatch):
        """DH_MCP_NO_WAIT_SECONDS overrides NO_WAIT_SECONDS."""
        monkeypatch.setenv("DH_MCP_NO_WAIT_SECONDS", "1.0")
        self._reload()
        assert _constants_module.NO_WAIT_SECONDS == 1.0

    def test_invalid_env_var_raises_value_error(self, monkeypatch):
        """A non-numeric env var value should raise ValueError at import time."""
        monkeypatch.setenv("DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS", "not_a_number")
        with pytest.raises(ValueError):
            self._reload()

    def test_default_used_when_env_var_absent(self, monkeypatch):
        """Default value is used when the env var is not set."""
        monkeypatch.delenv("DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS", raising=False)
        self._reload()
        assert _constants_module.SESSION_CONNECT_TIMEOUT_SECONDS == 60.0
