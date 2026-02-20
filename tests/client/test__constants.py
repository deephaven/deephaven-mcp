"""Unit tests for _constants.py timeout constants."""

import pytest

from deephaven_mcp.client._constants import (
    AUTH_TIMEOUT_SECONDS,
    CONNECTION_TIMEOUT_SECONDS,
    NO_WAIT_SECONDS,
    PQ_CONNECTION_TIMEOUT_SECONDS,
    PQ_OPERATION_TIMEOUT_SECONDS,
    PQ_WAIT_TIMEOUT_SECONDS,
    QUICK_OPERATION_TIMEOUT_SECONDS,
    SAML_AUTH_TIMEOUT_SECONDS,
    SUBSCRIBE_TIMEOUT_SECONDS,
    WORKER_CREATION_TIMEOUT_SECONDS,
)


class TestTimeoutConstants:
    """Test that timeout constants are defined with correct types and reasonable values."""

    def test_connection_timeout_seconds_is_positive_float(self):
        """CONNECTION_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(CONNECTION_TIMEOUT_SECONDS, float)
        assert CONNECTION_TIMEOUT_SECONDS > 0

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
        """PQ_OPERATION_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(PQ_OPERATION_TIMEOUT_SECONDS, float)
        assert PQ_OPERATION_TIMEOUT_SECONDS > 0

    def test_pq_wait_timeout_seconds_is_positive_float(self):
        """PQ_WAIT_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(PQ_WAIT_TIMEOUT_SECONDS, float)
        assert PQ_WAIT_TIMEOUT_SECONDS > 0

    def test_pq_wait_timeout_longer_than_operation_timeout(self):
        """PQ wait timeout should be longer since it waits for state changes."""
        assert PQ_WAIT_TIMEOUT_SECONDS >= PQ_OPERATION_TIMEOUT_SECONDS

    def test_quick_operation_timeout_seconds_is_positive_float(self):
        """QUICK_OPERATION_TIMEOUT_SECONDS should be a positive float."""
        assert isinstance(QUICK_OPERATION_TIMEOUT_SECONDS, float)
        assert QUICK_OPERATION_TIMEOUT_SECONDS > 0

    def test_quick_operation_timeout_shorter_than_connection_timeout(self):
        """Quick operation timeout should be shorter than connection timeout."""
        assert QUICK_OPERATION_TIMEOUT_SECONDS <= CONNECTION_TIMEOUT_SECONDS

    def test_no_wait_seconds_is_zero(self):
        """NO_WAIT_SECONDS should be zero (meaning return immediately in SDK)."""
        assert isinstance(NO_WAIT_SECONDS, float)
        assert NO_WAIT_SECONDS == 0.0
