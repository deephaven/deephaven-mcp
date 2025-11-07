"""
Unit tests for community utility functions.

Tests port allocation and authentication token generation utilities.
"""

import socket
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import SessionLaunchError
from deephaven_mcp.resource_manager import find_available_port, generate_auth_token


class TestFindAvailablePort:
    """Tests for find_available_port function."""

    def test_returns_valid_port(self):
        """Test that find_available_port returns a valid port number."""
        port = find_available_port()
        assert isinstance(port, int)
        assert 1024 <= port <= 65535

    def test_returns_different_ports_on_multiple_calls(self):
        """Test that multiple calls return different available ports."""
        port1 = find_available_port()
        port2 = find_available_port()
        # Ports should be different (very high probability)
        assert port1 != port2

    def test_socket_error_raises(self):
        """Test that socket errors are wrapped in SessionLaunchError."""
        with patch("socket.socket") as mock_socket:
            mock_socket.side_effect = OSError("Socket error")
            
            with pytest.raises(SessionLaunchError, match="Failed to find available port"):
                find_available_port()


class TestGenerateAuthToken:
    """Tests for generate_auth_token function."""

    def test_returns_string(self):
        """Test that generate_auth_token returns a string."""
        token = generate_auth_token()
        assert isinstance(token, str)

    def test_returns_32_char_hex(self):
        """Test that token is 32 characters of hex."""
        token = generate_auth_token()
        assert len(token) == 32
        # Should be valid hex
        int(token, 16)

    def test_returns_different_tokens(self):
        """Test that multiple calls return different tokens."""
        token1 = generate_auth_token()
        token2 = generate_auth_token()
        assert token1 != token2
