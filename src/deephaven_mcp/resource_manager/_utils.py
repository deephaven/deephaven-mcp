"""
Utility functions for dynamic Deephaven Community session management.

This module provides low-level utilities for dynamically launched Deephaven sessions:

- **Port allocation**: Find available TCP ports for session binding
- **Authentication token generation**: Create secure PSK (pre-shared key) tokens

These utilities are primarily used by session launchers (DockerLaunchedSession,
PythonLaunchedSession) but can be imported independently for custom workflows requiring
dynamic port assignment or token generation.

Note:
    These utilities are specific to Community sessions that are launched dynamically.
    Static (pre-configured) sessions use ports and tokens from configuration files.
"""

import logging
import secrets
import socket

from deephaven_mcp._exceptions import SessionLaunchError

_LOGGER = logging.getLogger(__name__)


def find_available_port() -> int:
    """
    Find an available TCP port on localhost for session binding.

    Uses the OS to assign an available port by binding to port 0. The OS
    automatically selects an available port from the ephemeral port range
    (typically 32768-60999 on Linux, 49152-65535 on macOS, 1025-5000 on Windows).
    This is the recommended approach for avoiding port conflicts in dynamic
    session creation.

    The function uses a TCP socket (SOCK_STREAM) to match the protocol used
    by Deephaven server instances.

    Returns:
        int: An available port number assigned by the OS from the ephemeral port range.

    Raises:
        SessionLaunchError: If unable to find an available port due to
            socket errors (e.g., permission denied, address already in use)
            or system resource limitations (e.g., no available ports, file
            descriptor limit reached).

    Note:
        The port is released immediately after detection, creating a small
        time-of-check-time-of-use (TOCTOU) race condition window where another
        process could claim the port before the caller binds to it. This is
        generally acceptable for session launchers that bind immediately after
        calling this function. For scenarios requiring guaranteed port availability,
        consider passing the socket file descriptor directly instead.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
            _LOGGER.debug(f"[_utils:find_available_port] Found available port: {port}")
            return port
    except Exception as e:
        _LOGGER.error(
            f"[_utils:find_available_port] Failed to find available port: {e}"
        )
        raise SessionLaunchError(f"Failed to find available port: {e}") from e


def generate_auth_token() -> str:
    """
    Generate a cryptographically secure authentication token for PSK auth.

    Uses secrets.token_hex() to generate a 32-character hexadecimal token
    (16 bytes of randomness). This provides sufficient entropy for secure
    pre-shared key (PSK) authentication in dynamically launched Deephaven
    Community sessions.

    The token is suitable for use with Deephaven's PSK authentication handler
    and should be kept confidential. Typically used when launching sessions
    with auth_type='psk' where an explicit auth_token is not provided.

    Returns:
        str: A 32-character hexadecimal authentication token (lowercase).
            Format: [0-9a-f]{32}

    Security:
        - Uses cryptographically secure random number generation (secrets module)
        - 128 bits of entropy (2^128 ≈ 3.4×10^38 possible values)
        - Resistant to brute-force attacks for session lifetime
        - Suitable for temporary session authentication (not long-term secrets)
        - Should be transmitted over HTTPS/TLS in production environments
        - Token should be treated as a password and never logged or committed

    Example:
        >>> token = generate_auth_token()
        >>> len(token)
        32
        >>> all(c in '0123456789abcdef' for c in token)
        True
        >>> # Use in session launch
        >>> session = await launch_session(
        ...     launch_method='docker',
        ...     session_name='my-session',
        ...     port=10000,
        ...     auth_token=token  # Use generated token
        ... )
    """
    token = secrets.token_hex(16)  # 16 bytes = 32 hex characters
    _LOGGER.debug("[_utils:generate_auth_token] Generated new auth token")
    return token
