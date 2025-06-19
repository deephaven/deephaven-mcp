"""
io.py - Async file I/O utilities for deephaven_mcp.

This module provides asynchronous I/O helpers for the Deephaven MCP project, including
coroutine-safe file reading for sensitive binary files such as TLS certificates and private keys.

Features:
- Asynchronous, non-blocking binary file loading with aiofiles.
- Designed for use in session configuration and secure credential loading.
- Centralizes I/O logic for easier testing and patching in unit tests.

Example:
    from deephaven_mcp.io import load_bytes
    cert_bytes = await load_bytes('/path/to/cert.pem')

"""

import logging

import aiofiles

_LOGGER = logging.getLogger(__name__)


async def load_bytes(path: str | None) -> bytes | None:
    """
    Asynchronously load the contents of a binary file.

    This helper is used to read certificate and private key files for secure Deephaven session creation.
    It is designed to be coroutine-safe and leverages aiofiles for non-blocking I/O.

    Args:
        path (Optional[str]): Path to the file to load. If None, returns None.

    Returns:
        Optional[bytes]: The contents of the file as bytes, or None if the path is None.

    Raises:
        Exception: Propagates any exceptions encountered during file I/O (e.g., file not found, permission denied).

    Side Effects:
        - Logs the file path being loaded (info level).
        - Logs and re-raises any exceptions encountered (error level).

    Example:
        >>> cert_bytes = await load_bytes('/path/to/cert.pem')
        >>> if cert_bytes is not None:
        ...     # Use cert_bytes for TLS configuration
    """
    _LOGGER.info(f"Loading binary file: {path}")
    if path is None:
        return None
    try:
        async with aiofiles.open(path, "rb") as f:
            return await f.read()
    except Exception as e:
        _LOGGER.error(f"Error loading binary file {path}: {e}")
        raise
