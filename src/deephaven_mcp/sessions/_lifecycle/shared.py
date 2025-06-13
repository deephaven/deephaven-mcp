"""
Session lifecycle helpers for Deephaven sessions.

This module provides coroutine-compatible utilities for safely closing sessions and other lifecycle operations.
"""

import asyncio
import logging

from pydeephaven import Session

_LOGGER = logging.getLogger(__name__)


async def close_session_safely(session: Session, session_name: str) -> None:
    """
    Safely close a Deephaven session for the given session name, if it is alive.

    Args:
        session (Session): The Deephaven session instance to close.
        session_name (str): The name or key identifying the session owner.

    Any exceptions during closure are logged and do not prevent cleanup of other sessions.
    """
    try:
        if session.is_alive:
            _LOGGER.info(f"Closing alive session: {session_name}")
            await asyncio.to_thread(session.close)
            _LOGGER.info(f"Successfully closed session: {session_name}")
        else:
            _LOGGER.debug(f"Session '{session_name}' is already closed")
    except Exception as e:
        _LOGGER.error(f"Failed to close session: {session_name}: {e}")
        _LOGGER.debug(
            f"Session state after error: {session_name} is_alive={session.is_alive}",
            exc_info=True,
        )
