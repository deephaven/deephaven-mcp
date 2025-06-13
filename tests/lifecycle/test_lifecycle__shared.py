import asyncio
import logging
from unittest.mock import MagicMock

import pytest
from pydeephaven import Session

from deephaven_mcp.sessions._lifecycle.shared import close_session_safely


@pytest.mark.asyncio
async def test_close_session_safely_closes_alive_session(caplog):
    caplog.set_level(logging.INFO)
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close = MagicMock()
    await close_session_safely(session, "session1")
    session.close.assert_called_once()
    assert any("Successfully closed session" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_close_session_safely_skips_closed_session(caplog):

    caplog.set_level(logging.DEBUG)
    session = MagicMock(spec=Session)
    session.is_alive = False
    session.close = MagicMock()
    await close_session_safely(session, "session2")
    session.close.assert_not_called()
    assert any("already closed" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_close_session_safely_handles_exceptions(caplog):
    session = MagicMock(spec=Session)
    session.is_alive = True
    session.close.side_effect = Exception("fail-close")
    await close_session_safely(session, "session3")
    assert any("Failed to close session" in r for r in caplog.text.splitlines())


@pytest.mark.asyncio
async def test_close_session_safely_is_alive_raises(caplog):
    session = MagicMock(spec=Session)
    calls = {"count": 0}

    def is_alive_side_effect(self):
        if calls["count"] == 0:
            calls["count"] += 1
            raise Exception("fail-attr")
        return False

    type(session).is_alive = property(is_alive_side_effect)
    await close_session_safely(session, "session4")
    assert any(
        "Failed to close session" in r or "Error" in r for r in caplog.text.splitlines()
    )
