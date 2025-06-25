import asyncio
import importlib
import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


def test_setup_logging_sets_basic_config():
    # Patch logging.basicConfig to capture arguments
    called = {}

    def fake_basicConfig(**kwargs):
        called.update(kwargs)

    with patch("logging.basicConfig", fake_basicConfig):
        import deephaven_mcp._logging as logging_mod

        importlib.reload(logging_mod)
        logging_mod.setup_logging()
    assert called["level"] == "INFO"
    assert called["stream"] == sys.stderr
    assert called["force"] is True
    assert "format" in called


def test_setup_logging_respects_env(monkeypatch):
    monkeypatch.setenv("PYTHONLOGLEVEL", "DEBUG")
    called = {}

    def fake_basicConfig(**kwargs):
        called.update(kwargs)

    with patch("logging.basicConfig", fake_basicConfig):
        import deephaven_mcp._logging as logging_mod

        importlib.reload(logging_mod)
        logging_mod.setup_logging()
    assert called["level"] == "DEBUG"


def test_setup_global_exception_logging_idempotent():
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)
    # Patch asyncio.get_event_loop to avoid real event loop
    mock_loop = MagicMock()
    with patch.object(logging_mod.asyncio, "get_event_loop", return_value=mock_loop):
        logging_mod.setup_global_exception_logging()
        assert logging_mod._EXC_LOGGING_INSTALLED is True
        # Call again, should be a no-op
        logging_mod.setup_global_exception_logging()
        assert logging_mod._EXC_LOGGING_INSTALLED is True
        mock_loop.set_exception_handler.assert_called()


def test_setup_global_exception_logging_sets_excepthook(caplog):
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)
    caplog.set_level("ERROR")
    # Patch asyncio.get_event_loop to avoid real event loop
    mock_loop = MagicMock()
    with patch.object(logging_mod.asyncio, "get_event_loop", return_value=mock_loop):
        logging_mod.setup_global_exception_logging()

        class DummyExc(Exception):
            pass

        try:
            raise DummyExc("fail-sync")
        except DummyExc as e:
            sys.excepthook(DummyExc, e, e.__traceback__)
        assert any("UNHANDLED EXCEPTION" in r.message for r in caplog.records)
        found = False
        for r in caplog.records:
            if r.exc_info:
                import traceback

                tb_str = "".join(traceback.format_exception(*r.exc_info))
                if "fail-sync" in tb_str:
                    found = True
                    break
        assert found, "Exception message 'fail-sync' not found in exc_info traceback"
        mock_loop.set_exception_handler.assert_called()


def test_setup_global_exception_logging_keyboardinterrupt(caplog):
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)
    caplog.set_level("ERROR")
    # Patch asyncio.get_event_loop to avoid real event loop
    mock_loop = MagicMock()
    with patch.object(logging_mod.asyncio, "get_event_loop", return_value=mock_loop):
        logging_mod.setup_global_exception_logging()

        class DummyKI(KeyboardInterrupt):
            pass

        sys.excepthook(DummyKI, DummyKI(), None)
        assert not caplog.records, "KeyboardInterrupt should not be logged"
        mock_loop.set_exception_handler.assert_called()


def test_setup_global_exception_logging_asyncio(caplog):
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)
    caplog.set_level("ERROR")
    # Patch asyncio.get_event_loop to avoid real event loop
    mock_loop = MagicMock()
    with patch.object(logging_mod.asyncio, "get_event_loop", return_value=mock_loop):
        logging_mod.setup_global_exception_logging()
        # Simulate an unhandled async exception
        loop = asyncio.new_event_loop()
        try:
            context = {"message": "fail-async", "exception": RuntimeError("fail-async")}
            loop.call_exception_handler(context)
            assert any("UNHANDLED ASYNC EXCEPTION" in r.message for r in caplog.records)
            found = False
            for r in caplog.records:
                if r.exc_info:
                    import traceback

                    tb_str = "".join(traceback.format_exception(*r.exc_info))
                    if "fail-async" in tb_str:
                        found = True
                        break
            assert (
                found
            ), "Exception message 'fail-async' not found in exc_info traceback"
        finally:
            loop.close()


def test_setup_global_exception_logging_no_event_loop():
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)
    # Patch asyncio.get_event_loop to raise RuntimeError
    with patch.object(
        logging_mod.asyncio, "get_event_loop", side_effect=RuntimeError("no event loop")
    ) as get_event_loop_mock:
        # Should not raise
        logging_mod._EXC_LOGGING_INSTALLED = False  # reset idempotency for test
        logging_mod.setup_global_exception_logging()
        get_event_loop_mock.assert_called_once()
