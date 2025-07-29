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


def test_setup_signal_handler_logging_idempotent():
    """Test that setup_signal_handler_logging is idempotent."""
    import deephaven_mcp._logging as logging_mod
    importlib.reload(logging_mod)
    
    # Mock signal.signal to verify calls
    with patch("signal.signal") as mock_signal:
        # First call should register handlers
        logging_mod._SIGNAL_HANDLERS_INSTALLED = False  # reset idempotency for test
        logging_mod.setup_signal_handler_logging()
        assert mock_signal.call_count > 0, "Signal handlers should be registered"
        call_count = mock_signal.call_count
        
        # Second call should be a no-op due to idempotency
        mock_signal.reset_mock()
        logging_mod.setup_signal_handler_logging()
        assert mock_signal.call_count == 0, "Signal handlers should not be registered again"


def test_signal_handler_coverage(caplog):
    """Test signal handler function for coverage."""
    import signal
    import deephaven_mcp._logging as logging_mod
    importlib.reload(logging_mod)
    
    caplog.set_level("INFO")
    
    # We need to mock both logging.warning (for handler) and logging.info (for registration)
    with patch("logging.warning") as mock_warning, patch("logging.info") as mock_info:
        # Need to reset the flag to ensure handler gets registered
        logging_mod._SIGNAL_HANDLERS_INSTALLED = False
        
        # Register handlers
        logging_mod.setup_signal_handler_logging()
        
        # Verify the registration message was logged
        assert mock_info.call_count >= 1, "Registration message should be logged"
        
        # Find the registration message
        registration_msg = None
        for call_args in mock_info.call_args_list:
            args, _ = call_args
            if args and "Signal handlers registered" in args[0]:
                registration_msg = args[0]
                break
        
        assert registration_msg, "Expected registration message with 'Signal handlers registered'"
        
        # Reset mocks to test actual handler calls
        mock_warning.reset_mock()
        mock_info.reset_mock()
        
        # Get access to the signal handler function via the mock
        with patch.object(signal, "signal") as mock_signal:
            logging_mod._SIGNAL_HANDLERS_INSTALLED = False
            logging_mod.setup_signal_handler_logging()
            # Extract the handler function from the first call
            _signal_handler = mock_signal.call_args_list[0][0][1]
            
            # Create a frame mock object
            frame = object()
            
            # Now call the handler directly
            _signal_handler(signal.SIGTERM, frame)
            
            # Verify correct logging - handler uses warning level
            assert mock_warning.call_count == 3, "Should log 3 warning messages"
            messages = [args[0] for args, _ in mock_warning.call_args_list]
            assert any("Received signal" in msg and "SIGTERM" in msg for msg in messages)
            assert any(f"Signal frame: {frame}" in msg for msg in messages)
            assert any("Process will likely terminate soon" in msg for msg in messages)


def test_signal_handler_logging_registration_failure(monkeypatch):
    """Test signal handler registration failure for coverage."""
    import deephaven_mcp._logging as logging_mod
    importlib.reload(logging_mod)
    
    # Force signal.signal to raise an exception
    def mock_signal_raises(*args, **kwargs):
        raise ValueError("Signal registration failed")
    
    with patch("signal.signal", side_effect=mock_signal_raises):
        with patch("logging.warning") as mock_warning:
            # Reset the flag to ensure handler registration is attempted
            logging_mod._SIGNAL_HANDLERS_INSTALLED = False
            logging_mod.setup_signal_handler_logging()
            
            # Check that the failure was logged properly
            mock_warning.assert_called_with("[signal_handler] Failed to register signal handlers: Signal registration failed")


def test_log_process_state_standard(monkeypatch):
    """Test log_process_state functionality in standard case."""
    import deephaven_mcp._logging as logging_mod
    importlib.reload(logging_mod)
    
    # Mock logging and psutil
    with patch("logging.info") as mock_info, \
         patch("logging.warning") as mock_warning, \
         patch("psutil.Process") as mock_process:
        
        # Set up return values for the mocks
        mock_process_instance = MagicMock()
        mock_process_instance.memory_info.return_value.rss = 104857600  # 100MB
        mock_process_instance.cpu_percent.return_value = 5.5
        mock_process_instance.num_fds.return_value = 42
        mock_process_instance.pid = 12345
        mock_process.return_value = mock_process_instance
        
        # Call the function with startup context
        logging_mod.log_process_state("test_tag", "startup")
        
        # Verify correct logs were produced
        assert mock_info.call_count == 4, "Should produce 4 info logs for startup"
        mock_info.assert_any_call("[test_tag] memory usage: 100.00 MB")
        mock_info.assert_any_call("[test_tag] CPU percent: 5.5%")
        mock_info.assert_any_call("[test_tag] open file descriptors: 42")
        mock_info.assert_any_call("[test_tag] Process PID: 12345")
        
        # Reset mocks
        mock_info.reset_mock()
        
        # Call with shutdown context
        logging_mod.log_process_state("test_tag", "shutdown")
        
        # Verify correct logs were produced with "Final" prefix and no PID
        assert mock_info.call_count == 3, "Should produce 3 info logs for shutdown"
        mock_info.assert_any_call("[test_tag] Final memory usage: 100.00 MB")
        mock_info.assert_any_call("[test_tag] Final CPU percent: 5.5%")
        mock_info.assert_any_call("[test_tag] Final open file descriptors: 42")


def test_log_process_state_exception_handling(monkeypatch):
    """Test log_process_state exception handling for coverage."""
    import deephaven_mcp._logging as logging_mod
    importlib.reload(logging_mod)
    
    # Mock psutil.Process to raise an exception
    with patch("psutil.Process", side_effect=Exception("Process access failed")), \
         patch("logging.error") as mock_error, \
         patch("logging.warning") as mock_warning:
        
        # Call the function
        logging_mod.log_process_state("test_tag", "test")
        
        # Verify error was logged
        mock_error.assert_called_with(
            "[test_tag] Error getting test process state: Process access failed"
        )


