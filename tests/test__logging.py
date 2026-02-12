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
        assert (
            mock_signal.call_count == 0
        ), "Signal handlers should not be registered again"


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

        assert (
            registration_msg
        ), "Expected registration message with 'Signal handlers registered'"
        # Verify multiple signals were registered
        assert "SIGTERM" in registration_msg
        assert "SIGINT" in registration_msg

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
            assert any(
                "Received signal" in msg and "SIGTERM" in msg for msg in messages
            )
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
        with patch("logging.debug") as mock_debug:
            # Reset the flag to ensure handler registration is attempted
            logging_mod._SIGNAL_HANDLERS_INSTALLED = False
            logging_mod.setup_signal_handler_logging()

            # Check that the failures were logged to debug
            # All signals should fail with our mock
            assert mock_debug.call_count >= 1
            # Verify debug log contains failed signal information
            debug_calls = [str(call) for call in mock_debug.call_args_list]
            assert any("Failed to register handlers" in call for call in debug_calls)


def test_log_process_state_standard(monkeypatch):
    """Test log_process_state functionality in standard case."""
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)

    # Mock logging and psutil
    with (
        patch("logging.info") as mock_info,
        patch("logging.warning") as mock_warning,
        patch("psutil.Process") as mock_process,
    ):

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
    with (
        patch("psutil.Process", side_effect=Exception("Process access failed")),
        patch("logging.error") as mock_error,
        patch("logging.warning") as mock_warning,
    ):

        # Call the function
        logging_mod.log_process_state("test_tag", "test")

        # Verify error was logged
        mock_error.assert_called_with(
            "[test_tag] Error getting test process state: Process access failed"
        )


def test_signal_handler_defensive_logging_failure():
    """Test that signal handler handles logging failures gracefully."""
    import signal

    from deephaven_mcp._logging import _signal_handler

    # Mock logging to raise an exception
    with patch("logging.warning", side_effect=Exception("Logging broken")):
        # Mock sys.stderr.write to verify fallback
        with (
            patch.object(sys.stderr, "write") as mock_stderr_write,
            patch.object(sys.stderr, "flush") as mock_stderr_flush,
        ):
            frame = None

            # Should not raise even though logging fails
            _signal_handler(signal.SIGTERM, frame)

            # Verify fallback to stderr was attempted
            assert mock_stderr_write.call_count >= 1
            stderr_output = "".join(
                str(call[0][0]) for call in mock_stderr_write.call_args_list
            )
            assert "CRITICAL" in stderr_output
            assert "SIGTERM" in stderr_output
            assert "Logging broken" in stderr_output
            mock_stderr_flush.assert_called()


def test_signal_handler_defensive_stderr_failure():
    """Test that signal handler handles complete failure gracefully (even stderr fails)."""
    import signal

    from deephaven_mcp._logging import _signal_handler

    # Mock everything to fail
    with patch("logging.warning", side_effect=Exception("Logging broken")):
        with patch.object(sys.stderr, "write", side_effect=Exception("stderr broken")):
            frame = None

            # Should not raise even though everything fails - last resort catch
            # This should complete without raising
            _signal_handler(signal.SIGTERM, frame)


def test_signal_handler_unknown_signal_number():
    """Test that signal handler handles unknown signal numbers gracefully."""
    from deephaven_mcp._logging import _signal_handler

    # Mock logging to capture calls
    with patch("logging.warning") as mock_warning:
        frame = None
        # Use a nonsensical signal number
        fake_signum = 9999

        # Should not raise
        _signal_handler(fake_signum, frame)

        # Verify it logged with UNKNOWN
        assert mock_warning.call_count == 3
        messages = [args[0] for args, _ in mock_warning.call_args_list]
        assert any("9999" in msg and "UNKNOWN" in msg for msg in messages)


def test_signal_handler_platform_specific_signals():
    """Test that platform-specific signals are handled correctly."""
    import signal

    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)

    with patch("logging.info") as mock_info, patch("logging.debug") as mock_debug:
        logging_mod._SIGNAL_HANDLERS_INSTALLED = False
        logging_mod.setup_signal_handler_logging()

        # Verify registration was logged
        assert mock_info.call_count >= 1

        # Find the registration message
        registration_msg = None
        for call_args in mock_info.call_args_list:
            args, _ = call_args
            if args and "Signal handlers registered" in args[0]:
                registration_msg = args[0]
                break

        assert registration_msg is not None

        # Verify critical signals are registered (present on all platforms)
        assert "SIGTERM" in registration_msg
        assert "SIGINT" in registration_msg
        assert "SIGABRT" in registration_msg

        # Platform-specific signals may or may not be present
        # On Unix-like systems (Linux/macOS), these should be present
        if hasattr(signal, "SIGHUP"):
            assert "SIGHUP" in registration_msg
        if hasattr(signal, "SIGQUIT"):
            assert "SIGQUIT" in registration_msg
        if hasattr(signal, "SIGUSR1"):
            assert "SIGUSR1" in registration_msg


def test_signal_handler_multiple_signals_registered():
    """Test that multiple signals are registered and share the same handler."""
    import signal

    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)

    with patch.object(signal, "signal") as mock_signal:
        logging_mod._SIGNAL_HANDLERS_INSTALLED = False
        logging_mod.setup_signal_handler_logging()

        # Should have registered multiple signals
        assert mock_signal.call_count >= 3  # At least SIGTERM, SIGINT, SIGABRT

        # Extract all registered signal numbers and handlers
        registered_handlers = {}
        for call_args in mock_signal.call_args_list:
            sig_num, handler = call_args[0]
            registered_handlers[sig_num] = handler

        # Verify SIGTERM and SIGINT use the same handler function
        assert signal.SIGTERM in registered_handlers
        assert signal.SIGINT in registered_handlers
        assert registered_handlers[signal.SIGTERM] is registered_handlers[signal.SIGINT]


def test_signal_handler_os_error_handling():
    """Test that OSError during signal registration is handled properly."""
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)

    # Create a mock that fails only for specific signals
    call_count = [0]

    def selective_signal_mock(sig, handler):
        call_count[0] += 1
        # Fail on the second call (let first succeed)
        if call_count[0] == 2:
            raise OSError("Signal not supported on this platform")
        return None

    with patch("signal.signal", side_effect=selective_signal_mock):
        with patch("logging.info") as mock_info, patch("logging.debug") as mock_debug:
            logging_mod._SIGNAL_HANDLERS_INSTALLED = False
            logging_mod.setup_signal_handler_logging()

            # Should have logged successes and failures
            assert mock_info.call_count >= 1  # At least one success
            assert mock_debug.call_count >= 1  # At least one failure

            # Check that failure was logged to debug
            debug_calls = [str(call) for call in mock_debug.call_args_list]
            assert any("Failed to register handlers" in call for call in debug_calls)


def test_signal_handler_runtime_error_handling():
    """Test that RuntimeError during signal registration is handled properly."""
    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)

    # Simulate signal already registered by another handler
    def signal_already_registered(sig, handler):
        raise RuntimeError("Signal already registered")

    with patch("signal.signal", side_effect=signal_already_registered):
        with patch("logging.debug") as mock_debug:
            logging_mod._SIGNAL_HANDLERS_INSTALLED = False
            logging_mod.setup_signal_handler_logging()

            # All registrations should fail, should be logged to debug
            assert mock_debug.call_count >= 1
            debug_calls = [str(call) for call in mock_debug.call_args_list]
            assert any("Failed to register handlers" in call for call in debug_calls)


def test_signal_handler_critical_signal_missing_from_platform():
    """Test that critical signals missing from platform are logged properly."""
    import signal

    import deephaven_mcp._logging as logging_mod

    importlib.reload(logging_mod)

    # Mock hasattr to simulate a critical signal missing from the platform
    original_hasattr = hasattr

    def mock_hasattr(obj, name):
        # Simulate SIGTERM not being available on this platform
        if obj is signal and name == "SIGTERM":
            return False
        return original_hasattr(obj, name)

    with patch("builtins.hasattr", side_effect=mock_hasattr):
        with patch("logging.info") as mock_info, patch("logging.debug") as mock_debug:
            logging_mod._SIGNAL_HANDLERS_INSTALLED = False
            logging_mod.setup_signal_handler_logging()

            # Should have logged failures to debug
            assert mock_debug.call_count >= 1

            # Find the debug message with failed signals
            debug_messages = [str(call) for call in mock_debug.call_args_list]
            failed_signals_logged = False
            for msg in debug_messages:
                if "Failed to register handlers" in msg and "SIGTERM" in msg:
                    # Verify it has the "not available on this platform" message
                    assert "not available on this platform" in msg
                    failed_signals_logged = True
                    break

            assert (
                failed_signals_logged
            ), "Expected debug log for SIGTERM not available on platform"
