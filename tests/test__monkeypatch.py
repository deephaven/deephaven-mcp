import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


def test_monkeypatch_uvicorn_exception_handling_warns_and_patches(caplog):
    # Patch before import so the function uses the mock
    with patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle:
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        dummy_orig_run_asgi = MagicMock()
        MockCycle.run_asgi = dummy_orig_run_asgi
        with caplog.at_level("WARNING"):
            monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        # Should warn
        assert any(
            "Monkey-patching Uvicorn's RequestResponseCycle" in r.message
            for r in caplog.records
        )
        # Should patch run_asgi
        assert MockCycle.run_asgi != dummy_orig_run_asgi


def test_monkeypatch_uvicorn_exception_handling_wrapped_app_logs_and_raises(caplog):
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("traceback.print_exc") as print_exc_mock,
        patch("traceback.print_exception") as print_exception_mock,
    ):
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi
        with patch("sys.stderr", new_callable=lambda: sys.stdout):
            monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
            run_asgi = MockCycle.run_asgi
            dummy_self = MagicMock()

            async def bad_app(*args, **kwargs):
                raise ValueError("fail!")

            with pytest.raises(ValueError):
                import asyncio

                asyncio.run(run_asgi(dummy_self, bad_app))
            print_exc_mock.assert_called()
            print_exception_mock.assert_called()
            # Check that error logs were emitted
            assert any(
                "Unhandled exception in ASGI application" in r.getMessage()
                for r in caplog.records
            )
