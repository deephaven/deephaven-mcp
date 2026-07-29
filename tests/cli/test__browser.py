"""Tests for ``deephaven_mcp.cli._browser``."""

from __future__ import annotations

import webbrowser
from unittest.mock import patch

import pytest

from deephaven_mcp.cli._browser import launch_browser
from deephaven_mcp.cli._errors import CliError, ErrorCode

_URL = "http://localhost:10000/ide/?psk=secret"


def test_launch_browser_returns_true_when_a_browser_opens() -> None:
    with patch("webbrowser.open", return_value=True) as opener:
        assert launch_browser(_URL) is True
    opener.assert_called_once_with(_URL)


def test_launch_browser_raises_when_no_browser_is_found() -> None:
    """``webbrowser.open`` returning False means nothing handled the URL."""
    with (
        patch("webbrowser.open", return_value=False),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL)
    assert exc_info.value.code is ErrorCode.BROWSER_LAUNCH_FAILED
    # The URL must survive into the message so it can be opened by hand.
    assert _URL in str(exc_info.value)


def test_launch_browser_translates_webbrowser_error() -> None:
    """A ``webbrowser.Error`` becomes a structured CLI failure, never a traceback."""
    with (
        patch("webbrowser.open", side_effect=webbrowser.Error("no display")),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL)
    message = str(exc_info.value)
    assert exc_info.value.code is ErrorCode.BROWSER_LAUNCH_FAILED
    assert "no display" in message
    assert _URL in message


def test_launch_browser_chains_the_original_error() -> None:
    """The underlying error is preserved as ``__cause__`` for diagnosis."""
    original = webbrowser.Error("boom")
    with (
        patch("webbrowser.open", side_effect=original),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL)
    assert exc_info.value.__cause__ is original
