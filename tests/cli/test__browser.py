"""Tests for ``deephaven_mcp.cli._browser``."""

from __future__ import annotations

import webbrowser
from unittest.mock import patch

import pytest

from deephaven_mcp.cli._browser import launch_browser
from deephaven_mcp.cli._errors import CliError, ErrorCode

_URL = "http://localhost:10000/ide/?psk=secret"
_SAFE_URL = "http://localhost:10000"
_HINT = "Run 'dhcli session url' for one you can log in with."


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


# ---------------------------------------------------------------------------
# manual_url / hint
# ---------------------------------------------------------------------------


def test_manual_url_replaces_the_url_in_the_message_only() -> None:
    """The browser needs the real URL to log in; the error message does
    not, and must not carry a credential the caller chose to withhold."""
    with (
        patch("webbrowser.open", return_value=False) as opener,
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL, manual_url=_SAFE_URL)
    opener.assert_called_once_with(_URL)
    message = str(exc_info.value)
    assert _SAFE_URL in message
    assert "psk=secret" not in message


def test_manual_url_also_applies_to_the_webbrowser_error_branch() -> None:
    """Both failure branches build their own message; a substitution
    that covered only one would leak on the other."""
    with (
        patch("webbrowser.open", side_effect=webbrowser.Error("no display")),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL, manual_url=_SAFE_URL)
    assert "psk=secret" not in str(exc_info.value)


def test_manual_url_defaults_to_the_opened_url() -> None:
    """Omitting it leaves the pre-existing behavior untouched, which is
    what keeps 'system open' (no credential involved) unchanged."""
    with (
        patch("webbrowser.open", return_value=False),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL)
    assert _URL in str(exc_info.value)


def test_hint_precedes_the_url() -> None:
    """Trailing prose after a URL gets swept into the link by terminal
    and chat autolinkers, so the URL has to come last."""
    with (
        patch("webbrowser.open", return_value=False),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL, manual_url=_SAFE_URL, hint=_HINT)
    message = str(exc_info.value)
    assert _HINT in message
    assert message.index(_HINT) < message.index(_SAFE_URL)
    assert message.endswith(_SAFE_URL)


def test_hint_precedes_the_url_on_the_webbrowser_error_branch() -> None:
    with (
        patch("webbrowser.open", side_effect=webbrowser.Error("no display")),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL, manual_url=_SAFE_URL, hint=_HINT)
    message = str(exc_info.value)
    assert message.index(_HINT) < message.index(_SAFE_URL)
    assert message.endswith(_SAFE_URL)


def test_omitted_hint_adds_no_stray_whitespace() -> None:
    """A caller with nothing to explain must not pay a double space."""
    with (
        patch("webbrowser.open", return_value=False),
        pytest.raises(CliError) as exc_info,
    ):
        launch_browser(_URL)
    message = str(exc_info.value)
    assert "  " not in message
    assert message == f"No usable browser was found. Open this URL manually: {_URL}"


def test_hint_is_ignored_for_a_successful_launch() -> None:
    """The parameters shape a failure message only; a launch that works
    raises nothing and still reports success."""
    with patch("webbrowser.open", return_value=True) as opener:
        assert launch_browser(_URL, manual_url=_SAFE_URL, hint=_HINT) is True
    opener.assert_called_once_with(_URL)
