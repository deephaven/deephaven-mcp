"""Default-browser launch helper shared by the ``open`` verbs."""

from __future__ import annotations

__all__ = ["launch_browser"]

import webbrowser

from deephaven_mcp.cli._errors import CliError, ErrorCode


def launch_browser(url: str) -> bool:
    """Open ``url`` in the user's default web browser.

    Args:
        url (str): The URL to open.

    Returns:
        bool: ``True`` when a browser was launched.

    Raises:
        CliError: When no browser could be launched (exit 2,
            ``browser_launch_failed``). The message includes ``url`` so it
            can be opened manually.
    """
    try:
        launched = webbrowser.open(url)
    except webbrowser.Error as exc:
        raise CliError(
            f"Could not launch a browser ({exc}). Open this URL manually: {url}",
            code=ErrorCode.BROWSER_LAUNCH_FAILED,
        ) from exc
    if not launched:
        raise CliError(
            f"No usable browser was found. Open this URL manually: {url}",
            code=ErrorCode.BROWSER_LAUNCH_FAILED,
        )
    return launched
