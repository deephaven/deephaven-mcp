"""Default-browser launch helper shared by the ``open`` verbs."""

from __future__ import annotations

__all__ = ["launch_browser"]

import webbrowser

from deephaven_mcp.cli._errors import CliError, ErrorCode


def launch_browser(
    url: str, *, manual_url: str | None = None, hint: str | None = None
) -> bool:
    """Open ``url`` in the user's default web browser.

    Args:
        url (str): The URL to open. Handed to the browser as given.
        manual_url (str | None): URL to name in the failure message
            instead of ``url``. Lets a caller keep a credential out of
            stderr while still opening a working URL — ``dhcli session
            open`` passes the token-free base URL unless
            ``--reveal-secrets`` was given. Defaults to ``url``.
        hint (str | None): Sentence inserted *before* the URL, for
            recovery guidance a generic helper cannot know (e.g. where
            to obtain an authenticated URL when ``manual_url``
            deliberately omits the token). Pass ``None`` when
            ``manual_url`` is the URL actually opened, so the message
            does not describe a difference that does not exist.

    Returns:
        bool: ``True`` when a browser was launched.

    Raises:
        CliError: When no browser could be launched (exit 2,
            ``browser_launch_failed``). The message names ``manual_url``
            so it can be opened by hand, and always *ends* with it --
            trailing prose after a URL gets swept into the link by
            terminal and chat autolinkers, corrupting a copy-paste.
    """
    shown = url if manual_url is None else manual_url
    guidance = f"{hint} " if hint else ""
    try:
        launched = webbrowser.open(url)
    except webbrowser.Error as exc:
        raise CliError(
            f"Could not launch a browser ({exc}). "
            f"{guidance}Open this URL manually: {shown}",
            code=ErrorCode.BROWSER_LAUNCH_FAILED,
        ) from exc
    if not launched:
        raise CliError(
            f"No usable browser was found. {guidance}Open this URL manually: {shown}",
            code=ErrorCode.BROWSER_LAUNCH_FAILED,
        )
    return launched
