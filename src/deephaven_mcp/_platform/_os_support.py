"""Shared OS-support contract for the :mod:`deephaven_mcp._platform` package.

This is a leaf module: it imports only :mod:`deephaven_mcp._exceptions`
(itself import-free), so every OS-dispatching submodule can import the
supported-OS set and the error factory from here without any risk of a
circular import through the package ``__init__``.
"""

from __future__ import annotations

__all__ = ["SUPPORTED_OS_NAMES", "unsupported_os_error"]

import os

from deephaven_mcp._exceptions import InternalError

SUPPORTED_OS_NAMES = frozenset({"posix", "nt"})
""":data:`os.name` values the platform primitives implement: ``"posix"``
(Linux, macOS, *BSD) and ``"nt"`` (Windows). Any other value is rejected
with :class:`~deephaven_mcp._exceptions.InternalError` rather than
silently falling through to one platform's code path. A new platform must
add an explicit branch at every dispatch site before it is allowed to
run."""


def unsupported_os_error(component: str) -> InternalError:
    """Build the standard :class:`InternalError` for an unsupported OS.

    Centralizes the message so every dispatch site reports an unsupported
    :data:`os.name` identically.

    Args:
        component (str): Name of the primitive that has no implementation
            for the current platform (e.g. ``"spawn_detached"``), used as
            the message prefix.

    Returns:
        InternalError: An exception (not raised) carrying ``component``,
            the current :data:`os.name`, and the sorted
            :data:`SUPPORTED_OS_NAMES`.
    """
    return InternalError(
        f"{component} has no implementation for os.name={os.name!r}; "
        f"supported: {sorted(SUPPORTED_OS_NAMES)!r}"
    )
