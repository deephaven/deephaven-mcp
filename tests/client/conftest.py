"""Shared fixtures for the enterprise client wrapper tests.

The wrappers in ``deephaven_mcp.client`` guard construction on
``deephaven_mcp.client._base.is_enterprise_available`` -- a flag computed once,
when ``_base`` is first imported, from whether ``deephaven_enterprise`` could be
imported. These tests simulate an enterprise environment by injecting mock
``deephaven_enterprise`` modules into ``sys.modules``, but that does not update
the already-computed flag: if ``_base`` was first imported before the mocks were
installed (for example transitively via the CLI tests, which import the client
package while the real enterprise package is absent), the flag is ``False`` and
every wrapper construction raises ``InternalError``.

This autouse fixture forces the flag ``True`` for the duration of each test so the
wrapper tests exercise enterprise code paths deterministically, independent of
import order. Tests that specifically exercise the not-available path override it
locally (e.g. ``patch.object(..., is_enterprise_available, False)``).
"""

import pytest


@pytest.fixture(autouse=True)
def _enterprise_available(monkeypatch):
    """Force ``is_enterprise_available`` ``True`` on the canonical ``_base`` module.

    Args:
        monkeypatch: Pytest's monkeypatch fixture; restores the original flag
            value after the test.
    """
    import deephaven_mcp.client._base as base

    monkeypatch.setattr(base, "is_enterprise_available", True)
