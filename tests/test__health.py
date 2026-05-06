"""Tests for :mod:`deephaven_mcp._health`."""

from deephaven_mcp import _health


def test_health_path_value():
    """``HEALTH_PATH`` is the canonical ``/health`` route string."""
    assert _health.HEALTH_PATH == "/health"


def test_health_path_is_str():
    """The constant must be a plain ``str`` (not bytes / Path / etc.)."""
    assert isinstance(_health.HEALTH_PATH, str)


def test_module_all_surface():
    """``__all__`` exports exactly the public constant."""
    assert _health.__all__ == ["HEALTH_PATH"]


def test_health_path_shape_contract():
    """Path matches the contract documented on the constant.

    Guards against future edits like ``"health"`` (missing leading slash),
    ``"/health/"`` (trailing slash would not match Starlette's exact-match
    routing), or accidental whitespace.
    """
    path = _health.HEALTH_PATH
    assert path.startswith("/"), "HEALTH_PATH must have a leading slash"
    assert (
        not path.endswith("/") or path == "/"
    ), "HEALTH_PATH must not have a trailing slash"
    assert (
        path == path.strip()
    ), "HEALTH_PATH must not contain leading/trailing whitespace"
    assert " " not in path, "HEALTH_PATH must not contain interior whitespace"
