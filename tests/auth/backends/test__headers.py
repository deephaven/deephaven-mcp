"""Tests for deephaven_mcp.auth.backends._headers.

This module contains only string constants. The tests below assert the
exact wire values (because any change to these strings is a backwards-
incompatible protocol change) and check that all values are lowercase
(because the middleware lowercases incoming header names before
dispatching to backends).
"""

from deephaven_mcp.auth.backends import _headers


def test_header_username_value():
    assert _headers.HEADER_USERNAME == "x-deephaven-username"


def test_header_password_value():
    assert _headers.HEADER_PASSWORD == "x-deephaven-password"


def test_header_effective_user_value():
    assert _headers.HEADER_EFFECTIVE_USER == "x-deephaven-effective-user"


def test_header_private_key_value():
    assert _headers.HEADER_PRIVATE_KEY == "x-deephaven-private-key"


def test_header_psk_value():
    assert _headers.HEADER_PSK == "x-deephaven-psk"


def test_all_header_values_are_lowercase():
    for name in _headers.__all__:
        value = getattr(_headers, name)
        assert value == value.lower(), f"{name}={value!r} is not lowercase"


def test_all_header_values_use_x_deephaven_prefix():
    for name in _headers.__all__:
        value = getattr(_headers, name)
        assert value.startswith(
            "x-deephaven-"
        ), f"{name}={value!r} does not follow the X-Deephaven-* convention"


def test_all_header_values_are_unique():
    values = [getattr(_headers, name) for name in _headers.__all__]
    assert len(values) == len(set(values)), "Duplicate header values in _headers"


def test_constants_are_re_exported_from_backends_package():
    """External consumers import from :mod:`deephaven_mcp.auth.backends`."""
    from deephaven_mcp.auth import backends

    assert backends.HEADER_USERNAME == _headers.HEADER_USERNAME
    assert backends.HEADER_PASSWORD == _headers.HEADER_PASSWORD
    assert backends.HEADER_EFFECTIVE_USER == _headers.HEADER_EFFECTIVE_USER
    assert backends.HEADER_PRIVATE_KEY == _headers.HEADER_PRIVATE_KEY
    assert backends.HEADER_PSK == _headers.HEADER_PSK
    for name in _headers.__all__:
        assert name in backends.__all__, f"{name} missing from backends.__all__"
