"""Tests for ``deephaven_mcp.auth.credentials`` package surface.

These tests pin the public re-exports of the package so that a refactor
of the underlying ``_credentials`` module cannot silently change what
``from deephaven_mcp.auth.credentials import ...`` resolves to.
"""

from __future__ import annotations

import deephaven_mcp.auth.credentials as credentials_pkg
from deephaven_mcp.auth.credentials import (
    AnonymousCredentials,
    Credentials,
    CustomTokenCredentials,
    PasswordCredentials,
    PrivateKeyCredentials,
    PSKCredentials,
)
from deephaven_mcp.auth.credentials import _credentials as _internal

_PUBLIC_NAMES = {
    "AnonymousCredentials",
    "Credentials",
    "CredentialsUnion",
    "CustomTokenCredentials",
    "PSKCredentials",
    "PasswordCredentials",
    "PrivateKeyCredentials",
}


def test_all_lists_documented_public_names() -> None:
    """``__all__`` must list exactly the package's public surface."""
    assert set(credentials_pkg.__all__) == _PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name advertised by ``__all__`` must exist on the module."""
    for name in credentials_pkg.__all__:
        assert hasattr(credentials_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    """Package-level names must be identical objects to the ``_credentials`` symbols."""
    assert Credentials is _internal.Credentials
    assert AnonymousCredentials is _internal.AnonymousCredentials
    assert PSKCredentials is _internal.PSKCredentials
    assert PasswordCredentials is _internal.PasswordCredentials
    assert PrivateKeyCredentials is _internal.PrivateKeyCredentials
    assert CustomTokenCredentials is _internal.CustomTokenCredentials


def test_concrete_credentials_subclass_credentials_base() -> None:
    """Every concrete kind must be a subclass of the abstract ``Credentials`` base."""
    for cls in (
        AnonymousCredentials,
        PSKCredentials,
        PasswordCredentials,
        PrivateKeyCredentials,
        CustomTokenCredentials,
    ):
        assert issubclass(cls, Credentials), cls.__name__


def test_no_tls_symbols_leak_into_credentials_package() -> None:
    """TLS material lives in the peer ``deephaven_mcp.auth.tls`` package.

    The credentials package must not re-export TLS types so that the
    bearer-credential / channel-trust split stays clean.
    """
    leaked = {n for n in credentials_pkg.__all__ if "tls" in n.lower()}
    assert leaked == set()
    assert not hasattr(credentials_pkg, "TlsConfig")
