"""Tests for ``deephaven_mcp.auth.tls`` package surface.

These tests pin the public re-exports of the package so that a refactor
of the underlying ``_tls`` module cannot silently change what
``from deephaven_mcp.auth.tls import ...`` resolves to.
"""

from __future__ import annotations

import deephaven_mcp.auth.tls as tls_pkg
from deephaven_mcp.auth.tls import ClientCertificate, TlsConfig
from deephaven_mcp.auth.tls import _tls as _internal

_PUBLIC_NAMES = {"ClientCertificate", "TlsConfig"}


def test_all_lists_documented_public_names() -> None:
    """``__all__`` must list exactly the package's public surface."""
    assert set(tls_pkg.__all__) == _PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name advertised by ``__all__`` must exist on the module."""
    for name in tls_pkg.__all__:
        assert hasattr(tls_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    """Package-level names must be identical objects to the ``_tls`` symbols."""
    assert ClientCertificate is _internal.ClientCertificate
    assert TlsConfig is _internal.TlsConfig


def test_no_credentials_symbols_leak_into_tls_package() -> None:
    """Bearer credential types live in ``deephaven_mcp.auth.credentials``.

    The TLS package must not re-export bearer-credential types so that the
    channel-trust / bearer-credential split stays clean.
    """
    leaked = {
        n for n in tls_pkg.__all__ if n.endswith("Credentials") or n == "Credentials"
    }
    assert leaked == set()
    assert not hasattr(tls_pkg, "Credentials")
