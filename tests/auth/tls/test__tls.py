"""Tests for :mod:`deephaven_mcp.auth.tls._tls`."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.tls import ClientCertificate, TlsConfig

# ---------------------------------------------------------------------------
# ClientCertificate — inline construction
# ---------------------------------------------------------------------------


def test_client_certificate_holds_pair():
    cert = ClientCertificate(cert_chain="-----CERT-----", private_key="-----KEY-----")
    assert cert.cert_chain == "-----CERT-----"
    assert cert.private_key.get_secret_value() == "-----KEY-----"


def test_client_certificate_is_frozen():
    cert = ClientCertificate(cert_chain="c", private_key="k")
    with pytest.raises(ValidationError):
        cert.cert_chain = "other"  # type: ignore[misc]


def test_client_certificate_repr_masks_private_key():
    cert = ClientCertificate(
        cert_chain="-----CERT-----", private_key="SECRET-PEM-CONTENT"
    )
    r = repr(cert)
    assert "SECRET-PEM-CONTENT" not in r
    assert "**********" in r
    assert "-----CERT-----" in r  # cert_chain is public


def test_client_certificate_str_masks_private_key():
    cert = ClientCertificate(cert_chain="c", private_key="topsecret")
    assert "topsecret" not in str(cert)
    assert "topsecret" not in f"{cert}"


def test_client_certificate_equality_and_hash():
    a = ClientCertificate(cert_chain="c", private_key="k")
    b = ClientCertificate(cert_chain="c", private_key="k")
    c = ClientCertificate(cert_chain="c", private_key="DIFFERENT")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c


def test_client_certificate_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        ClientCertificate(
            cert_chain="c", private_key="k", extra="x"  # type: ignore[call-arg]
        )


def test_client_certificate_requires_both_halves():
    with pytest.raises(ValidationError):
        ClientCertificate.model_validate({"cert_chain": "c"})
    with pytest.raises(ValidationError):
        ClientCertificate.model_validate({"private_key": "k"})


def test_client_certificate_redacted_dump():
    out = ClientCertificate(
        cert_chain="public-cert", private_key="secret-key"
    ).model_dump(mode="json", context={"redact": True})
    assert out["cert_chain"] == "public-cert"
    assert out["private_key"] == REDACTED


# ---------------------------------------------------------------------------
# ClientCertificate — legacy ``*_path`` shadow fields are now rejected.
# File indirection is expressed via ``"${file:/path}"`` in the source
# JSON and resolved by :mod:`deephaven_mcp.config._templating` before the
# model sees the value.
# ---------------------------------------------------------------------------


def test_client_certificate_rejects_legacy_cert_chain_path():
    with pytest.raises(ValidationError, match="Extra inputs"):
        ClientCertificate.model_validate(
            {"cert_chain_path": "/tmp/c", "private_key": "k"}
        )


def test_client_certificate_rejects_legacy_private_key_path():
    with pytest.raises(ValidationError, match="Extra inputs"):
        ClientCertificate.model_validate(
            {"cert_chain": "c", "private_key_path": "/tmp/k"}
        )


# ---------------------------------------------------------------------------
# TlsConfig — inline construction
# ---------------------------------------------------------------------------


def test_client_certificate_non_dict_input_passes_through():
    with pytest.raises(ValidationError):
        ClientCertificate.model_validate("not-a-dict")


def test_tls_config_non_dict_input_passes_through():
    with pytest.raises(ValidationError):
        TlsConfig.model_validate("not-a-dict")


def test_tls_config_defaults_are_none():
    cfg = TlsConfig()
    assert cfg.root_certs is None
    assert cfg.client_certificate is None


def test_tls_config_empty_dict_means_defaults():
    cfg = TlsConfig.model_validate({})
    assert cfg.root_certs is None
    assert cfg.client_certificate is None


def test_tls_config_with_root_certs_inline():
    cfg = TlsConfig(root_certs="-----CA-----")
    assert cfg.root_certs == "-----CA-----"


def test_tls_config_with_client_certificate():
    cert = ClientCertificate(cert_chain="c", private_key="k")
    cfg = TlsConfig(client_certificate=cert)
    assert cfg.client_certificate is cert


def test_tls_config_is_frozen():
    cfg = TlsConfig(root_certs="x")
    with pytest.raises(ValidationError):
        cfg.root_certs = "y"  # type: ignore[misc]


def test_tls_config_equality_and_hash():
    cert = ClientCertificate(cert_chain="c", private_key="k")
    a = TlsConfig(root_certs="r", client_certificate=cert)
    b = TlsConfig(root_certs="r", client_certificate=cert)
    assert a == b
    assert hash(a) == hash(b)


def test_tls_config_redacted_dump_with_full_chain():
    cert = ClientCertificate(cert_chain="c", private_key="k")
    cfg = TlsConfig(root_certs="r", client_certificate=cert)
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["root_certs"] == "r"  # public
    assert out["client_certificate"]["cert_chain"] == "c"  # public
    assert out["client_certificate"]["private_key"] == REDACTED


def test_tls_config_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        TlsConfig.model_validate({"unknown": 1})


# ---------------------------------------------------------------------------
# TlsConfig — legacy ``*_path`` shadow fields are now rejected. Use
# ``"${file:/path}"`` in the source JSON.
# ---------------------------------------------------------------------------


def test_tls_config_rejects_legacy_root_certs_path():
    with pytest.raises(ValidationError, match="Extra inputs"):
        TlsConfig.model_validate({"root_certs_path": "/tmp/ca"})
