"""Tests for :mod:`deephaven_mcp.auth.credentials._credentials`."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.credentials import (
    AnonymousCredentials,
    Credentials,
    CredentialsUnion,
    CustomTokenCredentials,
    PasswordCredentials,
    PrivateKeyCredentials,
    PSKCredentials,
)

# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


def test_credentials_base_class_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="abstract base class"):
        Credentials()  # type: ignore[abstract]


def test_concrete_credentials_are_instances_of_base() -> None:
    assert isinstance(PasswordCredentials(username="a", password="b"), Credentials)
    assert isinstance(PrivateKeyCredentials(key_text="k"), Credentials)
    assert isinstance(AnonymousCredentials(), Credentials)
    assert isinstance(PSKCredentials(token="x"), Credentials)
    assert isinstance(
        CustomTokenCredentials(auth_type="t", auth_token="x"), Credentials
    )


# ---------------------------------------------------------------------------
# AnonymousCredentials
# ---------------------------------------------------------------------------


def test_anonymous_construct_default():
    c = AnonymousCredentials()
    assert c.type == "anonymous"


def test_anonymous_equality_and_hashing():
    assert AnonymousCredentials() == AnonymousCredentials()
    assert hash(AnonymousCredentials()) == hash(AnonymousCredentials())


def test_anonymous_redacted_dump():
    out = AnonymousCredentials().model_dump(context={"redact": True})
    assert out == {"type": "anonymous"}


# ---------------------------------------------------------------------------
# PSKCredentials
# ---------------------------------------------------------------------------


def test_psk_inline_construct():
    c = PSKCredentials(token="shh")
    assert c.token.get_secret_value() == "shh"
    assert c.type == "psk"


def test_psk_inline_str_coerces_to_secretstr():
    c = PSKCredentials(token="shh")
    assert isinstance(c.token, SecretStr)


def test_psk_token_required():
    # Env-var indirection is now expressed via ``"${env:NAME}"`` in the
    # source JSON and resolved by the templating engine before the
    # model sees the value. The model itself simply requires ``token``.
    with pytest.raises(ValidationError, match="token"):
        PSKCredentials.model_validate({})


def test_psk_rejects_legacy_token_env_var_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        PSKCredentials.model_validate({"token_env_var": "X"})


def test_psk_frozen():
    c = PSKCredentials(token="x")
    with pytest.raises(ValidationError):
        c.token = SecretStr("y")  # type: ignore[misc]


def test_psk_equality_and_hashing():
    a = PSKCredentials(token="x")
    b = PSKCredentials(token="x")
    c = PSKCredentials(token="y")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    cache: dict[Credentials, str] = {a: "factory1"}
    assert cache[b] == "factory1"


def test_psk_extra_field_rejected():
    with pytest.raises(ValidationError, match="Extra inputs"):
        PSKCredentials.model_validate({"token": "x", "wat": 1})


def test_psk_redacted_dump():
    out = PSKCredentials(token="shh").model_dump(context={"redact": True})
    assert out["token"] == REDACTED
    assert out["type"] == "psk"


def test_psk_repr_masks_secret():
    c = PSKCredentials(token="hunter2")
    r = repr(c)
    assert "hunter2" not in r
    assert "**********" in r


def test_psk_str_masks_secret():
    c = PSKCredentials(token="hunter2")
    assert "hunter2" not in str(c)
    assert "hunter2" not in f"{c}"


# ---------------------------------------------------------------------------
# PasswordCredentials
# ---------------------------------------------------------------------------


def test_password_inline_without_effective_user():
    c = PasswordCredentials(username="alice", password="pw")
    assert c.username == "alice"
    assert c.password.get_secret_value() == "pw"
    assert c.effective_user is None
    assert c.type == "password"


def test_password_inline_with_effective_user():
    c = PasswordCredentials(username="svc", password="pw", effective_user="alice")
    assert c.effective_user == "alice"


def test_password_password_required():
    with pytest.raises(ValidationError, match="password"):
        PasswordCredentials.model_validate({"username": "u"})


def test_password_rejects_legacy_password_env_var_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        PasswordCredentials.model_validate(
            {"username": "u", "password": "p", "password_env_var": "X"}
        )


def test_password_missing_username_rejected():
    with pytest.raises(ValidationError, match="username"):
        PasswordCredentials.model_validate({"password": "p"})


def test_password_frozen():
    c = PasswordCredentials(username="u", password="p")
    with pytest.raises(ValidationError):
        c.username = "v"  # type: ignore[misc]


def test_password_equality_and_hashing():
    a = PasswordCredentials(username="u", password="p", effective_user="e")
    b = PasswordCredentials(username="u", password="p", effective_user="e")
    c = PasswordCredentials(username="u", password="DIFFERENT", effective_user="e")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    cache: dict[Credentials, str] = {a: "factory1"}
    assert cache[b] == "factory1"


def test_password_redacted_dump():
    out = PasswordCredentials(username="alice", password="pw").model_dump(
        context={"redact": True}
    )
    assert out["username"] == "alice"
    assert out["password"] == REDACTED
    assert out["effective_user"] is None


def test_password_repr_masks_only_password():
    c = PasswordCredentials(username="alice", password="hunter2", effective_user="bob")
    r = repr(c)
    assert "hunter2" not in r
    assert "alice" in r
    assert "bob" in r
    assert "**********" in r


def test_password_str_masks_password():
    c = PasswordCredentials(username="alice", password="hunter2")
    assert "hunter2" not in str(c)


def test_password_container_does_not_leak():
    c = PasswordCredentials(username="alice", password="hunter2")
    container = {"creds": c, "list": [c]}
    assert "hunter2" not in repr(container)


# ---------------------------------------------------------------------------
# PrivateKeyCredentials
# ---------------------------------------------------------------------------


def test_private_key_non_dict_input_rejected():
    with pytest.raises(ValidationError):
        PrivateKeyCredentials.model_validate("not-a-dict")


def test_private_key_inline_text():
    c = PrivateKeyCredentials(key_text="-----BEGIN KEY-----\n...\n")
    assert c.key_text.get_secret_value().startswith("-----BEGIN KEY-----")
    assert c.type == "private_key"


def test_private_key_rejects_legacy_private_key_path_field():
    # File indirection is now expressed via ``"${file:/path}"`` in the
    # source JSON and resolved by the templating engine. The model
    # itself only knows about ``key_text``.
    with pytest.raises(ValidationError, match="Extra inputs"):
        PrivateKeyCredentials.model_validate({"private_key_path": "/tmp/k"})


def test_private_key_required():
    with pytest.raises(ValidationError):
        PrivateKeyCredentials.model_validate({})


def test_private_key_frozen():
    c = PrivateKeyCredentials(key_text="k")
    with pytest.raises(ValidationError):
        c.key_text = SecretStr("other")  # type: ignore[misc]


def test_private_key_equality_and_hashing():
    a = PrivateKeyCredentials(key_text="k")
    b = PrivateKeyCredentials(key_text="k")
    c = PrivateKeyCredentials(key_text="OTHER")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c


def test_private_key_redacted_dump():
    out = PrivateKeyCredentials(key_text="secret").model_dump(context={"redact": True})
    assert out["key_text"] == REDACTED


def test_private_key_repr_masks_text():
    key = "-----BEGIN PRIVATE KEY-----\nMIIBVQIBAD..."
    c = PrivateKeyCredentials(key_text=key)
    r = repr(c)
    assert "MIIBVQIBAD" not in r
    assert "BEGIN PRIVATE KEY" not in r
    assert "**********" in r


def test_private_key_str_masks_text():
    c = PrivateKeyCredentials(key_text="supersecretkey")
    assert "supersecretkey" not in str(c)


# ---------------------------------------------------------------------------
# CustomTokenCredentials
# ---------------------------------------------------------------------------


def test_custom_token_inline():
    c = CustomTokenCredentials(auth_type="com.example.Auth", auth_token="opaque")
    assert c.auth_type == "com.example.Auth"
    assert c.auth_token.get_secret_value() == "opaque"
    assert c.type == "custom"


def test_custom_token_rejects_legacy_auth_token_env_var_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        CustomTokenCredentials.model_validate(
            {"auth_type": "x", "auth_token": "a", "auth_token_env_var": "B"}
        )


def test_custom_token_redacted_dump():
    out = CustomTokenCredentials(auth_type="com.x.Y", auth_token="opaque").model_dump(
        context={"redact": True}
    )
    assert out["auth_type"] == "com.x.Y"
    assert out["auth_token"] == REDACTED


def test_custom_token_repr_masks_token():
    c = CustomTokenCredentials(auth_type="com.example.Auth", auth_token="opaque")
    r = repr(c)
    assert "opaque" not in r
    assert "com.example.Auth" in r
    assert "**********" in r


# ---------------------------------------------------------------------------
# CredentialsUnion (discriminated union)
# ---------------------------------------------------------------------------


def test_credentials_union_dispatches_on_type():
    """A containing model with `CredentialsUnion` parses each kind."""
    from pydantic import TypeAdapter

    adapter = TypeAdapter(CredentialsUnion)

    a = adapter.validate_python({"type": "anonymous"})
    assert isinstance(a, AnonymousCredentials)

    p = adapter.validate_python({"type": "psk", "token": "x"})
    assert isinstance(p, PSKCredentials)

    pw = adapter.validate_python({"type": "password", "username": "u", "password": "p"})
    assert isinstance(pw, PasswordCredentials)

    pk = adapter.validate_python({"type": "private_key", "key_text": "k"})
    assert isinstance(pk, PrivateKeyCredentials)

    ct = adapter.validate_python(
        {"type": "custom", "auth_type": "x", "auth_token": "y"}
    )
    assert isinstance(ct, CustomTokenCredentials)


def test_credentials_union_unknown_type_rejected():
    from pydantic import TypeAdapter

    adapter = TypeAdapter(CredentialsUnion)
    with pytest.raises(ValidationError):
        adapter.validate_python({"type": "wat"})


def test_credentials_union_missing_type_rejected():
    from pydantic import TypeAdapter

    adapter = TypeAdapter(CredentialsUnion)
    with pytest.raises(ValidationError):
        adapter.validate_python({"token": "x"})
