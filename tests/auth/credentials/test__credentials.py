"""Tests for deephaven_mcp.auth.credentials._credentials."""

import pytest

from deephaven_mcp.auth.credentials import (
    Credentials,
    PasswordCredentials,
    PrivateKeyCredentials,
    PSKCredentials,
)


def test_psk_credentials_carry_the_key():
    c = PSKCredentials(psk="secret-token")
    assert c.psk == "secret-token"


def test_psk_credentials_are_hashable_and_equal_by_value():
    a = PSKCredentials(psk="k")
    b = PSKCredentials(psk="k")
    c = PSKCredentials(psk="OTHER")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    cache: dict[Credentials, str] = {a: "backend"}
    assert cache[b] == "backend"


def test_psk_credentials_is_frozen():
    c = PSKCredentials(psk="k")
    with pytest.raises((AttributeError, Exception)):
        c.psk = "other"  # type: ignore[misc]


def test_password_creds_without_effective_user():
    c = PasswordCredentials(username="alice", password="hunter2")
    assert c.username == "alice"
    assert c.password == "hunter2"
    assert c.effective_user is None


def test_password_creds_with_effective_user():
    c = PasswordCredentials(
        username="svc",
        password="pw",
        effective_user="alice",
    )
    assert c.effective_user == "alice"


def test_password_creds_is_frozen():
    c = PasswordCredentials(username="alice", password="pw")
    with pytest.raises((AttributeError, Exception)):
        c.password = "other"  # type: ignore[misc]


def test_private_key_creds_holds_text():
    c = PrivateKeyCredentials(key_text="-----BEGIN KEY-----\n...\n")
    assert c.key_text.startswith("-----BEGIN KEY-----")


def test_private_key_creds_is_frozen():
    c = PrivateKeyCredentials(key_text="k")
    with pytest.raises((AttributeError, Exception)):
        c.key_text = "other"  # type: ignore[misc]


def test_credentials_base_class_cannot_be_instantiated():
    """The Credentials ABC is for typing/inheritance only."""
    with pytest.raises(TypeError):
        Credentials()  # type: ignore[abstract]


def test_concrete_credentials_are_instances_of_base():
    """Every concrete credential kind subclasses :class:`Credentials`."""
    assert isinstance(PSKCredentials(psk="k"), Credentials)
    assert isinstance(PasswordCredentials(username="a", password="b"), Credentials)
    assert isinstance(PrivateKeyCredentials(key_text="k"), Credentials)


def test_credentials_base_class_carries_no_fields():
    """The base class is a marker ABC with empty ``__slots__``."""
    assert Credentials.__slots__ == ()


def test_password_credentials_are_hashable_and_equal_by_value():
    a = PasswordCredentials(username="u", password="p", effective_user="e")
    b = PasswordCredentials(username="u", password="p", effective_user="e")
    c = PasswordCredentials(username="u", password="DIFFERENT", effective_user="e")
    # Registry uses credentials as cache keys; equality + hashing must be
    # structural (all fields).
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    cache: dict[Credentials, str] = {a: "factory1"}
    assert cache[b] == "factory1"


def test_private_key_credentials_are_hashable_and_equal_by_value():
    a = PrivateKeyCredentials(key_text="k")
    b = PrivateKeyCredentials(key_text="k")
    c = PrivateKeyCredentials(key_text="OTHER")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c


# ---------------------------------------------------------------------------
# __repr__ must redact secret fields so accidental logging does not leak them.
# These are security-critical invariants: do not weaken them.
# ---------------------------------------------------------------------------


def test_psk_repr_redacts_the_key():
    c = PSKCredentials(psk="hunter2")
    r = repr(c)
    assert "hunter2" not in r
    assert "[REDACTED]" in r
    assert r == "PSKCredentials(psk=[REDACTED])"


def test_psk_str_redacts_the_key():
    # str() falls back to __repr__ when __str__ is not defined; the
    # redaction must hold for every common stringification route.
    c = PSKCredentials(psk="hunter2")
    assert "hunter2" not in str(c)
    assert "hunter2" not in f"{c}"
    assert "hunter2" not in "%s" % c
    assert "hunter2" not in "{!s}".format(c)


def test_password_repr_redacts_only_password():
    c = PasswordCredentials(
        username="alice",
        password="hunter2",
        effective_user="bob",
    )
    r = repr(c)
    assert "hunter2" not in r
    assert "[REDACTED]" in r
    # Non-secret fields remain visible for debugging.
    assert "alice" in r
    assert "bob" in r
    assert (
        r == "PasswordCredentials(username='alice', password=[REDACTED], "
        "effective_user='bob')"
    )


def test_password_repr_with_none_effective_user():
    c = PasswordCredentials(username="alice", password="hunter2")
    r = repr(c)
    assert "hunter2" not in r
    assert "None" in r


def test_password_str_redacts_password():
    c = PasswordCredentials(username="alice", password="hunter2")
    assert "hunter2" not in str(c)
    assert "hunter2" not in f"{c}"
    assert "hunter2" not in "%s" % c


def test_private_key_repr_redacts_text_but_shows_length():
    key = "-----BEGIN PRIVATE KEY-----\nMIIBVQIBAD..."
    c = PrivateKeyCredentials(key_text=key)
    r = repr(c)
    assert "MIIBVQIBAD" not in r
    assert "BEGIN PRIVATE KEY" not in r
    assert "[REDACTED]" in r
    assert f"{len(key)} chars" in r


def test_private_key_str_redacts_text():
    key = "supersecretkey"
    c = PrivateKeyCredentials(key_text=key)
    assert "supersecretkey" not in str(c)
    assert "supersecretkey" not in f"{c}"
    assert "supersecretkey" not in "%s" % c


def test_credential_repr_does_not_change_equality_or_hashing():
    # Redacting __repr__ must not affect the value-based equality and
    # hashing that cache keys depend on.
    a = PSKCredentials(psk="k")
    b = PSKCredentials(psk="k")
    assert a == b
    assert hash(a) == hash(b)


def test_credentials_containing_container_does_not_leak():
    # A common leak vector: logging a dict or list that contains a
    # credential. Both container reprs go through the credential's
    # __repr__, so the redaction must propagate.
    c = PSKCredentials(psk="hunter2")
    container = {"creds": c, "list": [c]}
    r = repr(container)
    assert "hunter2" not in r
    assert "[REDACTED]" in r
