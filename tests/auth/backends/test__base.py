"""Tests for deephaven_mcp.auth.backends._base (ABC + AuthenticationError)."""

import pytest

from deephaven_mcp.auth.backends import AuthBackend, AuthenticationError
from deephaven_mcp.auth.credentials import Principal, PSKCredentials


def test_authentication_error_is_exception():
    err = AuthenticationError("nope")
    assert isinstance(err, Exception)
    assert str(err) == "nope"


# ---------------------------------------------------------------------------
# Concrete-subclass-conforming fixture for testing inherited helpers.
# ---------------------------------------------------------------------------


class _ConformingBackend(AuthBackend):
    name = "conforming"

    async def authenticate(self, headers):
        return self._make_principal("alice")

    async def derive_credentials(self, principal, headers):
        return PSKCredentials(psk="x")

    def _challenge_scheme(self) -> str:
        return "Bearer"


class _BackendWithChallengeHeaders(AuthBackend):
    name = "with-headers"

    async def authenticate(self, headers):
        return None

    async def derive_credentials(self, principal, headers):
        raise NotImplementedError

    def _challenge_scheme(self) -> str:
        return "DeephavenTest"

    def _challenge_headers(self) -> tuple[str, ...]:
        return ("x-foo", "x-bar")


# ---------------------------------------------------------------------------
# AuthBackend cannot be instantiated directly.
# ---------------------------------------------------------------------------


def test_authbackend_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        AuthBackend()  # type: ignore[abstract]


def test_subclass_missing_authenticate_cannot_instantiate():
    class _Bad(AuthBackend):
        name = "bad"

        async def derive_credentials(self, principal, headers):
            return PSKCredentials(psk="x")

        def _challenge_scheme(self) -> str:
            return "X"

    with pytest.raises(TypeError, match="abstract"):
        _Bad()  # type: ignore[abstract]


def test_subclass_missing_derive_credentials_cannot_instantiate():
    class _Bad(AuthBackend):
        name = "bad"

        async def authenticate(self, headers):
            return None

        def _challenge_scheme(self) -> str:
            return "X"

    with pytest.raises(TypeError, match="abstract"):
        _Bad()  # type: ignore[abstract]


def test_subclass_missing_challenge_scheme_cannot_instantiate():
    class _Bad(AuthBackend):
        name = "bad"

        async def authenticate(self, headers):
            return None

        async def derive_credentials(self, principal, headers):
            return PSKCredentials(psk="x")

    with pytest.raises(TypeError, match="abstract"):
        _Bad()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# __init_subclass__ enforces a non-empty class-level `name`.
# ---------------------------------------------------------------------------


def test_concrete_subclass_without_name_rejected_at_class_definition():
    with pytest.raises(TypeError, match="name"):

        class _NoName(AuthBackend):
            async def authenticate(self, headers):
                return None

            async def derive_credentials(self, principal, headers):
                return PSKCredentials(psk="x")

            def _challenge_scheme(self) -> str:
                return "X"


def test_concrete_subclass_with_empty_name_rejected_at_class_definition():
    with pytest.raises(TypeError, match="name"):

        class _EmptyName(AuthBackend):
            name = ""

            async def authenticate(self, headers):
                return None

            async def derive_credentials(self, principal, headers):
                return PSKCredentials(psk="x")

            def _challenge_scheme(self) -> str:
                return "X"


def test_concrete_subclass_with_non_string_name_rejected():
    with pytest.raises(TypeError, match="name"):

        class _BadName(AuthBackend):
            name = 42  # type: ignore[assignment]

            async def authenticate(self, headers):
                return None

            async def derive_credentials(self, principal, headers):
                return PSKCredentials(psk="x")

            def _challenge_scheme(self) -> str:
                return "X"


def test_abstract_intermediate_subclass_skips_name_check():
    # An intermediate subclass that is itself abstract (does not implement
    # all abstractmethods) should NOT trigger the name check, so callers
    # can build mixin hierarchies.
    class _Mixin(AuthBackend):
        # Intentionally still abstract (no _challenge_scheme).
        async def authenticate(self, headers):
            return None

        async def derive_credentials(self, principal, headers):
            return PSKCredentials(psk="x")

    # Concrete leaf must declare name and implement remaining abstractmethod.
    class _Leaf(_Mixin):
        name = "leaf"

        def _challenge_scheme(self) -> str:
            return "X"

    leaf = _Leaf()
    assert leaf.name == "leaf"


# ---------------------------------------------------------------------------
# Default __init__ stores realm with the documented fallback.
# ---------------------------------------------------------------------------


def test_default_realm_used_when_none_passed():
    from deephaven_mcp.auth.backends._base import _DEFAULT_REALM

    b = _ConformingBackend()
    assert b.realm == _DEFAULT_REALM
    assert b.realm == "deephaven-mcp"


def test_explicit_realm_overrides_default():
    b = _ConformingBackend(realm="custom-realm")
    assert b.realm == "custom-realm"


# ---------------------------------------------------------------------------
# Default challenge() formatting.
# ---------------------------------------------------------------------------


def test_challenge_without_headers_omits_headers_clause():
    b = _ConformingBackend(realm="r")
    assert b.challenge() == 'Bearer realm="r"'


def test_challenge_with_headers_includes_headers_clause():
    b = _BackendWithChallengeHeaders()
    assert b.challenge() == (
        'DeephavenTest realm="deephaven-mcp", headers="x-foo, x-bar"'
    )


# ---------------------------------------------------------------------------
# _make_principal helper.
# ---------------------------------------------------------------------------


def test_make_principal_tags_backend_name_in_raw():
    b = _ConformingBackend()
    p = b._make_principal("alice")
    assert isinstance(p, Principal)
    assert p.subject == "alice"
    assert p.display_name == "alice"
    assert p.raw == {"backend": "conforming"}


def test_make_principal_uses_explicit_display_name():
    b = _ConformingBackend()
    p = b._make_principal("alice", display_name="Alice In Wonderland")
    assert p.subject == "alice"
    assert p.display_name == "Alice In Wonderland"


def test_make_principal_merges_extra_raw():
    b = _ConformingBackend()
    p = b._make_principal("alice", extra_raw={"effective_user": "bob"})
    assert p.raw == {"backend": "conforming", "effective_user": "bob"}


def test_make_principal_extra_raw_can_override_backend_key():
    # Documented behaviour: extra_raw keys take precedence if explicitly set.
    b = _ConformingBackend()
    p = b._make_principal("alice", extra_raw={"backend": "spoofed"})
    assert p.raw == {"backend": "spoofed"}


# ---------------------------------------------------------------------------
# Conforming subclass behaves end-to-end.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_conforming_subclass_authenticate_and_derive():
    b = _ConformingBackend()
    p = await b.authenticate({})
    assert p is not None
    assert p.subject == "alice"
    creds = await b.derive_credentials(p, {})
    assert isinstance(creds, PSKCredentials)


def test_conforming_subclass_isinstance_authbackend():
    assert isinstance(_ConformingBackend(), AuthBackend)


# ---------------------------------------------------------------------------
# _require_header helper.
# ---------------------------------------------------------------------------


def test_require_header_absent_returns_none():
    b = _ConformingBackend()
    assert b._require_header({}, "x-missing") is None


def test_require_header_present_returns_value():
    b = _ConformingBackend()
    assert b._require_header({"x-token": "abc"}, "x-token") == "abc"


def test_require_header_empty_raises():
    b = _ConformingBackend()
    with pytest.raises(AuthenticationError, match="x-token header must not be empty"):
        b._require_header({"x-token": ""}, "x-token")
