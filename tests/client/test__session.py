import asyncio
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import grpc
import pyarrow as pa
import pydantic
import pytest

from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)

# Shared fixtures for community session configs. The on-disk schema
# requires `auth.credentials`; tests below mostly exercise error paths
# rather than auth selection, so they default to anonymous.
_ANON_AUTH = {"auth": {"credentials": {"type": "anonymous"}}}


def _cfg(**extra) -> dict:
    """Return a minimal-but-valid community session config."""
    return {**_ANON_AUTH, **extra}


def _from_config(cfg, *, name: str = "test", timeout_seconds: float | None = None):
    """Test helper: validate ``cfg`` to a ``CommunitySessionConfig`` then connect.

    The production API now accepts the typed declaration directly via
    :meth:`CoreSession.from_session_config`. Most tests in this file
    were authored against the prior dict-based ``from_config`` entry
    point; this helper preserves that calling style by validating
    ``cfg`` first and routing through the new typed entry point.

    The legacy ``timeout_seconds`` parameter (now removed from the
    production API) is preserved here as a test convenience: when set,
    it is folded into a per-call ``CommunityClientTimeouts`` override so
    timeout-failure tests can still trigger fast.
    """
    from deephaven_mcp.client import CommunityClientTimeouts
    from deephaven_mcp.sessions import CommunitySessionConfig

    if isinstance(cfg, dict):
        payload = {"name": name, **cfg}
    else:
        payload = cfg  # type: ignore[assignment]
    session_config = CommunitySessionConfig.model_validate(payload)
    if timeout_seconds is None:
        timeouts = CommunityClientTimeouts()
    else:
        timeouts = CommunityClientTimeouts(
            session_connect_timeout_seconds=timeout_seconds
        )
    return CoreSession.from_session_config(session_config, timeouts)


# Patch pydeephaven Table, InputTable, and Query with dummy types for isinstance checks
import types as _types

mock_table_mod = _types.ModuleType("pydeephaven.table")
mock_query_mod = _types.ModuleType("pydeephaven.query")


class DummyTable:
    pass


class DummyInputTable:
    pass


class DummyQuery:
    pass


class DummyPDHSession:
    def __init__(self, *args, **kwargs):
        pass


mock_table_mod.Table = DummyTable
mock_table_mod.InputTable = DummyInputTable
mock_query_mod.Query = DummyQuery
sys.modules["pydeephaven.table"] = mock_table_mod
sys.modules["pydeephaven.query"] = mock_query_mod
from pydeephaven.query import Query
from pydeephaven.table import InputTable, Table

from deephaven_mcp._exceptions import (
    ConfigurationError,
    DeephavenConnectionError,
    QueryError,
    ResourceError,
    SessionError,
)
from deephaven_mcp.client._session import CoreSession


class DummySession:
    def close(self):
        pass

    def is_alive(self):
        return True

    @property
    def tables(self):
        return ["foo", "bar"]

    def open_table(self, name):
        if name == "missing":
            raise KeyError("not found")
        if name == "conn":
            raise ConnectionError("fail")
        if name == "exc":
            raise Exception("fail")
        return Table()

    def empty_table(self, size):
        if size < 0:
            raise Exception("fail")
        if size == 42:
            raise ConnectionError("fail")
        return Table()

    def import_table(self, data):
        if data == "bad":
            raise Exception("fail")
        if data == "conn":
            raise ConnectionError("fail")
        return Table()

    def input_table(self, schema, init_table, key_cols, blink_table):
        if schema == "bad":
            raise ValueError("fail")
        if schema == "conn":
            raise ConnectionError("fail")
        if schema == "exc":
            raise Exception("fail")
        return InputTable()

    def bind_table(self, name, table):
        if name == "conn":
            raise ConnectionError("fail")
        if name == "exc":
            raise Exception("fail")

    def query(self, table):
        if table == "conn":
            raise ConnectionError("fail")
        if table == "exc":
            raise Exception("fail")
        return Query()

    def run_script(self, script, systemic=None):
        if script == "conn":
            raise ConnectionError("fail")
        if script == "exc":
            raise Exception("fail")


@pytest.fixture
def core_session():
    return CoreSession(DummySession(), programming_language="python")


@pytest.mark.asyncio
async def test_close_success(core_session):
    await core_session.close()


@pytest.mark.asyncio
async def test_close_connection_error(core_session):
    core_session.wrapped.close = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.close()


@pytest.mark.asyncio
async def test_close_other_error(core_session):
    core_session.wrapped.close = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(SessionError):
        await core_session.close()


@pytest.mark.asyncio
async def test_is_alive_success(core_session):
    assert await core_session.is_alive()


from unittest.mock import PropertyMock


@pytest.mark.asyncio
async def test_is_alive_connection_error(core_session):
    type(core_session.wrapped).is_alive = PropertyMock(
        side_effect=ConnectionError("fail")
    )
    with pytest.raises(DeephavenConnectionError):
        await core_session.is_alive()


from unittest.mock import PropertyMock


@pytest.mark.asyncio
async def test_is_alive_other_error(core_session):
    type(core_session.wrapped).is_alive = PropertyMock(side_effect=Exception("fail"))
    with pytest.raises(SessionError):
        await core_session.is_alive()


@pytest.mark.asyncio
async def test_tables_success(core_session):
    assert await core_session.tables() == ["foo", "bar"]


@pytest.mark.asyncio
async def test_tables_connection_error(core_session):
    with patch.object(
        type(core_session.wrapped), "tables", new_callable=PropertyMock
    ) as mock_tables:
        mock_tables.side_effect = ConnectionError("fail")
        with pytest.raises(DeephavenConnectionError):
            await core_session.tables()


@pytest.mark.asyncio
async def test_tables_other_error(core_session):
    with patch.object(
        type(core_session.wrapped), "tables", new_callable=PropertyMock
    ) as mock_tables:
        mock_tables.side_effect = Exception("fail")
        with pytest.raises(QueryError):
            await core_session.tables()


@pytest.mark.asyncio
async def test_open_table_success(core_session):
    assert isinstance(await core_session.open_table("foo"), Table)


@pytest.mark.asyncio
async def test_open_table_resource_error(core_session):
    with pytest.raises(ResourceError):
        await core_session.open_table("missing")


@pytest.mark.asyncio
async def test_open_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.open_table("conn")


@pytest.mark.asyncio
async def test_open_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.open_table("exc")


@pytest.mark.asyncio
async def test_empty_table_success(core_session):
    assert isinstance(await core_session.empty_table(1), Table)


@pytest.mark.asyncio
async def test_empty_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.empty_table(42)


@pytest.mark.asyncio
async def test_empty_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.empty_table(-1)


@pytest.mark.asyncio
async def test_import_table_success(core_session):
    assert isinstance(await core_session.import_table(pa.table({"a": [1]})), Table)


@pytest.mark.asyncio
async def test_import_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.import_table("conn")


@pytest.mark.asyncio
async def test_import_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.import_table("bad")


@pytest.mark.asyncio
async def test_input_table_success(core_session):
    assert isinstance(await core_session.input_table(), InputTable)


@pytest.mark.asyncio
async def test_input_table_value_error(core_session):
    with pytest.raises(ValueError):
        await core_session.input_table(schema="bad")


@pytest.mark.asyncio
async def test_input_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.input_table(schema="conn")


@pytest.mark.asyncio
async def test_input_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.input_table(schema="exc")


@pytest.mark.asyncio
async def test_bind_table_success(core_session):
    await core_session.bind_table("foo", Table())


@pytest.mark.asyncio
async def test_bind_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.bind_table("conn", Table())


@pytest.mark.asyncio
async def test_bind_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.bind_table("exc", Table())


@pytest.mark.asyncio
async def test_query_success(core_session):
    assert isinstance(await core_session.query(Table()), Query)


@pytest.mark.asyncio
async def test_query_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.query("conn")


@pytest.mark.asyncio
async def test_query_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.query("exc")


# __str__
def test_str(core_session):
    assert str(core_session) == str(core_session.wrapped)


def test_str_dunder_direct(core_session):
    # Directly call the dunder method to force coverage
    assert core_session.__str__() == str(core_session.wrapped)


# Minimal, non-mocked test to ensure coverage for __str__ and __repr__
class DummyStrRepr:
    def __str__(self):
        return "dummy-str"

    def __repr__(self):
        return "dummy-repr"


def test_str_minimal():
    cs = CoreSession(DummyStrRepr(), programming_language="python")
    assert str(cs) == "dummy-str"
    assert cs.__str__() == "dummy-str"


def test_repr_minimal():
    cs = CoreSession(DummyStrRepr(), programming_language="python")
    assert repr(cs) == "dummy-repr"
    assert cs.__repr__() == "dummy-repr"


# ========== CoreSession.from_config tests (migrated from test_core_session.py) ========== #


@pytest.mark.asyncio
async def test_core_from_config_session_creation_error(monkeypatch):
    class FailingPDHSession:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("session creation failed")

    monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)
    with pytest.raises(SessionCreationError) as exc_info:
        await _from_config(_cfg(host="localhost"))
    assert "Failed to create Deephaven Community (Core) Session" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_timeout(monkeypatch):
    """Test that from_config() raises DeephavenConnectionError on timeout."""
    import time

    class SlowPDHSession:
        def __init__(self, *args, **kwargs):
            time.sleep(0.05)

    monkeypatch.setattr("deephaven_mcp.client._session.Session", SlowPDHSession)
    with pytest.raises(DeephavenConnectionError) as exc_info:
        await _from_config(_cfg(host="localhost"), timeout_seconds=0.01)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_session_error_logging_configuration_constants(monkeypatch, caplog):
    """Test error logging for 'failed to get the configuration constants' error."""

    class FailingPDHSession:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("failed to get the configuration constants")

    monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

    with pytest.raises(SessionCreationError):
        await _from_config(_cfg(host="localhost"))

    # Check that specific error guidance was logged
    assert (
        "[CoreSession:_log_session_creation_error_details] This error indicates a connection issue when trying to connect to the server."
        in caplog.text
    )
    assert (
        "[CoreSession:_log_session_creation_error_details] Verify that: 1) Server address and port are correct"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_core_session_error_logging_certificate_errors(monkeypatch, caplog):
    """Test error logging for certificate/TLS related errors."""
    test_cases = [
        "SSL certificate error",
        "TLS handshake failed",
        "certificate expired",
        "PKIX path building failed",
        "CERT_AUTHORITY_INVALID",
        "CERT_COMMON_NAME_INVALID",
    ]

    for error_msg in test_cases:
        caplog.clear()

        class FailingPDHSession:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(error_msg)

        monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

        with pytest.raises(SessionCreationError):
            await _from_config(_cfg(host="localhost"))

        # Check that TLS/SSL error guidance was logged
        assert (
            "[CoreSession:_log_session_creation_error_details] This error indicates a TLS/SSL certificate issue."
            in caplog.text
        )
        assert (
            "[CoreSession:_log_session_creation_error_details] Verify that: 1) Server certificate is valid and not expired"
            in caplog.text
        )


@pytest.mark.asyncio
async def test_core_session_error_logging_authentication_errors(monkeypatch, caplog):
    """Test error logging for authentication related errors."""
    test_cases = [
        "authentication failed",
        "unauthorized access",
        "invalid credentials provided",
        "invalid token supplied",
        "token expired",
        "access denied",
    ]

    for error_msg in test_cases:
        caplog.clear()

        class FailingPDHSession:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(error_msg)

        monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

        with pytest.raises(SessionCreationError):
            await _from_config(_cfg(host="localhost"))

        # Check that authentication error guidance was logged
        assert (
            "[CoreSession:_log_session_creation_error_details] This error indicates an authentication issue."
            in caplog.text
        )
        assert (
            "[CoreSession:_log_session_creation_error_details] Verify that: 1) Authentication credentials are correct"
            in caplog.text
        )


@pytest.mark.asyncio
async def test_core_session_error_logging_network_errors(monkeypatch, caplog):
    """Test error logging for network connectivity errors."""
    test_cases = [
        "connection timeout",
        "connection refused",
        "connection reset by peer",
        "network unreachable",
    ]

    for error_msg in test_cases:
        caplog.clear()

        class FailingPDHSession:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(error_msg)

        monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

        with pytest.raises(SessionCreationError):
            await _from_config(_cfg(host="localhost"))

        # Check that network connectivity error guidance was logged
        assert (
            "[CoreSession:_log_session_creation_error_details] This error indicates a network connectivity issue."
            in caplog.text
        )
        assert (
            "[CoreSession:_log_session_creation_error_details] Verify that: 1) Server is running and accessible"
            in caplog.text
        )


@pytest.mark.asyncio
async def test_core_session_error_logging_dns_errors(monkeypatch, caplog):
    """Test error logging for DNS resolution errors."""
    test_cases = [
        "name resolution failed",
        "host not found",
        "nodename nor servname provided",
    ]

    for error_msg in test_cases:
        caplog.clear()

        class FailingPDHSession:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(error_msg)

        monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

        with pytest.raises(SessionCreationError):
            await _from_config(_cfg(host="localhost"))

        # Check that DNS resolution error guidance was logged
        assert (
            "[CoreSession:_log_session_creation_error_details] This error indicates a DNS resolution issue."
            in caplog.text
        )
        assert (
            "[CoreSession:_log_session_creation_error_details] Verify that: 1) Hostname is correct and resolvable"
            in caplog.text
        )


@pytest.mark.asyncio
async def test_core_session_error_logging_port_binding_errors(monkeypatch, caplog):
    """Test error logging for port binding related errors."""
    test_cases = [
        "address already in use",
        "bind failed",
        "port already in use",
    ]

    for error_msg in test_cases:
        caplog.clear()

        class FailingPDHSession:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(error_msg)

        monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

        with pytest.raises(SessionCreationError):
            await _from_config(_cfg(host="localhost"))

        # Check that port binding error guidance was logged
        assert (
            "[CoreSession:_log_session_creation_error_details] This error indicates a port binding issue."
            in caplog.text
        )
        assert (
            "[CoreSession:_log_session_creation_error_details] Verify that: 1) Port is not already in use by another process, 2) You have permission to bind to the port, 3) Try a different port number"
            in caplog.text
        )


@pytest.mark.asyncio
async def test_core_session_error_logging_unknown_error(monkeypatch, caplog):
    """Test that unknown errors don't trigger specific guidance."""

    class FailingPDHSession:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("some unknown error message")

    monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

    with pytest.raises(SessionCreationError):
        await _from_config(_cfg(host="localhost"))

    # Check that no specific error guidance was logged for unknown errors
    assert (
        "[CoreSession:_log_session_creation_error_details] This error indicates a"
        not in caplog.text
    )
    assert (
        "[CoreSession:_log_session_creation_error_details] Verify that:"
        not in caplog.text
    )


@pytest.mark.asyncio
async def test_core_from_config_invalid_not_dict(monkeypatch):
    # Config is not a dict
    with pytest.raises(Exception) as exc_info:
        await _from_config("not a dict")  # type: ignore[arg-type]
    # Pydantic surfaces this as either a validation error mentioning
    # the dict type or an AttributeError on the str input.
    msg = str(exc_info.value)
    assert "dict" in msg or "attribute" in msg


@pytest.mark.asyncio
async def test_core_from_config_invalid_unknown_field(monkeypatch):
    # Config with unknown field
    config = _cfg(host="localhost", bad_field=123)
    with pytest.raises(Exception) as exc_info:
        await _from_config(config)
    msg = str(exc_info.value)
    assert "bad_field" in msg
    assert "Extra inputs" in msg or "not permitted" in msg


@pytest.mark.asyncio
async def test_core_from_config_rejects_legacy_token_env_var(monkeypatch):
    # Legacy ``token_env_var`` shadow field is no longer accepted; env-var
    # indirection lives in the JSON as ``"${env:NAME}"`` and is resolved
    # by the templating engine at file-load time.
    config = {
        "host": "localhost",
        "auth": {
            "credentials": {
                "type": "psk",
                "token": "tok",
                "token_env_var": "ENV",
            }
        },
    }
    with pytest.raises(Exception) as exc_info:
        await _from_config(config)
    assert "Extra inputs" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_invalid_type(monkeypatch):
    # Wrong type for port
    config = _cfg(host="localhost", port="not an int")
    with pytest.raises(Exception) as exc_info:
        await _from_config(config)
    assert "type" in str(exc_info.value) or "int" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_valid_minimal(monkeypatch):
    config = _cfg(host="localhost")
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await _from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
async def test_core_from_config_success(monkeypatch, tmp_path):
    """Test CoreSession.from_config creates a session with all parameters."""
    root_pem = tmp_path / "root.pem"
    root_pem.write_text("-----BEGIN CERT-----\nROOT\n-----END CERT-----\n")
    config = {
        "host": "localhost",
        "port": 10000,
        "never_timeout": True,
        "programming_language": "Python",
        "tls": {"root_certs": root_pem.read_text()},
        "auth": {
            "credentials": {
                "type": "psk",
                "token": "tok",
            }
        },
    }
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await _from_config(config)
    assert isinstance(session, CoreSession)
    assert isinstance(session.wrapped, DummyPDHSession)


@pytest.mark.asyncio
async def test_core_from_config_rejects_legacy_root_certs_path():
    """Legacy ``root_certs_path`` shadow field is no longer accepted.

    File indirection is expressed via ``"${file:/path}"`` in the
    source JSON and resolved by :mod:`deephaven_mcp.config._templating`
    before the model sees the value.
    """
    from pydantic import ValidationError

    config = _cfg(tls={"root_certs_path": "/no/such/file/root.pem"})
    with pytest.raises(ValidationError, match="Extra inputs"):
        await _from_config(config)


@pytest.mark.asyncio
async def test_core_from_config_psk_token_from_env_var(monkeypatch):
    # Templating happens at file-load time. The typed model accepts
    # the already-resolved token directly here.
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    config = {"auth": {"credentials": {"type": "psk", "token": "token_from_env"}}}
    session = await _from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
async def test_core_from_config_psk_env_var_field_rejected(monkeypatch):
    """Legacy ``token_env_var`` shadow field is no longer accepted.

    The new flow expects callers to template ``"${env:NAME}"`` into the
    ``token`` value at file-load time; passing the old shadow field is
    a config-schema error.
    """
    env_var = "MY_MISSING_TOKEN_VAR"
    monkeypatch.delenv(env_var, raising=False)
    config = {"auth": {"credentials": {"type": "psk", "token_env_var": env_var}}}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    # Validation fails because ``token_env_var`` is now an unknown
    # field; the strict schema surfaces it as a
    # :class:`pydantic.ValidationError`.
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match=env_var):
        await _from_config(config)


@pytest.mark.asyncio
async def test_core_from_config_psk_token_inline(monkeypatch):
    expected = "token_from_config_direct"
    config = {"auth": {"credentials": {"type": "psk", "token": expected}}}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await _from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
async def test_core_from_config_anonymous(monkeypatch):
    config = _cfg(host="localhost")
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await _from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cfg",
    [
        _cfg(host="localhost"),
        _cfg(host="localhost", port=123),
        {
            "host": "localhost",
            "auth": {"credentials": {"type": "psk", "token": "tok"}},
        },
        _cfg(host="localhost", never_timeout=True, programming_language="Groovy"),
    ],
)
async def test_core_from_config_defaults(monkeypatch, cfg):
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await _from_config(cfg)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
async def test_core_from_config_tls_root_certs_loaded(monkeypatch, tmp_path):
    """A populated tls.root_certs (decoded text) is forwarded as encoded
    bytes to pydeephaven; mTLS client material is covered by
    test_core_from_config_with_mtls below."""
    root_pem = tmp_path / "ca.pem"
    root_pem.write_text("-----BEGIN CERT-----\nCA\n-----END CERT-----\n")

    captured_kwargs: dict[str, object] = {}

    class CapturingSession:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("deephaven_mcp.client._session.Session", CapturingSession)

    config = _cfg(host="localhost", tls={"root_certs": root_pem.read_text()})
    session = await _from_config(config)
    assert isinstance(session, CoreSession)
    assert captured_kwargs["use_tls"] is True
    assert captured_kwargs["tls_root_certs"] == root_pem.read_bytes()
    assert captured_kwargs["client_cert_chain"] is None
    assert captured_kwargs["client_private_key"] is None


@pytest.mark.asyncio
async def test_core_from_config_with_mtls(monkeypatch, tmp_path):
    """tls.client_certificate is read at load time and forwarded as
    cert_chain / private_key bytes to pydeephaven."""
    cert = tmp_path / "client.pem"
    key = tmp_path / "client.key"
    cert.write_text("-----BEGIN CERT-----\nCLIENT\n-----END CERT-----\n")
    key.write_text("-----BEGIN KEY-----\nKEY\n-----END KEY-----\n")

    captured_kwargs: dict[str, object] = {}

    class CapturingSession:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("deephaven_mcp.client._session.Session", CapturingSession)

    config = _cfg(
        host="localhost",
        tls={
            "client_certificate": {
                "cert_chain": cert.read_text(),
                "private_key": key.read_text(),
            }
        },
    )
    session = await _from_config(config)
    assert isinstance(session, CoreSession)
    assert captured_kwargs["use_tls"] is True
    assert captured_kwargs["client_cert_chain"] == cert.read_bytes()
    assert captured_kwargs["client_private_key"] == key.read_bytes()
    assert captured_kwargs["tls_root_certs"] is None


@pytest.mark.asyncio
async def test_core_from_config_no_tls_means_plaintext(monkeypatch):
    """Absence of a tls block disables TLS entirely."""
    captured_kwargs: dict[str, object] = {}

    class CapturingSession:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("deephaven_mcp.client._session.Session", CapturingSession)
    await _from_config(_cfg(host="localhost"))
    assert captured_kwargs["use_tls"] is False
    assert captured_kwargs["tls_root_certs"] is None
    assert captured_kwargs["client_cert_chain"] is None
    assert captured_kwargs["client_private_key"] is None


@pytest.mark.asyncio
async def test_time_table_success(core_session):
    core_session.wrapped.time_table = MagicMock(return_value=Table())
    assert isinstance(await core_session.time_table("PT1S"), Table)


@pytest.mark.asyncio
async def test_time_table_connection_error(core_session):
    core_session.wrapped.time_table = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.time_table("PT1S")


@pytest.mark.asyncio
async def test_time_table_other_error(core_session):
    core_session.wrapped.time_table = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.time_table("PT1S")


@pytest.mark.asyncio
async def test_merge_tables_success(core_session):
    core_session.wrapped.merge_tables = MagicMock(return_value=Table())
    assert isinstance(await core_session.merge_tables([Table(), Table()]), Table)


@pytest.mark.asyncio
async def test_merge_tables_connection_error(core_session):
    core_session.wrapped.merge_tables = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.merge_tables([Table()])


@pytest.mark.asyncio
async def test_merge_tables_other_error(core_session):
    core_session.wrapped.merge_tables = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.merge_tables([Table()])


@pytest.mark.asyncio
async def test_run_script_success(core_session):
    core_session.wrapped.run_script = MagicMock()
    await core_session.run_script("print('hi')")


@pytest.mark.asyncio
async def test_run_script_connection_error(core_session):
    core_session.wrapped.run_script = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.run_script("print('hi')")


@pytest.mark.asyncio
async def test_run_script_other_error(core_session):
    core_session.wrapped.run_script = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.run_script("print('hi')")


# --- Programming Language Property Tests ---


def test_base_session_programming_language():
    """Test that BaseSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import BaseSession

    # Create a session with programming_language specified
    session = BaseSession(DummySession(), programming_language="python")

    # Verify the programming_language property returns the specified value
    assert session.programming_language == "python"


def test_base_session_programming_language_custom():
    """Test that BaseSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import BaseSession

    # Create a session with a custom programming_language using a unique test value
    test_lang = "test_unique_language_xyz123"
    session = BaseSession(DummySession(), programming_language=test_lang)

    # Verify the programming_language property returns the custom value
    assert session.programming_language == test_lang


def test_core_session_programming_language():
    """Test that CoreSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import CoreSession

    # Create a session with programming_language specified using a unique test value
    test_lang = "test_core_lang_abc456"
    session = CoreSession(DummySession(), programming_language=test_lang)

    # Verify the programming_language property returns the specified value
    assert session.programming_language == test_lang


def test_core_session_programming_language_custom():
    """Test that CoreSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import CoreSession

    # Create a session with a custom programming_language using a unique test value
    test_lang = "test_custom_lang_def789"
    session = CoreSession(DummySession(), programming_language=test_lang)

    # Verify the programming_language property returns the custom value
    assert session.programming_language == test_lang


def test_core_session_from_config_programming_language():
    """Test that CoreSession.from_config sets programming_language from the session config."""
    from deephaven_mcp.client._session import CoreSession

    # Mock the PDHSession class
    with patch("deephaven_mcp.client._session.Session", DummyPDHSession):
        # Create a config with a non-default programming_language
        config = _cfg(host="localhost", port=10000, programming_language="Groovy")

        # Create a session using from_config
        session = asyncio.run(_from_config(config))

        # Verify the programming_language property matches the input
        assert session.programming_language == "Groovy"


def test_core_session_from_config_programming_language_lowercase_rejected():
    """Lowercase ``"groovy"`` fails config validation: the vocabulary is exact-case."""
    from deephaven_mcp.sessions import CommunitySessionConfig

    config = _cfg(host="localhost", port=10000, programming_language="groovy")
    with pytest.raises(pydantic.ValidationError):
        CommunitySessionConfig.model_validate({"name": "test", **config})


def test_core_plus_session_programming_language():
    """Test that CorePlusSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import CorePlusSession

    # Create a session with programming_language specified
    session = CorePlusSession(DummyDndSession(), programming_language="python")

    # Verify the programming_language property returns the specified value
    assert session.programming_language == "python"


def test_core_plus_session_programming_language_custom():
    """Test that CorePlusSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import CorePlusSession

    # Create a session with a custom programming_language
    session = CorePlusSession(DummyDndSession(), programming_language="groovy")

    # Verify the programming_language property returns the custom value
    assert session.programming_language == "groovy"


# --- Enterprise / CorePlusSession tests ---
from deephaven_mcp.client._session import CorePlusSession


class DummyDndSession:
    def pqinfo(self):
        return "pqinfo_obj"

    def historical_table(self, namespace, table_name):
        if namespace == "conn":
            raise ConnectionError("fail")
        if namespace == "missing":
            raise KeyError("not found")
        if namespace == "exc":
            raise Exception("fail")
        return Table()

    def live_table(self, namespace, table_name):
        if namespace == "conn":
            raise ConnectionError("fail")
        if namespace == "missing":
            raise KeyError("not found")
        if namespace == "exc":
            raise Exception("fail")
        return Table()

    def catalog_table(self):
        if getattr(self, "should_succeed", False):
            return Table()
        raise Exception("fail")


class DummyCorePlusQueryInfo:
    def __init__(self, obj):
        self.obj = obj


# Patch CorePlusQueryInfo in module namespace
import deephaven_mcp.client._session as session_mod

session_mod.CorePlusQueryInfo = DummyCorePlusQueryInfo


@pytest.fixture
def core_plus_session():
    cps = CorePlusSession(DummyDndSession(), programming_language="python")
    cps._session = DummyDndSession()  # patch only _session for enterprise methods
    return cps


def test_core_plus_session_init():
    cps = CorePlusSession(DummyDndSession(), programming_language="python")
    cps._session = DummyDndSession()
    assert isinstance(cps, CorePlusSession)


@pytest.mark.asyncio
async def test_pqinfo_success(core_plus_session):
    info = await core_plus_session.pqinfo()
    assert isinstance(info, DummyCorePlusQueryInfo)


@pytest.mark.asyncio
async def test_pqinfo_connection_error(core_plus_session):
    core_plus_session.wrapped.pqinfo = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.pqinfo()


@pytest.mark.asyncio
async def test_pqinfo_other_error(core_plus_session):
    core_plus_session.wrapped.pqinfo = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_plus_session.pqinfo()


@pytest.mark.asyncio
async def test_historical_table_success(core_plus_session):
    table = await core_plus_session.historical_table("ns", "tbl")
    assert isinstance(table, Table)


@pytest.mark.asyncio
async def test_historical_table_connection_error(core_plus_session):
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.historical_table("conn", "tbl")


@pytest.mark.asyncio
async def test_historical_table_key_error(core_plus_session):
    with pytest.raises(ResourceError):
        await core_plus_session.historical_table("missing", "tbl")


@pytest.mark.asyncio
async def test_historical_table_other_error(core_plus_session):
    with pytest.raises(QueryError):
        await core_plus_session.historical_table("exc", "tbl")


@pytest.mark.asyncio
async def test_live_table_success(core_plus_session):
    table = await core_plus_session.live_table("ns", "tbl")
    assert isinstance(table, Table)


@pytest.mark.asyncio
async def test_live_table_connection_error(core_plus_session):
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.live_table("conn", "tbl")


@pytest.mark.asyncio
async def test_live_table_key_error(core_plus_session):
    with pytest.raises(ResourceError):
        await core_plus_session.live_table("missing", "tbl")


@pytest.mark.asyncio
async def test_live_table_other_error(core_plus_session):
    with pytest.raises(QueryError):
        await core_plus_session.live_table("exc", "tbl")


@pytest.mark.asyncio
async def test_catalog_table_success(core_plus_session):
    # Patch to return a Table for success case
    core_plus_session.wrapped.should_succeed = True
    table = await core_plus_session.catalog_table()
    assert isinstance(table, Table)


@pytest.mark.asyncio
async def test_catalog_table_connection_error(core_plus_session):
    # Patch to raise ConnectionError first
    core_plus_session.wrapped.catalog_table = MagicMock(
        side_effect=ConnectionError("fail")
    )
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.catalog_table()
    # Patch to raise generic Exception
    core_plus_session.wrapped.catalog_table = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_plus_session.catalog_table()


# ---------------------------------------------------------------------------
# _credentials_to_pydeephaven_auth — direct unit tests
# ---------------------------------------------------------------------------


def test_credentials_to_pydeephaven_auth_anonymous():
    from deephaven_mcp.auth.credentials import AnonymousCredentials
    from deephaven_mcp.client._session import _credentials_to_pydeephaven_auth

    assert _credentials_to_pydeephaven_auth(AnonymousCredentials()) == (
        "Anonymous",
        "",
    )


def test_credentials_to_pydeephaven_auth_psk():
    from deephaven_mcp.auth.credentials import PSKCredentials
    from deephaven_mcp.client._session import _credentials_to_pydeephaven_auth

    assert _credentials_to_pydeephaven_auth(PSKCredentials(token="hunter2")) == (
        "io.deephaven.authentication.psk.PskAuthenticationHandler",
        "hunter2",
    )


def test_credentials_to_pydeephaven_auth_password_uses_basic_format():
    from deephaven_mcp.auth.credentials import PasswordCredentials
    from deephaven_mcp.client._session import _credentials_to_pydeephaven_auth

    creds = PasswordCredentials(username="alice", password="pw")
    assert _credentials_to_pydeephaven_auth(creds) == ("Basic", "alice:pw")


def test_credentials_to_pydeephaven_auth_custom_passes_through():
    from deephaven_mcp.auth.credentials import CustomTokenCredentials
    from deephaven_mcp.client._session import _credentials_to_pydeephaven_auth

    creds = CustomTokenCredentials(auth_type="com.example.Auth", auth_token="opaque")
    assert _credentials_to_pydeephaven_auth(creds) == (
        "com.example.Auth",
        "opaque",
    )


def test_credentials_to_pydeephaven_auth_rejects_private_key():
    from deephaven_mcp.auth.credentials import PrivateKeyCredentials
    from deephaven_mcp.client._session import _credentials_to_pydeephaven_auth

    with pytest.raises(ConfigurationError, match="Private-key credentials"):
        _credentials_to_pydeephaven_auth(PrivateKeyCredentials(key_text="k"))


def test_credentials_to_pydeephaven_auth_rejects_unknown_type():
    """Defensive guard for any future Credentials subclass."""
    from deephaven_mcp.auth.credentials import Credentials
    from deephaven_mcp.client._session import _credentials_to_pydeephaven_auth

    class MysteryCreds(Credentials):
        pass

    with pytest.raises(ConfigurationError, match="Unsupported credential type"):
        _credentials_to_pydeephaven_auth(MysteryCreds())


# ---------------------------------------------------------------------------
# gRPC exception-detail surfacing (describe_exception_chain integration)
# ---------------------------------------------------------------------------


class _FakeGrpcCall(grpc.RpcError, grpc.Call):
    """Minimal real ``grpc.Call``/``grpc.RpcError`` double for chain tests."""

    def __init__(self, code, details):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

    def initial_metadata(self):
        return ()

    def trailing_metadata(self):
        return ()

    def is_active(self):
        return False

    def time_remaining(self):
        return None

    def cancel(self):
        return False

    def add_callback(self, _callback):
        return False


@pytest.mark.asyncio
async def test_historical_table_surfaces_grpc_detail(core_plus_session):
    grpc_err = _FakeGrpcCall(
        grpc.StatusCode.INVALID_ARGUMENT, "Column Foo has unsupported type"
    )
    dh_err = Exception("failed to finish FetchTableOp operation")
    dh_err.__cause__ = grpc_err
    core_plus_session.wrapped.historical_table = MagicMock(side_effect=dh_err)

    with pytest.raises(QueryError) as excinfo:
        await core_plus_session.historical_table("ns", "tbl")

    message = str(excinfo.value)
    assert "Failed to fetch historical table" in message
    assert "gRPC INVALID_ARGUMENT: Column Foo has unsupported type" in message


@pytest.mark.asyncio
async def test_live_table_surfaces_grpc_detail(core_plus_session):
    grpc_err = _FakeGrpcCall(
        grpc.StatusCode.INVALID_ARGUMENT, "Column Bar has unsupported type"
    )
    dh_err = Exception("failed to finish FetchTableOp operation")
    dh_err.__cause__ = grpc_err
    core_plus_session.wrapped.live_table = MagicMock(side_effect=dh_err)

    with pytest.raises(QueryError) as excinfo:
        await core_plus_session.live_table("ns", "tbl")

    message = str(excinfo.value)
    assert "Failed to fetch live table" in message
    assert "gRPC INVALID_ARGUMENT: Column Bar has unsupported type" in message
