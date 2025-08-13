import asyncio
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def patch_load_bytes():
    with patch(
        "deephaven_mcp.client._session.load_bytes",
        new=AsyncMock(return_value=b"binary"),
    ):
        yield


import pyarrow as pa
import pytest

from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)

# Patch sys.modules for enterprise imports BEFORE any tested imports
mock_enterprise = types.ModuleType("deephaven_enterprise")
mock_sm = types.ModuleType("deephaven_enterprise.client.session_manager")
mock_sm.SessionManager = MagicMock()
sys.modules["deephaven_enterprise"] = mock_enterprise
sys.modules["deephaven_enterprise.client"] = types.ModuleType(
    "deephaven_enterprise.client"
)
sys.modules["deephaven_enterprise.client.session_manager"] = mock_sm
# Patch controller client as well for _protobuf.py import
mock_controller = types.ModuleType("deephaven_enterprise.client.controller")
mock_controller.ControllerClient = MagicMock()
sys.modules["deephaven_enterprise.client.controller"] = mock_controller

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
    core_session.wrapped.tables = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.tables()


@pytest.mark.asyncio
async def test_tables_other_error(core_session):
    core_session.wrapped.tables = MagicMock(side_effect=Exception("fail"))
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
        await CoreSession.from_config({"host": "localhost"})
    assert "Failed to create Deephaven Community (Core) Session" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_session_error_logging_configuration_constants(monkeypatch, caplog):
    """Test error logging for 'failed to get the configuration constants' error."""

    class FailingPDHSession:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("failed to get the configuration constants")

    monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

    with pytest.raises(SessionCreationError):
        await CoreSession.from_config({"host": "localhost"})

    # Check that specific error guidance was logged
    assert (
        "[Community] This error indicates a connection issue when trying to connect to the server."
        in caplog.text
    )
    assert (
        "[Community] Verify that: 1) Server address and port are correct" in caplog.text
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
            await CoreSession.from_config({"host": "localhost"})

        # Check that TLS/SSL error guidance was logged
        assert (
            "[Community] This error indicates a TLS/SSL certificate issue."
            in caplog.text
        )
        assert (
            "[Community] Verify that: 1) Server certificate is valid and not expired"
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
            await CoreSession.from_config({"host": "localhost"})

        # Check that authentication error guidance was logged
        assert (
            "[Community] This error indicates an authentication issue." in caplog.text
        )
        assert (
            "[Community] Verify that: 1) Authentication credentials are correct"
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
            await CoreSession.from_config({"host": "localhost"})

        # Check that network connectivity error guidance was logged
        assert (
            "[Community] This error indicates a network connectivity issue."
            in caplog.text
        )
        assert (
            "[Community] Verify that: 1) Server is running and accessible"
            in caplog.text
        )


@pytest.mark.asyncio
async def test_core_session_error_logging_port_binding_errors(monkeypatch, caplog):
    """Test error logging for port binding errors."""
    test_cases = [
        "address already in use",
        "bind failed on port",
        "port already in use",
    ]

    for error_msg in test_cases:
        caplog.clear()

        class FailingPDHSession:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(error_msg)

        monkeypatch.setattr("deephaven_mcp.client._session.Session", FailingPDHSession)

        with pytest.raises(SessionCreationError):
            await CoreSession.from_config({"host": "localhost"})

        # Check that port binding error guidance was logged
        assert "[Community] This error indicates a port binding issue." in caplog.text
        assert (
            "[Community] Verify that: 1) Port is not already in use by another process"
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
            await CoreSession.from_config({"host": "localhost"})

        # Check that DNS resolution error guidance was logged
        assert "[Community] This error indicates a DNS resolution issue." in caplog.text
        assert (
            "[Community] Verify that: 1) Hostname is correct and resolvable"
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
        await CoreSession.from_config({"host": "localhost"})

    # Check that no specific error guidance was logged for unknown errors
    assert "[Community] This error indicates a" not in caplog.text
    assert "[Community] Verify that:" not in caplog.text


@pytest.mark.asyncio
async def test_core_from_config_invalid_not_dict(monkeypatch):
    # Config is not a dict
    with pytest.raises(Exception) as exc_info:
        await CoreSession.from_config("not a dict")
    assert "dictionary" in str(exc_info.value) or "dict" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_invalid_unknown_field(monkeypatch):
    # Config with unknown field
    config = {"host": "localhost", "bad_field": 123}
    with pytest.raises(Exception) as exc_info:
        await CoreSession.from_config(config)
    assert "Unknown field 'bad_field'" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_invalid_mutually_exclusive(monkeypatch):
    # Both auth_token and auth_token_env_var set
    config = {"host": "localhost", "auth_token": "tok", "auth_token_env_var": "ENV"}
    with pytest.raises(Exception) as exc_info:
        await CoreSession.from_config(config)
    assert "both 'auth_token' and 'auth_token_env_var' are set" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_invalid_type(monkeypatch):
    # Wrong type for port
    config = {"host": "localhost", "port": "not an int"}
    with pytest.raises(Exception) as exc_info:
        await CoreSession.from_config(config)
    assert "type" in str(exc_info.value) or "int" in str(exc_info.value)


@pytest.mark.asyncio
async def test_core_from_config_valid_minimal(monkeypatch):
    config = {"host": "localhost"}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
async def test_core_from_config_success(monkeypatch):
    """Test CoreSession.from_config creates a session with all parameters."""
    config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "token",
        "auth_token": "tok",
        "never_timeout": True,
        "session_type": "python",
        "use_tls": True,
        "tls_root_certs": "/no/such/file/root.pem",
        "client_cert_chain": "/no/such/file/chain.pem",
        "client_private_key": "/no/such/file/key.pem",
    }
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)
    assert isinstance(session.wrapped, DummyPDHSession)


@pytest.mark.asyncio
async def test_core_from_config_tls_file_error(monkeypatch):
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        "deephaven_mcp.client._session.load_bytes",
        AsyncMock(side_effect=IOError("fail")),
    )
    config = {"tls_root_certs": "/bad/path"}
    with pytest.raises(IOError):
        await CoreSession.from_config(config)


@pytest.mark.asyncio
async def test_core_from_config_auth_token_from_env_var(monkeypatch):
    env_var = "MY_TEST_TOKEN_VAR"
    expected = "token_from_env"
    monkeypatch.setenv(env_var, expected)
    config = {"auth_token_env_var": env_var}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)
    monkeypatch.delenv(env_var)


@pytest.mark.asyncio
async def test_core_from_config_auth_token_env_var_not_set(monkeypatch, caplog):
    env_var = "MY_MISSING_TOKEN_VAR"
    monkeypatch.delenv(env_var, raising=False)
    config = {"auth_token_env_var": env_var}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)
    assert (
        f"Environment variable {env_var} specified for auth_token but not found. Using empty token."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_core_from_config_auth_token_from_config(monkeypatch):
    expected = "token_from_config_direct"
    config = {"auth_token": expected}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
async def test_core_from_config_no_auth_token(monkeypatch):
    config = {"host": "localhost"}
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cfg,expected",
    [
        (
            {"host": "localhost"},
            {
                "host": "localhost",
                "port": None,
                "auth_type": "Anonymous",
                "auth_token": "",
                "never_timeout": False,
                "session_type": "python",
                "use_tls": False,
                "tls_root_certs": None,
                "client_cert_chain": None,
                "client_private_key": None,
            },
        ),
        (
            {"host": "localhost", "port": 123},
            {
                "host": "localhost",
                "port": 123,
                "auth_type": "Anonymous",
                "auth_token": "",
                "never_timeout": False,
                "session_type": "python",
                "use_tls": False,
                "tls_root_certs": None,
                "client_cert_chain": None,
                "client_private_key": None,
            },
        ),
        (
            {"host": "localhost", "auth_type": "token", "auth_token": "tok"},
            {
                "host": "localhost",
                "port": None,
                "auth_type": "token",
                "auth_token": "tok",
                "never_timeout": False,
                "session_type": "python",
                "use_tls": False,
                "tls_root_certs": None,
                "client_cert_chain": None,
                "client_private_key": None,
            },
        ),
        (
            {"host": "localhost", "never_timeout": True, "session_type": "custom"},
            {
                "host": "localhost",
                "port": None,
                "auth_type": "Anonymous",
                "auth_token": "",
                "never_timeout": True,
                "session_type": "custom",
                "use_tls": False,
                "tls_root_certs": None,
                "client_cert_chain": None,
                "client_private_key": None,
            },
        ),
    ],
)
async def test_core_from_config_defaults(monkeypatch, cfg, expected):
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(cfg)
    actual = (
        session.wrapped.__dict__
        if hasattr(session.wrapped, "__dict__")
        else {k: getattr(session.wrapped, k, None) for k in expected}
    )
    assert isinstance(session, CoreSession)


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_core_from_config_tls_and_client_keys(monkeypatch):
    # All present
    config = {
        "host": "localhost",
        "tls_root_certs": "/no/such/file/a",
        "client_cert_chain": "/no/such/file/b",
        "client_private_key": "/no/such/file/c",
    }
    monkeypatch.setattr("deephaven_mcp.client._session.Session", DummyPDHSession)
    session = await CoreSession.from_config(config)
    assert isinstance(session, CoreSession)


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
    session = BaseSession(
        DummySession(), is_enterprise=False, programming_language="python"
    )

    # Verify the programming_language property returns the specified value
    assert session.programming_language == "python"


def test_base_session_programming_language_custom():
    """Test that BaseSession's programming_language property returns the value passed to the constructor."""
    from deephaven_mcp.client._session import BaseSession

    # Create a session with a custom programming_language using a unique test value
    test_lang = "test_unique_language_xyz123"
    session = BaseSession(
        DummySession(), is_enterprise=False, programming_language=test_lang
    )

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
    """Test that CoreSession.from_config sets programming_language from session_type."""
    from deephaven_mcp.client._session import CoreSession

    # Mock the PDHSession class
    with patch("deephaven_mcp.client._session.Session", DummyPDHSession):
        # Create a config with a custom session_type
        config = {"host": "localhost", "port": 10000, "session_type": "groovy"}

        # Create a session using from_config
        session = asyncio.run(CoreSession.from_config(config))

        # Verify the programming_language property matches the session_type
        assert session.programming_language == "groovy"


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
