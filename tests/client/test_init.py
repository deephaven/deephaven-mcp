"""
Unit tests for deephaven_mcp.client.__init__
Covers importability and __all__.
"""

import pytest


def test_import_client_module():
    import deephaven_mcp.client


def test_pq_states_in_all():
    import deephaven_mcp.client

    assert "PQ_STATES" in deephaven_mcp.client.__all__


@pytest.mark.parametrize(
    "symbol",
    [
        "ClientObjectWrapper",
        "BaseSession",
        "CoreSession",
        "CorePlusSession",
        "CorePlusAuthClient",
        "CorePlusSessionFactory",
        "CorePlusControllerClient",
        "ProtobufWrapper",
        "CorePlusQueryStatus",
        "CorePlusQuerySerial",
        "CorePlusQueryConfig",
        "CorePlusQueryState",
        "CorePlusQueryInfo",
        "CorePlusToken",
    ],
)
def test_symbol_in_module(symbol):
    import deephaven_mcp.client

    assert hasattr(deephaven_mcp.client, symbol)


def test___all__():
    import deephaven_mcp.client as client

    expected = {
        "ClientObjectWrapper",
        "CommunityClientTimeouts",
        "EnterpriseClientTimeouts",
        "BaseSession",
        "CoreSession",
        "CorePlusSession",
        "CorePlusAuthClient",
        "CorePlusSessionFactory",
        "CorePlusControllerClient",
        "ProtobufWrapper",
        "CorePlusQueryStatus",
        "CorePlusQuerySerial",
        "CorePlusQueryConfig",
        "CorePlusQueryState",
        "CorePlusQueryInfo",
        "CorePlusToken",
        "PQ_STATES",
    }
    assert isinstance(client.__all__, list)
    assert set(client.__all__) == expected


def test_import_star_behavior():
    expected = {
        "ClientObjectWrapper",
        "CommunityClientTimeouts",
        "EnterpriseClientTimeouts",
        "BaseSession",
        "CoreSession",
        "CorePlusSession",
        "CorePlusAuthClient",
        "CorePlusSessionFactory",
        "CorePlusControllerClient",
        "ProtobufWrapper",
        "CorePlusQueryStatus",
        "CorePlusQuerySerial",
        "CorePlusQueryConfig",
        "CorePlusQueryState",
        "CorePlusQueryInfo",
        "CorePlusToken",
        "PQ_STATES",
    }
    ns = {}
    exec("from deephaven_mcp.client import *", ns)
    for symbol in expected:
        assert symbol in ns
    # Optionally, check that no unexpected symbols are present (no leading underscores)
    for k in ns:
        if not k.startswith("__"):
            assert k in expected
