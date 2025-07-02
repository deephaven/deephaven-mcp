"""
Unit tests for deephaven_mcp.client.__init__
Covers importability, __all__, and is_enterprise_available attribute.
Should pass in both community-only and enterprise environments by patching sys.modules if needed.
"""

import sys
import types

import pytest


def test_import_client_module():
    import deephaven_mcp.client


def test___all__():
    import deephaven_mcp.client

    assert isinstance(deephaven_mcp.client.__all__, list)


def test_expected_symbols_in_all():
    import deephaven_mcp.client

    expected = {
        "ClientObjectWrapper",
        "CoreSession",
        "CorePlusSession",
        "CorePlusAuthClient",
        "CorePlusSessionManager",
        "CorePlusControllerClient",
        "is_enterprise_available",
        "ProtobufWrapper",
        "CorePlusQueryStatus",
        "CorePlusQuerySerial",
        "CorePlusQueryConfig",
        "CorePlusQueryState",
        "CorePlusQueryInfo",
        "CorePlusToken",
    }
    assert expected <= set(deephaven_mcp.client.__all__)


@pytest.mark.parametrize(
    "symbol",
    [
        "ClientObjectWrapper",
        "CoreSession",
        "CorePlusSession",
        "CorePlusAuthClient",
        "CorePlusSessionManager",
        "CorePlusControllerClient",
        "is_enterprise_available",
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


def test_import_client_init():
    client = pytest.importorskip("deephaven_mcp.client")
    assert hasattr(client, "CoreSession")
    assert hasattr(client, "CorePlusSession")
    assert hasattr(client, "CorePlusAuthClient")
    assert hasattr(client, "is_enterprise_available")


def test___all__():
    client = pytest.importorskip("deephaven_mcp.client")
    exported = set(client.__all__)
    expected = {
        "ClientObjectWrapper",
        "CoreSession",
        "CorePlusSession",
        "CorePlusAuthClient",
        "CorePlusSessionManager",
        "CorePlusControllerClient",
        "is_enterprise_available",
        "ProtobufWrapper",
        "CorePlusQueryStatus",
        "CorePlusQuerySerial",
        "CorePlusQueryConfig",
        "CorePlusQueryState",
        "CorePlusQueryInfo",
        "CorePlusToken",
    }
    assert expected <= exported


def test_is_enterprise_available_type():
    client = pytest.importorskip("deephaven_mcp.client")
    assert isinstance(client.is_enterprise_available, bool)


import importlib

COMMUNITY_SYMBOLS = [
    "ClientObjectWrapper",
    "CoreSession",
    "is_enterprise_available",
]
ENTERPRISE_SYMBOLS = [
    "CorePlusSession",
    "CorePlusAuthClient",
    "CorePlusSessionManager",
    "CorePlusControllerClient",
    "ProtobufWrapper",
    "CorePlusQueryStatus",
    "CorePlusQuerySerial",
    "CorePlusQueryConfig",
    "CorePlusQueryState",
    "CorePlusQueryInfo",
    "CorePlusToken",
]


@pytest.mark.parametrize("symbol", COMMUNITY_SYMBOLS)
def test_symbol_in_module_community(symbol):
    client = pytest.importorskip("deephaven_mcp.client")
    assert hasattr(client, symbol)


@pytest.mark.parametrize("symbol", ENTERPRISE_SYMBOLS)
def test_symbol_in_module_enterprise(symbol):
    # Only run if all enterprise modules are importable
    for mod in [
        "deephaven_enterprise.client.controller",
        "deephaven_enterprise.proto.auth_pb2",
        "deephaven_enterprise.proto.common_pb2",
    ]:
        pytest.importorskip(mod)
    client = importlib.import_module("deephaven_mcp.client")
    assert hasattr(client, symbol)
