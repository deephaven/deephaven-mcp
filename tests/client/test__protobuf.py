"""
Unit tests for deephaven_mcp.client._protobuf module.
"""

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# Patch sys.modules so _protobuf can be imported even if enterprise modules are missing
mock_enterprise = types.ModuleType("deephaven_enterprise")
mock_proto = types.ModuleType("deephaven_enterprise.proto")
mock_auth_pb2 = types.ModuleType("deephaven_enterprise.proto.auth_pb2")
mock_controller = types.ModuleType("deephaven_enterprise.client.controller")
mock_controller.ControllerClient = MagicMock()

with patch.dict(
    sys.modules,
    {
        "deephaven_enterprise": mock_enterprise,
        "deephaven_enterprise.proto": mock_proto,
        "deephaven_enterprise.proto.auth_pb2": mock_auth_pb2,
        "deephaven_enterprise.client": types.ModuleType("deephaven_enterprise.client"),
        "deephaven_enterprise.client.controller": mock_controller,
        "deephaven_enterprise.client.util": types.ModuleType(
            "deephaven_enterprise.client.util"
        ),
    },
):
    from deephaven_mcp.client import _protobuf


# Helper: create a mock protobuf message
class DummyMessage:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __eq__(self, other):
        return isinstance(other, DummyMessage) and self.__dict__ == other.__dict__


def test_coreplus_query_status_eq_with_instance(monkeypatch):
    # Patch ControllerClient so status_name works
    monkeypatch.setattr(
        _protobuf,
        "ControllerClient",
        type("CC", (), {"status_name": staticmethod(lambda x: x.name)}),
    )

    class EnumVal:
        def __init__(self, value, name):
            self.value = value
            self.name = name

        def __eq__(self, other):
            return isinstance(other, EnumVal) and self.value == other.value

    pb_enum1 = EnumVal(1, "RUNNING")
    pb_enum2 = EnumVal(1, "RUNNING")
    s1 = _protobuf.CorePlusQueryStatus(pb_enum1)
    s2 = _protobuf.CorePlusQueryStatus(pb_enum2)
    assert s1 == s2


def test_protobuf_wrapper_raises_on_none():
    with pytest.raises(ValueError, match="Protobuf message cannot be None"):
        _protobuf.ProtobufWrapper(None)


def test_protobuf_wrapper_repr_and_properties():
    pb = DummyMessage(field1=123, field2="abc")
    wrapper = _protobuf.ProtobufWrapper(pb)
    assert wrapper.pb == pb
    assert "ProtobufWrapper wrapping DummyMessage" in repr(wrapper)


def test_protobuf_wrapper_to_dict_and_json(monkeypatch):
    pb = DummyMessage()
    wrapper = _protobuf.ProtobufWrapper(pb)
    # Patch MessageToDict and MessageToJson
    monkeypatch.setattr(_protobuf, "MessageToDict", lambda msg, **_: {"foo": "bar"})
    monkeypatch.setattr(_protobuf, "MessageToJson", lambda msg, **_: '{"foo": "bar"}')
    assert wrapper.to_dict() == {"foo": "bar"}
    assert wrapper.to_json() == '{"foo": "bar"}'


def test_coreplus_token_inherits_protobuf_wrapper():
    pb = DummyMessage(token="tok", expires_at=123456)
    token = _protobuf.CorePlusToken(pb)
    assert isinstance(token, _protobuf.ProtobufWrapper)
    assert token.pb == pb


def test_coreplus_query_config_inherits_protobuf_wrapper():
    pb = DummyMessage(name="test_query")
    config = _protobuf.CorePlusQueryConfig(pb)
    assert isinstance(config, _protobuf.ProtobufWrapper)
    assert config.pb == pb


def test_coreplus_query_state_inherits_protobuf_wrapper():
    pb = DummyMessage(status=MagicMock())
    state = _protobuf.CorePlusQueryState(pb)
    assert isinstance(state, _protobuf.ProtobufWrapper)
    assert state.pb == pb


def test_coreplus_query_state_status_returns_wrapper(monkeypatch):
    pb = DummyMessage(status="RUNNING")
    state = _protobuf.CorePlusQueryState(pb)
    monkeypatch.setattr(_protobuf, "CorePlusQueryStatus", lambda s: f"wrapped-{s}")
    # status property should wrap the status field
    assert state.status == "wrapped-RUNNING"


def test_coreplus_query_info_wraps_config_state_replicas_spares(monkeypatch):
    pb = DummyMessage(
        config="config_pb", state="state_pb", replicas=["r1", "r2"], spares=["s1"]
    )
    monkeypatch.setattr(_protobuf, "CorePlusQueryConfig", lambda pb: f"config-{pb}")
    monkeypatch.setattr(_protobuf, "CorePlusQueryState", lambda pb: f"state-{pb}")
    info = _protobuf.CorePlusQueryInfo(pb)
    assert info.config == "config-config_pb"
    assert info.state == "state-state_pb"
    assert info.replicas == ["state-r1", "state-r2"]
    assert info.spares == ["state-s1"]


def test_coreplus_query_info_handles_missing_state(monkeypatch):
    pb = DummyMessage(config="cfg", state=None, replicas=[], spares=[])
    monkeypatch.setattr(_protobuf, "CorePlusQueryConfig", lambda pb: f"C-{pb}")
    monkeypatch.setattr(_protobuf, "CorePlusQueryState", lambda pb: f"S-{pb}")
    info = _protobuf.CorePlusQueryInfo(pb)
    assert info.state is None
    assert info.replicas == []
    assert info.spares == []


def test_coreplus_query_status_comparisons(monkeypatch):
    # Patch ControllerClient for is_running, etc.
    monkeypatch.setattr(
        _protobuf,
        "ControllerClient",
        type(
            "CC",
            (),
            {
                "is_running": staticmethod(lambda x: x == 1),
                "is_completed": staticmethod(lambda x: x == 2),
                "is_terminal": staticmethod(lambda x: x == 3),
                "is_status_uninitialized": staticmethod(lambda x: x == 0),
                "status_name": staticmethod(lambda x: x.name),
                "PersistentQueryStatusEnum": type(
                    "Enum",
                    (),
                    {"RUNNING": 1, "COMPLETED": 2, "TERMINAL": 3, "UNINITIALIZED": 0},
                ),
            },
        ),
    )

    class EnumVal:
        def __init__(self, value, name):
            self.value = value
            self.name = name

        def __eq__(self, other):
            if isinstance(other, EnumVal):
                return self.value == other.value
            if isinstance(other, int):
                return self.value == other
            if isinstance(other, str):
                return self.name == other
            return False

    pb_enum = EnumVal(1, "RUNNING")
    status = _protobuf.CorePlusQueryStatus(pb_enum)
    assert status.is_running
    assert not status.is_completed
    assert not status.is_terminal
    assert not status.is_uninitialized
    assert status == "RUNNING"
    assert status == 1
    assert status == pb_enum
    assert str(status) == "RUNNING"
