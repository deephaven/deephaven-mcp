import json
from unittest.mock import MagicMock, patch

import pytest

from deephaven_mcp.client._pq_config import (
    _apply_auto_delete_timeout,
    _apply_pq_config_list_fields,
    _apply_pq_config_simple_fields,
    _convert_restart_users_to_enum,
    _normalize_programming_language,
    _set_termination_delay,
    apply_pq_config_fields,
    validate_pq_config_args,
)


@pytest.fixture(scope="session")
def pq_config_mod():
    from deephaven_mcp.client import _pq_config

    return _pq_config


# The real protobuf class is used by the oneof test (protobuf oneof
# semantics cannot be exercised with a MagicMock).
from deephaven_enterprise.proto.persistent_query_pb2 import (
    PersistentQueryConfigMessage as _PQConfigMessage,
)


def _make_config_mock():
    """Build a MagicMock PQ config protobuf usable by the field appliers.

    ``typeSpecificFieldsJson`` is a real empty string so ``_set_termination_delay``
    can ``json.loads`` it; ``scheduling`` is a MagicMock that records ``del x[:]``
    (``__delitem__``) and ``extend`` calls.
    """
    config = MagicMock()
    config.typeSpecificFieldsJson = ""
    config.scheduling = MagicMock()
    return config


# ===========================================================================
# Module-level PQ-config helper tests
# ===========================================================================


def test_normalize_programming_language_python():
    assert _normalize_programming_language("python") == "Python"
    assert _normalize_programming_language("PYTHON") == "Python"
    assert _normalize_programming_language("Python") == "Python"


def test_normalize_programming_language_groovy():
    assert _normalize_programming_language("groovy") == "Groovy"
    assert _normalize_programming_language("GROOVY") == "Groovy"


def test_normalize_programming_language_invalid():
    with pytest.raises(ValueError, match="Invalid programming_language"):
        _normalize_programming_language("javascript")


def test_convert_restart_users_to_enum_valid(pq_config_mod):
    with patch.object(pq_config_mod, "RestartUsersEnum") as mock_enum:
        mock_enum.Value.return_value = 1
        assert _convert_restart_users_to_enum("RU_ADMIN") == 1
        mock_enum.Value.assert_called_once_with("RU_ADMIN")


def test_convert_restart_users_to_enum_invalid_name(pq_config_mod):
    with patch.object(pq_config_mod, "RestartUsersEnum") as mock_enum:
        mock_enum.Value.side_effect = ValueError("bad")
        mock_enum.keys.return_value = ["RU_ADMIN", "RU_VIEWERS_WHEN_DOWN"]
        with pytest.raises(ValueError, match="Invalid restart_users: 'NOPE'"):
            _convert_restart_users_to_enum("NOPE")


def test_set_termination_delay_set_on_empty():
    config = MagicMock()
    config.typeSpecificFieldsJson = ""
    _set_termination_delay(config, 60000)

    assert json.loads(config.typeSpecificFieldsJson) == {
        "TerminationDelay": {"type": "long", "value": "60000"}
    }


def test_set_termination_delay_preserves_other_keys():
    config = MagicMock()
    config.typeSpecificFieldsJson = json.dumps(
        {"Other": {"type": "string", "value": "keep"}}
    )
    _set_termination_delay(config, 5000)
    decoded = json.loads(config.typeSpecificFieldsJson)
    assert decoded["Other"] == {"type": "string", "value": "keep"}
    assert decoded["TerminationDelay"] == {"type": "long", "value": "5000"}


def test_set_termination_delay_remove_keeps_others():
    config = MagicMock()
    config.typeSpecificFieldsJson = json.dumps(
        {
            "TerminationDelay": {"type": "long", "value": "5000"},
            "Other": {"type": "string", "value": "keep"},
        }
    )
    _set_termination_delay(config, None)
    assert json.loads(config.typeSpecificFieldsJson) == {
        "Other": {"type": "string", "value": "keep"}
    }


def test_set_termination_delay_remove_empties_when_only_key():
    config = MagicMock()
    config.typeSpecificFieldsJson = json.dumps(
        {"TerminationDelay": {"type": "long", "value": "5000"}}
    )
    _set_termination_delay(config, None)
    assert config.typeSpecificFieldsJson == ""


def test_apply_auto_delete_timeout_none_is_noop():
    config = MagicMock()
    config.typeSpecificFieldsJson = "existing"
    assert _apply_auto_delete_timeout(config, None) is False
    config.scheduling.__delitem__.assert_not_called()
    assert config.typeSpecificFieldsJson == "existing"


def test_apply_auto_delete_timeout_zero_installs_continuous(pq_config_mod):
    config = MagicMock()
    config.typeSpecificFieldsJson = (
        '{"TerminationDelay": {"type": "long", "value": "1"}}'
    )
    assert _apply_auto_delete_timeout(config, 0) is True
    config.scheduling.__delitem__.assert_called_once_with(slice(None))
    config.scheduling.extend.assert_called_once_with(
        pq_config_mod._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING
    )
    # TerminationDelay removed (was the only key) -> empty string.
    assert config.typeSpecificFieldsJson == ""


def test_apply_auto_delete_timeout_positive_installs_temporary(pq_config_mod):
    config = MagicMock()
    config.typeSpecificFieldsJson = ""
    temp_scheduler = ["SchedulerType=Temporary"]
    with patch.object(pq_config_mod, "GenerateScheduling") as mock_gen:
        mock_gen.generate_temporary_scheduler.return_value = temp_scheduler
        assert _apply_auto_delete_timeout(config, 300) is True
        mock_gen.generate_temporary_scheduler.assert_called_once_with(
            expiration_time_minutes=pq_config_mod._TEMPORARY_EXPIRATION_MINUTES,
            queue_name=pq_config_mod._TEMPORARY_QUEUE_NAME,
            auto_delete=True,
        )
    config.scheduling.__delitem__.assert_called_once_with(slice(None))
    config.scheduling.extend.assert_called_once_with(temp_scheduler)
    assert json.loads(config.typeSpecificFieldsJson)["TerminationDelay"] == {
        "type": "long",
        "value": "300000",
    }


def test_apply_pq_config_simple_fields_all_none():
    config = MagicMock()
    assert (
        _apply_pq_config_simple_fields(
            config, None, None, None, None, None, None, None, None, None
        )
        is False
    )


def test_apply_pq_config_simple_fields_sets_protobuf_names():
    config = MagicMock()
    assert (
        _apply_pq_config_simple_fields(
            config,
            "pq",
            4.0,
            "Script",
            True,
            "srv",
            "Engine",
            "profile",
            "venv",
            123,
        )
        is True
    )
    assert config.name == "pq"
    assert config.heapSizeGb == 4.0
    assert config.configurationType == "Script"
    assert config.enabled is True
    assert config.serverName == "srv"
    assert config.workerKind == "Engine"
    assert config.jvmProfile == "profile"
    # python_virtual_environment maps to pythonControl (NOT pythonVirtualEnvironment).
    assert config.pythonControl == "venv"
    # init_timeout_nanos maps to timeoutNanos (NOT initTimeoutNanos).
    assert config.timeoutNanos == 123


def test_apply_pq_config_list_fields_all_none_preserves_existing():
    config = MagicMock()
    assert (
        _apply_pq_config_list_fields(config, None, None, None, None, None, None)
        is False
    )
    config.scheduling.__delitem__.assert_not_called()
    config.extraJvmArguments.__delitem__.assert_not_called()


def test_apply_pq_config_list_fields_empty_clears():
    config = MagicMock()
    assert _apply_pq_config_list_fields(config, [], [], [], [], [], []) is True
    config.scheduling.__delitem__.assert_called_once_with(slice(None))
    config.scheduling.extend.assert_called_once_with([])
    config.extraJvmArguments.extend.assert_called_once_with([])
    config.classPathAdditions.extend.assert_called_once_with([])
    config.extraEnvironmentVariables.extend.assert_called_once_with([])
    config.adminGroups.extend.assert_called_once_with([])
    config.viewerGroups.extend.assert_called_once_with([])


def test_apply_pq_config_list_fields_explicit_replaces_each():
    config = MagicMock()
    assert (
        _apply_pq_config_list_fields(
            config,
            schedule=["s1"],
            extra_jvm_args=["-Xmx1g"],
            extra_class_path=["/jar"],
            extra_environment_vars=["K=V"],
            admin_groups=["a"],
            viewer_groups=["v"],
        )
        is True
    )
    config.scheduling.extend.assert_called_once_with(["s1"])
    config.extraJvmArguments.extend.assert_called_once_with(["-Xmx1g"])
    config.classPathAdditions.extend.assert_called_once_with(["/jar"])
    config.extraEnvironmentVariables.extend.assert_called_once_with(["K=V"])
    config.adminGroups.extend.assert_called_once_with(["a"])
    config.viewerGroups.extend.assert_called_once_with(["v"])


def test_validate_pq_config_args_script_conflict():
    with pytest.raises(ValueError, match="script_body and script_path"):
        validate_pq_config_args(None, None, "code", "path")


def test_validate_pq_config_args_auto_delete_schedule_conflict():
    with pytest.raises(ValueError, match="auto_delete_timeout and schedule"):
        validate_pq_config_args(60, ["sched"], None, None)


def test_validate_pq_config_args_ok():
    assert validate_pq_config_args(None, None, "code", None) is None
    assert validate_pq_config_args(60, None, None, None) is None
    assert validate_pq_config_args(None, ["sched"], None, None) is None


def test_apply_pq_config_fields_script_body_oneof():
    """scriptCode and scriptPath are a protobuf oneof; setting one clears the other."""
    nones = dict(
        pq_name=None,
        heap_size_gb=None,
        programming_language=None,
        configuration_type=None,
        enabled=None,
        schedule=None,
        server=None,
        engine=None,
        jvm_profile=None,
        extra_jvm_args=None,
        extra_class_path=None,
        python_virtual_environment=None,
        extra_environment_vars=None,
        init_timeout_nanos=None,
        auto_delete_timeout=None,
        admin_groups=None,
        viewer_groups=None,
        restart_users=None,
        owner=None,
    )

    pb = _PQConfigMessage()
    pb.scriptPath = "/old/path.py"
    assert apply_pq_config_fields(pb, script_body="t = 42", script_path=None, **nones)
    assert pb.scriptCode == "t = 42"
    assert pb.scriptPath == ""
    assert pb.WhichOneof("scriptData") == "scriptCode"

    pb2 = _PQConfigMessage()
    pb2.scriptCode = "t = None"
    assert apply_pq_config_fields(
        pb2, script_body=None, script_path="/new/path.py", **nones
    )
    assert pb2.scriptPath == "/new/path.py"
    assert pb2.scriptCode == ""
    assert pb2.WhichOneof("scriptData") == "scriptPath"


def _all_none_fields_kwargs():
    return dict(
        pq_name=None,
        heap_size_gb=None,
        programming_language=None,
        script_body=None,
        script_path=None,
        configuration_type=None,
        enabled=None,
        schedule=None,
        server=None,
        engine=None,
        jvm_profile=None,
        extra_jvm_args=None,
        extra_class_path=None,
        python_virtual_environment=None,
        extra_environment_vars=None,
        init_timeout_nanos=None,
        auto_delete_timeout=None,
        admin_groups=None,
        viewer_groups=None,
        restart_users=None,
        owner=None,
    )


def test_apply_pq_config_fields_all_none_returns_false():
    config = MagicMock()
    assert apply_pq_config_fields(config, **_all_none_fields_kwargs()) is False


def test_apply_pq_config_fields_restart_users_enum_conversion(pq_config_mod):
    config = MagicMock()
    kwargs = _all_none_fields_kwargs()
    kwargs["restart_users"] = "RU_ADMIN"
    with patch.object(pq_config_mod, "RestartUsersEnum") as mock_enum:
        mock_enum.Value.return_value = 7
        assert apply_pq_config_fields(config, **kwargs) is True
    assert config.restartUsers == 7


def test_apply_pq_config_fields_owner():
    config = MagicMock()
    kwargs = _all_none_fields_kwargs()
    kwargs["owner"] = "svc"
    assert apply_pq_config_fields(config, **kwargs) is True
    assert config.owner == "svc"


def test_apply_pq_config_fields_routes_to_each_applier(pq_config_mod):
    """programming_language plus the simple/list/auto-delete branches all apply."""
    config = _make_config_mock()
    kwargs = _all_none_fields_kwargs()
    kwargs["programming_language"] = "python"
    kwargs["heap_size_gb"] = 4.0  # simple-field applier
    kwargs["extra_class_path"] = ["/jar"]  # list-field applier
    kwargs["auto_delete_timeout"] = 0  # auto-delete applier (permanent)

    assert apply_pq_config_fields(config, **kwargs) is True

    assert config.scriptLanguage == "Python"
    assert config.heapSizeGb == 4.0
    config.classPathAdditions.extend.assert_called_once_with(["/jar"])
    config.scheduling.extend.assert_called_once_with(
        pq_config_mod._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING
    )
