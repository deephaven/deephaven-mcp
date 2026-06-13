"""Construction and mutation of Deephaven Enterprise persistent-query configurations.

Pure, synchronous helpers that build and modify ``PersistentQueryConfigMessage`` protobufs
for the controller client's ``make_pq_config`` (create) and ``update_pq_config`` (modify).
The public entry points are :func:`validate_pq_config_args` and :func:`apply_pq_config_fields`;
everything else is internal. These helpers perform no IO.
"""

import json
from typing import cast

from deephaven_enterprise.client.generate_scheduling import GenerateScheduling
from deephaven_enterprise.proto.persistent_query_pb2 import (
    PersistentQueryConfigMessage,
    RestartUsersEnum,
)

# Default scheduling entries applied to a *permanent* PQ (auto_delete_timeout None or 0).
# Produces a continuous scheduler that auto-starts the PQ after the controller accepts it.
_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING: tuple[str, ...] = (
    "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerContinuous",
    "StartTime=00:00:00",
    "TimeZone=America/New_York",
    "DailyRestart=false",
    "StopTimeDisabled=true",
    "RestartErrorCount=0",
    "RestartErrorDelay=0",
    "RestartWhenRunning=Yes",
    "SchedulingDisabled=false",
)

# Temporary-scheduler parameters used when auto_delete_timeout requests an auto-deleting
# (temporary) PQ. These mirror the values the enterprise controller's make_temporary_config
# applies, so PQs created via make_pq_config and modified via update_pq_config are identical.
_TEMPORARY_QUEUE_NAME = "InteractiveConsoleTemporaryQueue"
_TEMPORARY_EXPIRATION_MINUTES = 2880


def _normalize_programming_language(language: str) -> str:
    """Normalize and validate a programming language string for PQ configuration.

    Accepts case-insensitive input and returns the canonical capitalized form expected by
    the Deephaven Enterprise controller API.

    Args:
        language (str): Programming language string, case-insensitive
            (e.g., "python", "Python", "PYTHON", "groovy", "Groovy").

    Returns:
        str: Canonical form accepted by the controller: "Python" or "Groovy".

    Raises:
        ValueError: If language is not a case-insensitive match for "Python" or "Groovy".
    """
    lang_lower = language.lower()
    if lang_lower == "python":
        return "Python"
    elif lang_lower == "groovy":
        return "Groovy"
    else:
        raise ValueError(
            f"Invalid programming_language: '{language}'. "
            "Must be 'Python' or 'Groovy' (case-insensitive)."
        )


def _convert_restart_users_to_enum(
    restart_users_str: str,
) -> "RestartUsersEnum.ValueType":
    """Convert a restart_users string to its protobuf enum value.

    Args:
        restart_users_str (str): Restart users enum name (e.g., "RU_ADMIN").

    Returns:
        RestartUsersEnum.ValueType: Typed protobuf enum value suitable for direct assignment
            to ``PersistentQueryConfigMessage.restartUsers``.

    Raises:
        ValueError: If restart_users_str is not a valid enum value.
    """
    try:
        # EnumTypeWrapper.Value() is untyped (Any) in the runtime google.protobuf package;
        # the cast pins the precise protobuf type at the point upstream type info is lost so
        # callers assigning to ``restartUsers`` need no cast or suppression.
        return cast(
            "RestartUsersEnum.ValueType", RestartUsersEnum.Value(restart_users_str)
        )
    except ValueError:
        valid_values = list(RestartUsersEnum.keys())
        raise ValueError(
            f"Invalid restart_users: '{restart_users_str}'. "
            f"Must be one of: {', '.join(sorted(valid_values))}"
        ) from None


def _set_termination_delay(
    config_pb: PersistentQueryConfigMessage, delay_ms: int | None
) -> None:
    """Set or remove the ``TerminationDelay`` entry in ``typeSpecificFieldsJson``.

    Args:
        config_pb (PersistentQueryConfigMessage): Protobuf config object to modify in-place.
        delay_ms (int | None): Grace period in milliseconds to encode, or None to remove the
            entry. Other type-specific fields already present are preserved.
    """
    # Key the controller reads as the temporary-query auto-delete grace period (ms).
    raw = config_pb.typeSpecificFieldsJson
    fields = json.loads(raw) if raw else {}
    if delay_ms is None:
        fields.pop("TerminationDelay", None)
    else:
        fields["TerminationDelay"] = {"type": "long", "value": str(delay_ms)}
    config_pb.typeSpecificFieldsJson = json.dumps(fields) if fields else ""


def _apply_auto_delete_timeout(
    config_pb: PersistentQueryConfigMessage, auto_delete_timeout: int | None
) -> bool:
    """Apply the auto-delete timeout to a PQ config's scheduler and ``TerminationDelay``.

    Auto-deletion is driven by the scheduler, so this pairs the scheduler with the grace
    period as a unit — the single source of truth shared by ``make_pq_config`` and
    ``update_pq_config``.

    Args:
        config_pb (PersistentQueryConfigMessage): Protobuf config object to modify in-place.
        auto_delete_timeout (int | None): Seconds of inactivity before auto-deletion. None
            leaves the config untouched. 0 installs the permanent continuous scheduler and
            clears the grace period. A positive integer installs the temporary scheduler and
            sets the grace period to that many seconds (stored as ``TerminationDelay``
            milliseconds).

    Returns:
        bool: True if the config was modified, False when auto_delete_timeout is None.
    """
    if auto_delete_timeout is None:
        return False

    del config_pb.scheduling[:]
    if auto_delete_timeout == 0:
        config_pb.scheduling.extend(_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING)
        _set_termination_delay(config_pb, None)
    else:
        config_pb.scheduling.extend(
            GenerateScheduling.generate_temporary_scheduler(
                expiration_time_minutes=_TEMPORARY_EXPIRATION_MINUTES,
                queue_name=_TEMPORARY_QUEUE_NAME,
                auto_delete=True,
            )
        )
        _set_termination_delay(config_pb, auto_delete_timeout * 1000)
    return True


def _apply_pq_config_simple_fields(
    config_pb: PersistentQueryConfigMessage,
    pq_name: str | None,
    heap_size_gb: float | int | None,
    configuration_type: str | None,
    enabled: bool | None,
    server: str | None,
    engine: str | None,
    jvm_profile: str | None,
    python_virtual_environment: str | None,
    init_timeout_nanos: int | None,
) -> bool:
    """Apply simple (scalar) field updates to a PersistentQueryConfigMessage protobuf.

    Updates only the fields that are not None.

    Args:
        config_pb (PersistentQueryConfigMessage): Protobuf config object to modify in-place.
        pq_name (str | None): New PQ name → ``config_pb.name``.
        heap_size_gb (float | int | None): Worker heap size in GB → ``config_pb.heapSizeGb``.
        configuration_type (str | None): Config type → ``config_pb.configurationType``.
        enabled (bool | None): Whether the PQ is enabled → ``config_pb.enabled``.
        server (str | None): Target server name → ``config_pb.serverName``.
        engine (str | None): Worker kind/engine type → ``config_pb.workerKind``.
        jvm_profile (str | None): JVM profile name → ``config_pb.jvmProfile``.
        python_virtual_environment (str | None): Python venv control → ``config_pb.pythonControl``.
        init_timeout_nanos (int | None): Initialization timeout in nanoseconds → ``config_pb.timeoutNanos``.

    Returns:
        bool: True if any field was updated, False if all parameters were None.
    """
    has_changes = False
    if pq_name is not None:
        config_pb.name = pq_name
        has_changes = True
    if heap_size_gb is not None:
        config_pb.heapSizeGb = heap_size_gb
        has_changes = True
    if configuration_type is not None:
        config_pb.configurationType = configuration_type
        has_changes = True
    if enabled is not None:
        config_pb.enabled = enabled
        has_changes = True
    if server is not None:
        config_pb.serverName = server
        has_changes = True
    if engine is not None:
        config_pb.workerKind = engine
        has_changes = True
    if jvm_profile is not None:
        config_pb.jvmProfile = jvm_profile
        has_changes = True
    if python_virtual_environment is not None:
        config_pb.pythonControl = python_virtual_environment
        has_changes = True
    if init_timeout_nanos is not None:
        config_pb.timeoutNanos = init_timeout_nanos
        has_changes = True
    return has_changes


def _apply_pq_config_list_fields(
    config_pb: PersistentQueryConfigMessage,
    schedule: list[str] | None,
    extra_jvm_args: list[str] | None,
    extra_class_path: list[str] | None,
    extra_environment_vars: list[str] | None,
    admin_groups: list[str] | None,
    viewer_groups: list[str] | None,
) -> bool:
    """Apply list (repeated) field updates to a PersistentQueryConfigMessage protobuf.

    Each non-None field fully replaces the existing contents (``del`` + ``extend``), so an
    empty list clears the field.

    Args:
        config_pb (PersistentQueryConfigMessage): Protobuf config object to modify in-place.
        schedule (list[str] | None): Scheduling entries as ``Key=Value`` strings → ``config_pb.scheduling``.
        extra_jvm_args (list[str] | None): JVM arguments → ``config_pb.extraJvmArguments``.
        extra_class_path (list[str] | None): Classpath entries → ``config_pb.classPathAdditions``.
        extra_environment_vars (list[str] | None): Env vars (KEY=VALUE) → ``config_pb.extraEnvironmentVariables``.
        admin_groups (list[str] | None): Admin group names → ``config_pb.adminGroups``.
        viewer_groups (list[str] | None): Viewer group names → ``config_pb.viewerGroups``.

    Returns:
        bool: True if any field was updated, False if all parameters were None.
    """
    has_changes = False
    if schedule is not None:
        del config_pb.scheduling[:]
        config_pb.scheduling.extend(schedule)
        has_changes = True
    if extra_jvm_args is not None:
        del config_pb.extraJvmArguments[:]
        config_pb.extraJvmArguments.extend(extra_jvm_args)
        has_changes = True
    if extra_class_path is not None:
        del config_pb.classPathAdditions[:]
        config_pb.classPathAdditions.extend(extra_class_path)
        has_changes = True
    if extra_environment_vars is not None:
        del config_pb.extraEnvironmentVariables[:]
        config_pb.extraEnvironmentVariables.extend(extra_environment_vars)
        has_changes = True
    if admin_groups is not None:
        del config_pb.adminGroups[:]
        config_pb.adminGroups.extend(admin_groups)
        has_changes = True
    if viewer_groups is not None:
        del config_pb.viewerGroups[:]
        config_pb.viewerGroups.extend(viewer_groups)
        has_changes = True
    return has_changes


def validate_pq_config_args(
    auto_delete_timeout: int | None,
    schedule: list[str] | None,
    script_body: str | None,
    script_path: str | None,
) -> None:
    """Validate mutually exclusive PQ configuration arguments.

    Args:
        auto_delete_timeout (int | None): Auto-delete timeout in seconds.
        schedule (list[str] | None): Scheduling entries.
        script_body (str | None): Inline script code.
        script_path (str | None): Git script path.

    Raises:
        ValueError: If script_body and script_path are both set, or if auto_delete_timeout
            and schedule are both set (auto_delete_timeout installs its own scheduler).
    """
    if script_body is not None and script_path is not None:
        raise ValueError(
            "script_body and script_path are mutually exclusive - specify only one"
        )
    if auto_delete_timeout is not None and schedule is not None:
        raise ValueError(
            "auto_delete_timeout and schedule are mutually exclusive - "
            "auto_delete_timeout installs its own scheduler"
        )


def apply_pq_config_fields(
    config_pb: PersistentQueryConfigMessage,
    *,
    pq_name: str | None,
    heap_size_gb: float | int | None,
    programming_language: str | None,
    script_body: str | None,
    script_path: str | None,
    configuration_type: str | None,
    enabled: bool | None,
    schedule: list[str] | None,
    server: str | None,
    engine: str | None,
    jvm_profile: str | None,
    extra_jvm_args: list[str] | None,
    extra_class_path: list[str] | None,
    python_virtual_environment: str | None,
    extra_environment_vars: list[str] | None,
    init_timeout_nanos: int | None,
    auto_delete_timeout: int | None,
    admin_groups: list[str] | None,
    viewer_groups: list[str] | None,
    restart_users: str | None,
    owner: str | None,
) -> bool:
    """Apply caller-supplied PQ configuration fields to a protobuf config in place.

    The single field-applier shared by ``make_pq_config`` (create) and ``update_pq_config``
    (modify). Every field follows a "None means skip" rule. ``auto_delete_timeout`` and
    ``schedule`` are mutually exclusive (see :func:`validate_pq_config_args`);
    ``auto_delete_timeout`` installs its own scheduler.

    Args:
        config_pb (PersistentQueryConfigMessage): Protobuf config object to modify in-place.
        pq_name (str | None): New PQ name.
        heap_size_gb (float | int | None): Worker heap size in GB.
        programming_language (str | None): "Python" or "Groovy" (case-insensitive; normalized).
        script_body (str | None): Inline script code (oneof with script_path).
        script_path (str | None): Git script path (oneof with script_body).
        configuration_type (str | None): Config type (e.g., "Script", "RunAndDone").
        enabled (bool | None): Whether the PQ is enabled.
        schedule (list[str] | None): Scheduling entries; replaces existing wholesale.
        server (str | None): Target server name.
        engine (str | None): Worker kind/engine type.
        jvm_profile (str | None): JVM profile name.
        extra_jvm_args (list[str] | None): JVM arguments; replaces existing.
        extra_class_path (list[str] | None): Classpath entries; replaces existing.
        python_virtual_environment (str | None): Python venv control.
        extra_environment_vars (list[str] | None): Env vars; replaces existing.
        init_timeout_nanos (int | None): Initialization timeout in nanoseconds.
        auto_delete_timeout (int | None): Seconds of inactivity before auto-deletion. None
            skips; 0 makes the PQ permanent; a positive integer makes it temporary, deleted
            after that many seconds.
        admin_groups (list[str] | None): Admin groups; replaces existing.
        viewer_groups (list[str] | None): Viewer groups; replaces existing.
        restart_users (str | None): Restart-permission enum name (e.g., "RU_ADMIN").
        owner (str | None): Query owner.

    Returns:
        bool: True if any field was changed, False if every parameter was None.
    """
    has_changes = False
    if programming_language is not None:
        config_pb.scriptLanguage = _normalize_programming_language(programming_language)
        has_changes = True
    if script_body is not None:
        config_pb.scriptCode = script_body
        has_changes = True
    if script_path is not None:
        config_pb.scriptPath = script_path
        has_changes = True
    if restart_users is not None:
        config_pb.restartUsers = _convert_restart_users_to_enum(restart_users)
        has_changes = True
    if owner is not None:
        config_pb.owner = owner
        has_changes = True
    if _apply_auto_delete_timeout(config_pb, auto_delete_timeout):
        has_changes = True
    if _apply_pq_config_simple_fields(
        config_pb,
        pq_name,
        heap_size_gb,
        configuration_type,
        enabled,
        server,
        engine,
        jvm_profile,
        python_virtual_environment,
        init_timeout_nanos,
    ):
        has_changes = True
    if _apply_pq_config_list_fields(
        config_pb,
        schedule,
        extra_jvm_args,
        extra_class_path,
        extra_environment_vars,
        admin_groups,
        viewer_groups,
    ):
        has_changes = True
    return has_changes
