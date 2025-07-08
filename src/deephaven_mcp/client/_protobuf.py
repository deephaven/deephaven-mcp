"""Wrapper classes for protobuf messages used by the Deephaven client module.

This module provides convenience wrappers around Google Protocol Buffer (protobuf) messages
to offer more Pythonic interfaces and utility methods for working with protobuf objects.
These wrappers simplify interaction with the underlying protobuf API by providing
standardized access methods, property accessors, and serialization helpers.

Protobuf messages are the core data structures used for communication between the
Deephaven client and server components. They provide a compact, efficient, and
schema-driven serialization format for data exchange. However, working directly with
protobuf objects can be cumbersome in Python due to their C++-like accessor patterns
and lack of Pythonic features.

These wrapper classes solve several problems:
1. Provide a more natural Python interface with properties and methods
2. Abstract away protobuf-specific implementation details
3. Add helper methods for common operations and queries
4. Enable easy JSON and dictionary conversions for debugging and logging
5. Facilitate type checking and IDE auto-completion

The architecture follows a consistent pattern where each wrapper class:
- Wraps a specific protobuf message or enum type
- Inherits from the ProtobufWrapper base class
- Provides specialized methods relevant to the wrapped object's domain
- Maintains access to the underlying protobuf object when needed

The module contains wrapper classes for various protobuf message types used
in client-server communication, including query configurations, state information,
status enums, and authentication tokens.

Classes:
    ProtobufWrapper: Base class providing common functionality for protobuf wrappers.
    CorePlusQueryStatus: Wrapper for query status enum values with convenience methods.
    CorePlusToken: Wrapper for authentication token messages.
    CorePlusQueryConfig: Wrapper for query configuration messages.
    CorePlusQueryState: Wrapper for query state messages.
    CorePlusQueryInfo: Wrapper for comprehensive query information messages.

Type Definitions:
    CorePlusQuerySerial: Type representing the serial number of a query.

Usage Note:
    These wrapper classes are designed to be used with the Deephaven Enterprise
    protobuf definitions. The wrappers themselves do not depend on the Enterprise
    package at import time (conditional imports are used), but they expect to
    wrap specific protobuf message types at runtime.
"""

import sys
from typing import TYPE_CHECKING, Any, NewType, cast

from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.message import Message

if TYPE_CHECKING:
    import deephaven_enterprise.client.auth  # pragma: no cover
    import deephaven_enterprise.client.controller  # pragma: no cover
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from ._base import is_enterprise_available

if is_enterprise_available:
    from deephaven_enterprise.client.controller import ControllerClient
else:
    ControllerClient = None  # pragma: no cover


# Type definitions
CorePlusQuerySerial = NewType("CorePlusQuerySerial", int)
"""Type representing the serial number of a persistent query.

A query serial is a unique identifier assigned to each persistent query in the Deephaven system.
It is used to reference, lookup, and manage specific query instances. Query serials are
integers that are assigned incrementally by the controller service when queries are created.

Unlike query names (which are optional and user-defined), serials are guaranteed to be unique
within a Deephaven server instance and are the primary key for query identification in the API.

Example:
    >>> query_serial = CorePlusQuerySerial(12345)
    >>> info = await controller_client.get_persistent_query(query_serial)
"""


class ProtobufWrapper:
    """A wrapper for a protobuf message that provides convenience methods.

    This base class provides common functionality for all protobuf wrapper classes,
    including dictionary and JSON serialization. It enforces non-null protobuf messages
    and provides a consistent interface for accessing the underlying protobuf object.

    Example:
        >>> wrapper = ProtobufWrapper(pb_message)
        >>> dict_data = wrapper.to_dict()
        >>> json_str = wrapper.to_json()
    """

    def __init__(self, pb: Message):
        """Initialize with a protobuf message object.

        This constructor validates that the provided protobuf message is not None
        and stores it as the wrapped object. All wrapper subclasses will inherit
        this validation behavior.

        Args:
            pb: The protobuf message object to wrap. Must not be None.

        Raises:
            ValueError: If the provided protobuf message is None.
        """
        if pb is None:
            raise ValueError("Protobuf message cannot be None")

        self._pb = pb

    def __repr__(self) -> str:
        """Return a string representation of the wrapper."""
        pb_type = type(self._pb).__name__
        return f"<{self.__class__.__name__} wrapping {pb_type}>"

    @property
    def pb(self) -> Message:
        """The underlying protobuf message."""
        return self._pb

    def to_dict(self) -> dict[str, Any]:
        """Return the protobuf message as a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the protobuf message
        """
        return cast(
            dict[str, Any], MessageToDict(self._pb, preserving_proto_field_name=True)
        )

    def to_json(self) -> str:
        """Return the protobuf message as a JSON string.

        Returns:
            str: A JSON string representation of the protobuf message
        """
        return cast(str, MessageToJson(self._pb, preserving_proto_field_name=True))


class CorePlusQueryStatus(ProtobufWrapper):
    """Wrapper for a PersistentQueryStatusEnum value providing status checking functionality.

    This class wraps a protobuf enum value for persistent query status, which represents
    the current lifecycle state of a query or worker process in the Deephaven system.
    It provides utility methods and properties for checking status conditions
    by delegating to ControllerClient methods, simplifying status-based decision making.

    Common status values include:
    - UNINITIALIZED: Initial state before query execution begins
    - INITIALIZING: Query is being set up but not yet running
    - RUNNING: Query is actively executing and processing data
    - STOPPING: Query is in the process of shutting down gracefully
    - STOPPED: Query has been gracefully terminated
    - COMPLETED: Query has finished execution successfully
    - FAILED: Query encountered an error and terminated abnormally
    - KILLED: Query was forcibly terminated

    Typical status transitions follow this pattern:
    UNINITIALIZED → INITIALIZING → RUNNING → [STOPPING] → STOPPED/COMPLETED/FAILED/KILLED

    This class simplifies status checking with properties like is_running, is_completed,
    is_terminal and is_uninitialized. It also supports flexible equality comparison with:
    - Other CorePlusQueryStatus objects
    - String status names (e.g., "RUNNING")
    - Raw enum values

    Example:
        >>> status = CorePlusQueryStatus(pb_status_enum)
        >>> if status.is_running:
        ...     print(f"Query is running with status: {status}")
        >>> elif status.is_completed:
        ...     print(f"Query has completed successfully")
        >>> elif status.is_terminal and not status.is_completed:
        ...     print(f"Query terminated abnormally with status: {status}")
        >>>
        >>> # String comparison for specific status values
        >>> if status == "RUNNING":
        ...     print("Status matches string 'RUNNING'")
        >>> elif status == "FAILED":
        ...     print("Query has failed and needs attention")

    Note:
        The properties provided by this class (is_running, is_completed, etc.) are
        preferred over direct string comparisons because they handle groups of related
        states. For example, is_terminal will return True for all end states regardless
        of whether they represent success or failure.

    This corresponds to PersistentQueryStatusEnum in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.controller.PersistentQueryStatusEnum
    """

    @override
    def __init__(
        self,
        status: "deephaven_enterprise.proto.controller.PersistentQueryStatusEnum",  # noqa: F821
    ):
        """Initialize with a protobuf status enum value.

        Args:
            status: The protobuf enum value for query status
        """
        super().__init__(status)

    @override
    def __str__(self) -> str:
        """Return the string representation of the status."""
        return self.name

    @override
    def __eq__(self, other: object) -> bool:
        """Compare this status with another status or string."""
        if isinstance(other, CorePlusQueryStatus):
            return cast(bool, self.pb == other.pb)
        elif isinstance(other, str):
            return self.name == other
        return cast(bool, self.pb == other)

    @property
    def name(self) -> str:
        """Get the string name of the status.

        Returns:
            str: The name of the status enum value (e.g., "RUNNING", "COMPLETED", "FAILED").
                This is useful for logging, display, and string-based comparisons.
        """
        return cast(str, ControllerClient.status_name(self.pb))

    @property
    def is_running(self) -> bool:
        """Check if the query status is running.

        A running query is actively processing data and executing its defined operations.
        The query may be in either the "RUNNING" state or certain transitional states
        that are considered equivalent to running for most practical purposes.

        Returns:
            bool: True if the query is in a running state, False otherwise.
        """
        return cast(bool, ControllerClient.is_running(self.pb))

    @property
    def is_completed(self) -> bool:
        """Check if the query status is completed.

        A completed query has finished its execution successfully. This is different from
        other terminal states like FAILED or KILLED, which indicate abnormal termination.
        The COMPLETED state generally indicates that the query finished all its work as expected.

        Returns:
            bool: True if the query has completed successfully, False otherwise.
        """
        return cast(bool, ControllerClient.is_completed(self.pb))

    @property
    def is_terminal(self) -> bool:
        """Check if the query status is in a terminal state.

        Terminal states represent the end of a query's lifecycle. No further state transitions
        will occur once a query reaches a terminal state. Terminal states include:
        COMPLETED, FAILED, KILLED, and sometimes STOPPED depending on the configuration.

        A query in a terminal state will not resume processing without explicit
        user intervention (such as restarting it).

        Returns:
            bool: True if the query is in a terminal state, False otherwise.
        """
        return cast(bool, ControllerClient.is_terminal(self.pb))

    @property
    def is_uninitialized(self) -> bool:
        """Check if the query status is uninitialized.

        The uninitialized state is the initial state of a query before it starts executing.
        A query in this state has been defined but has not yet begun the initialization process.

        Returns:
            bool: True if the query is in the uninitialized state, False otherwise.
        """
        return cast(bool, ControllerClient.is_status_uninitialized(self.pb))


class CorePlusToken(ProtobufWrapper):
    """
    Wrapper for authentication Token message in the Deephaven authentication system.

    This class wraps a protobuf Token message (type: deephaven_enterprise.proto.auth_pb2.Token)
    to provide a more convenient interface for accessing token information such as service name,
    issuer, and expiration time. Tokens are central to Deephaven's authentication and
    authorization system, representing a user's validated identity and permissions.

    It simplifies the interaction with authentication tokens in the Deephaven environment,
    allowing for easier token management and validation. The wrapped token contains
    information about authentication credentials, including:
    - The token value itself (a string used for authentication)
    - Service information (what service the token authenticates with)
    - Expiration time (when the token becomes invalid)
    - Issuer information (who created/issued the token)
    - User identity information (who the token represents)
    - Roles and permissions (what operations the token authorizes)

    Tokens are typically obtained through authentication methods like password, SAML, or
    private key authentication, and then used for subsequent API calls. They have a limited
    lifetime and may need to be refreshed periodically during long-running operations.

    In the authentication flow:
    1. A user authenticates using credentials (username/password, private key, SAML)
    2. The auth service returns a token (wrapped by this class)
    3. The token is included in subsequent API requests
    4. Services validate the token before processing requests
    5. When the token expires, re-authentication is required

    Args:
        token: The protobuf Token message to wrap (type: deephaven_enterprise.proto.auth_pb2.Token)

    Example:
        >>> token = CorePlusToken(pb_token)
        >>> token_dict = token.to_dict()
        >>> print(f"Token expires: {token_dict.get('expires_at')}")
        >>> print(f"Token issuer: {token_dict.get('issuer')}")
        >>> print(f"Authenticated user: {token_dict.get('user_identity', {}).get('username')}")
        >>>
        >>> # Check if token is expired
        >>> import datetime
        >>> now = datetime.datetime.now().isoformat()
        >>> if token_dict.get('expires_at', now) < now:
        ...     print("Token has expired, re-authentication required")

    This corresponds to Token in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.auth.Token
    """

    @override
    def __init__(
        self, token: "deephaven_enterprise.proto.auth_pb2.Token"  # noqa: F821
    ):
        """Initialize with a protobuf Token message.

        Args:
            token: The protobuf Token message to wrap
        """
        super().__init__(token)


class CorePlusQueryConfig(ProtobufWrapper):
    """Wrapper for a PersistentQueryConfigMessage defining how a query should be executed.

    Provides a more Pythonic interface to the query configuration. This class wraps
    the protobuf configuration message for persistent queries to make it easier to
    work with in Python code. It enables defining all aspects of how a query should
    be instantiated and executed in the Deephaven system.

    The configuration contains settings that determine how a persistent query is executed,
    including but not limited to:
    - Query name and description: Identifiers and metadata for the query
    - Memory allocation (heap_size_mb): JVM heap size in megabytes allocated to the query
    - CPU allocation and priority: Resource limits and scheduling priority
    - Server/node placement constraints: Which physical or virtual servers can run the query
    - Engine type and version: The processing engine implementation to use (e.g., "DeephavenCommunity")
    - Query source definition: The source code, table, or application to execute
      - script: Python, Groovy, or other script code to execute
      - table: Reference to an existing table to process
      - application: Custom application-specific configuration
    - Initialization parameters: Parameters passed to the query at startup
    - Timeout settings: How long the query can run or remain idle
    - Replication settings: How many replicas should be maintained for fault tolerance
    - Auto-start policy: Whether the query should start automatically after creation
    - Restart policy: How to handle query failures (restart automatically or not)

    Configuration objects are immutable - to modify a configuration, you must create a new
    one with the desired changes.

    Query configurations are typically created using helper methods like
    `make_temporary_config()` from the controller client, rather than constructed manually.
    These helper methods provide sensible defaults and validation to ensure a valid configuration.

    Example - Basic config inspection:
        >>> config = CorePlusQueryConfig(pb_config)
        >>> config_dict = config.to_dict()
        >>> print(f"Query name: {config_dict.get('name')}")
        >>> print(f"Heap size: {config_dict.get('heap_size_mb')} MB")
        >>> print(f"Engine: {config_dict.get('engine_type')}")
        >>>
        >>> # Check script source if present
        >>> source = config_dict.get('source', {})
        >>> if 'script' in source:
        ...     print(f"Script language: {source['script'].get('language')}")
        ...     script_text = source['script'].get('text', '')
        ...     print(f"Script preview: {script_text[:50]}..." if len(script_text) > 50 else script_text)
        >>>
        >>> # Check replication settings
        >>> if 'replication' in config_dict:
        ...     rep_config = config_dict['replication']
        ...     print(f"Replicas: {rep_config.get('replicas', 0)}, Spares: {rep_config.get('spares', 0)}")

    This corresponds to PersistentQueryConfigMessage in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.persistent_query.PersistentQueryConfigMessage
    """

    @override
    def __init__(
        self,
        config: "deephaven_enterprise.proto.persistent_query_pb2.PersistentQueryConfigMessage",  # noqa: F821
    ):
        """Initialize with a protobuf PersistentQueryConfigMessage.

        Args:
            config: The protobuf configuration message to wrap
        """
        super().__init__(config)


class CorePlusQueryState(ProtobufWrapper):
    """Wrapper for a PersistentQueryStateMessage.

    This class wraps the protobuf state message for persistent queries to provide
    a more convenient interface for accessing state information such as query status.

    The state contains runtime information about a persistent query, including:
    - Current execution status (running, stopped, failed, etc.)
    - Runtime metrics and resource usage statistics
    - Initialization and update timestamps
    - Execution history and progress information
    - Error information and diagnostics (if applicable)
    - Worker node/host information

    The most commonly accessed property is `status`, which provides a CorePlusQueryStatus
    object that can be used to determine the current execution state of the query.

    Example:
        >>> state = CorePlusQueryState(pb_state)
        >>> status = state.status
        >>> if status.is_running:
        ...     print(f"Query is running")
        >>> elif status.is_terminal:
        ...     print(f"Query has terminated with status: {status}")
        ...     # Access error information if available
        ...     state_dict = state.to_dict()
        ...     if 'error_message' in state_dict:
        ...         print(f"Error: {state_dict['error_message']}")

    This corresponds to PersistentQueryStateMessage in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.persistent_query.PersistentQueryStateMessage
    """

    @override
    def __init__(
        self,
        state: "deephaven_enterprise.proto.persistent_query_pb2.PersistentQueryStateMessage",  # noqa: F821
    ):
        """Initialize with a protobuf PersistentQueryStateMessage.

        Args:
            state: The protobuf state message to wrap
        """
        super().__init__(state)

    @property
    def status(self) -> CorePlusQueryStatus:
        """Returns the status of the query."""
        return CorePlusQueryStatus(self.pb.status)


class CorePlusQueryInfo(ProtobufWrapper):
    """Wrapper for a PersistentQueryInfoMessage.

    Provides a more Pythonic interface to the query info by wrapping the
    nested config and state messages into their respective wrapper classes.

    This is a comprehensive wrapper that combines configuration, state, and replication
    information for a persistent query. It serves as the main access point for working
    with persistent queries and provides convenient access to all aspects of a query's
    definition and runtime state.

    Key components of a CorePlusQueryInfo include:
    - config: The CorePlusQueryConfig containing the query's configuration parameters
    - state: A CorePlusQueryState representing the primary query's current state (may be None)
    - replicas: A list of CorePlusQueryState objects for any replica instances of the query
    - spares: A list of CorePlusQueryState objects for any spare instances of the query

    This class is typically obtained from the controller client's `map()` or `get()`
    methods and provides all the information needed to monitor and manage a query.

    Example:
        >>> info = CorePlusQueryInfo(pb_info)
        >>> # Access query configuration
        >>> config = info.config
        >>> config_dict = config.to_dict()
        >>> print(f"Query name: {config_dict.get('name')}")
        >>>
        >>> # Access query state
        >>> state = info.state
        >>> if state and state.status.is_running:
        ...     print(f"Query is running with {len(info.replicas)} replicas")
        >>>
        >>> # Check replication status
        >>> if info.replicas:
        ...     print(f"Query has {len(info.replicas)} active replicas")
        ...     for i, replica in enumerate(info.replicas):
        ...         print(f"Replica {i} status: {replica.status}")

    This corresponds to PersistentQueryInfoMessage in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.persistent_query.PersistentQueryInfoMessage
    """

    @override
    def __init__(
        self,
        info: "deephaven_enterprise.proto.persistent_query_pb2.PersistentQueryInfoMessage",  # noqa: F821
    ):
        """Initialize with a protobuf PersistentQueryInfoMessage.

        Args:
            info: The protobuf query info message to wrap
        """
        super().__init__(info)
        self._config: CorePlusQueryConfig = CorePlusQueryConfig(info.config)
        self._state: CorePlusQueryState | None = (
            CorePlusQueryState(info.state) if info.state else None
        )
        self._replicas: list[CorePlusQueryState] = [
            CorePlusQueryState(r) for r in info.replicas
        ]
        self._spares: list[CorePlusQueryState] = [
            CorePlusQueryState(s) for s in info.spares
        ]

    @property
    def config(self) -> CorePlusQueryConfig:
        """The wrapped configuration of the query.

        Returns:
            CorePlusQueryConfig: A wrapper for the query's configuration settings,
                containing parameters like name, heap size, and engine type.
        """
        return self._config

    @property
    def state(self) -> CorePlusQueryState | None:
        """The wrapped state of the query, if present.

        The state may be None if the query hasn't been initialized or if state
        information wasn't included in the original protobuf message.

        Returns:
            CorePlusQueryState | None: A wrapper for the query's primary state
                information if available, or None if no state exists.
        """
        return self._state

    @property
    def replicas(self) -> list[CorePlusQueryState]:
        """A list of wrapped replica states for the query.

        Replicas are additional instances of the query that may be running for
        high availability or load balancing purposes. Each replica has its own
        state that can be monitored independently.

        Returns:
            list[CorePlusQueryState]: A list of state wrappers for all active replicas.
                Returns an empty list if no replicas exist.
        """
        return self._replicas

    @property
    def spares(self) -> list[CorePlusQueryState]:
        """A list of wrapped spare states for the query.

        Spares are pre-initialized but inactive instances of the query that can
        quickly take over if the primary or a replica instance fails. They are
        part of the high availability strategy for critical queries.

        Returns:
            list[CorePlusQueryState]: A list of state wrappers for all spare instances.
                Returns an empty list if no spares exist.
        """
        return self._spares
