"""Wrapper classes for protobuf messages used by the Deephaven client module.

This module provides convenience wrappers around protobuf messages to offer
more Pythonic interfaces and utility methods for working with protobuf objects.
These wrappers simplify interaction with the underlying protobuf API by providing
standardized access methods, property accessors, and serialization helpers.

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
"""

from typing import Any, NewType

from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.message import Message

from ._base import is_enterprise_available

if is_enterprise_available:
    from deephaven_enterprise.client.controller import ControllerClient
else:
    ControllerClient = None


# Type definitions
CorePlusQuerySerial = NewType("CorePlusQuerySerial", int)
"""Type representing the serial number of a query."""


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
        if pb is None:
            raise ValueError("Protobuf message cannot be None")

        self._pb = pb

    def __repr__(self) -> str:
        """Returns a string representation of the wrapper."""
        pb_type = type(self._pb).__name__
        return f"<{self.__class__.__name__} wrapping {pb_type}>"

    @property
    def pb(self) -> Message:
        """The underlying protobuf message."""
        return self._pb

    def to_dict(self) -> dict[str, Any]:
        """Returns the protobuf message as a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the protobuf message
        """
        return MessageToDict(self._pb, preserving_proto_field_name=True)

    def to_json(self) -> str:
        """Returns the protobuf message as a JSON string.

        Returns:
            str: A JSON string representation of the protobuf message
        """
        return MessageToJson(self._pb, preserving_proto_field_name=True)


class CorePlusQueryStatus(ProtobufWrapper):
    """Wrapper for a PersistentQueryStatusEnum value.

    This class wraps a protobuf enum value for query status.
    It provides utility methods for checking status conditions
    by delegating to ControllerClient methods.

    This class simplifies status checking with properties like is_running, is_completed,
    is_terminal and is_uninitialized. It also supports string comparison with the status
    name and direct comparison with other status objects.

    Example:
        >>> status = CorePlusQueryStatus(pb_status_enum)
        >>> if status.is_running:
        ...     print(f"Query is running with status: {status}")
        >>> if status == "RUNNING":
        ...     print("Status matches string 'RUNNING'")

    This corresponds to PersistentQueryStatusEnum in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.controller.PersistentQueryStatusEnum
    """

    def __init__(
        self,
        status: "deephaven_enterprise.proto.controller.PersistentQueryStatusEnum",  # noqa: F821
    ):
        """Initialize with a protobuf status enum value.

        Args:
            status: The protobuf enum value for query status
        """
        super().__init__(status)

    def __str__(self) -> str:
        """Return the string representation of the status."""
        return self.name

    def __eq__(self, other: object) -> bool:
        """Compare this status with another status or string."""
        if isinstance(other, CorePlusQueryStatus):
            return self.pb == other.pb
        elif isinstance(other, str):
            return self.name == other
        return self.pb == other

    @property
    def name(self) -> str:
        """Get the string name of the status."""
        return ControllerClient.status_name(self.pb)

    @property
    def is_running(self) -> bool:
        """Check if the query status is running."""
        return ControllerClient.is_running(self.pb)

    @property
    def is_completed(self) -> bool:
        """Check if the query status is completed."""
        return ControllerClient.is_completed(self.pb)

    @property
    def is_terminal(self) -> bool:
        """Check if the query status is in a terminal state."""
        return ControllerClient.is_terminal(self.pb)

    @property
    def is_uninitialized(self) -> bool:
        """Check if the query status is uninitialized."""
        return ControllerClient.is_status_uninitialized(self.pb)


class CorePlusToken(ProtobufWrapper):
    """
    Wrapper for authentication Token message.

    This class wraps a protobuf Token message (type: deephaven_enterprise.proto.auth_pb2.Token)
    to provide a more convenient interface for accessing token information such as service name,
    issuer, and expiration time.

    It simplifies the interaction with authentication tokens in the Deephaven environment,
    allowing for easier token management and validation. The wrapped token contains
    information about authentication credentials, including:
    - The token value itself (a string used for authentication)
    - Service information (what service the token authenticates with)
    - Expiration time (when the token becomes invalid)
    - Issuer information (who created/issued the token)
    - User identity information (who the token represents)

    Args:
        token: The protobuf Token message to wrap (type: deephaven_enterprise.proto.auth_pb2.Token)

    Example:
        >>> token = CorePlusToken(pb_token)
        >>> token_dict = token.to_dict()
        >>> print(f"Token expires: {token_dict.get('expires_at')}")
        >>> print(f"Token issuer: {token_dict.get('issuer')}")

    This corresponds to Token in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.auth.Token
    """

    def __init__(
        self, token: "deephaven_enterprise.proto.auth_pb2.Token"  # noqa: F821
    ):
        """Initialize with a protobuf Token message.

        Args:
            token: The protobuf Token message to wrap
        """
        super().__init__(token)


class CorePlusQueryConfig(ProtobufWrapper):
    """Wrapper for a PersistentQueryConfigMessage.

    Provides a more Pythonic interface to the query configuration. This class wraps
    the protobuf configuration message for persistent queries to make it easier to
    work with in Python code.

    The configuration contains settings that determine how a persistent query is executed,
    including but not limited to:
    - Query name and description
    - Memory allocation (heap size)
    - CPU allocation and priority
    - Server/node placement constraints
    - Engine type and version
    - Query source definition (script, table, application)
    - Initialization parameters
    - Timeout settings
    - Replication settings

    Query configurations are typically created using helper methods like
    `make_temporary_config()` from the controller client, rather than constructed manually.

    Example:
        >>> config = CorePlusQueryConfig(pb_config)
        >>> config_dict = config.to_dict()
        >>> print(f"Query name: {config_dict.get('name')}")
        >>> print(f"Heap size: {config_dict.get('heap_size_mb')} MB")
        >>> print(f"Engine: {config_dict.get('engine_type')}")

    This corresponds to PersistentQueryConfigMessage in the protobuf definition:
    https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.persistent_query.PersistentQueryConfigMessage
    """

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
        """The wrapped configuration of the query."""
        return self._config

    @property
    def state(self) -> CorePlusQueryState | None:
        """The wrapped state of the query, if present."""
        return self._state

    @property
    def replicas(self) -> list[CorePlusQueryState]:
        """A list of wrapped replica states for the query."""
        return self._replicas

    @property
    def spares(self) -> list[CorePlusQueryState]:
        """A list of wrapped spare states for the query."""
        return self._spares
