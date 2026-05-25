"""Asynchronous wrapper for the Deephaven ControllerClient.

This module provides an asynchronous wrapper around the Deephaven ControllerClient, enabling non-blocking
operations with the Persistent Query Controller in the Deephaven MCP environment. It manages persistent queries
and their state changes while maintaining the same interface as the original ControllerClient.

The Persistent Query Controller is a core component of Deephaven Enterprise responsible for:
- Creating and managing long-running query processes (workers)
- Monitoring query lifecycle and state changes
- Resource allocation and management for queries
- Query replication and fault tolerance

Key features of this asynchronous wrapper:
1. Full compatibility with modern async/await programming paradigms
2. Non-blocking operations that won't stall the Python event loop
3. Enhanced error handling with specific exception types for better diagnostics
4. Consistent logging for operations and error conditions

All blocking operations are performed using asyncio.to_thread, allowing client code to use async/await syntax
without blocking the event loop. The wrapper also enhances error handling by wrapping exceptions in more specific
and informative custom exception types (e.g., QueryError, DeephavenConnectionError).

The controller client requires subscription initialization via subscribe() before query state operations.
When created through CorePlusSessionFactory, subscription is handled automatically during factory initialization.

Typical usage flow:
1. Create query configurations and add queries
2. Start queries and wait for them to reach the running state
3. Monitor query status and handle state changes
4. Stop, restart, or delete queries as needed

Classes:
    CorePlusControllerClient: Async wrapper around deephaven_enterprise.client.controller.ControllerClient

See Also:
    - ._protobuf: Contains wrapper classes for query state, configuration, and other protobuf objects
    - ._auth_client: Provides authentication functionality used by the controller client
"""

import asyncio
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import deephaven_enterprise.client.controller  # pragma: no cover

from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    InternalError,
    QueryError,
    ResourceError,
)

from ._base import ClientObjectWrapper
from ._protobuf import (
    CorePlusQueryConfig,
    CorePlusQueryInfo,
    CorePlusQuerySerial,
)
from ._timeouts import EnterpriseClientTimeouts

_LOGGER = logging.getLogger(__name__)


# Default scheduling entries applied by ``CorePlusControllerClient.make_pq_config`` to
# a *permanent* PQ (``auto_delete_timeout=None``) when the caller passes
# ``schedule=None``. Produces a continuous scheduler that auto-starts the PQ after
# the controller accepts it. See ``make_pq_config`` for the full three-way semantics.
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


class CorePlusControllerClient(
    ClientObjectWrapper["deephaven_enterprise.client.controller.ControllerClient"]
):
    """Asynchronous wrapper around the ControllerClient for managing persistent queries.

    This class provides an asynchronous interface to the ControllerClient, which connects to the
    Deephaven PersistentQueryController process. It enables management of persistent queries,
    including creation, modification, and deletion of those queries.

    The controller client facilitates the entire lifecycle of persistent queries, including:
    - Managing query state changes
    - Creating query configurations with appropriate resource allocations
    - Adding new queries to the controller
    - Starting, stopping, restarting, and deleting queries
    - Monitoring query state and health
    - Managing query metadata and configuration

    All blocking calls are performed in separate threads using asyncio.to_thread to avoid blocking
    the event loop. The wrapper maintains the same interface as the underlying ControllerClient
    while making it compatible with asynchronous code.

    Error handling is enhanced with specific exception types that provide more context and clarity
    than the underlying gRPC errors surfaced from the Java controller server. Network issues
    typically result in DeephavenConnectionError and query-related issues in QueryError.

    Attributes:
        wrapped (deephaven_enterprise.client.controller.ControllerClient):
            The underlying Python ``ControllerClient`` being wrapped. (The
            controller's *server side* is a Java process; this attribute is the
            Python gRPC client that talks to it, not a Java object.)

    Example:
        # Create a controller client from an authenticated session factory
        # (URL must point at the server's connection.json; see CorePlusSessionFactory.from_url)
        session_factory = await CorePlusSessionFactory.from_url(
            "https://deephaven.example.com:10000/iris/connection.json"
        )
        await session_factory.password("username", "password")
        controller_client = session_factory.controller_client

        # Create a query configuration and add it
        config = await controller_client.make_pq_config("my-worker", heap_size_gb=2.0)
        serial = await controller_client.add_query(config)

        # Start the query and wait for it to initialize
        await controller_client.start_and_wait(serial)

        # Monitor the query state
        query_info = await controller_client.get(serial)
        print(f"Query state: {query_info.state}")

        # Clean up when done
        await controller_client.stop_query(serial)
        await controller_client.delete_query(serial)

    Notes:
        - All methods are asynchronous and use asyncio.to_thread to run blocking operations in a background thread.
        - Exceptions are wrapped in custom types for clarity (e.g., QueryError, DeephavenConnectionError).
        - Logging is performed for entry, success, and error events at appropriate levels.

    """

    def __init__(
        self,
        controller_client: "deephaven_enterprise.client.controller.ControllerClient",  # noqa: F821
        timeouts: EnterpriseClientTimeouts,
    ):
        """Initialize the CorePlusControllerClient with a ControllerClient instance.

        Args:
            controller_client (deephaven_enterprise.client.controller.ControllerClient): The ControllerClient instance to wrap.
            timeouts (EnterpriseClientTimeouts): Timeout values applied to this
                client's methods. Read field-by-field at use sites.
                Constructed by the :class:`CorePlusSessionFactory` that
                owns this client and forwarded here at construction time.
        """
        super().__init__(controller_client, is_enterprise=True)
        self._subscribed = False
        self._timeouts = timeouts
        _LOGGER.debug("[CorePlusControllerClient] Initialized")

    # ===========================================================================
    # Initialization & Connection Management
    # ===========================================================================

    async def ping(self) -> bool:
        """Ping the controller and refresh the cookie asynchronously.

        This method sends a lightweight ping request to the controller service to verify
        connectivity and refresh the authentication cookie. It's useful for:

        1. Verifying that the controller service is reachable and responsive
        2. Keeping the authentication session active by refreshing the cookie
        3. Detecting network or server issues early

        You can use this method periodically in long-running applications to ensure
        the connection remains active and detect any connectivity issues promptly.

        The ping timeout is sourced from
        ``EnterpriseClientTimeouts.quick_operation_timeout_seconds``.

        Returns:
            bool: True if the ping was sent successfully and the cookie was refreshed, False if
            there was no cookie to refresh (indicating the client may not be authenticated).

        Raises:
            DeephavenConnectionError: If the connection to the server fails due to network
                                    issues, if the controller service is unavailable, timeout,
                                    or if there are communication errors with the server.
        """
        timeout_seconds = self._timeouts.quick_operation_timeout_seconds
        _LOGGER.debug("[CorePlusControllerClient:ping] Sending ping to controller")
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self.wrapped.ping),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            _LOGGER.error(
                f"[CorePlusControllerClient:ping] Timed out after {timeout_seconds}s. "
                f"Increase enterprise/settings.json: timeouts.client.quick_operation_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Controller ping timed out after {timeout_seconds} seconds. "
                f"To allow more time, increase enterprise/settings.json: "
                f"timeouts.client.quick_operation_timeout_seconds in the operator config."
            ) from None
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:ping] Failed to ping controller: {e}"
            )
            raise DeephavenConnectionError(f"Failed to ping controller: {e}") from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:ping] Unexpected error during ping: {e}"
            )
            raise DeephavenConnectionError(f"Connection error during ping: {e}") from e

    async def subscribe(self) -> None:
        """Subscribe to persistent query state updates asynchronously.

        This method establishes a subscription to the controller's persistent query state
        and waits for the initial query state snapshot to be populated. It MUST be called
        before using state query methods like map(), get(), and wait_for_change().

        The subscription enables the controller client to receive and track changes to
        persistent queries, including:
        - New queries being created
        - Existing queries changing state (RUNNING, STOPPED, FAILED, etc.)
        - Queries being deleted or modified
        - Query configuration updates

        After subscription completes successfully, you can call:
        - map() to retrieve the complete query state map
        - get(serial) to fetch specific queries by serial number
        - wait_for_change() to wait for state updates

        A successful call to authenticate should have happened before this call.

        This method is idempotent - calling it multiple times is safe and will only
        subscribe once. Subsequent calls will return immediately without error.

        The subscription timeout is sourced from
        ``EnterpriseClientTimeouts.subscribe_timeout_seconds``.

        Raises:
            DeephavenConnectionError: If not authenticated, if unable to connect to the
                                    controller service due to network issues, if the
                                    controller is unavailable, or if subscription times out.
            QueryError: If the subscription fails due to invalid state, permission issues,
                       or any other operational reason.

        Note:
            When using CorePlusSessionFactory.from_url() or from_config(), this method
            is called automatically during factory initialization. Manual subscription
            is only needed if you construct the CorePlusControllerClient directly.
        """
        timeout_seconds = self._timeouts.subscribe_timeout_seconds
        # If already subscribed, return early (idempotent behavior)
        if self._subscribed:
            _LOGGER.debug(
                "[CorePlusControllerClient:subscribe] Already subscribed, skipping"
            )
            return

        _LOGGER.debug(
            f"[CorePlusControllerClient:subscribe] Subscribing to query state (timeout_seconds={timeout_seconds})"
        )
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self.wrapped.subscribe),
                timeout=timeout_seconds,
            )
            self._subscribed = True
            _LOGGER.debug(
                "[CorePlusControllerClient:subscribe] Successfully subscribed to query state"
            )
        except TimeoutError:
            _LOGGER.error(
                f"[CorePlusControllerClient:subscribe] Subscription timed out after {timeout_seconds}s. "
                f"Increase enterprise/settings.json: timeouts.client.subscribe_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Controller subscription timed out after {timeout_seconds} seconds. "
                f"To allow more time, increase enterprise/settings.json: "
                f"timeouts.client.subscribe_timeout_seconds in the operator config."
            ) from None
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:subscribe] Connection error during subscription: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:subscribe] Failed to subscribe to query state: {e}"
            )
            raise QueryError(
                f"Failed to subscribe to persistent query state: {e}"
            ) from e

    # ===========================================================================
    # Query State Management
    # ===========================================================================

    async def map(self) -> dict[CorePlusQuerySerial, CorePlusQueryInfo]:
        """Retrieve a copy of the current persistent query state asynchronously.

        This method returns a complete snapshot of all queries managed by the controller,
        including their configurations, status information, and current state. The returned
        dictionary provides a comprehensive view of all queries at the time of calling.

        The dictionary is keyed by query serial numbers, with each value being a CorePlusQueryInfo
        object containing details about that specific query such as:
        - Name and description
        - Current state (UNINITIALIZED, INITIALIZING, RUNNING, STOPPED, FAILED, etc.)
        - Creation time and last update time
        - Resource allocation and utilization
        - Configuration parameters

        A successful call to subscribe should have happened before this call, as this method
        retrieves data from the subscription snapshot.

        Returns:
            dict[CorePlusQuerySerial, CorePlusQueryInfo]: A dictionary mapping query serial numbers to
            CorePlusQueryInfo objects containing detailed information about each persistent query
            managed by the controller. The dictionary will be empty if no queries are managed.

        Raises:
            InternalError: If ``subscribe()`` was not called before this method.
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller is unavailable.
            QueryError: If the subscription state is invalid (for example, if the
                       subscription has been invalidated server-side).
        """
        if not self._subscribed:
            _LOGGER.error(
                "[CorePlusControllerClient:map] subscribe() must be called before map(). "
                "This indicates a programming bug - the controller client was not properly initialized."
            )
            raise InternalError(
                "subscribe() must be called before map(). This indicates a programming bug - "
                "the controller client was not properly initialized."
            )
        _LOGGER.debug("[CorePlusControllerClient:map] Retrieving query map")
        try:
            # The map is from int to QueryInfo, but we need to cast the keys to QuerySerial
            # for type safety. The values are wrapped in CorePlusQueryInfo.
            raw_map = await asyncio.to_thread(self.wrapped.map)
            return {
                cast(CorePlusQuerySerial, k): CorePlusQueryInfo(v)
                for k, v in raw_map.items()
            }
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:map] Connection error while retrieving query map: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:map] Failed to retrieve query map: {e}"
            )
            raise QueryError(f"Failed to retrieve query state: {e}") from e

    async def map_and_version(
        self,
    ) -> tuple[dict[CorePlusQuerySerial, CorePlusQueryInfo], int]:
        """Retrieve query state with version number for synchronization.

        This method returns the current persistent query state alongside a version number
        that tracks changes to the subscription map. The version number is monotonically
        increasing and increments every time the map changes (query created, deleted, or
        state modified).

        This is the proper way to detect stale data - if you cache the version number and
        later call this method again, a different version indicates the map has changed.

        Returns:
            tuple[dict[CorePlusQuerySerial, CorePlusQueryInfo], int]: A tuple containing:
                - Dictionary mapping query serial numbers to CorePlusQueryInfo objects
                - Version number (int) representing the current map state

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service.
            QueryError: If the subscription state is invalid (for example, if the
                subscription has been invalidated server-side).
            InternalError: If subscribe() was not called before this method.
        """
        if not self._subscribed:
            _LOGGER.error(
                "[CorePlusControllerClient:map_and_version] subscribe() must be called before map_and_version(). "
                "This indicates a programming bug - the controller client was not properly initialized."
            )
            raise InternalError(
                "subscribe() must be called before map_and_version(). This indicates a programming bug - "
                "the controller client was not properly initialized."
            )
        _LOGGER.debug(
            "[CorePlusControllerClient:map_and_version] Retrieving query map with version"
        )
        try:
            raw_map, version = await asyncio.to_thread(self.wrapped.map_and_version)
            query_map = {
                cast(CorePlusQuerySerial, k): CorePlusQueryInfo(v)
                for k, v in raw_map.items()
            }
            _LOGGER.debug(
                f"[CorePlusControllerClient:map_and_version] Retrieved {len(query_map)} queries, version={version}"
            )
            return query_map, version
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:map_and_version] Connection error: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:map_and_version] Failed to retrieve query map: {e}"
            )
            raise QueryError(f"Failed to retrieve query state with version: {e}") from e

    async def get_serial_for_name(self, name: str) -> CorePlusQuerySerial:
        """Retrieve the serial number for a given query name asynchronously.

        This method looks up a query by its name and returns the corresponding serial number.
        Query names are human-readable identifiers specified when creating the query (e.g., in
        the make_pq_config method), while serial numbers are system-assigned unique
        identifiers used for most controller operations.

        This method is particularly useful when you want to reference a query by its human-readable
        name rather than tracking its serial number. For example, when connecting to an existing
        query that was created by another process or user.

        The wait duration is sourced from
        ``EnterpriseClientTimeouts.no_wait_seconds`` (default ``0.0`` — no wait).

        Args:
            name (str): The name of the query to find. This is the human-readable name specified
                 when the query was created.

        Returns:
            CorePlusQuerySerial: The serial number for the query with the given name. This can be used with
            other controller methods that require a CorePlusQuerySerial.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller is unavailable.
            QueryError: If no query with the given name is found within the wait period,
                       if the subscription state is invalid, or for any other operational
                       failure (the upstream ``RuntimeError`` raised on "not found" is
                       translated to ``QueryError`` by this wrapper).
            InternalError: If subscribe() was not called before this method.
        """
        timeout_seconds = self._timeouts.no_wait_seconds
        if not self._subscribed:
            _LOGGER.error(
                "[CorePlusControllerClient:get_serial_for_name] subscribe() must be called before get_serial_for_name(). "
                "This indicates a programming bug - the controller client was not properly initialized."
            )
            raise InternalError(
                "subscribe() must be called before get_serial_for_name(). This indicates a programming bug - "
                "the controller client was not properly initialized."
            )
        _LOGGER.debug(
            f"[CorePlusControllerClient:get_serial_for_name] Looking up serial for query name='{name}'"
        )
        try:
            return cast(
                CorePlusQuerySerial,
                await asyncio.to_thread(
                    self.wrapped.get_serial_for_name, name, timeout_seconds
                ),
            )
        except (TimeoutError, ValueError):
            # Re-raise native exceptions unchanged
            raise
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:get_serial_for_name] Connection error while retrieving serial for query '{name}': {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:get_serial_for_name] Failed to get serial for query name '{name}': {e}"
            )
            raise QueryError(f"Failed to find query with name '{name}': {e}") from e

    async def wait_for_change(self, timeout_seconds: float) -> None:
        """Wait for a change in the query map to occur asynchronously.

        This method blocks until there is a change in the query state managed by the controller,
        or until the specified timeout is reached. Changes can include:

        1. New queries being created
        2. Existing queries changing state (e.g., from INITIALIZING to RUNNING or from RUNNING to STOPPED)
        3. Queries being deleted
        4. Query configuration or metadata changes

        This method is particularly useful for building reactive applications that need to
        respond to query state changes, such as UIs that show the current state of all queries
        or monitoring tools that track query lifecycle events.

        A normal return means the wait ended; this method does not distinguish
        "a change was observed" from "the wait ended for any other reason". If
        that distinction matters, use ``wait_for_change_from_version``, which
        returns a ``bool``.

        Args:
            timeout_seconds (float): How long to wait for a change, in seconds.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller becomes unavailable.
            TimeoutError: Propagated unchanged when the underlying call raises one.
            QueryError: If there is an issue with the query state or subscription, such as if
                       the subscription was not properly established with subscribe().
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:wait_for_change] Waiting for query state change, timeout={timeout_seconds}"
        )
        try:
            await asyncio.to_thread(self.wrapped.wait_for_change, timeout_seconds)
        except TimeoutError:
            # Re-raise TimeoutError unchanged
            raise
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:wait_for_change] Connection error while waiting for query state change: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:wait_for_change] Failed to wait for change: {e}"
            )
            raise QueryError(f"Failed to wait for query state change: {e}") from e

    async def wait_for_change_from_version(
        self, map_version: int, timeout_seconds: float
    ) -> bool:
        """Wait for query map version to increment beyond specified version.

        This method blocks until the subscription map version becomes greater than
        ``map_version``, or until ``timeout_seconds`` elapses. It is a **long-poll**
        API: the underlying ``ControllerClient.wait_for_change_from_version`` call
        runs in a background thread (via ``asyncio.to_thread``) and parks on a Python
        ``threading.Condition.wait()`` for up to ``timeout_seconds``.

        **Important**: this wrapper rejects ``timeout_seconds <= 0`` with ``ValueError``.
        The upstream Python implementation does have defined behavior at zero (it returns
        ``False`` immediately if the version is unchanged), but a non-blocking staleness
        check has no business going through a long-poll API — callers that need an
        instant comparison should call ``map_and_version()`` and compare the returned
        version themselves.

        The version number is monotonically increasing and increments every time the
        subscription map changes (query created, deleted, or state modified).

        Typical usage pattern:
        1. Call map_and_version() to get current state and version
        2. Cache the data and version number
        3. Later, call wait_for_change_from_version(cached_version, timeout)
        4. If returns True, call map_and_version() again to get fresh data

        Args:
            map_version (int): The version number to wait to exceed. Typically obtained
                              from a previous map_and_version() call.
            timeout_seconds (float): Maximum time to wait for version change, in seconds.
                                    Must be a strictly positive value; zero and negative
                                    values are rejected by this wrapper.

        Returns:
            bool: True if version changed (version > map_version), False if timeout occurred

        Raises:
            ValueError: If ``timeout_seconds`` is not strictly positive.
            DeephavenConnectionError: If unable to connect to controller service.
            QueryError: If subscription state is invalid.
        """
        if timeout_seconds <= 0:
            raise ValueError(
                f"timeout_seconds must be a positive value, got {timeout_seconds!r}"
            )
        _LOGGER.debug(
            f"[CorePlusControllerClient:wait_for_change_from_version] "
            f"Waiting for version > {map_version}, timeout={timeout_seconds}s"
        )
        try:
            result = await asyncio.to_thread(
                self.wrapped.wait_for_change_from_version, map_version, timeout_seconds
            )
            _LOGGER.debug(
                f"[CorePlusControllerClient:wait_for_change_from_version] "
                f"Returned: {result} (version {'changed' if result else 'unchanged'})"
            )
            return bool(result)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:wait_for_change_from_version] Connection error: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:wait_for_change_from_version] Failed: {e}"
            )
            raise QueryError(
                f"Failed to wait for version change from {map_version}: {e}"
            ) from e

    async def get(self, serial: CorePlusQuerySerial) -> CorePlusQueryInfo:
        """Get a specific query's information from the subscription map asynchronously.

        This method retrieves detailed information about a single query identified by its
        serial number. It returns a CorePlusQueryInfo object containing the query's current
        state, configuration, and other metadata.

        A successful call to subscribe should have happened before this call, as this method
        retrieves data from the subscription snapshot.

        The wait duration is sourced from
        ``EnterpriseClientTimeouts.no_wait_seconds`` (default ``0.0`` — no wait).

        Args:
            serial (CorePlusQuerySerial): The serial number of the query to get. This must be a valid CorePlusQuerySerial
                   that identifies an existing query.

        Returns:
            CorePlusQueryInfo: The CorePlusQueryInfo associated with the serial number, containing detailed
            information about the query's configuration, state, and metadata.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller is unavailable.
            QueryError: If the query does not exist within the wait period (the upstream
                       ``KeyError`` is translated to ``QueryError`` by this wrapper), or if
                       the subscription state is invalid (e.g., if subscribe() was not called).
        """
        timeout_seconds = self._timeouts.no_wait_seconds
        _LOGGER.debug(
            f"[CorePlusControllerClient:get] Retrieving query info for serial={serial}, timeout={timeout_seconds}"
        )
        try:
            result = await asyncio.to_thread(self.wrapped.get, serial, timeout_seconds)
            return CorePlusQueryInfo(result)
        except KeyError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:get] Query {serial} does not exist: {e}"
            )
            raise QueryError(f"Query with serial {serial} does not exist") from e
        except (TimeoutError, ValueError):
            # Re-raise native exceptions unchanged
            raise
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:get] Connection error while retrieving query {serial}: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:get] Failed to get query {serial}: {e}"
            )
            raise QueryError(f"Failed to retrieve query {serial}: {e}") from e

    # ===========================================================================
    # Query Creation & Configuration
    # ===========================================================================

    async def add_query(
        self,
        query_config: CorePlusQueryConfig,
    ) -> CorePlusQuerySerial:
        """Add a persistent query asynchronously.

        This method creates a new persistent query in the Deephaven controller based on the provided
        configuration. A persistent query represents a Deephaven worker process that can execute
        tables, scripts, or applications. Once created, the query will be allocated resources and
        initialized according to its configuration.

        The query lifecycle begins with this method, which returns a serial number that uniquely
        identifies the query. This serial can be used with other methods like get(), start_and_wait(),
        stop_query(), and delete_query() to manage the query throughout its lifecycle.

        Note that adding a query does not automatically start it. After adding a query, you typically
        need to call start_and_wait() to ensure the query transitions to the RUNNING state and becomes
        usable.

        A successful call to authenticate should have happened before this call, as query creation
        requires an authenticated session.

        Args:
            query_config (CorePlusQueryConfig): The query configuration to add. This CorePlusQueryConfig object defines
                        parameters such as heap size, server placement, engine type, and other
                        settings that control how the query will be created and executed.
                        Consider using make_pq_config() to create a properly configured
                        configuration object.

        Returns:
            CorePlusQuerySerial: The serial number of the newly added query. This CorePlusQuerySerial uniquely
            identifies the query in the controller and can be used with other methods to reference this
            specific query.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues, if the controller is unavailable, or if
                                    the operation does not complete within the operator-configured
                                    ``EnterpriseClientTimeouts.pq_management_timeout_seconds``.
            QueryError: If the query creation fails for any other reason such as permission issues,
                       quota limitations, insufficient resources, or internal controller errors.
        """
        timeout_seconds = self._timeouts.pq_management_timeout_seconds
        pb = query_config.pb
        _LOGGER.debug(
            f"[CorePlusControllerClient:add_query] Adding query: "
            f"name='{pb.name}', heapSizeGb={pb.heapSizeGb}, "
            f"scriptLanguage={pb.scriptLanguage!r}, configurationType={pb.configurationType!r}, "
            f"enabled={pb.enabled}, "
            f"script_body={'<set>' if pb.scriptCode else None}, scriptPath={pb.scriptPath!r}, "
            f"serverName={pb.serverName!r}, workerKind={pb.workerKind!r}"
        )
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(self.wrapped.add_query, query_config.pb),
                timeout=timeout_seconds,
            )
            return cast(CorePlusQuerySerial, result)
        except TimeoutError:
            _LOGGER.error(
                f"[CorePlusControllerClient:add_query] Timed out after {timeout_seconds}s. "
                f"Increase enterprise/settings.json: timeouts.client.pq_management_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Query creation timed out after {timeout_seconds} seconds. "
                f"To allow more time, increase enterprise/settings.json: "
                f"timeouts.client.pq_management_timeout_seconds in the operator config."
            ) from None
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:add_query] Failed to connect to controller when adding query: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller: {e}"
            ) from e
        except (ValueError, ResourceError):
            # Re-raise native and resource exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:add_query] Failed to create query: {e}"
            )
            raise QueryError(f"Failed to create query: {e}") from e

    def _apply_pq_config_parameters(  # noqa: C901
        self,
        config: Any,
        programming_language: str | None,
        script_body: str | None,
        script_path: str | None,
        configuration_type: str | None,
        enabled: bool | None,
        restart_users: str | None,
        extra_class_path: list[str] | None,
        schedule: list[str] | None,
        init_timeout_nanos: int | None,
        jvm_profile: str | None,
        python_virtual_environment: str | None,
    ) -> None:
        """Apply caller-supplied configuration parameters to a protobuf config in place.

        Every field except ``schedule`` follows a "None means skip" rule, so the
        existing default is preserved when the caller does not supply a value.
        See the ``schedule`` entry below for its three-way semantics.

        Args:
            config (Any): The protobuf config object to modify
            programming_language (str | None): Programming language ("Python" or "Groovy"), or None to use default
            script_body (str | None): Inline script code, or None if not specified
            script_path (str | None): Path to script file in Git repository, or None if not specified
            configuration_type (str | None): Query type ("Script", "RunAndDone", etc.), or None to use default
            enabled (bool | None): Whether query is enabled, or None to use default
            restart_users (str | None): Restart permission setting, or None to use controller default
            extra_class_path (list[str] | None): Additional classpath entries, or None if not specified
            schedule (list[str] | None): Scheduling entries. ``None`` leaves the
                existing scheduling untouched; ``[]`` clears it; a non-empty list
                replaces it wholesale.
            init_timeout_nanos (int | None): Initialization timeout in nanoseconds, or None to use default
            jvm_profile (str | None): Named JVM profile, or None if not specified
            python_virtual_environment (str | None): Named Python venv, or None if not specified
        """
        if programming_language is not None:
            config.scriptLanguage = programming_language
        if script_body is not None:
            config.scriptCode = script_body
        if script_path is not None:
            config.scriptPath = script_path
        if configuration_type is not None:
            config.configurationType = configuration_type
        if enabled is not None:
            config.enabled = enabled
        if restart_users is not None:
            config.restartUsers = restart_users
        if extra_class_path:
            config.extraClassPath.extend(extra_class_path)
        # schedule has three-way semantics: None leaves config.scheduling
        # untouched; [] clears it; [...] replaces it wholesale.
        if schedule is not None:
            del config.scheduling[:]
            if schedule:
                config.scheduling.extend(schedule)
        if init_timeout_nanos is not None:
            config.initTimeoutNanos = init_timeout_nanos
        if jvm_profile is not None:
            config.jvmProfile = jvm_profile
        if python_virtual_environment is not None:
            config.pythonVirtualEnvironment = python_virtual_environment

    async def make_pq_config(
        self,
        name: str,
        heap_size_gb: float | int,
        script_body: str | None = None,
        script_path: str | None = None,
        programming_language: str | None = None,
        configuration_type: str | None = None,
        enabled: bool | None = None,
        schedule: list[str] | None = None,
        server: str | None = None,
        engine: str = "DeephavenCommunity",
        jvm_profile: str | None = None,
        extra_jvm_args: list[str] | None = None,
        extra_class_path: list[str] | None = None,
        python_virtual_environment: str | None = None,
        extra_environment_vars: list[str] | None = None,
        init_timeout_nanos: int | None = None,
        auto_delete_timeout: int | None = None,
        admin_groups: list[str] | None = None,
        viewer_groups: list[str] | None = None,
        restart_users: str | None = None,
    ) -> CorePlusQueryConfig:
        """Create a persistent query configuration.

        Creates an in-memory PQ configuration object that can be customized with script content,
        scheduling, resource settings, and access controls. The configuration is not persisted
        until passed to add_query().

        Scheduler semantics (three-way, based on ``schedule``):
            - ``schedule=None`` (default): install the default scheduler and make no
              further changes.
                - For a *permanent* PQ (``auto_delete_timeout=None``) the default is a
                  continuous scheduler with ``SchedulingDisabled=false`` and
                  ``RestartWhenRunning=Yes``; the controller will begin acquiring a worker
                  immediately after ``add_query()`` (assuming ``enabled`` is ``True``,
                  which is the controller default).
                - For a *temporary* PQ (``auto_delete_timeout`` is a positive integer)
                  the default is whatever ``make_temporary_config`` installs.
            - ``schedule=[]``: the caller is explicitly requesting no scheduling. The
              scheduling list is cleared before the config is returned; the server is
              responsible for accepting or rejecting a PQ with no scheduling entries.
            - ``schedule=[...]`` (non-empty list): the caller-supplied list **replaces**
              the scheduling block wholesale. No default keys are merged in; the caller
              is responsible for including ``SchedulerType`` and any other required
              entries.

        Args:
            name (str): The name of the persistent query. This is used for identification.
            heap_size_gb (float | int): The heap size of the worker in gigabytes (e.g., 8 or 2.5).
                The enterprise library handles JVM configuration internally.
            script_body (str | None): The inline script code to execute. Mutually exclusive with script_path.
            script_path (str | None): Path to script file in Git repository. Mutually exclusive with script_body.
            programming_language (str | None): Script language - "Python" or "Groovy", case-insensitive. None uses controller default.
            configuration_type (str | None): Query type - "Script", "RunAndDone", etc. None uses controller default.
            enabled (bool | None): Whether the query is enabled. None uses controller default.
            schedule (list[str] | None): Scheduling configuration as list of "Key=Value" strings (e.g.,
                ["SchedulerType=...", "StartTime=08:00:00", "StopTime=18:00:00"]).
            server (str | None): The specific server to run the worker on. If None, the controller
                will choose a suitable server.
            engine (str): The engine to use for the worker. Defaults to "DeephavenCommunity".
            jvm_profile (str | None): Named JVM profile configured in controller (e.g., "large-memory").
            extra_jvm_args (list[str] | None): A list of extra JVM arguments to pass to the worker.
            extra_class_path (list[str] | None): Additional classpath entries to prepend to worker's classpath.
            python_virtual_environment (str | None): Named Python virtual environment for Core+ workers.
            extra_environment_vars (list[str] | None): A list of extra environment variables for the worker.
            init_timeout_nanos (int | None): Initialization timeout in nanoseconds.
            auto_delete_timeout (int | None): The timeout in seconds for auto-deletion of the query
                after it becomes idle. None (default) creates a permanent query.
            admin_groups (list[str] | None): A list of user groups that will have admin access to the query.
            viewer_groups (list[str] | None): A list of user groups that will have viewer access to the query.
            restart_users (str | None): Who can restart the query. Values: "RU_ADMIN", "RU_ADMIN_AND_VIEWERS",
                "RU_VIEWERS_WHEN_DOWN". Defaults to controller setting.

        Returns:
            CorePlusQueryConfig: The configuration object for the persistent query.

        Raises:
            ValueError: If invalid parameters are provided.
            DeephavenConnectionError: If not authenticated or unable to communicate with the controller.
            QueryError: If configuration creation fails for any other reason.
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:make_pq_config] Creating PQ config: "
            f"name='{name}', heap_size_gb={heap_size_gb}, server={server!r}, engine={engine!r}, "
            f"auto_delete_timeout={auto_delete_timeout}, programming_language={programming_language!r}, "
            f"configuration_type={configuration_type!r}, enabled={enabled}, "
            f"script_body={'<set>' if script_body else None}, script_path={script_path!r}, "
            f"schedule={schedule}, jvm_profile={jvm_profile!r}, "
            f"python_virtual_environment={python_virtual_environment!r}, "
            f"admin_groups={admin_groups}, viewer_groups={viewer_groups}, restart_users={restart_users!r}"
        )

        # Validate mutually exclusive parameters
        if script_body is not None and script_path is not None:
            raise ValueError(
                "script_body and script_path are mutually exclusive - specify only one"
            )

        try:
            # Step 1: call make_temporary_config to produce a baseline config.
            # For permanent queries (auto_delete_timeout=None) we must still pass a
            # placeholder timeout because the server rejects temporary scheduling
            # that lacks a valid timeout; we overwrite the scheduling below.
            is_permanent = auto_delete_timeout is None
            effective_timeout = 600 if is_permanent else auto_delete_timeout

            config = await asyncio.to_thread(
                self.wrapped.make_temporary_config,
                name,
                heap_size_gb,
                server,
                extra_jvm_args,
                extra_environment_vars,
                engine,
                effective_timeout,
                admin_groups,
                viewer_groups,
            )

            # Step 2: install the default scheduler for permanent queries when the
            # caller did not supply a schedule. Replaces the temporary scheduling
            # that make_temporary_config installs with a continuous scheduler that
            # causes the PQ to auto-start once the controller accepts it. This step
            # lives here (not in _apply_pq_config_parameters) because it needs the
            # ``is_permanent`` context which is local to this method.
            # Note: protobuf RepeatedScalarContainer has no clear(); use slice del.
            if is_permanent and schedule is None:
                del config.scheduling[:]
                for entry in _DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING:
                    config.scheduling.append(entry)
                _LOGGER.debug(
                    f"[CorePlusControllerClient:make_pq_config] "
                    f"Installed default continuous scheduler for permanent query '{name}'"
                )

            # Step 3: apply all caller-supplied parameters (including any
            # caller-supplied scheduling override) through the single helper.
            # Scheduling is applied with three-way semantics inside the helper:
            #   schedule=None      -> no override (default from step 2 stays)
            #   schedule=[]        -> explicit "no scheduling"; clear the block
            #   schedule=[...]     -> replace wholesale with the caller's list
            self._apply_pq_config_parameters(
                config,
                programming_language,
                script_body,
                script_path,
                configuration_type,
                enabled,
                restart_users,
                extra_class_path,
                schedule,
                init_timeout_nanos,
                jvm_profile,
                python_virtual_environment,
            )

            _LOGGER.debug(
                f"[CorePlusControllerClient:make_pq_config] Successfully created config for '{name}'"
            )
            return CorePlusQueryConfig(config)
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:make_pq_config] Failed to create config for '{name}': {e}"
            )
            raise

    # ===========================================================================
    # Query Lifecycle Management
    # ===========================================================================

    async def delete_query(self, serial: CorePlusQuerySerial) -> None:
        """Delete a query asynchronously.

        This method permanently removes a query from the controller. When a query is deleted:

        1. The query process is terminated if it is still running
        2. All resources associated with the query are released
        3. The query definition is removed from the controller
        4. The serial number becomes invalid and can no longer be used
        5. Any data associated with the query that hasn't been persisted elsewhere is lost

        Deleting a query is a permanent operation that cannot be undone. If you only want to
        temporarily stop a query while preserving its definition, use stop_query() instead.

        A successful call to authenticate should have happened before this call, as query
        deletion requires an authenticated session.

        Args:
            serial (CorePlusQuerySerial): The serial number of the query to delete. This must reference a valid,
                   existing query that the authenticated user has permission to delete.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues, if the controller is unavailable, or if
                                    the operation does not complete within the operator-configured
                                    ``EnterpriseClientTimeouts.pq_management_timeout_seconds``.
            QueryError: If the query deletion fails for any other reason such as permission issues,
                       a non-existent serial number, internal controller errors, or if the query is
                       in a state that prevents deletion.
        """
        timeout_seconds = self._timeouts.pq_management_timeout_seconds
        _LOGGER.debug(
            f"[CorePlusControllerClient:delete_query] Starting query deletion for serial={serial}"
        )
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self.wrapped.delete_query, serial),
                timeout=timeout_seconds,
            )
            _LOGGER.debug(
                f"[CorePlusControllerClient:delete_query] Query {serial} deleted successfully"
            )
        except TimeoutError:
            _LOGGER.error(
                f"[CorePlusControllerClient:delete_query] Timed out after {timeout_seconds}s. "
                f"Increase enterprise/settings.json: timeouts.client.pq_management_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Query deletion timed out after {timeout_seconds} seconds. "
                f"To allow more time, increase enterprise/settings.json: "
                f"timeouts.client.pq_management_timeout_seconds in the operator config."
            ) from None
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:delete_query] Connection error while deleting query {serial}: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except (ValueError, KeyError):
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:delete_query] Failed to delete query {serial}: {e}"
            )
            raise QueryError(f"Failed to delete query {serial}: {e}") from e

    async def modify_query(
        self,
        updated_config: CorePlusQueryConfig,
        restart: bool = False,
    ) -> None:
        """Modify a persistent query configuration asynchronously.

        This method updates an existing persistent query's configuration. The query configuration
        must include the serial number of the query to modify. Changes can be applied to queries
        in any state (RUNNING, STOPPED, etc.).

        The restart parameter controls whether the query is restarted to apply the changes:
        - restart=True: The query is restarted immediately, applying all configuration changes.
                       This is required for changes like heap size, JVM args, or script content.
        - restart=False: Changes are saved but require a manual restart (via restart_query or
                        start_and_wait) to take effect. This is useful for preparing configuration
                        changes without disrupting a running query.

        Note that some configuration changes (like resource allocation or script changes) will
        only take effect after the query is restarted, regardless of the restart parameter.

        A successful call to authenticate should have happened before this call, as query
        modification requires an authenticated session.

        Args:
            updated_config (CorePlusQueryConfig): The complete updated configuration for the query.
                        The configuration must include the serial number of the query to modify.
                        All fields should be set to their desired values - this is not a partial
                        update mechanism.
            restart (bool): Whether to restart the query after modifying the configuration.
                        Defaults to False. Set to True to apply changes immediately.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues, if the controller is unavailable, or if
                                    the operation does not complete within the operator-configured
                                    ``EnterpriseTimeouts.pq_management_timeout_seconds``.
            QueryError: If the query modification fails for any other reason such as permission
                       issues, configuration conflicts, a non-existent serial number, or internal
                       controller errors.

        Example:
            # Get current query info and modify it
            query_info = await controller.get(serial)
            config = query_info.config

            # Update heap size in the protobuf
            config.pb.heapSizeGb = 16.0

            # Modify without restarting (changes saved for next restart)
            await controller.modify_query(config, restart=False)

            # Or modify and restart immediately
            await controller.modify_query(config, restart=True)
        """
        timeout_seconds = self._timeouts.pq_management_timeout_seconds
        pb = updated_config.pb
        _LOGGER.debug(
            f"[CorePlusControllerClient:modify_query] Modifying query: "
            f"serial={pb.serial}, name='{pb.name}', restart={restart}"
        )
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self.wrapped.modify_query, pb, restart),
                timeout=timeout_seconds,
            )
            _LOGGER.debug(
                f"[CorePlusControllerClient:modify_query] Query {pb.serial} modified successfully"
            )
        except TimeoutError:
            _LOGGER.error(
                f"[CorePlusControllerClient:modify_query] Timed out after {timeout_seconds}s. "
                f"Increase enterprise/settings.json: timeouts.client.pq_management_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Query modification timed out after {timeout_seconds} seconds. "
                f"To allow more time, increase enterprise/settings.json: "
                f"timeouts.client.pq_management_timeout_seconds in the operator config."
            ) from None
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:modify_query] Connection error while modifying query {pb.serial}: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except (ValueError, KeyError):
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:modify_query] Failed to modify query {pb.serial}: {e}"
            )
            raise QueryError(f"Failed to modify query {pb.serial}: {e}") from e

    async def _run_state_change(
        self,
        method_name: str,
        method_args: tuple[Any, ...],
        wait: bool,
        target_description: str,
        passthrough_excs: tuple[type[BaseException], ...],
    ) -> None:
        """Run a controller state-change call with the shared error envelope.

        Backs :meth:`restart_query`, :meth:`stop_query`,
        :meth:`start_and_wait`, and :meth:`stop_and_wait`, each of which
        delegates to a corresponding method on the wrapped controller
        (``self.wrapped.<method_name>``) executed in a worker thread.

        Args:
            method_name (str): Name of the method on ``self.wrapped`` to
                invoke. Also used as the log prefix and looked up via
                :func:`getattr` so callers cannot bind a non-method.
            method_args (tuple[Any, ...]): Positional arguments passed
                to the wrapped method, in order, before the trailing
                ``timeout_seconds`` argument that this helper appends.
            wait (bool): When ``True`` the wait duration is sourced from
                :attr:`EnterpriseClientTimeouts.pq_state_change_timeout_seconds`;
                when ``False`` the wrapped method is invoked with
                ``timeout_seconds=0`` (fire-and-forget).
            target_description (str): Human-readable description of the
                operation target used in log and error messages
                (e.g. ``"query(s)"`` or ``f"query {serial}"``).
            passthrough_excs (tuple[type[BaseException], ...]):
                Exception types from the wrapped method that should
                propagate unchanged rather than be wrapped in
                :class:`QueryError`. ``ConnectionError`` is always
                translated to :class:`DeephavenConnectionError` and is
                not affected by this tuple.

        Raises:
            DeephavenConnectionError: If the wrapped call raises
                :class:`ConnectionError`.
            QueryError: If the wrapped call raises any other exception
                not listed in ``passthrough_excs``.
        """
        timeout_seconds = self._timeouts.pq_state_change_timeout_seconds if wait else 0
        log_prefix = f"[CorePlusControllerClient:{method_name}]"
        _LOGGER.debug(f"{log_prefix} Starting {target_description} (wait={wait})")
        wrapped_callable = getattr(self.wrapped, method_name)
        all_args: tuple[Any, ...] = (*method_args, timeout_seconds)
        try:
            await asyncio.to_thread(wrapped_callable, *all_args)
            _LOGGER.debug(f"{log_prefix} {target_description} completed successfully")
        except ConnectionError as e:
            _LOGGER.error(
                f"{log_prefix} Connection error while running {target_description}: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except passthrough_excs:
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(f"{log_prefix} Failed to run {target_description}: {e}")
            raise QueryError(f"Failed to run {target_description}: {e}") from e

    async def restart_query(
        self,
        serials: Iterable[CorePlusQuerySerial] | CorePlusQuerySerial,
        wait: bool = True,
    ) -> None:
        """Restart one or more queries asynchronously.

        This method restarts stopped or failed queries, transitioning them from their current state to
        the RUNNING state. The restart process:

        1. Recreates the query process using the original query configuration
        2. Re-allocates necessary resources for the query
        3. Re-initializes the query state from scratch (previous data is not preserved)
        4. Makes the query available again for client connections

        Restarting is more efficient than deleting and re-adding a query when the same configuration
        is needed, as it preserves the serial number and query definition.

        A successful call to authenticate should have happened before this call.

        When ``wait=True`` (default), the wait duration is sourced from
        ``EnterpriseTimeouts.pq_state_change_timeout_seconds``. When
        ``wait=False``, the call returns immediately (fire-and-forget).

        Args:
            serials (Iterable[CorePlusQuerySerial] | CorePlusQuerySerial): A query serial number, or an iterable of serial numbers. Each serial must
                    reference a valid, existing query.
            wait (bool): When True, wait for the restart to complete using the operator-configured
                wait duration. When False, fire-and-forget (submit and return immediately).

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues or server unavailability.
            QueryError: If the query restart fails for any other reason such as insufficient resources,
                       a non-existent serial number, configuration errors, or internal controller issues.
        """
        await self._run_state_change(
            method_name="restart_query",
            method_args=(serials,),
            wait=wait,
            target_description="restart query(s)",
            passthrough_excs=(ValueError, KeyError),
        )

    async def start_and_wait(
        self,
        serial: CorePlusQuerySerial,
        wait: bool = True,
    ) -> None:
        """Start the given query and wait for it to become running asynchronously.

        This method initiates a query and waits until it transitions to the 'RUNNING' state, meaning
        the query has successfully initialized and is actively processing data. A query goes through
        several state transitions (UNINITIALIZED → INITIALIZING → RUNNING) during startup.

        If the query transitions to a failure state (e.g., FAILED, CRASHED) during startup,
        this method will raise an exception with the appropriate error information.

        When ``wait=True`` (default), the wait duration is sourced from
        ``EnterpriseTimeouts.pq_state_change_timeout_seconds``. When
        ``wait=False``, the call returns immediately after submitting the
        start request (fire-and-forget).

        Args:
            serial (CorePlusQuerySerial): The serial number of the query to start. This must reference a valid query that
                   has been previously created via add_query.
            wait (bool): When True, wait for the PQ to reach RUNNING using the operator-configured
                wait duration. When False, fire-and-forget (submit and return immediately).

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service.
            KeyError: If the query with the given serial does not exist (raised by the upstream
                ``self.map()[serial]`` lookup performed before the wait begins; propagated
                unchanged by this wrapper).
            QueryError: If the query fails to reach the RUNNING state within the wait period, or for
                any other operational issue such as initialization errors or resource constraints.
                The upstream ``RuntimeError`` raised on timeout-without-target-state is translated
                to ``QueryError`` by this wrapper.
        """
        await self._run_state_change(
            method_name="start_and_wait",
            method_args=(serial,),
            wait=wait,
            target_description=f"start query {serial}",
            passthrough_excs=(TimeoutError, ValueError, KeyError),
        )

    async def stop_query(
        self,
        serials: Iterable[CorePlusQuerySerial] | CorePlusQuerySerial,
        wait: bool = True,
    ) -> None:
        """Stop one or more queries asynchronously.

        This method gracefully stops running queries, transitioning them from the RUNNING state to
        the STOPPED state. When queries are stopped:

        1. The query processes are terminated, but their definitions remain in the controller
        2. All resources associated with the queries (memory, computation) are released
        3. Any client connections to the queries will be disconnected
        4. Data that was generated by the queries but not persisted elsewhere will be lost
        5. The queries can be restarted later using restart_query without recreating them

        Stopping queries is less resource-intensive than deleting and recreating them when you
        need to temporarily pause processing.

        A successful call to authenticate should have happened before this call.

        When ``wait=True`` (default), the wait duration is sourced from
        ``EnterpriseTimeouts.pq_state_change_timeout_seconds``. When
        ``wait=False``, the call returns immediately (fire-and-forget).

        Args:
            serials (Iterable[CorePlusQuerySerial] | CorePlusQuerySerial): A query serial number, or an iterable of serial numbers. Each serial must
                    reference a valid, existing query.
            wait (bool): When True, wait for the stop to complete using the operator-configured
                wait duration. When False, fire-and-forget (submit and return immediately).

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues or server unavailability.
            QueryError: If the query stop fails for any other reason such as permission issues,
                       a non-existent serial number, invalid query state transitions, or internal
                       controller errors.
        """
        await self._run_state_change(
            method_name="stop_query",
            method_args=(serials,),
            wait=wait,
            target_description="stop query(s)",
            passthrough_excs=(ValueError, KeyError),
        )

    async def stop_and_wait(
        self,
        serial: CorePlusQuerySerial,
        wait: bool = True,
    ) -> None:
        """Stop the given query and wait for it to become terminal asynchronously.

        This method gracefully stops a running query and waits until it transitions to a terminal
        state (STOPPED, FAILED, etc.). The query goes through state transitions during shutdown
        (RUNNING → STOPPING → STOPPED).

        After stopping, the query process is terminated and its resources are released, but the
        query definition remains in the controller. The query can be restarted later using
        restart_query without needing to recreate it.

        When ``wait=True`` (default), the wait duration is sourced from
        ``EnterpriseTimeouts.pq_state_change_timeout_seconds``. When
        ``wait=False``, the call returns immediately (fire-and-forget).

        Args:
            serial (CorePlusQuerySerial): The serial number of the query to stop. This must reference a valid query that
                   has been previously created via add_query.
            wait (bool): When True, wait for the PQ to reach a terminal state using the
                operator-configured wait duration. When False, fire-and-forget.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service.
            QueryError: If the query fails to reach a terminal state within the wait period, or for
                any other reason such as a non-existent serial number, internal errors, or invalid
                state transitions. The upstream ``RuntimeError`` raised on
                timeout-without-terminal-state is translated to ``QueryError`` by this wrapper.
        """
        await self._run_state_change(
            method_name="stop_and_wait",
            method_args=(serial,),
            wait=wait,
            target_description=f"stop query {serial}",
            passthrough_excs=(TimeoutError, ValueError, KeyError),
        )
