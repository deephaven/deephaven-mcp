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
from typing import TYPE_CHECKING, cast

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

_LOGGER = logging.getLogger(__name__)


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
    than the underlying Java exceptions. Network issues typically result in DeephavenConnectionError
    and query-related issues in QueryError.

    Attributes:
        wrapped: The underlying Java ControllerClient instance being wrapped

    Example:
        # Create a controller client from an authenticated session factory
        session_factory = await CorePlusSessionFactory.from_url("https://deephaven-server:10000")
        controller_client = await session_factory.create_controller_client()

        # Create a temporary query configuration and add it
        config = await controller_client.make_temporary_config("my-worker", heap_size_gb=2.0)
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
    ):
        """Initialize the CorePlusControllerClient with a ControllerClient instance.

        Args:
            controller_client: The ControllerClient instance to wrap.
        """
        super().__init__(controller_client, is_enterprise=True)
        self._subscribed = False
        _LOGGER.info("[CorePlusControllerClient] initialized")

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

        Returns:
            True if the ping was sent successfully and the cookie was refreshed, False if
            there was no cookie to refresh (indicating the client is not authenticated).

        Raises:
            DeephavenConnectionError: If the connection to the server fails due to network
                                    issues, if the controller service is unavailable, or
                                    if there are communication errors with the server.
        """
        _LOGGER.debug("[CorePlusControllerClient:ping] Sending ping to controller")
        try:
            return await asyncio.to_thread(self.wrapped.ping)
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

        Raises:
            DeephavenConnectionError: If not authenticated, if unable to connect to the
                                    controller service due to network issues, or if the
                                    controller is unavailable.
            QueryError: If the subscription fails due to invalid state, permission issues,
                       or any other operational reason.

        Note:
            This method is typically called automatically when the controller client is
            created by the CorePlusSessionFactory. Manual calls are generally not needed
            unless you're managing controller clients directly.
        """
        # If already subscribed, return early (idempotent behavior)
        if self._subscribed:
            _LOGGER.debug(
                "[CorePlusControllerClient:subscribe] Already subscribed, skipping"
            )
            return

        _LOGGER.debug("[CorePlusControllerClient:subscribe] Subscribing to query state")
        try:
            await asyncio.to_thread(self.wrapped.subscribe)
            self._subscribed = True
            _LOGGER.debug(
                "[CorePlusControllerClient:subscribe] Successfully subscribed to query state"
            )
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
            raise QueryError(f"Failed to subscribe to persistent query state: {e}") from e

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
            A dictionary mapping query serial numbers to CorePlusQueryInfo objects containing
            detailed information about each persistent query managed by the controller.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller is unavailable.
            QueryError: If not subscribed or if the subscription state is invalid, which can
                       happen if subscribe() was not called or if the subscription has been
                       invalidated.
        """
        if not self._subscribed:
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
                f"[CorePlusControllerClient:get_query_map] Connection error while retrieving query map: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:get_query_map] Failed to retrieve query map: {e}"
            )
            raise QueryError(f"Failed to retrieve query state: {e}") from e

    async def get_serial_for_name(
        self, name: str, timeout_seconds: float = 0
    ) -> CorePlusQuerySerial:
        """Retrieve the serial number for a given query name asynchronously.

        This method looks up a query by its name and returns the corresponding serial number.
        Query names are human-readable identifiers specified when creating the query (e.g., in
        the make_temporary_config method), while serial numbers are system-assigned unique
        identifiers used for most controller operations.

        This method is particularly useful when you want to reference a query by its human-readable
        name rather than tracking its serial number. For example, when connecting to an existing
        query that was created by another process or user.

        The timeout_seconds parameter allows waiting for a query with the specified name to
        appear, which is useful when working with queries that are being created concurrently.

        Args:
            name: The name of the query to find. This is the human-readable name specified
                 when the query was created.
            timeout_seconds: How long to wait for the query to be found, in seconds. Default is 0,
                           meaning no wait. If greater than 0, the method will wait up to this
                           many seconds for a query with the specified name to appear.

        Returns:
            The serial number for the query with the given name. This can be used with
            other controller methods that require a CorePlusQuerySerial.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller is unavailable.
            QueryError: If no query with the given name is found within the timeout period
                       or if the subscription state is invalid.
            TimeoutError: If the specified timeout period elapses while waiting for a query
                        with the given name to appear.
            ValueError: If the name parameter is invalid, empty, or malformed.
            InternalError: If subscribe() was not called before this method.
        """
        if not self._subscribed:
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

        After this method returns (indicating a change has occurred), you typically call map()
        to get the updated query state information.

        Args:
            timeout_seconds: How long to wait for a change, in seconds. This must be a positive
                           value. If no changes occur within this period, a TimeoutError is raised.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller becomes unavailable.
            TimeoutError: If the specified timeout elapses while waiting for a change. This is
                        not necessarily an error condition - it simply indicates that no changes
                        occurred within the specified time window.
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

    async def get(
        self, serial: CorePlusQuerySerial, timeout_seconds: float = 0
    ) -> CorePlusQueryInfo:
        """Get a specific query's information from the subscription map asynchronously.

        This method retrieves detailed information about a single query identified by its
        serial number. It returns a CorePlusQueryInfo object containing the query's current
        state, configuration, and other metadata.

        A successful call to subscribe should have happened before this call, as this method
        retrieves data from the subscription snapshot.

        The timeout_seconds parameter enables waiting for a query to appear in the subscription
        data. This is particularly useful in scenarios where you've just created a query using
        add_query() and need to wait for its state to be published by the controller before
        proceeding. Without a timeout, the method would immediately raise an exception if the
        query doesn't exist in the current snapshot.

        Args:
            serial: The serial number of the query to get. This must be a valid CorePlusQuerySerial
                   that identifies an existing query.
            timeout_seconds: How long to wait for the query to exist, in seconds. Default is 0,
                           meaning no wait. Setting this to a positive value will cause the method
                           to wait up to that many seconds for the query to appear in the
                           subscription data before failing.

        Returns:
            The CorePlusQueryInfo associated with the serial number, containing detailed
            information about the query's configuration, state, and metadata.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service due to
                                    network issues or if the controller is unavailable.
            QueryError: If the query does not exist within the timeout period or if the
                       subscription state is invalid (e.g., if subscribe() was not called).
            TimeoutError: If the specified timeout period elapses while waiting for the
                        query to appear in the subscription data.
            ValueError: If the serial parameter is invalid or malformed.
        """
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

    async def add_query(self, query_config: CorePlusQueryConfig) -> CorePlusQuerySerial:
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
            query_config: The query configuration to add. This CorePlusQueryConfig object defines
                        parameters such as heap size, server placement, engine type, and other
                        settings that control how the query will be created and executed.
                        Consider using make_temporary_config() to create a properly configured
                        configuration object.

        Returns:
            The serial number of the newly added query. This CorePlusQuerySerial uniquely identifies
            the query in the controller and can be used with other methods to reference this
            specific query.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues or if the controller is unavailable.
            ValueError: If the query_config is invalid, malformed, or contains incompatible settings.
            ResourceError: If there are insufficient resources (memory, CPU, etc.) to create the query
                        or if resource allocation fails for any reason.
            QueryError: If the query creation fails for any other reason such as permission issues,
                       quota limitations, or internal controller errors.
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:add_query] Adding query with name='{query_config.pb.name}'"
        )
        try:
            result = await asyncio.to_thread(self.wrapped.add_query, query_config)
            return cast(CorePlusQuerySerial, result)
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

    async def make_temporary_config(
        self,
        name: str,
        heap_size_gb: float | int,
        server: str | None = None,
        extra_jvm_args: list[str] | None = None,
        extra_environment_vars: list[str] | None = None,
        engine: str = "DeephavenCommunity",
        auto_delete_timeout: int | None = 600,
        admin_groups: list[str] | None = None,
        viewer_groups: list[str] | None = None,
    ) -> CorePlusQueryConfig:
        """Create a configuration for a temporary, private worker for interactive use.

        This method simplifies the creation of a temporary query that functions as a private,
        interactive console for a user. The resulting worker is configured with the
        DeephavenCommunity engine by default and has an auto-delete timeout to ensure
        it is cleaned up after a period of inactivity.

        Args:
            name: The name of the temporary query. This is used for identification.
            heap_size_gb: The heap size of the worker in gigabytes (e.g., 8 or 2.5).
                The enterprise library handles JVM configuration internally.
            server: The specific server to run the worker on. If None, the controller
                will choose a suitable server.
            extra_jvm_args: A list of extra JVM arguments to pass to the worker.
            extra_environment_vars: A list of extra environment variables for the worker.
            engine: The engine to use for the worker. Defaults to "DeephavenCommunity".
            auto_delete_timeout: The timeout in seconds for auto-deletion of the query
                after it becomes idle. Defaults to 600 seconds (10 minutes).
            admin_groups: A list of user groups that will have admin access to the query.
            viewer_groups: A list of user groups that will have viewer access to the query.

        Returns:
            CorePlusQueryConfig: The configuration object for the temporary worker.

        Raises:
            ValueError: If invalid parameters are provided.
            DeephavenConnectionError: If not authenticated or unable to communicate with the controller.
            QueryError: If configuration creation fails for any other reason.
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:make_temporary_config] Creating temporary config for name='{name}', heap_size_gb={heap_size_gb}"
        )
        try:
            config = await asyncio.to_thread(
                self.wrapped.make_temporary_config,
                name,
                heap_size_gb,
                server,
                extra_jvm_args,
                extra_environment_vars,
                engine,
                auto_delete_timeout,
                admin_groups,
                viewer_groups,
            )
            _LOGGER.debug(
                f"[CorePlusControllerClient:make_temporary_config] Successfully created config for '{name}'"
            )
            return CorePlusQueryConfig(config)
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:make_temporary_config] Failed to create config for '{name}': {e}"
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
            serial: The serial number of the query to delete. This must reference a valid,
                   existing query that the authenticated user has permission to delete.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues or if the controller is unavailable.
            ValueError: If the serial parameter is invalid or malformed.
            KeyError: If the query with the given serial does not exist.
            QueryError: If the query deletion fails for any other reason such as permission issues,
                       internal controller errors, or if the query is in a state that prevents deletion.
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:delete_query] Starting query deletion for serial={serial}"
        )
        try:
            await asyncio.to_thread(self.wrapped.delete_query, serial)
            _LOGGER.debug(
                f"[CorePlusControllerClient:delete_query] Query {serial} deleted successfully"
            )
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

    async def restart_query(
        self,
        serials: Iterable[CorePlusQuerySerial] | CorePlusQuerySerial,
        timeout_seconds: int | None = None,
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

        Args:
            serials: A query serial number, or an iterable of serial numbers. Each serial must
                    reference a valid, existing query.
            timeout_seconds: Timeout in seconds for the operation. If None, the client's default
                           timeout is used. For restarting multiple queries or complex queries,
                           a longer timeout may be appropriate.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues or server unavailability.
            ValueError: If any serial parameters are invalid or malformed.
            KeyError: If any queries with the given serials do not exist.
            QueryError: If the query restart fails for any other reason such as insufficient resources,
                       configuration errors, or internal controller issues.
        """
        _LOGGER.debug("[CorePlusControllerClient:restart_query] Starting query restart")
        try:
            await asyncio.to_thread(
                self.wrapped.restart_query, serials, timeout_seconds
            )
            _LOGGER.debug(
                "[CorePlusControllerClient:restart_query] Query restart completed successfully"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:restart_query] Connection error while restarting query(s): {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except (ValueError, KeyError):
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:restart_query] Failed to restart query(s): {e}"
            )
            raise QueryError(f"Failed to restart query(s): {e}") from e

    async def start_and_wait(
        self, serial: CorePlusQuerySerial, timeout_seconds: int = 120
    ) -> None:
        """Start the given query and wait for it to become running asynchronously.

        This method initiates a query and waits until it transitions to the 'RUNNING' state, meaning
        the query has successfully initialized and is actively processing data. A query goes through
        several state transitions (UNINITIALIZED → INITIALIZING → RUNNING) during startup.

        If the query transitions to a failure state (e.g., FAILED, CRASHED) during startup,
        this method will raise an exception with the appropriate error information.

        Args:
            serial: The serial number of the query to start. This must reference a valid query that
                   has been previously created via add_query.
            timeout_seconds: How long to wait for the query to become running, in seconds. Default is
                           120 seconds (2 minutes). For large or complex queries, a longer timeout
                           may be necessary.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service.
            TimeoutError: If the query does not reach the RUNNING state within the timeout period.
            ValueError: If the serial parameter is invalid or malformed.
            KeyError: If the query with the given serial does not exist.
            QueryError: If the query fails to start due to initialization errors, resource constraints,
                       or any other operational issues.
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:start_and_wait] Starting query and waiting for serial={serial}"
        )
        try:
            await asyncio.to_thread(
                self.wrapped.start_and_wait, serial, timeout_seconds
            )
            _LOGGER.debug(
                f"[CorePlusControllerClient:start_and_wait] Query {serial} started successfully"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:start_and_wait] Connection error while starting query {serial}: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except (TimeoutError, ValueError, KeyError):
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:start_and_wait] Query {serial} failed to start: {e}"
            )
            raise QueryError(f"Failed to start query {serial}: {e}") from e

    async def stop_query(
        self,
        serials: Iterable[CorePlusQuerySerial] | CorePlusQuerySerial,
        timeout_seconds: int | None = None,
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

        Args:
            serials: A query serial number, or an iterable of serial numbers. Each serial must
                    reference a valid, existing query.
            timeout_seconds: Timeout in seconds for the operation. If None, the client's default
                           timeout is used. For stopping multiple queries, a longer timeout may
                           be appropriate.

        Raises:
            DeephavenConnectionError: If not authenticated or unable to connect to the controller
                                    due to network issues or server unavailability.
            ValueError: If any serial parameters are invalid or malformed.
            KeyError: If any queries with the given serials do not exist.
            QueryError: If the query stop fails for any other reason such as permission issues,
                       invalid query state transitions, or internal controller errors.
        """
        _LOGGER.debug("[CorePlusControllerClient:stop_query] Starting query stop")
        try:
            await asyncio.to_thread(self.wrapped.stop_query, serials, timeout_seconds)
            _LOGGER.debug(
                "[CorePlusControllerClient:stop_query] Query stop completed successfully"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:stop_query] Connection error when stopping query: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error when stopping query: {e}"
            ) from e
        except (ValueError, KeyError):
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:stop_query] Failed to stop query(s): {e}"
            )
            raise QueryError(f"Failed to stop query(s): {e}") from e

    async def stop_and_wait(
        self, serial: CorePlusQuerySerial, timeout_seconds: int = 120
    ) -> None:
        """Stop the given query and wait for it to become terminal asynchronously.

        This method gracefully stops a running query and waits until it transitions to a terminal
        state (STOPPED, FAILED, etc.). The query goes through state transitions during shutdown
        (RUNNING → STOPPING → STOPPED).

        After stopping, the query process is terminated and its resources are released, but the
        query definition remains in the controller. The query can be restarted later using
        restart_query without needing to recreate it.

        Args:
            serial: The serial number of the query to stop. This must reference a valid query that
                   has been previously created via add_query.
            timeout_seconds: How long to wait for the query to stop, in seconds. Default is
                           120 seconds (2 minutes). For large queries with significant cleanup,
                           a longer timeout may be necessary.

        Raises:
            DeephavenConnectionError: If unable to connect to the controller service.
            TimeoutError: If the query does not reach a terminal state within the timeout period.
            ValueError: If the serial parameter is invalid or malformed.
            KeyError: If the query with the given serial does not exist.
            QueryError: If the query fails to stop due to internal errors or invalid state transitions.
        """
        _LOGGER.debug(
            f"[CorePlusControllerClient:stop_and_wait] Stopping query and waiting for serial={serial}"
        )
        try:
            await asyncio.to_thread(self.wrapped.stop_and_wait, serial, timeout_seconds)
            _LOGGER.debug(
                f"[CorePlusControllerClient:stop_and_wait] Query {serial} stopped successfully"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:stop_and_wait] Connection error while stopping query {serial}: {e}"
            )
            raise DeephavenConnectionError(
                f"Unable to connect to controller service: {e}"
            ) from e
        except (TimeoutError, ValueError, KeyError):
            # Re-raise native exceptions unchanged
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusControllerClient:stop_and_wait] Failed to stop query {serial}: {e}"
            )
            raise QueryError(f"Failed to stop query {serial}: {e}") from e
