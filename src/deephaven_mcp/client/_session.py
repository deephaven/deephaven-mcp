"""Asynchronous session wrappers for Deephaven standard and enterprise functionality.

This module provides asynchronous wrappers for Deephaven session classes, making all blocking
operations non-blocking through asyncio.to_thread. It supports both standard Deephaven sessions
and enterprise-specific functionality through two main classes:

Two main classes are provided:
- CoreSession: An asynchronous wrapper around the standard pydeephaven Session class for basic table
  operations like creating empty tables, importing data, and executing queries
- CorePlusSession: An asynchronous wrapper around the enterprise DndSession class that extends
  CoreSession with additional enterprise-specific features like persistent query information, 
  historical data, and catalog operations

All session operations that would normally block the event loop are executed in background threads
using asyncio.to_thread to ensure application responsiveness when using asyncio.

Example usage of CoreSession (standard functionality):
    ```python
    import asyncio
    import pyarrow as pa
    from deephaven_mcp.client import CoreSessionManager
    
    async def main():
        # Create a session manager and connect to the server
        manager = CoreSessionManager("localhost", 10000)
        
        # Get a session
        session = await manager.get_session()
        
        # Create a time table that ticks every second
        time_table = await session.time_table("PT1S")
        
        # Create a derived table using query operations
        result = await (await session.query(time_table))\
            .update_view(["Timestamp = now()", "Value = i % 10"])\
            .to_table()
            
        # Create an input table with a schema
        schema = pa.schema([
            pa.field('name', pa.string()),
            pa.field('value', pa.int64())
        ])
        input_table = await session.input_table(schema=schema)
        
        # Save a table to be accessible by name
        await session.bind_table("my_result_table", result)
        
    asyncio.run(main())
    ```

Example usage of CorePlusSession (enterprise features):
    ```python
    import asyncio
    from deephaven_mcp.client import CorePlusSessionManager
    
    async def main():
        # Create an enterprise session manager and authenticate
        manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")
        await manager.password("username", "password")
        
        # Connect to a new worker (creates a new persistent query)
        session = await manager.connect_to_new_worker()
        
        # Get information about this persistent query
        query_info = await session.pqinfo()
        print(f"Connected to query {query_info.id} with status {query_info.status}")
        
        # Access historical data
        historical_data = await session.historical_table("market_data", "daily_prices")
        
        # Access live data that updates in real-time
        live_data = await session.live_table("market_data", "live_trades")
        
        # Query the catalog to see all available tables
        catalog = await session.catalog_table()
        
        # Filter the catalog to find tables containing price data
        price_tables = await (await session.query(catalog))\
            .where("TableName.contains('price')")\
            .to_table()
            
    asyncio.run(main())
    ```
    
    async def main():
        # Create a session manager
        manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")
        await manager.password("username", "password")
        
        # Connect to a worker
        session = await manager.connect_to_new_worker()
        
        # Access enterprise-specific features
        query_info = await session.pqinfo()
        historical_table = await session.historical_table("my_namespace", "my_table")
        
    asyncio.run(main())
    ```

Thread safety: These wrapper classes are designed for use in asynchronous applications and
use asyncio.to_thread to prevent blocking the event loop. However, they do not provide
additional thread safety beyond what the underlying Deephaven objects provide. Multiple
concurrent calls to methods of the same session object from different threads may lead to
race conditions.
"""

import asyncio
import logging

import pyarrow as pa
from pydeephaven import Session
from pydeephaven.query import Query
from pydeephaven.table import InputTable, Table

from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    QueryError,
    ResourceError,
    SessionError,
)

from ._base import ClientObjectWrapper
from ._protobuf import CorePlusQueryInfo

_LOGGER = logging.getLogger(__name__)


class CoreSession(ClientObjectWrapper[Session]):
    """
    An asynchronous wrapper around the Deephaven Session class.
    
    This class delegates to the underlying Session object while making all blocking operations
    asynchronous using asyncio.to_thread. This ensures that the caller's event loop is not blocked
    by potentially long-running operations.
    
    CoreSession provides methods for creating, querying, and manipulating Deephaven tables
    with a fully asynchronous API. This enables efficient integration with other asyncio-based
    applications and prevents blocking operations from affecting application responsiveness.
    
    Thread Safety:
      - CoreSession methods are not thread-safe and should only be called from a single thread
      - However, multiple CoreSession instances can be used concurrently from different threads
      - Each method should be awaited before calling another method on the same session
    
    Example:
        ```python
        import asyncio
        from deephaven_mcp.client import CoreSessionManager
        
        async def main():
            # Create a session manager and connect to a server
            manager = CoreSessionManager("localhost", 10000)
            
            # Get a session
            session = await manager.get_session()
            
            # Create a time table
            table = await session.time_table("PT1S")
            
            # Perform operations
            result = await (await session.query(table))\
                .update_view(["Value = i % 10"])\
                .to_table()
                
            # Work with the result
            print(await result.to_string())
        
        asyncio.run(main())
        ```
    """

    def __init__(self, session: Session, is_enterprise: bool = False):
        """
        Initialize with an underlying Session instance.

        This constructor wraps an existing pydeephaven Session object and exposes its methods
        as asynchronous coroutines. The wrapped session object is stored as self.wrapped and
        all method calls are delegated to it after ensuring they're executed in a non-blocking
        manner using asyncio.to_thread.

        Args:
            session: A pydeephaven Session instance that will be wrapped by this class. This
                   object must be a valid, already initialized Session object.
            is_enterprise: Whether the wrapped object is an enterprise session type. This affects
                         how the object is handled internally. Set to True for enterprise sessions
                         and False for standard sessions.

        Note:
            This class is typically not instantiated directly by users but rather obtained
            through a SessionManager's connect or get_session methods.
        """
        super().__init__(session, is_enterprise=is_enterprise)

    # ===== String representation methods =====

    def __str__(self) -> str:
        """
        Return a string representation of the underlying session.

        Returns:
            String representation of the wrapped session
        """
        return str(self.wrapped)

    def __repr__(self) -> str:
        """
        Return the official string representation of the underlying session.

        Returns:
            Official representation of the wrapped session
        """
        return repr(self.wrapped)

    # ===== Primary Table Operations =====

    async def empty_table(self, size: int) -> Table:
        """
        Asynchronously creates an empty table with the specified number of rows on the server.

        An empty table contains the specified number of rows with no columns. It is often used
        as a starting point for building tables programmatically by adding columns with formulas
        or as a placeholder structure for further operations.

        Args:
            size: The number of rows to include in the empty table. Must be a non-negative integer.
                 A size of 0 creates an empty table with no rows.

        Returns:
            Table: A Table object representing the newly created empty table

        Raises:
            ValueError: If size is negative
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error

        Example:
            ```python
            # Create an empty table with 100 rows
            table = await session.empty_table(100)

            # Use the empty table as a basis for creating a table with calculated columns
            # (Using the table in a query would be done after this)
            ```
        """
        _LOGGER.debug("CoreSession.empty_table called with size=%d", size)
        try:
            return await asyncio.to_thread(self.wrapped.empty_table, size)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error creating empty table: {e}")
            raise DeephavenConnectionError(
                f"Connection error creating empty table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to create empty table: {e}")
            raise QueryError(f"Failed to create empty table: {e}") from e

    async def time_table(
        self,
        period: int | str,
        start_time: int | str | None = None,
        blink_table: bool = False,
    ) -> Table:
        """
        Asynchronously creates a time table on the server.

        A time table is a special table that automatically adds new rows at regular intervals
        defined by the period parameter. It is commonly used as a driver for time-based operations
        and for triggering periodic calculations or updates.

        Args:
            period: The interval at which the time table ticks (adds a row);
                   units are nanoseconds or a time interval string, e.g. "PT00:00:.001" or "PT1S"
            start_time: The start time for the time table in nanoseconds or as a date time
                       formatted string; default is None (meaning now)
            blink_table: If True, creates a blink table which only keeps the most recent row and
                        discards previous rows. If False (default), creates an append-only time
                        table that retains all rows.

        Returns:
            A Table object representing the time table

        Raises:
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error

        Example:
            ```python
            # Create a time table that ticks every second
            time_table = await session.time_table("PT1S")

            # Create a blink time table that ticks every 100ms
            blink_table = await session.time_table("PT0.1S", blink_table=True)
            ```
        """
        _LOGGER.debug("CoreSession.time_table called")
        try:
            return await asyncio.to_thread(
                self.wrapped.time_table, period, start_time, blink_table
            )
        except ConnectionError as e:
            _LOGGER.error(f"Connection error creating time table: {e}")
            raise DeephavenConnectionError(
                f"Connection error creating time table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to create time table: {e}")
            raise QueryError(f"Failed to create time table: {e}") from e

    async def import_table(self, data: pa.Table) -> Table:
        """
        Asynchronously imports a PyArrow table as a new Deephaven table on the server.

        This method allows you to convert data from PyArrow format into a Deephaven table, enabling
        seamless integration between PyArrow data processing and Deephaven's real-time data analysis.

        Deephaven supports most common Arrow data types including:
        - Integer types (int8, int16, int32, int64)
        - Floating point types (float32, float64)
        - Boolean type
        - String type
        - Timestamp type
        - Date32 and date64 types
        - Binary type

        However, if the PyArrow table contains any field with a data type not supported by Deephaven,
        such as nested structures or certain extension types, the import operation will fail with
        a QueryError.

        Args:
            data: A PyArrow Table object to import into Deephaven. For large tables, be aware that
                 this operation requires transferring all data to the server, which may impact
                 performance for very large datasets.

        Returns:
            Table: A Deephaven Table object representing the imported data

        Raises:
            DeephavenConnectionError: If there is a network or connection error during import
            QueryError: If the operation fails due to a query-related error, such as unsupported
                      data types or server resource constraints

        Example:
            ```python
            import pyarrow as pa
            import numpy as np

            # Create a PyArrow table
            data = {
                'id': pa.array(range(100)),
                'value': pa.array(np.random.rand(100)),
                'category': pa.array(['A', 'B', 'C', 'D'] * 25)
            }
            arrow_table = pa.Table.from_pydict(data)

            # Import the table into Deephaven
            dh_table = await session.import_table(arrow_table)
            ```
        """
        _LOGGER.debug("CoreSession.import_table called")
        try:
            return await asyncio.to_thread(self.wrapped.import_table, data)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error importing table: {e}")
            raise DeephavenConnectionError(
                f"Connection error importing table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to import table: {e}")
            raise QueryError(f"Failed to import table: {e}") from e

    async def merge_tables(
        self, tables: list[Table], order_by: str | None = None
    ) -> Table:
        """
        Asynchronously merges several tables into one table on the server.

        Args:
            tables: The list of Table objects to merge
            order_by: If specified, the resultant table will be sorted on this column

        Returns:
            A Table object

        Raises:
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error
        """
        _LOGGER.debug("CoreSession.merge_tables called with %d tables", len(tables))
        try:
            return await asyncio.to_thread(self.wrapped.merge_tables, tables, order_by)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error merging tables: {e}")
            raise DeephavenConnectionError(
                f"Connection error merging tables: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to merge tables: {e}")
            raise QueryError(f"Failed to merge tables: {e}") from e

    async def query(self, table: Table) -> Query:
        """
        Asynchronously creates a Query object to define a sequence of operations on a Deephaven table.
        
        A Query object represents a chainable sequence of operations to be performed on a table.
        It provides a fluent interface for building complex data transformations in steps, with
        each operation returning a new Query object. The operations are not executed until the
        result is materialized by calling methods like `to_table()`, `to_pandas()`, or similar.
        
        The Query object allows for operations such as:
        - Filtering rows
        - Adding or modifying columns
        - Grouping and aggregating data
        - Joining tables
        - Sorting and limiting results
        
        Args:
            table: A Table object to use as the starting point for the query. This is the table
                  that operations will be performed on.
            
        Returns:
            Query: A Query object that can be used to chain operations and transformations
                 on the provided table
            
        Raises:
            DeephavenConnectionError: If there is a network or connection error when communicating
                                    with the server
            QueryError: If the operation fails due to a query-related error such as invalid
                      table references or server-side query processing errors
                      
        Example:
            ```python
            # Create a table
            table = await session.time_table("PT1S")
            
            # Create a query and chain operations
            result = await (await session.query(table))\
                .update_view([
                    "Timestamp = now()",
                    "Value = i % 10"
                ])\
                .where("Value > 5")\
                .sort("Value")\
                .to_table()
            ```
        """
        _LOGGER.debug("CoreSession.query called")
        try:
            return await asyncio.to_thread(self.wrapped.query, table)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error creating query: {e}")
            raise DeephavenConnectionError(
                f"Connection error creating query: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to create query: {e}")
            raise QueryError(f"Failed to create query: {e}") from e

    async def input_table(
        self,
        schema: pa.Schema | None = None,
        init_table: Table | None = None,
        key_cols: str | list[str] | None = None,
        blink_table: bool = False,
    ) -> InputTable:
        """
        Asynchronously creates an InputTable from either Arrow schema or initial table.

        An InputTable is a special type of table that allows direct data insertion from the client.
        Three different types of InputTable can be created:

        1. **Append-only table**: Created when blink_table=False and key_cols=None.
           Rows are only ever added, never updated or removed.

        2. **Keyed table**: Created when blink_table=False and key_cols is specified.
           Rows with the same key values will update existing rows rather than append.

        3. **Blink table**: Created when blink_table=True.
           Only the most recent row(s) are retained, all previous rows are discarded.

        Args:
            schema: The PyArrow schema defining the structure of the input table. Required if
                   init_table is not provided.
            init_table: An existing table to use as the initial state for the input table.
                       Required if schema is not provided.
            key_cols: The column name or list of column names to use as the unique key for the table.
                     When specified and blink_table is False, creates a keyed table where rows with
                     matching key values update existing rows instead of appending.
            blink_table: If True, creates a blink table that only retains the most recent rows.
                        If False (default), creates either an append-only table (if key_cols is None)
                        or a keyed table (if key_cols is specified).

        Returns:
            An InputTable object that can be used to add or update data

        Raises:
            ValueError: If neither schema nor init_table is provided, or if other invalid
                      parameter combinations are used
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error

        Example:
            ```python
            import pyarrow as pa

            # Create an append-only table
            schema = pa.schema([
                pa.field('name', pa.string()),
                pa.field('value', pa.int64())
            ])
            append_table = await session.input_table(schema=schema)

            # Create a keyed table that updates rows with matching keys
            keyed_table = await session.input_table(schema=schema, key_cols='name')

            # Create a blink table that only keeps the latest row
            blink_table = await session.input_table(schema=schema, blink_table=True)
            ```
        """
        _LOGGER.debug("CoreSession.input_table called")
        try:
            return await asyncio.to_thread(
                self.wrapped.input_table, schema, init_table, key_cols, blink_table
            )
        except ValueError:
            # Re-raise ValueError directly for invalid inputs
            raise
        except ConnectionError as e:
            _LOGGER.error(f"Connection error creating input table: {e}")
            raise DeephavenConnectionError(
                f"Connection error creating input table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to create input table: {e}")
            raise QueryError(f"Failed to create input table: {e}") from e

    # ===== Table Management =====

    async def open_table(self, name: str) -> Table:
        """
        Asynchronously opens a table in the global scope with the given name on the server.

        Args:
            name: The name of the table

        Returns:
            A Table object

        Raises:
            ResourceError: If no table exists with the given name
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error
        """
        _LOGGER.debug("CoreSession.open_table called with name=%s", name)
        try:
            return await asyncio.to_thread(self.wrapped.open_table, name)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error opening table: {e}")
            raise DeephavenConnectionError(
                f"Connection error opening table: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(f"Table not found: {e}")
            raise ResourceError(f"Table not found: {name}") from e
        except Exception as e:
            _LOGGER.error(f"Failed to open table: {e}")
            raise QueryError(f"Failed to open table: {e}") from e

    async def bind_table(self, name: str, table: Table) -> None:
        """
        Asynchronously binds a table to the given name on the server so that it can be referenced by that name.

        Args:
            name: Name for the table
            table: A Table object

        Raises:
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error
        """
        _LOGGER.debug("CoreSession.bind_table called with name=%s", name)
        try:
            await asyncio.to_thread(self.wrapped.bind_table, name, table)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error binding table: {e}")
            raise DeephavenConnectionError(
                f"Connection error binding table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to bind table: {e}")
            raise QueryError(f"Failed to bind table: {e}") from e

    # ===== Session Management =====

    async def close(self) -> None:
        """
        Asynchronously closes the Session object if it hasn't timed out already.

        This method should be called when the session is no longer needed to properly
        clean up resources on the server.

        Raises:
            DeephavenConnectionError: If there is a network or connection error closing the session
            SessionError: If the session cannot be closed for non-connection reasons
        """
        _LOGGER.debug("CoreSession.close called")
        try:
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug("Session closed successfully")
        except ConnectionError as e:
            _LOGGER.error(f"Connection error closing session: {e}")
            raise DeephavenConnectionError(
                f"Connection error closing session: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to close session: {e}")
            raise SessionError(f"Failed to close session: {e}") from e

    async def run_script(self, script: str, systemic: bool | None = None) -> None:
        """
        Asynchronously runs the supplied Python script on the server.

        Args:
            script: The Python script code
            systemic: Whether to treat the code as systemically important.
                    Defaults to None which uses the default system behavior

        Raises:
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the script cannot be run or encounters an error
        """
        _LOGGER.debug("CoreSession.run_script called")
        try:
            await asyncio.to_thread(self.wrapped.run_script, script, systemic)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error running script: {e}")
            raise DeephavenConnectionError(
                f"Connection error running script: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to run script: {e}")
            raise QueryError(f"Failed to run script: {e}") from e

    # ===== Table and Session Status Methods =====

    async def tables(self) -> list[str]:
        """
        Asynchronously get the names of global tables available in the server.

        This method wraps the potentially blocking operation in a background thread
        to prevent blocking the event loop.

        Returns:
            A list of table names

        Raises:
            DeephavenConnectionError: If there is a network or connection error
            QueryError: If the operation fails due to a query-related error
        """
        _LOGGER.debug("CoreSession.tables called")
        try:
            return await asyncio.to_thread(self.wrapped.tables)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error listing tables: {e}")
            raise DeephavenConnectionError(
                f"Connection error listing tables: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to list tables: {e}")
            raise QueryError(f"Failed to list tables: {e}") from e

    async def is_alive(self) -> bool:
        """
        Asynchronously check if the session is still alive.

        This method wraps the potentially blocking session refresh operation in a
        background thread to prevent blocking the event loop.

        Returns:
            True if the session is alive, False otherwise

        Raises:
            DeephavenConnectionError: If there is a network or connection error
            SessionError: If there's an error checking session status
        """
        _LOGGER.debug("CoreSession.is_alive called")
        try:
            return await asyncio.to_thread(self.wrapped.is_alive)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error checking session status: {e}")
            raise DeephavenConnectionError(
                f"Connection error checking session status: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to check session status: {e}")
            raise SessionError(f"Failed to check session status: {e}") from e


class CorePlusSession(CoreSession):
    """
    A wrapper around the enterprise DndSession class, providing a standardized interface
    while delegating to the underlying enterprise session implementation.

    This class provides access to enterprise-specific functionality like persistent queries,
    historical data access, and catalog operations while maintaining compatibility with
    the standard Session interface. CorePlusSession extends CoreSession with additional
    methods for enterprise-specific features.

    Key enterprise-specific features include:
    - Persistent query information (pqinfo)
    - Historical and live table access (historical_table, live_table)
    - Catalog operations (catalog_table)

    Example:
        ```python
        import asyncio
        from deephaven_mcp.client import CorePlusSessionManager

        async def work_with_enterprise_session():
            # Create a session manager and authenticate
            manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")
            await manager.password("username", "password")

            # Connect to a worker to get a CorePlusSession
            session = await manager.connect_to_new_worker()

            # Get information about this persistent query
            query_info = await session.pqinfo()
            print(f"Query ID: {query_info.id}")
            print(f"Query status: {query_info.status}")

            # Access historical data
            historical_data = await session.historical_table("my_namespace", "my_historical_table")

            # View available tables in the catalog
            catalog = await session.catalog_table()
        ```

    See Also:
        - CoreSession: Base class for standard session operations
        - CorePlusSessionManager: For creating and managing enterprise sessions
    """

    def __init__(self, session: Session):
        """
        Initialize with an underlying DndSession instance.

        DndSession is the enterprise-specific session class in Deephaven Enterprise that extends
        the standard Session class with additional enterprise capabilities like persistent queries,
        historical tables, and catalog operations. This class wraps DndSession to provide an
        asynchronous API while maintaining enterprise functionality.

        This constructor wraps an existing DndSession object from the enterprise client package
        and exposes its methods as asynchronous coroutines. The wrapped session object is stored
        as self.wrapped and all method calls are delegated to it after ensuring they're executed
        in a non-blocking manner using asyncio.to_thread.

        Args:
            session: A DndSession instance from deephaven_enterprise.client.session_manager
                     that will be wrapped by this class. This must be a valid, already initialized
                     DndSession object with proper enterprise capabilities.

        Raises:
            InternalError: If the provided session is not a DndSession instance or doesn't
                         support the required enterprise functionality.

        Note:
            - This class is typically not instantiated directly by users but rather obtained
              through a CorePlusSessionManager's connect_to_new_worker or connect_to_persistent_query
              methods.
            - The wrapped DndSession maintains its own connection to the server and enterprise
              resources like persistent queries and historical tables.
            - The session is automatically marked as an enterprise session (is_enterprise=True)
              which enables specialized handling of enterprise-specific methods and objects.

        Thread Safety:
            As with CoreSession, methods of this class are not thread-safe and should only be called
            from a single thread. Each method should be awaited before calling another method on the
            same session.
        """
        super().__init__(session, is_enterprise=True)

    async def pqinfo(self) -> CorePlusQueryInfo:
        """
        Asynchronously retrieve the persistent query information for this session as a CorePlusQueryInfo object.

        A persistent query in Deephaven is a query that continues to run on the server even after
        the client disconnects, allowing for continuous data processing and analysis. Each session
        connected to a persistent query can access information about that query through this method.

        This method obtains the protobuf persistent query information from the underlying
        session and wraps it in a CorePlusQueryInfo object to provide a more convenient
        interface for accessing the query information. The returned object includes details like
        the query ID, name, state, creation time, and associated metadata.

        Returns:
            CorePlusQueryInfo: A wrapper around the persistent query info protobuf message containing
                            information about the current persistent query session, including ID,
                            status, created time, and other metadata.

        Raises:
            DeephavenConnectionError: If there is a network or connection error when attempting
                                    to communicate with the server
            QueryError: If the persistent query information cannot be retrieved due to an error
                      in the query processing system

        Example:
            ```python
            # Get information about the current persistent query
            query_info = await session.pqinfo()

            # Access query attributes
            print(f"Query ID: {query_info.id}")
            print(f"Query name: {query_info.name}")
            print(f"Query status: {query_info.status}")
            print(f"Query creation time: {query_info.created_time}")

            # Check query state
            if query_info.is_running():
                print("Query is currently running")
            elif query_info.is_completed():
                print("Query has completed")
            ```
        """
        _LOGGER.debug("CorePlusSession.pqinfo called")
        try:
            protobuf_obj = await asyncio.to_thread(self._session.pqinfo)
            return CorePlusQueryInfo(protobuf_obj)
        except ConnectionError as e:
            _LOGGER.error(
                f"Connection error retrieving persistent query information: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error retrieving persistent query information: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to retrieve persistent query information: {e}")
            raise QueryError(
                f"Failed to retrieve persistent query information: {e}"
            ) from e

    async def historical_table(self, namespace: str, table_name: str) -> Table:
        """
        Asynchronously fetches a historical table from the database on the server.
        
        Historical tables in Deephaven represent point-in-time snapshots of data that have been
        persisted to storage. These tables contain immutable historical data and are typically
        used for:
        
        - Analysis of historical trends and patterns
        - Backtesting of algorithms and strategies
        - Audit and compliance purposes
        - Data archiving and retrieval
        
        Historical tables are identified by a namespace and table name combination. The namespace
        provides a way to organize tables and prevent name collisions across different data domains
        or applications.
        
        Args:
            namespace: The namespace of the table, which helps organize tables into logical groups
                     or domains (e.g., 'market_data', 'user_analytics', etc.)
            table_name: The name of the table within the specified namespace
            
        Returns:
            Table: A Table object representing the requested historical table. This table
                 is immutable and represents data as it existed at the time of storage.
            
        Raises:
            DeephavenConnectionError: If there is a network or connection error when attempting
                                    to communicate with the server
            ResourceError: If the table cannot be found in the specified namespace
            QueryError: If the table exists but cannot be accessed due to a query-related error
                      such as permission issues or data corruption
                      
        Example:
            ```python
            # Retrieve a historical market data table
            stock_data = await session.historical_table("market_data", "daily_stock_prices")
            
            # Use the table in analysis
            filtered_data = await (await session.query(stock_data))\
                .where("Symbol == 'AAPL'")\
                .sort("Date")\
                .to_table()
            ```
        """
        _LOGGER.debug(
            "CorePlusSession.historical_table called with namespace=%s, table_name=%s",
            namespace,
            table_name,
        )
        try:
            return await asyncio.to_thread(
                self._session.historical_table, namespace, table_name
            )
        except ConnectionError as e:
            _LOGGER.error(f"Connection error fetching historical table: {e}")
            raise DeephavenConnectionError(
                f"Connection error fetching historical table: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(f"Historical table not found: {e}")
            raise ResourceError(
                f"Historical table not found: {namespace}.{table_name}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to fetch historical table: {e}")
            raise QueryError(f"Failed to fetch historical table: {e}") from e

    async def live_table(self, namespace: str, table_name: str) -> Table:
        """
        Asynchronously fetches a live table from the database on the server.
        
        Live tables in Deephaven are dynamic tables that update in real-time as new data arrives.
        Unlike historical tables which represent point-in-time snapshots, live tables provide:
        
        - Real-time updates as new data becomes available
        - Continuous processing of incoming data
        - Dynamic views that reflect the latest state of the data
        
        Live tables are particularly useful for monitoring current market conditions,
        tracking real-time metrics, or implementing active trading strategies. They
        maintain a connection to the data source and automatically update when new
        data arrives.
        
        The relationship between live and historical tables:
        - A historical table is a snapshot of data at a specific point in time
        - A live table is continuously updated with new data
        - The same table can often be accessed in both live and historical modes
        
        Args:
            namespace: The namespace of the table, which helps organize tables into logical
                     groups or domains (e.g., 'market_data', 'system_metrics')
            table_name: The name of the table within the specified namespace
            
        Returns:
            Table: A Table object representing the requested live table. This table
                 will automatically update as new data arrives at the server.
            
        Raises:
            DeephavenConnectionError: If there is a network or connection error when
                                    attempting to communicate with the server
            ResourceError: If the table cannot be found in the specified namespace
            QueryError: If the table exists but cannot be accessed due to a
                      query-related error such as permission issues
                      
        Example:
            ```python
            # Retrieve a live market data table that updates with new trades
            live_trades = await session.live_table("market_data", "trade_feed")
            
            # Create a derived table that updates automatically when live_trades updates
            filtered_trades = await (await session.query(live_trades))\
                .where("Price > 100.0")\
                .to_table()
                
            # The filtered_trades table will continue to update as new trades arrive
            ```
        """
        _LOGGER.debug(
            "CorePlusSession.live_table called with namespace=%s, table_name=%s",
            namespace,
            table_name,
        )
        try:
            return await asyncio.to_thread(
                self._session.live_table, namespace, table_name
            )
        except ConnectionError as e:
            _LOGGER.error(f"Connection error fetching live table: {e}")
            raise DeephavenConnectionError(
                f"Connection error fetching live table: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(f"Live table not found: {e}")
            raise ResourceError(
                f"Live table not found: {namespace}.{table_name}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to fetch live table: {e}")
            raise QueryError(f"Failed to fetch live table: {e}") from e

    async def catalog_table(self) -> Table:
        """
        Asynchronously fetches the catalog table from the database on the server.

        The catalog table provides a comprehensive inventory of all tables available in the
        Deephaven server environment. This includes system tables, user-created tables,
        tables from database connections, and any other tables registered in the system.

        The catalog table contains metadata about each table, such as:
        - Table name
        - Table type/source
        - Column information
        - Creation time
        - Size information
        - Owner/permission data

        This method is particularly useful for discovery and exploration in environments
        with many tables or when connecting to a server with unknown data structures.

        Returns:
            Table: A Deephaven Table object representing the catalog with rows containing
                metadata about all available tables in the system

        Raises:
            DeephavenConnectionError: If there is a network or connection error when
                                    attempting to connect to the server
            QueryError: If the operation fails due to a query-related error or
                      insufficient permissions

        Example:
            ```python
            # Fetch the catalog table
            catalog = await session.catalog_table()

            # Print the names of all available tables
            print("Available tables:")
            for table_name in catalog["TableName"].to_list():
                print(f"- {table_name}")

            # Find tables with a specific column
            tables_with_timestamp = catalog.where("Columns.contains('Timestamp')")
            ```
        """
        _LOGGER.debug("CorePlusSession.catalog_table called")
        try:
            return await asyncio.to_thread(self._session.catalog_table)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error fetching catalog table: {e}")
            raise DeephavenConnectionError(
                f"Connection error fetching catalog table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to fetch catalog table: {e}")
            raise QueryError(f"Failed to fetch catalog table: {e}") from e
