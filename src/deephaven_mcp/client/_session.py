"""Async wrappers for Deephaven standard and enterprise sessions.

This module provides asynchronous wrappers for Deephaven session classes, ensuring all blocking operations are executed in background threads via `asyncio.to_thread`. It supports both standard and enterprise (Core+) sessions, exposing a unified async API for table creation, data import, querying, and advanced enterprise features.

Classes:
    - BaseSession: Abstract base class for all asynchronous session wrappers with common functionality.
    - CoreSession: Async wrapper for basic pydeephaven Session, supporting standard table operations.
    - CorePlusSession: Async wrapper for enterprise DndSession, extending BaseSession with persistent query, historical data, and catalog features.

Key Features:
    - Non-blocking API: All operations that interact with the server are asynchronous and do not block the event loop
    - Unified interface: Common API across standard and enterprise sessions
    - Robust error handling: Consistent exception translation with detailed error messages
    - Comprehensive logging: Detailed logs for debugging and monitoring

Example (standard):
    ```python
    import asyncio
    import pyarrow as pa
    from deephaven_mcp.sessions import CommunitySessionManager

    async def main():
        manager = CommunitySessionManager("localhost", 10000)
        session = await manager.get_session()
        table = await session.time_table("PT1S")
        result = await (await session.query(table)).update_view(["Value = i % 10"]).to_table()
        schema = pa.schema([
            pa.field('name', pa.string()),
            pa.field('value', pa.int64())
        ])
        input_table = await session.input_table(schema=schema)
        await session.bind_table("my_result_table", result)

    asyncio.run(main())
    ```

Example (enterprise):
    ```python
    import asyncio
    from deephaven_mcp.client import CorePlusSessionFactory

    async def main():
        factory = await CorePlusSessionFactory.from_config({"url": "https://myserver.example.com/iris/connection.json"})
        await factory.password("username", "password")
        session = await factory.connect_to_new_worker()
        info = await session.pqinfo()
        print(f"Connected to query {info.id} with status {info.status}")
        hist = await session.historical_table("market_data", "daily_prices")
        live = await session.live_table("market_data", "live_trades")
        catalog = await session.catalog_table()
        price_tables = await (await session.query(catalog)).where("TableName.contains('price')").to_table()
        # Create another session factory
        factory2 = await CorePlusSessionFactory.from_config({"url": "https://myserver.example.com/iris/connection.json"})
        await factory2.password("username", "password")

        # Connect to a worker
        session = await factory2.connect_to_new_worker()

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
import os
import sys
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import pyarrow as pa
from pydeephaven import Session
from pydeephaven.query import Query
from pydeephaven.table import InputTable, Table

if TYPE_CHECKING:
    import deephaven_enterprise.client.session_manager  # pragma: no cover
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)
from deephaven_mcp.config import (
    CommunitySessionConfigurationError,
    redact_community_session_config,
    validate_single_community_session_config,
)
from deephaven_mcp.io import load_bytes

from ._base import ClientObjectWrapper
from ._protobuf import CorePlusQueryInfo

_LOGGER = logging.getLogger(__name__)


T = TypeVar("T", bound=Session)


class BaseSession(ClientObjectWrapper[T], Generic[T]):
    """
    Base class for asynchronous Deephaven session wrappers.

    Provides a unified async interface for all Deephaven session types (standard and enterprise).
    All blocking operations are executed using `asyncio.to_thread` to prevent blocking the event loop.
    Intended for subclassing by `CoreSession` (standard) and `CorePlusSession` (enterprise).

    Key Features:
        - Asynchronous API: Converts synchronous Deephaven session operations to async coroutines
        - Non-blocking: All potentially blocking operations run in separate threads
        - Exception translation: Converts native exceptions to async-compatible exception types
        - Resource management: Proper cleanup of sessions through __del__ or explicit close() calls
        - Type safety: Preserves proper typing information for editor support and static analysis

    Implementation Details:
        - Uses asyncio.to_thread for executing blocking operations without affecting the event loop
        - Delegates method calls to the wrapped Session object
        - Wraps returned objects in appropriate async wrappers when necessary
        - Handles proper cleanup of resources

    Usage Guidelines:
        - Do not instantiate directly; use a SessionManager to obtain a session instance
        - All methods are async and must be awaited
        - Do not call methods of the same session object concurrently from multiple threads
        - Multiple session objects may be used in parallel
        - Always await the close() method when done or use async with
        - For enterprise features, use CorePlusSession subclass

    Example:
        ```python
        import asyncio
        from deephaven_mcp.sessions import CommunitySessionManager

        async def main():
            manager = CommunitySessionManager("localhost", 10000)
            session = await manager.get_session()

            # Create a time table
            table = await session.time_table("PT1S")

            # Query and transform the table
            result = await (await session.query(table)).update_view(["Value = i % 10"]).to_table()

            # Display the results
            print(await result.to_string())

            # Cleanup
            await session.close()

        asyncio.run(main())
        ```
    """

    def __init__(self, session: T, is_enterprise: bool, programming_language: str):
        """
        Initialize the async session wrapper with a pydeephaven Session instance.

        Args:
            session: An initialized pydeephaven Session object to wrap.
            is_enterprise: Set True for enterprise (Core+) sessions, False for standard sessions.
            programming_language: The programming language associated with this session (e.g., "python", "groovy").

        Note:
            Do not instantiate this class directly; use a SessionManager to obtain session instances.
        """
        super().__init__(session, is_enterprise=is_enterprise)
        self._programming_language = programming_language

    # ===== Properties =====

    @property
    def programming_language(self) -> str:
        """
        Get the programming language associated with this session.

        Returns:
            str: The programming language (e.g., "python", "groovy")
        """
        return self._programming_language

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
        _LOGGER.debug(f"[CoreSession:empty_table] Called with size={size}")
        try:
            return await asyncio.to_thread(self.wrapped.empty_table, size)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:empty_table] Connection error creating empty table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error creating empty table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CoreSession:empty_table] Failed to create empty table: {e}"
            )
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
        _LOGGER.debug(f"[CoreSession:time_table] Called")
        try:
            # TODO: remove type: ignore after pydeephaven is updated.  See https://deephaven.atlassian.net/browse/DH-19874
            return await asyncio.to_thread(
                self.wrapped.time_table, period, start_time, blink_table  # type: ignore[arg-type]
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:time_table] Connection error creating time table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error creating time table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:time_table] Failed to create time table: {e}")
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
        _LOGGER.debug(f"[CoreSession:import_table] Called")
        try:
            return await asyncio.to_thread(self.wrapped.import_table, data)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:import_table] Connection error importing table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error importing table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:import_table] Failed to import table: {e}")
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
        _LOGGER.debug(f"[CoreSession:merge_tables] Called with {len(tables)} tables")
        try:
            # TODO: remove type: ignore after pydeephaven is updated.  See https://deephaven.atlassian.net/browse/DH-19874
            return await asyncio.to_thread(self.wrapped.merge_tables, tables, order_by)  # type: ignore[arg-type]
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:merge_tables] Connection error merging tables: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error merging tables: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:merge_tables] Failed to merge tables: {e}")
            raise QueryError(f"Failed to merge tables: {e}") from e

    async def query(self, table: Table) -> Query:
        """
        Asynchronously creates a Query object to define a sequence of operations on a Deephaven table.
        
        A Query object represents a chainable sequence of operations to be performed on a table.
        It provides a fluent interface for building complex data transformations in steps, with
        each operation returning a new Query object. The operations are not executed until the
        result is materialized by calling methods like `to_table()`, `to_pandas()`, or similar.
        
        Common Query Operations:
        - Filtering rows: `where("condition")`
        - Adding/modifying columns: `update_view(["NewCol = expression", ...])`
        - Grouping: `group_by(["col1", "col2"]).agg(["Sum = sum(Value)"])`
        - Joining tables: `join(other_table, on=["key"])`
        - Natural joins: `natural_join(other_table, on=["key"])`
        - Cross joins: `cross_join(other_table)`
        - Sorting: `sort(["col1", "col2 DESC"])`
        - Limiting results: `head(n)`, `tail(n)`, `take_range(start, end)`
        - Selecting columns: `select(["col1", "col2"])`
        - Renaming columns: `rename({"OldName": "NewName"})`
        
        Materialization Methods:
        - `to_table()`: Create a new Table with the results
        - `to_pandas()`: Convert results to a pandas DataFrame
        - `to_arrow()`: Convert results to a PyArrow Table
        - `to_list()`: Convert results to a Python list of rows
        - `count()`: Count the number of rows
        - `first()`: Get the first row as a Python dict
        
        Args:
            table: A Table object to use as the starting point for the query. This is the table
                   that operations will be performed on. This can be any Table object returned by
                   other session methods or previous query operations.
            
        Returns:
            Query: A Query object that can be used to chain operations and transformations
                  on the provided table. This object provides a fluent interface where each
                  operation returns a new Query object.
            
        Raises:
            DeephavenConnectionError: If there is a network or connection error when communicating
                                     with the server
            QueryError: If the operation fails due to a query-related error such as invalid
                       table references or server-side query processing errors
                       
        Example - Basic filtering and transformation:
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
            
        Example - Joining tables:
            ```python
            # Load two tables
            trades = await session.table_from_pandas(trades_df)
            symbols = await session.table_from_pandas(symbols_df)
            
            # Join the tables and perform calculations
            enriched_trades = await (await session.query(trades))\
                .join(
                    symbols,
                    on=["Symbol"], 
                    joins=["SecurityType", "Exchange"])\
                .update_view(["TradeValue = Price * Size"])\
                .sort(["Timestamp DESC"])\
                .to_table()
            ```
            
        Example - Grouping and aggregation:
            ```python
            # Calculate statistics by symbol and exchange
            stats = await (await session.query(trades))\
                .group_by(["Symbol", "Exchange"])\
                .agg([
                    "AvgPrice = avg(Price)",
                    "TotalVolume = sum(Size)",
                    "TradeCount = count()",
                    "MaxPrice = max(Price)",
                    "MinPrice = min(Price)"
                ])\
                .sort(["TotalVolume DESC"])\
                .to_table()
            ```
            
        Note:
            - Query objects are immutable; each operation creates a new Query instance
            - The query is not executed until a materialization method is called
            - For tables with many rows, use limit operations like head() when appropriate
            - Live tables produce live results that update automatically
        """
        _LOGGER.debug(f"[CoreSession:query] Called")
        try:
            return await asyncio.to_thread(self.wrapped.query, table)
        except ConnectionError as e:
            _LOGGER.error(f"[CoreSession:query] Connection error creating query: {e}")
            raise DeephavenConnectionError(
                f"Connection error creating query: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:query] Failed to create query: {e}")
            raise QueryError(f"Failed to create query: {e}") from e

    async def input_table(
        self,
        schema: pa.Schema | None = None,
        init_table: Table | None = None,
        key_cols: str | list[str] | None = None,
        blink_table: bool = False,
    ) -> InputTable:
        """
        Asynchronously create an InputTable on the server using a PyArrow schema or an existing Table.

        InputTables allow direct, client-driven data insertion and updates. Three modes are supported:

        1. **Append-only**: (blink_table=False, key_cols=None) Rows are only appended.
        2. **Keyed**: (blink_table=False, key_cols specified) Rows with duplicate keys update existing rows.
        3. **Blink**: (blink_table=True) Only the most recent row(s) are retained; previous rows are discarded.

        Args:
            schema (pa.Schema, optional): PyArrow schema for the input table. Required if init_table is not provided.
            init_table (Table, optional): Existing Table to use as the initial state. Required if schema is not provided.
            key_cols (str or list[str], optional): Column(s) to use as unique key. If set and blink_table is False, creates a keyed table.
            blink_table (bool, optional): If True, creates a blink table; if False (default), creates append-only or keyed table.

        Returns:
            InputTable: An object supporting direct data insertion and updates.

        Raises:
            ValueError: If neither schema nor init_table is provided, or if parameters are invalid.
            DeephavenConnectionError: If a network or connection error occurs.
            QueryError: If the operation fails due to query or server error.

        Example:
            ```python
            import pyarrow as pa
            schema = pa.schema([
                pa.field('name', pa.string()),
                pa.field('value', pa.int64())
            ])
            # Append-only
            append_table = await session.input_table(schema=schema)
            # Keyed
            keyed_table = await session.input_table(schema=schema, key_cols='name')
            # Blink
            blink_table = await session.input_table(schema=schema, blink_table=True)
            ```
        """
        _LOGGER.debug(f"[CoreSession:input_table] Called")
        try:
            # TODO: remove type: ignore after pydeephaven is updated.  See https://deephaven.atlassian.net/browse/DH-19874
            return await asyncio.to_thread(
                self.wrapped.input_table, schema, init_table, key_cols, blink_table  # type: ignore[arg-type]
            )
        except ValueError:
            # Re-raise ValueError directly for invalid inputs
            raise
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:input_table] Connection error creating input table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error creating input table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CoreSession:input_table] Failed to create input table: {e}"
            )
            raise QueryError(f"Failed to create input table: {e}") from e

    # ===== Table Management =====

    async def open_table(self, name: str) -> Table:
        """
        Asynchronously open a global table by name from the server.

        Args:
            name (str): Name of the table to open. Must exist in the global namespace.

        Returns:
            Table: The opened Table object.

        Raises:
            ResourceError: If no table exists with the given name.
            DeephavenConnectionError: If a network or connection error occurs.
            QueryError: If the operation fails due to a query-related error (e.g., permissions, server error).

        Example:
            ```python
            table = await session.open_table("my_table")
            ```
        """
        _LOGGER.debug(f"[CoreSession:open_table] Called with name={name}")
        try:
            return await asyncio.to_thread(self.wrapped.open_table, name)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:open_table] Connection error opening table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error opening table: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(f"[CoreSession:open_table] Table not found: {e}")
            raise ResourceError(f"Table not found: {name}") from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:open_table] Failed to open table: {e}")
            raise QueryError(f"Failed to open table: {e}") from e

    async def bind_table(self, name: str, table: Table) -> None:
        """
        Asynchronously bind a Table object to a global name on the server.

        This method makes a table accessible by name in the Deephaven global namespace. Once bound,
        the table can be referenced by its name in subsequent operations, queries, and by other
        users or sessions connected to the same server. This is particularly useful for:

        - Sharing results with other users or applications
        - Creating persistent references to computed tables
        - Building up complex multi-step data processing workflows
        - Making tables available for external tools that connect to Deephaven

        The table remains bound until explicitly removed or until the server session ends,
        depending on server configuration. Binding a table with a name that already exists
        will overwrite the previous binding.

        Args:
            name (str): Name to assign to the table in the global namespace. Should be a valid
                      identifier that follows Deephaven naming conventions. Names are case-sensitive
                      and should not contain spaces or special characters.
            table (Table): The Table object to bind. This can be any Table instance, including
                         tables created from files, databases, or query results.

        Raises:
            DeephavenConnectionError: If a network or connection error occurs while attempting
                                     to communicate with the server.
            QueryError: If the operation fails due to a query-related error, such as an invalid
                      table object, name formatting issues, or server permissions problems.

        Example - Binding a simple table:
            ```python
            # Create a table
            data_table = await session.table_from_pandas(df)

            # Make it available globally
            await session.bind_table("daily_prices", data_table)
            ```

        Example - Creating and sharing derived tables:
            ```python
            # Create a base table
            base_table = await session.time_table("PT1S")

            # Create derived tables through queries
            filtered = await (await session.query(base_table)).where("i % 2 == 0").to_table()
            aggregated = await (await session.query(base_table)).group_by(["i % 10 as bucket"]).agg(["count=count()"]).to_table()

            # Bind both for access by others
            await session.bind_table("even_records", filtered)
            await session.bind_table("record_counts", aggregated)
            ```

        Note:
            - Table bindings persist until explicitly removed or the server session ends
            - Binding large tables does not duplicate the data; only a reference is created
            - The same table can be bound to multiple different names
            - To access bound tables from other sessions, use the catalog_table method to discover them
        """
        _LOGGER.debug(f"[CoreSession:bind_table] Called with name={name}")
        try:
            await asyncio.to_thread(self.wrapped.bind_table, name, table)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:bind_table] Connection error binding table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error binding table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:bind_table] Failed to bind table: {e}")
            raise QueryError(f"Failed to bind table: {e}") from e

    # ===== Session Management =====

    async def close(self) -> None:
        """
        Asynchronously close the session and release all associated server resources.

        This method should be called when the session is no longer needed to prevent resource leaks.
        After closing, the session object should not be used for further operations. The method
        performs the following cleanup tasks:

        - Terminates the connection to the server
        - Releases memory and other resources on the server side
        - Marks the session as closed locally
        - Logs the session closure for audit purposes

        Resource Management Best Practices:
        1. Always explicitly close sessions when done with them
        2. Use try/finally blocks or async context managers to ensure sessions are closed
        3. Do not attempt to use a session after closing it
        4. One session instance should be closed only once

        While the BaseSession implements __del__ to attempt cleanup during garbage collection,
        explicit closure is strongly recommended as garbage collection timing is unpredictable.

        Raises:
            DeephavenConnectionError: If a network or connection error occurs during close,
                                    such as connection timeouts or network disruptions.
            SessionError: If the session cannot be closed for non-connection reasons,
                        such as server errors, invalid session state, or permission issues.

        Example - Basic usage:
            ```python
            # Close a session when done
            await session.close()
            ```

        Example - Using try/finally for reliable cleanup:
            ```python
            session = await manager.get_session()
            try:
                # Use the session for operations
                table = await session.time_table("PT1S")
                # ... more operations
            finally:
                # Ensure session is closed even if an error occurs
                await session.close()
            ```

        Example - Using as an async context manager:
            ```python
            async with (await manager.get_session()) as session:
                # Session will be automatically closed after this block
                table = await session.time_table("PT1S")
                # ... more operations
            # Session is now closed
            ```
        """
        _LOGGER.debug(f"[CoreSession:close] Called")
        try:
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug(f"[CoreSession:close] Session closed successfully")
        except ConnectionError as e:
            _LOGGER.error(f"[CoreSession:close] Connection error closing session: {e}")
            raise DeephavenConnectionError(
                f"Connection error closing session: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:close] Failed to close session: {e}")
            raise SessionError(f"Failed to close session: {e}") from e

    async def run_script(self, script: str, systemic: bool | None = None) -> None:
        """
        Asynchronously execute a Python script on the server in the context of this session.

        This method sends Python code to be executed on the Deephaven server, allowing for
        complex operations that might not be directly exposed through the API. The script
        runs in the same context as the session, with access to:

        - All tables bound in the global namespace
        - Server-side imports and libraries
        - Server-side Deephaven API and functionality
        - Variables and objects previously defined in the session

        The script execution is server-side only; local variables from your client application
        are not automatically available to the script unless explicitly passed as part of the
        script string. Any output from print() statements will appear in the server logs, not
        in the client application.

        Use cases include:
        - Complex table transformations and calculations
        - Custom data processing logic
        - Administrative tasks on the server
        - Creating tables programmatically
        - Loading and processing data from server-accessible locations

        Args:
            script (str): The Python script code to execute. This can be a single line or a
                        multi-line string containing complete Python code.
            systemic (bool, optional): If True, treat the script as systemically important,
                                      which may affect how the server prioritizes or logs the
                                      execution. If None, uses the server's default behavior.
                                      System scripts may have different timeout or resource limits.

        Raises:
            DeephavenConnectionError: If a network or connection error occurs while sending
                                     the script to the server or receiving results.
            QueryError: If the script cannot be run or encounters an error during execution,
                      such as syntax errors, runtime errors, or permission issues. The error
                      message typically includes the Python traceback from the server.

        Example - Simple hello world:
            ```python
            await session.run_script("print('Hello from server!')")
            ```

        Example - Creating and binding a table:
            ```python
            # Define a multi-line script with proper Deephaven imports and functions
            script = '''
            import numpy as np
            import pandas as pd
            from deephaven import new_table

            # Create sample data
            dates = pd.date_range('20230101', periods=100)
            values = np.random.randn(100).cumsum()

            # Create and bind a Deephaven table
            df = pd.DataFrame({'Date': dates, 'Value': values})
            table = new_table(df)
            bind_table('random_walk', table)
            '''

            # Execute the script on the server
            await session.run_script(script)
            ```

        Example - Executing data science calculations:
            ```python
            # Run complex calculations on server-side data
            advanced_script = '''
            from deephaven.plot import Figure

            # Get a table that should already exist on the server
            table = get_table('daily_prices')

            # Calculate moving averages
            result = table.update_view([
                'SMA_5 = rolling_avg(Price, 5)',
                'SMA_20 = rolling_avg(Price, 20)',
                'Signal = SMA_5 > SMA_20 ? 1 : -1'
            ])

            # Create a plot
            fig = Figure()
            fig.plot_xy(series_name='Price', t=result.get_column('Date'), y=result.get_column('Price'))
            fig.plot_xy(series_name='SMA_5', t=result.get_column('Date'), y=result.get_column('SMA_5'))
            fig.plot_xy(series_name='SMA_20', t=result.get_column('Date'), y=result.get_column('SMA_20'))

            # Bind results
            bind_table('moving_avg_result', result)
            bind_figure('price_chart', fig)
            '''

            # Execute the script on the server
            await session.run_script(advanced_script)
            ```

        Note:
            - Scripts are executed synchronously on the server but the method returns asynchronously
            - Table bindings created in the script persist after the script completes
            - Server-side variable scope is maintained between script executions
            - For security reasons, some server configurations may restrict certain imports or operations
            - Large result sets should be bound to tables rather than returned directly
        """
        _LOGGER.debug(f"[CoreSession:run_script] Called")
        try:
            await asyncio.to_thread(self.wrapped.run_script, script, systemic)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:run_script] Connection error running script: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error running script: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:run_script] Failed to run script: {e}")
            raise QueryError(f"Failed to run script: {e}") from e

    # ===== Table and Session Status Methods =====

    async def tables(self) -> list[str]:
        """
        Asynchronously retrieve the names of all global tables available on the server.
        
        This method returns a list of table names that are currently bound in the Deephaven
        global namespace. These tables could have been created by any session connected to 
        the same server. The method is useful for discovering available data sources and 
        for programmatically working with tables that may have been created by other users 
        or processes.
        
        Use cases include:
        - Checking if a specific table exists before attempting to use it
        - Listing all available tables in a UI or dashboard
        - Data discovery and exploration
        - Building dynamic workflows that process all available tables
        - Cleaning up tables by checking what exists before removing

        Returns:
            list[str]: List of table names currently registered in the global namespace.
                     The list may be empty if no tables are currently bound.

        Raises:
            DeephavenConnectionError: If a network or connection error occurs while attempting
                                    to communicate with the server.
            QueryError: If the operation fails due to a query-related error, such as
                      permission issues or server-side errors.

        Example - Basic usage:
            ```python
            # Get all table names
            table_names = await session.tables()
            print(f"Available tables: {table_names}")
            ```
            
        Example - Check for specific table:
            ```python
            # Check if a specific table exists
            table_names = await session.tables()
            if "daily_prices" in table_names:
                # Table exists, we can use it
                prices_table = await session.get_table("daily_prices")
            else:
                # Table doesn't exist, create it
                # ... code to create the table ...
                await session.bind_table("daily_prices", new_table)
            ```
            
        Example - Process all available tables:
            ```python
            # Apply an operation to all tables matching a pattern
            table_names = await session.tables()
            for name in table_names:
                if name.startswith("stock_"):
                    # Get the table
                    table = await session.get_table(name)
                    # Process it
                    processed = await (await session.query(table))\
                        .update_view(["ProcessedTime = now()"])\
                        .to_table()
                    # Save the result
                    await session.bind_table(f"{name}_processed", processed)
            ```
        """
        _LOGGER.debug(f"[CoreSession:tables] Called")
        try:
            return await asyncio.to_thread(self.wrapped.tables)
        except ConnectionError as e:
            _LOGGER.error(f"[CoreSession:tables] Connection error listing tables: {e}")
            raise DeephavenConnectionError(
                f"Connection error listing tables: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:tables] Failed to list tables: {e}")
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
        _LOGGER.debug(f"[CoreSession:is_alive] Called")
        try:
            return await asyncio.to_thread(lambda: self.wrapped.is_alive)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CoreSession:is_alive] Connection error checking session status: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error checking session status: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"[CoreSession:is_alive] Failed to check session status: {e}")
            raise SessionError(f"Failed to check session status: {e}") from e


class CoreSession(BaseSession[Session]):
    """
    An asynchronous wrapper around the standard Deephaven Session class.

    CoreSession provides a fully asynchronous interface for interacting with standard Deephaven servers.
    It delegates all blocking operations to background threads using asyncio.to_thread, ensuring that
    the event loop is never blocked. This class is suitable for use in asyncio-based applications
    and provides methods for table creation, querying, and manipulation.

    This class is intended for standard (non-enterprise) Deephaven sessions. For enterprise-specific
    features, use CorePlusSession.

    Example:
        ```python
        import asyncio
        from deephaven_mcp.sessions import CommunitySessionManager

        async def main():
            manager = CommunitySessionManager("localhost", 10000)
            session = await manager.get_session()
            table = await session.time_table("PT1S")
            result = await (await session.query(table))\
                .update_view(["Value = i % 10"]).to_table()
            print(await result.to_string())

        asyncio.run(main())
        ```

    See Also:
        - BaseSession: Parent class for all Deephaven session types
        - CoreSessionManager: For creating and managing standard sessions
        - CorePlusSession: For enterprise-specific session features
    """

    @override
    def __init__(self, session: Session, programming_language: str):
        """
        Initialize with an underlying Session instance.

        Args:
            session: A pydeephaven Session instance that will be wrapped by this class.
            programming_language: The programming language associated with this session (e.g., "python", "groovy").
        """
        super().__init__(
            session, is_enterprise=False, programming_language=programming_language
        )

    @classmethod
    async def from_config(cls, worker_cfg: dict[str, Any]) -> "CoreSession":
        """
        Asynchronously create a CoreSession from a community (core) session configuration dictionary.

        This method first validates the configuration using validate_single_community_session_config.
        It then prepares all session parameters (including TLS and auth logic),
        creates the underlying pydeephaven.Session, and returns a CoreSession instance.
        Sensitive fields in the config are redacted before logging. If session creation fails,
        a SessionCreationError is raised with details.

        Args:
            worker_cfg (dict): The worker's community session configuration.

        Returns:
            CoreSession: A new CoreSession instance wrapping a pydeephaven Session.

        Raises:
            CommunitySessionConfigurationError: If the configuration is invalid.
            SessionCreationError: If session creation fails for any reason.
        """
        try:
            validate_single_community_session_config("from_config", worker_cfg)
        except CommunitySessionConfigurationError as e:
            _LOGGER.error(
                f"[CoreSession:from_config] Invalid community session config: {e}"
            )
            raise

        def redact(cfg: dict[str, Any]) -> dict[str, Any]:
            return (
                redact_community_session_config(cfg)
                if "auth_token" in cfg or "client_private_key" in cfg
                else cfg
            )

        # Prepare session parameters
        log_cfg = redact(worker_cfg)
        _LOGGER.info(
            f"[CoreSession:from_config] Community session configuration: {log_cfg}"
        )
        host = worker_cfg.get("host", None)
        port = worker_cfg.get("port", None)
        auth_type = worker_cfg.get("auth_type", "Anonymous")
        auth_token = worker_cfg.get("auth_token")
        auth_token_env_var = worker_cfg.get("auth_token_env_var")
        if auth_token_env_var:
            _LOGGER.info(
                f"[CoreSession:from_config] Attempting to read auth token from environment variable: {auth_token_env_var}"
            )
            token_from_env = os.getenv(auth_token_env_var)
            if token_from_env is not None:
                auth_token = token_from_env
                _LOGGER.info(
                    f"[CoreSession:from_config] Successfully read auth token from environment variable {auth_token_env_var}."
                )
            else:
                auth_token = ""
                _LOGGER.warning(
                    f"[CoreSession:from_config] Environment variable {auth_token_env_var} specified for auth_token but not found. Using empty token."
                )
        elif auth_token is None:
            auth_token = ""
        never_timeout = worker_cfg.get("never_timeout", False)
        session_type = worker_cfg.get("session_type", "python")
        programming_language = session_type
        use_tls = worker_cfg.get("use_tls", False)
        tls_root_certs = worker_cfg.get("tls_root_certs", None)
        client_cert_chain = worker_cfg.get("client_cert_chain", None)
        client_private_key = worker_cfg.get("client_private_key", None)
        if tls_root_certs:
            _LOGGER.info(
                f"[CoreSession:from_config] Loading TLS root certs from: {worker_cfg.get('tls_root_certs')}"
            )
            tls_root_certs = await load_bytes(tls_root_certs)
            _LOGGER.info(
                f"[CoreSession:from_config] Loaded TLS root certs successfully."
            )
        else:
            _LOGGER.debug(
                f"[CoreSession:from_config] No TLS root certs provided for community session."
            )
        if client_cert_chain:
            _LOGGER.info(
                f"[CoreSession:from_config] Loading client cert chain from: {worker_cfg.get('client_cert_chain')}"
            )
            client_cert_chain = await load_bytes(client_cert_chain)
            _LOGGER.info(
                f"[CoreSession:from_config] Loaded client cert chain successfully."
            )
        else:
            _LOGGER.debug(
                f"[CoreSession:from_config] No client cert chain provided for community session."
            )
        if client_private_key:
            _LOGGER.info(
                f"[CoreSession:from_config] Loading client private key from: {worker_cfg.get('client_private_key')}"
            )
            client_private_key = await load_bytes(client_private_key)
            _LOGGER.info(
                f"[CoreSession:from_config] Loaded client private key successfully."
            )
        else:
            _LOGGER.debug(
                f"[CoreSession:from_config] No client private key provided for community session."
            )
        session_config = {
            "host": host,
            "port": port,
            "auth_type": auth_type,
            "auth_token": auth_token,
            "never_timeout": never_timeout,
            "session_type": session_type,
            "use_tls": use_tls,
            "tls_root_certs": tls_root_certs,
            "client_cert_chain": client_cert_chain,
            "client_private_key": client_private_key,
        }
        log_cfg = redact(session_config)
        _LOGGER.info(
            f"[CoreSession:from_config] Prepared Deephaven Community (Core) Session config: {log_cfg}"
        )
        try:
            _LOGGER.info(
                f"[CoreSession:from_config] Creating new Deephaven Community (Core) Session with config: {log_cfg}"
            )
            session = await asyncio.to_thread(Session, **session_config)
            _LOGGER.info(
                f"[CoreSession:from_config] Successfully created Deephaven Community (Core) Session: {session}"
            )
            return cls(session, programming_language=programming_language)
        except Exception as e:
            _LOGGER.warning(
                f"[CoreSession:from_config] Failed to create Deephaven Community (Core) Session with config: {log_cfg}: {e}"
            )
            cls._log_session_creation_error_details(e)
            raise SessionCreationError(
                f"Failed to create Deephaven Community (Core) Session with config: {log_cfg}: {e}"
            ) from e

    @classmethod
    def _log_session_creation_error_details(cls, exception: Exception) -> None:
        """Log documented guidance for specific known session creation errors.

        Analyzes the exception message and provides targeted troubleshooting guidance
        based on documented Deephaven error patterns. This method can be extended
        to handle additional error cases as they are identified and documented.

        Args:
            exception: The exception that occurred during session creation
        """
        error_msg = str(exception).lower()

        # Handle "failed to get the configuration constants" - documented connection issue
        if "failed to get the configuration constants" in error_msg:
            _LOGGER.error(
                f"[CoreSession:from_config] This error indicates a connection issue when trying to connect to the server."
            )
            _LOGGER.error(
                f"[CoreSession:from_config] Verify that: 1) Server address and port are correct, 2) Deephaven server is running and accessible, 3) Network connectivity is available"
            )

        # Handle certificate/TLS related errors
        elif any(
            pattern in error_msg
            for pattern in [
                "certificate",
                "ssl",
                "tls",
                "handshake",
                "pkix path building failed",
                "cert_authority_invalid",
                "cert_common_name_invalid",
            ]
        ):
            _LOGGER.error(
                f"[CoreSession:from_config] This error indicates a TLS/SSL certificate issue."
            )
            _LOGGER.error(
                f"[CoreSession:from_config] Verify that: 1) Server certificate is valid and not expired, 2) Certificate hostname matches connection URL, 3) CA certificate is trusted by the client"
            )

        # Handle authentication errors
        elif any(
            pattern in error_msg
            for pattern in [
                "authentication failed",
                "unauthorized",
                "invalid credentials",
                "invalid token",
                "token expired",
                "access denied",
            ]
        ):
            _LOGGER.error(
                f"[CoreSession:from_config] This error indicates an authentication issue."
            )
            _LOGGER.error(
                f"[CoreSession:from_config] Verify that: 1) Authentication credentials are correct, 2) Token is valid and not expired, 3) User has proper permissions, 4) Authentication service is running"
            )

        # Handle connection timeout errors
        elif any(
            pattern in error_msg
            for pattern in [
                "timeout",
                "connection refused",
                "connection reset",
                "network unreachable",
            ]
        ):
            _LOGGER.error(
                f"[CoreSession:from_config] This error indicates a network connectivity issue."
            )
            _LOGGER.error(
                f"[CoreSession:from_config] Verify that: 1) Server is running and accessible, 2) Network connectivity is available, 3) Firewall is not blocking the connection, 4) Port is correct and open"
            )

        # Handle port/address binding errors
        elif any(
            pattern in error_msg
            for pattern in [
                "address already in use",
                "bind failed",
                "port already in use",
            ]
        ):
            _LOGGER.error(
                f"[CoreSession:from_config] This error indicates a port binding issue."
            )
            _LOGGER.error(
                f"[CoreSession:from_config] Verify that: 1) Port is not already in use by another process, 2) You have permission to bind to the port, 3) Try a different port number"
            )

        # Handle DNS resolution errors
        elif any(
            pattern in error_msg
            for pattern in [
                "name resolution failed",
                "host not found",
                "nodename nor servname provided",
            ]
        ):
            _LOGGER.error(
                f"[CoreSession:from_config] This error indicates a DNS resolution issue."
            )
            _LOGGER.error(
                f"[CoreSession:from_config] Verify that: 1) Hostname is correct and resolvable, 2) DNS server is accessible, 3) Network connectivity is available, 4) Try using IP address instead of hostname"
            )


class CorePlusSession(
    BaseSession["deephaven_enterprise.client.session_manager.DndSession"]
):
    """A wrapper around the enterprise DndSession class.

    This class provides a standardized interface while delegating to the
    underlying enterprise session implementation.

    This class provides access to enterprise-specific functionality like persistent queries,
    historical data access, and catalog operations while maintaining compatibility with
    the standard Session interface. CorePlusSession inherits core functionality from BaseSession
    and adds enterprise-specific methods.

    Architecture:
    - CorePlusSession wraps an enterprise DndSession instance
    - All operations run in background threads via asyncio.to_thread to prevent blocking
    - Method calls are delegated to the wrapped session with proper error translation
    - All operations return properly typed objects with rich interfaces

    Key enterprise-specific features include:
    - Persistent query information (pqinfo): Access details about long-running queries
    - Historical data access (historical_table): Retrieve point-in-time snapshots from the database
    - Live data access (live_table): Connect to continuously updating data sources
    - Catalog operations (catalog_table): Discover available tables and their metadata

    Example:
        ```python
        import asyncio
        from deephaven_mcp.client import CorePlusSessionFactory

        async def work_with_enterprise_session():
            # Create a session factory and authenticate
            factory = await CorePlusSessionFactory.from_config({"url": "https://myserver.example.com/iris/connection.json"})
            await factory.password("username", "password")

            # Connect to a worker to get a CorePlusSession
            session = await factory.connect_to_new_worker()

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
        - BaseSession: Parent class for all Deephaven session types
        - CorePlusSessionFactory: For creating and managing enterprise sessions
    """

    @override
    def __init__(
        self,
        session: "deephaven_enterprise.client.session_manager.DndSession",  # noqa: F821
        programming_language: str,
    ):
        """
        Initialize with an underlying DndSession instance.

        DndSession is the enterprise-specific session class in Deephaven Enterprise that extends
        the standard Session class with additional enterprise capabilities like persistent queries,
        historical tables, and catalog operations. This class wraps DndSession to provide an
        asynchronous API while maintaining enterprise functionality.

        Architecture and Design:
        - This constructor creates an asynchronous wrapper around a synchronous DndSession
        - The wrapped session object is stored as self._session (via BaseSession)
        - All method calls are delegated to the wrapped session using asyncio.to_thread
        - Exceptions from the underlying session are properly translated to async-compatible exceptions
        - The class implements __del__ to ensure proper cleanup when garbage collected

        Lifecycle Management:
        - CorePlusSession objects are typically short-lived and should be closed when no longer needed
        - The session maintains a connection to server resources that should be released
        - Multiple CorePlusSession instances can connect to the same persistent query

        Args:
            session: A DndSession instance from deephaven_enterprise.client.session_manager
                     that will be wrapped by this class. This must be a valid, already initialized
                     DndSession object with proper enterprise capabilities.

        Raises:
            InternalError: If the provided session is not a DndSession instance or doesn't
                         support the required enterprise functionality.
            ValueError: If the session parameter is None or invalid.

        Note:
            - This class is typically not instantiated directly by users but rather obtained
              through a CorePlusSessionFactory's connect_to_new_worker or connect_to_persistent_query
              methods.
            - The wrapped DndSession maintains its own connection to the server and enterprise
              resources like persistent queries and historical tables.
            - Closing a CorePlusSession does not necessarily terminate the underlying persistent query,
              which can continue running on the server.
            - The session is automatically marked as an enterprise session (is_enterprise=True)
              which enables specialized handling of enterprise-specific methods and objects.

        Thread Safety:
            As with CoreSession, methods of this class are not thread-safe and should only be called
            from a single thread. Each method should be awaited before calling another method on the
            same session.
        """
        super().__init__(
            session, is_enterprise=True, programming_language=programming_language
        )

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

        Key attributes available in the returned CorePlusQueryInfo include:
        - id: Unique identifier for the persistent query
        - name: Human-readable name of the query
        - status: Current execution status (e.g., RUNNING, COMPLETED, FAILED)
        - created_time: Timestamp when the query was created
        - system_name: Name of the system where the query is running
        - source: Source information for the query
        - user_id: ID of the user who created the query
        - tags: Any tags associated with the query

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

            # Basic attributes
            print(f"Query ID: {query_info.id}")
            print(f"Query name: {query_info.name}")
            print(f"Query status: {query_info.status}")
            print(f"Query creation time: {query_info.created_time}")

            # Check query state
            if query_info.is_running():
                print("Query is currently running")
            elif query_info.is_completed():
                print("Query has completed")
            elif query_info.is_failed():
                print(f"Query failed with message: {query_info.status_message}")

            # Use query info in application logic
            if query_info.is_running() and query_info.system_name == "production":
                print(f"Production query {query_info.name} has been running since {query_info.created_time}")
            ```
        """
        _LOGGER.debug(f"[CorePlusSession:pqinfo] Called")
        try:
            protobuf_obj = await asyncio.to_thread(self.wrapped.pqinfo)
            return CorePlusQueryInfo(protobuf_obj)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSession:pqinfo] Connection error retrieving persistent query information: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error retrieving persistent query information: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSession:pqinfo] Failed to retrieve persistent query information: {e}"
            )
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
        or applications. Common namespaces include:
        
        - market_data: Financial market data such as prices, quotes, and trades
        - metrics: System or application performance metrics
        - analytics: Pre-computed analytical results
        - reference: Reference data like securities information or customer records
        
        When a historical table is loaded, it represents an immutable snapshot of data as it
        existed at the time of storage. This is in contrast to live tables, which are continuously
        updated with new data.
        
        Args:
            namespace: The namespace of the table, which helps organize tables into logical groups
                     or domains (e.g., 'market_data', 'user_analytics', etc.). This parameter is
                     case-sensitive.
            table_name: The name of the table within the specified namespace. This parameter is
                      also case-sensitive.
            
        Returns:
            Table: A Table object representing the requested historical table. This table
                 is immutable and represents data as it existed at the time of storage.
                 The table has all the standard Table methods for querying, filtering,
                 and transforming data.
            
        Raises:
            DeephavenConnectionError: If there is a network or connection error when attempting
                                    to communicate with the server
            ResourceError: If the table cannot be found in the specified namespace or if the
                         namespace itself does not exist
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
                
            # Compare multiple historical datasets
            earnings = await session.historical_table("reference", "quarterly_earnings")
            combined = await session.query(filtered_data).natural_join(earnings, on=["Symbol"]).to_table()
            
            # Export results
            pa_table = await combined.to_arrow()
            ```
            
        Note:
            - Historical tables are read-only and cannot be modified
            - For time-series data, the columns typically include a timestamp
            - Historical tables can be joined with other tables (both historical and live)
        """
        _LOGGER.debug(
            f"[CorePlusSession:historical_table] Called with namespace={namespace}, table_name={table_name}"
        )
        try:
            return await asyncio.to_thread(
                self.wrapped.historical_table, namespace, table_name
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSession:historical_table] Connection error fetching historical table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error fetching historical table: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(
                f"[CorePlusSession:historical_table] Historical table not found: {e}"
            )
            raise ResourceError(
                f"Historical table not found: {namespace}.{table_name}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSession:historical_table] Failed to fetch historical table: {e}"
            )
            raise QueryError(f"Failed to fetch historical table: {e}") from e

    async def live_table(self, namespace: str, table_name: str) -> Table:
        """
        Asynchronously fetches a live table from the database on the server.
        
        Live tables in Deephaven are dynamic tables that update in real-time as new data arrives.
        Unlike historical tables which represent point-in-time snapshots, live tables provide:
        
        - Real-time updates as new data becomes available
        - Continuous processing of incoming data
        - Dynamic views that reflect the latest state of the data
        - Automated propagation of updates to derived tables
        
        Live tables are particularly useful for monitoring current market conditions,
        tracking real-time metrics, or implementing active trading strategies. They
        maintain a connection to the data source and automatically update when new
        data arrives.
        
        Common use cases for live tables include:
        - Market data feeds for trading applications
        - Real-time analytics dashboards
        - System monitoring and alerting
        - Event-driven applications and stream processing
        
        The relationship between live and historical tables:
        - A historical table is a snapshot of data at a specific point in time
        - A live table is continuously updated with new data
        - The same table can often be accessed in both live and historical modes
        - Live tables can be converted to historical tables using snapshot operations
        
        Args:
            namespace: The namespace of the table, which helps organize tables into logical
                     groups or domains (e.g., 'market_data', 'system_metrics'). This parameter
                     is case-sensitive.
            table_name: The name of the table within the specified namespace. This parameter is
                      also case-sensitive.
            
        Returns:
            Table: A Table object representing the requested live table. This table
                 will automatically update as new data arrives at the server. All operations
                 performed on this table (filters, joins, etc.) will also automatically
                 update to reflect the latest state of the data.
            
        Raises:
            DeephavenConnectionError: If there is a network or connection error when
                                    attempting to communicate with the server
            ResourceError: If the table cannot be found in the specified namespace or if the
                         namespace itself does not exist
            QueryError: If the table exists but cannot be accessed due to a
                      query-related error such as permission issues or data corruption
                      
        Example:
            ```python
            # Retrieve a live market data table that updates with new trades
            live_trades = await session.live_table("market_data", "trade_feed")
            
            # Create a derived table that updates automatically when live_trades updates
            filtered_trades = await (await session.query(live_trades))\
                .where("Price > 100.0")\
                .to_table()
                
            # The filtered_trades table will continue to update as new trades arrive
            
            # Create a joined view that combines live and historical data
            historical_reference = await session.historical_table("reference", "securities")
            enriched_trades = await (await session.query(filtered_trades))\
                .join(historical_reference, on=["Symbol"], joins=["SecurityName", "SecurityType"])\
                .to_table()
                
            # Bind the enriched table for access by other users
            await session.bind_table("enriched_live_trades", enriched_trades)
            ```
            
        Note:
            - All derived tables (via query operations) inherit the live update behavior
            - Performance considerations: complex operations on frequently updating live tables
              can consume significant resources
            - For extremely high-frequency data, consider using aggregations or time-based
              sampling to reduce update frequency
        """
        _LOGGER.debug(
            f"[CorePlusSession:live_table] Called with namespace={namespace}, table_name={table_name}"
        )
        try:
            return await asyncio.to_thread(
                self.wrapped.live_table, namespace, table_name
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSession:live_table] Connection error fetching live table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error fetching live table: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(f"[CorePlusSession:live_table] Live table not found: {e}")
            raise ResourceError(
                f"Live table not found: {namespace}.{table_name}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSession:live_table] Failed to fetch live table: {e}"
            )
            raise QueryError(f"Failed to fetch live table: {e}") from e

    async def catalog_table(self) -> Table:
        """
        Asynchronously retrieves the catalog table, which contains metadata about available tables.

        The catalog table in Deephaven provides a comprehensive overview of all tables accessible
        to the current session. This includes both historical and live tables across all namespaces
        the user has permission to access. Each row in the catalog table represents a table in the
        system, with columns describing its properties.

        Common columns in the catalog table include:
        - TableName: The name of the table
        - Namespace: The namespace the table belongs to
        - Type: The table type (e.g., 'HISTORICAL', 'LIVE', 'TEMP')
        - Schema: Information about the table's column structure
        - CreateTime: When the table was created
        - UpdateTime: When the table was last updated
        - Description: Optional description of the table's contents
        - Owner: User or system that owns the table
        - AccessControl: Information about who can access the table

        This method allows users to explore available data without needing to know specific table
        names in advance. It's particularly useful for data discovery and exploration in large or
        unfamiliar Deephaven deployments.

        Returns:
            Table: A Table object representing the catalog of available tables in the system.
                Each row represents a table, with columns describing the table's properties
                such as name, namespace, schema, and type. The catalog itself is a live table
                that updates automatically when new tables are created or existing tables are
                removed.

        Raises:
            DeephavenConnectionError: If there is a network or connection error when
                attempting to communicate with the server
            QueryError: If the catalog table cannot be retrieved due to a server-side error
            SessionError: If the session has been closed or is otherwise invalid

        Example:
            ```python
            # Get the catalog table
            catalog = await session.catalog_table()

            # Find all tables in the 'market_data' namespace
            market_tables = await (await session.query(catalog))\
                .where("Namespace == 'market_data'")\
                .to_table()

            # Print the names of all available tables
            table_names = await (await session.query(catalog)).select("TableName").to_table()
            for name in await table_names.to_list():
                print(name)

            # Find all tables that contain price data
            price_tables = await (await session.query(catalog))\
                .where("TableName.contains('price') || Description.contains('price')")\
                .to_table()

            # Get metadata for a specific table
            my_table_info = await (await session.query(catalog))\
                .where(f"Namespace == 'analytics' && TableName == 'portfolio_metrics'")\
                .to_table()

            # Check if the catalog contains a specific table
            has_table = await (await session.query(catalog))\
                .where(f"Namespace == 'market_data' && TableName == 'daily_prices'")\
                .count() > 0
            ```

        Note:
            - The catalog table is a live table that updates as tables are created and deleted
            - Column names and availability may vary slightly between Deephaven versions
            - Performance: For large deployments with many tables, consider using more specific
              filters when querying the catalog table
        """
        _LOGGER.debug(f"[CorePlusSession:catalog_table] Called")
        try:
            return await asyncio.to_thread(self.wrapped.catalog_table)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSession:catalog_table] Connection error fetching catalog table: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error fetching catalog table: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSession:catalog_table] Failed to fetch catalog table: {e}"
            )
            raise QueryError(f"Failed to fetch catalog table: {e}") from e
