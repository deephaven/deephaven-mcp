"""Deephaven Core+ Session Manager Wrapper for asynchronous interaction with Deephaven Core+.

This module provides an asynchronous wrapper around the Deephaven Core+ SessionManager
(deephaven_enterprise.client.session_manager.SessionManager) that enhances functionality
while maintaining strict interface compatibility. The wrapper adds comprehensive
documentation, robust logging, and ensures non-blocking operation by running potentially
blocking operations in separate threads.

The CorePlusSessionFactory delegates all method calls to the underlying session manager
instance and wraps returned sessions in CorePlusSession objects for consistent behavior.
It provides methods for:
  - Authentication (password, private_key, saml)
  - Worker management (connect_to_new_worker, connect_to_persistent_query)
  - Connection verification (ping)
  - Key management (upload_key, delete_key)

Use this class when you need to interact with Deephaven Core+ servers in an asynchronous
context, such as within asyncio-based applications or when non-blocking operations are
required. The class is thread-safe for concurrent access across different methods, but
care should be taken when accessing the same method concurrently.

Example:
    import asyncio
    from deephaven_mcp.client import CorePlusSessionFactory

    # Create a Core+ session factory connected to a server
    async def main():
        # Create a session factory using the from_url classmethod
        manager = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")

        # Authenticate
        await manager.password("username", "password")

        # Connect to a new worker
        session = await manager.connect_to_new_worker()

        # Use the session
        table = session.empty_table()

        # Close the manager when done
        await manager.close()

    asyncio.run(main())

You can also directly instantiate the class with an existing SessionManager:

    from deephaven_enterprise.client.session_manager import SessionManager
    from deephaven_mcp.client import CorePlusSessionFactory

    # Create and wrap an existing session manager
    session_manager = SessionManager("https://myserver.example.com/iris/connection.json")
    wrapped_manager = CorePlusSessionFactory(session_manager)

Note:
    All methods returning a connection or session are asynchronous and return awaitable
    coroutines. Remember to await these methods when using them.
"""

# Standard library imports
import asyncio
import io
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import pydeephaven

if TYPE_CHECKING:
    import deephaven_enterprise.client.session_manager  # pragma: no cover

from deephaven_mcp._exceptions import (
    AuthenticationError,
    DeephavenConnectionError,
    MissingEnterprisePackageError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)
from deephaven_mcp.config import (
    EnterpriseSystemConfigurationError,
    validate_single_enterprise_system,
)

from ._auth_client import CorePlusAuthClient

# Local application imports
from ._base import ClientObjectWrapper, is_enterprise_available
from ._controller_client import CorePlusControllerClient, CorePlusQuerySerial
from ._session import CorePlusSession

# Define the logger for this module
_LOGGER = logging.getLogger(__name__)


class CorePlusSessionFactory(
    ClientObjectWrapper["deephaven_enterprise.client.session_manager.SessionManager"]
):
    """Asynchronous wrapper for the Deephaven Core+ SessionManager providing non-blocking operations.

    This class wraps an existing Deephaven Core+ session manager instance, delegating all
    method calls to the underlying instance while providing enhanced documentation, robust error
    handling, and comprehensive logging. The wrapper runs potentially blocking operations in
    separate threads using asyncio.to_thread to ensure non-blocking behavior in asynchronous
    contexts.

    CorePlusSessionFactory serves as a primary entry point for interacting with Deephaven Core+
    servers in asynchronous applications. It manages the lifecycle of server connections and
    provides methods for authentication, worker creation, and session establishment.

    Key features:
    - Full asyncio compatibility with all operations returning awaitable coroutines
    - Comprehensive error handling with specific exception types for different error scenarios
    - Detailed logging of all operations for debugging and monitoring
    - Automatic wrapping of returned sessions in CorePlusSession objects for consistent behavior

    The wrapper preserves the same interface as the original SessionManager class
    but with async methods, making it suitable as a drop-in replacement in asynchronous code.
    All returned sessions are wrapped in CorePlusSession objects for consistent behavior.

    Typical usage flow:

    1. Create a session manager using the from_url classmethod
    2. Authenticate using one of the authentication methods (password, private_key, or saml)
    3. Connect to an existing worker or create a new one
    4. Work with tables and data through the session
    5. Close the session manager when done

    Key operations provided by this class include:
    - Authentication and token management
    - Creating and connecting to persistent queries (workers)
    - Managing public/private key pairs
    - Interacting with the controller to manage server resources

    All methods that might block are implemented as async methods that use asyncio.to_thread
    to prevent blocking the event loop, making this class safe to use in async applications.
    """

    def __init__(
        self,
        session_manager: "deephaven_enterprise.client.session_manager.SessionManager",  # noqa: F821
    ):
        """Initialize the CorePlusSessionFactory wrapper with an existing SessionManager.

        This constructor creates a new CorePlusSessionFactory that wraps an existing SessionManager
        object, providing an asynchronous interface to its methods while preserving all functionality.
        This class serves as the primary entry point for interacting with Deephaven Enterprise servers,
        handling authentication, worker management, and session establishment.

        The constructor automatically initializes both the controller_client and auth_client properties
        by accessing the corresponding properties from the wrapped session manager.

        The CorePlusSessionFactory is designed to provide a convenient async/await interface around
        the synchronous Enterprise SessionManager, running operations in separate threads to avoid
        blocking the event loop. This makes it ideal for integration into async applications, web
        servers, or any environment where non-blocking operations are important.

        Args:
            session_manager: The SessionManager instance to wrap. Must be an instance
                           of deephaven_enterprise.client.session_manager.SessionManager.
                           This should be a properly initialized session manager with valid
                           connection information, though it does not need to be authenticated
                           yet (authentication can be performed through this wrapper's methods).

        Raises:
            TypeError: If the provided session_manager is not an instance of the expected
                       SessionManager class from deephaven_enterprise.client.session_manager.
            ValueError: If the session_manager is not properly initialized or is missing
                       required configuration.
            SessionError: If there was an error initializing the controller_client property.
            AuthenticationError: If there was an error initializing the auth_client property.

        Note:
            In most cases, you should use the class factory methods instead of this constructor:
            - Use from_url() when you have a connection URL to the Deephaven server
            - Use from_config() when you have a configuration dictionary
            - Use from_json() when you have a JSON configuration file

            These factory methods handle initialization details, dependency management, and
            error handling for you in a more convenient way than direct instantiation.

        Example:
            ```python
            # Direct instantiation (not typically recommended)
            from deephaven_enterprise.client import session_manager
            from deephaven_mcp.client import CorePlusSessionFactory

            # Create the underlying session manager
            sm = session_manager.SessionManager.from_url("https://example.com/iris/connection.json")

            # Wrap it in the async factory
            factory = CorePlusSessionFactory(sm)
            ```

        """
        super().__init__(session_manager, is_enterprise=True)
        _LOGGER.info(
            "[CorePlusSessionFactory:__init__] Successfully initialized CorePlusSessionFactory"
        )

        # Initialize controller client in constructor
        try:
            controller_client = self.wrapped.controller_client
            self._controller_client = CorePlusControllerClient(controller_client)
            _LOGGER.debug(
                "[CorePlusSessionFactory:__init__] Successfully initialized controller client"
            )
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:__init__] Failed to initialize controller client: {e}"
            )
            raise SessionError(f"Failed to initialize controller client: {e}") from e

        # Initialize auth client in constructor
        try:
            auth_client = self.wrapped.auth_client
            self._auth_client = CorePlusAuthClient(auth_client)
            _LOGGER.debug(
                "[CorePlusSessionFactory:__init__] Successfully initialized auth client"
            )
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:__init__] Failed to initialize auth client: {e}"
            )
            raise AuthenticationError(
                f"Failed to initialize authentication client: {e}"
            ) from e

    @classmethod
    def from_url(cls, url: str) -> "CorePlusSessionFactory":
        """Create a CorePlusSessionFactory connected to a Deephaven server specified by URL.

        This is the recommended and most convenient way to create a CorePlusSessionFactory when you
        have a URL to a Deephaven server. The method handles all the necessary setup:

        1. Validates and parses the provided URL
        2. Establishes initial connection information to the Deephaven server
        3. Creates and configures the underlying SessionManager
        4. Wraps the SessionManager in a CorePlusSessionFactory for asynchronous access

        The method automatically loads the required enterprise dependencies and handles
        initialization details and error conditions, making it much simpler than manually
        creating the session manager.

        Args:
            url: The connection URL for the Deephaven server. This should point to a
                connection.json file that describes how to connect to the server,
                typically in the format "https://<server>/iris/connection.json".
                Both HTTP and HTTPS protocols are supported, though HTTPS is strongly
                recommended for production environments. The connection.json file contains
                server endpoints and configuration details necessary for establishing
                connections to various services.

        Returns:
            CorePlusSessionFactory: A new, initialized factory instance connected to the specified
                server. Note that the returned factory is not yet authenticated; you must call one
                of the authentication methods (password(), private_key(), or saml()) before using
                other methods like connect_to_new_worker().

        Raises:
            ModuleNotFoundError: If the required enterprise dependencies are not installed.
            ValueError: If the URL is malformed or cannot be parsed.
            ConnectionError: If the connection.json file cannot be accessed or downloaded.
            JsonDecodeError: If the connection.json file contains invalid JSON.
            ConfigurationError: If the connection configuration is invalid or incomplete.

        Example:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionFactory

            async def connect_to_server():
                # Create the factory pointing to a Deephaven server
                factory = CorePlusSessionFactory.from_url(
                    "https://example.deephaven.io/iris/connection.json"
                )

                # Authenticate (required before using other methods)
                await factory.password("username", "password")

                # Now you can create sessions, etc.
                session = await factory.connect_to_new_worker()
                return session
            ```

        Note:
            If you need to connect through a proxy, configure your system's proxy settings
            before calling this method, as it uses the system's default HTTP client
            configuration for the initial connection.json download.
        """
        if not is_enterprise_available:
            raise MissingEnterprisePackageError()
        else:
            from deephaven_enterprise.client.session_manager import SessionManager

            try:
                _LOGGER.debug(
                    f"[CorePlusSessionFactory:from_url] Creating SessionManager for URL: {url}"
                )
                return cls(SessionManager(url))
            except Exception as e:
                _LOGGER.error(
                    f"[CorePlusSessionFactory:from_url] Failed to create SessionManager with URL {url}: {e}"
                )
                raise DeephavenConnectionError(
                    f"Failed to establish connection to Deephaven at {url}: {e}"
                ) from e

    @classmethod
    async def from_config(cls, worker_cfg: dict[str, Any]) -> "CorePlusSessionFactory":
        """
        Create and authenticate a CorePlusSessionFactory from a configuration dictionary.

        This factory method provides a complete solution for creating and initializing a
        CorePlusSessionFactory from a configuration dictionary. It handles the entire process:

        1. Validates the configuration format and required fields
        2. Creates a connection to the specified Deephaven server
        3. Automatically authenticates using the provided credentials
        4. Returns a fully ready-to-use factory instance

        This is the recommended approach when working with configuration files, environment-based
        setups, or any scenario where connection details and authentication information are stored
        in a structured format rather than hardcoded in the application.

        Configuration Format:
        The configuration dictionary must follow the standard enterprise system format with these
        required fields:

        - 'connection_json_url': URL to the Deephaven server's connection.json file
        - 'auth_type': Authentication method to use ('password', 'private_key', or 'saml')

        Additional fields based on auth_type:

        - For 'password' authentication:
            * 'username': The username for authentication
            * Either 'password': The actual password (not recommended for production)
              or 'password_env_var': Name of environment variable containing the password
            * Optional 'effective_user': User to operate as after authentication

        - For 'private_key' authentication:
            * 'private_key_path': The path to the private key file

        - For 'saml' authentication:
            * No additional fields required, but SAML must be configured on server

        Args:
            worker_cfg: Configuration dictionary for the enterprise system connection.
                Must contain the required fields as described above.

        Returns:
            CorePlusSessionFactory: A fully initialized and authenticated factory instance
                ready for immediate use. You can directly call methods like connect_to_new_worker()
                without needing to perform separate authentication steps.

        Raises:
            MissingEnterprisePackageError: If the required deephaven-coreplus-client
                package is not installed.
            DeephavenConnectionError: If unable to connect to the specified server URL,
                such as network issues or invalid connection.json format.
            AuthenticationError: If authentication fails due to missing or invalid credentials,
                incorrect format, or server-side authentication issues.
            EnterpriseSystemConfigurationError: If the configuration dictionary is invalid,
                missing required fields, or contains incompatible settings.
            EnvironmentError: If a password environment variable is specified but not found
                in the environment.

        Example - Password authentication with environment variable:
            ```python
            import asyncio
            import os
            from deephaven_mcp.client import CorePlusSessionFactory

            # Set password in environment (in practice, this would be set externally)
            os.environ["DH_PASSWORD"] = "my_secure_password"

            async def create_from_config():
                # Define configuration with environment variable for password
                config = {
                    "connection_json_url": "https://example.deephaven.io/iris/connection.json",
                    "auth_type": "password",
                    "username": "admin",
                    "password_env_var": "DH_PASSWORD"
                }

                # Create and authenticate in one step
                factory = await CorePlusSessionFactory.from_config(config)

                # Use the factory directly - no authentication needed
                session = await factory.connect_to_new_worker()
                return session
            ```

        Example - Private key authentication:
            ```python
            async def create_with_key():
                # Define configuration with private key path
                config = {
                    "connection_json_url": "https://example.deephaven.io/iris/connection.json",
                    "auth_type": "private_key",
                    "private_key_path": "/path/to/private_key.pem"
                }

                # Create and authenticate in one step
                factory = await CorePlusSessionFactory.from_config(config)
                return factory
            ```

        Note:
            This method performs authentication as part of initialization. If authentication
            fails, an exception will be raised and no factory will be returned. For security
            best practices, avoid storing credentials directly in the configuration and instead
            use environment variables or secure credential storage systems.
        """
        if not is_enterprise_available:
            raise MissingEnterprisePackageError()

        # Validate config
        try:
            validate_single_enterprise_system("from_config", worker_cfg)
        except EnterpriseSystemConfigurationError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:from_config] Invalid enterprise system config: {e}"
            )
            raise

        url = worker_cfg["connection_json_url"]
        auth_type = worker_cfg["auth_type"]
        _LOGGER.debug(
            f"[CorePlusSessionFactory:from_config] Creating SessionManager from config: url={url}, auth_type={auth_type}"
        )
        from deephaven_enterprise.client.session_manager import SessionManager

        try:
            manager = SessionManager(url)
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:from_config] Failed to create SessionManager with URL {url}: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to establish connection to Deephaven at {url}: {e}"
            ) from e

        instance = cls(manager)

        # Perform authentication if credentials are provided
        if auth_type == "password":
            username = worker_cfg.get("username")
            password = worker_cfg.get("password")
            password_env_var = worker_cfg.get("password_env_var")
            effective_user = worker_cfg.get("effective_user")

            # Prefer password_env_var if present
            if password is None and password_env_var is not None:
                import os

                password = os.environ.get(password_env_var)
                if password is None:
                    _LOGGER.error(
                        f"[CorePlusSessionFactory:from_config] Environment variable '{password_env_var}' not set for password authentication."
                    )
                    raise AuthenticationError(
                        f"Environment variable '{password_env_var}' not set for password authentication."
                    )
            if password is None:
                _LOGGER.error(
                    "[CorePlusSessionFactory:from_config] No password provided for password authentication."
                )
                raise AuthenticationError(
                    "No password provided for password authentication."
                )
            await instance.password(
                cast(str, username), cast(str, password), effective_user
            )
        elif auth_type == "private_key":
            private_key_path = worker_cfg.get("private_key_path")
            if private_key_path is None:
                _LOGGER.error(
                    "[CorePlusSessionFactory:from_config] No private_key_path provided for private_key authentication."
                )
                raise AuthenticationError(
                    "No private_key_path provided for private_key authentication."
                )
            await instance.private_key(private_key_path)
        else:
            _LOGGER.warning(
                f"[CorePlusSessionFactory:from_config] Auth type '{auth_type}' is not supported for automatic authentication. Returning unauthenticated manager."
            )

        _LOGGER.info(
            f"[CorePlusSessionFactory:from_config] Successfully created and authenticated SessionManager from config (auth_type={auth_type})"
        )
        return instance

    @property
    def auth_client(self) -> CorePlusAuthClient:
        """Authentication client for direct interaction with the Deephaven authentication service.

        This property provides access to the Deephaven server's authentication service API, which offers
        capabilities for user authentication, key management, and token operations. The auth client
        provides methods for working with authentication tokens, SSH keys, and other authentication
        related operations.

        Key capabilities of the auth client include:
        - Managing SSH public keys for authentication
        - Working with authentication tokens
        - User information and authentication verification

        Returns:
            CorePlusAuthClient: A client for interacting with the Deephaven authentication service.
                This client provides methods for operations such as token validation, key management,
                and other authentication-related functionality.

        Raises:
            AuthenticationError: If accessing the auth client fails for any reason. This should
                not normally happen as the client is initialized during factory creation.
                If this occurs, it typically indicates a connection issue or authentication problem.

        Note:
            The auth client is initialized when the CorePlusSessionFactory is created, so
            this property access is non-blocking and does not perform network operations.

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionFactory

            # Create a session factory
            factory = CorePlusSessionFactory.from_url("https://example.com/iris/connection.json")
            # Authenticate
            factory.password("username", "password")

            # Access the auth client property
            auth = factory.auth_client

            # Use the auth client for direct operations
            token_info = auth.validate_token(token)
            ```
        """
        # Auth client initialization is guaranteed to succeed in the constructor
        # or an exception would have been raised
        return self._auth_client

    @property
    def controller_client(self) -> CorePlusControllerClient:
        """Controller client for direct management of server-side resources and workers.

        This property provides access to the Deephaven server's controller service API, which offers
        advanced capabilities for managing server-side resources including persistent queries (workers),
        tables, users, and system information. The controller client is particularly useful for
        administrative operations, monitoring, and programmatic management of server resources.

        Key capabilities of the controller client include:
        - Listing, filtering, and searching for persistent queries/workers across the server
        - Retrieving detailed information about any worker (memory usage, uptime, tables, etc.)
        Returns:
            CorePlusControllerClient: A client for interacting with the Deephaven controller service.
                This client provides methods for administrative operations such as listing
                workers, creating and managing persistent queries, and monitoring server resources.

        Raises:
            SessionError: If accessing the controller client fails for any reason. This should
                not normally happen as the client is initialized during factory creation.
                If this occurs, it typically indicates a connection issue or server problem.

        Note:
            The controller client is initialized when the CorePlusSessionFactory is created, so
            this property access is non-blocking and does not perform network operations.

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionFactory

            # Create a session factory
            factory = CorePlusSessionFactory.from_url("https://example.com/iris/connection.json")
            # Authenticate
            factory.password("username", "password")

            # Access the controller client property
            controller = factory.controller_client

            # List all persistent queries (workers)
            workers = controller.list_persistent_queries()
            ```
        """
        # Controller client initialization is guaranteed to succeed in the constructor
        # or an exception would have been raised
        return self._controller_client

    async def close(self) -> None:
        """Terminate this factory's connections to the authentication server and controller.

        This method properly closes all connections associated with this CorePlusSessionFactory
        instance, releasing server-side resources and cleaning up any outstanding connections. It's
        critical to call this method when you're finished with the factory to avoid resource leaks
        and ensure proper cleanup of server-side resources.

        Important behaviors:
        - All authentication tokens and credentials are invalidated
        - Network connections to the server are closed
        - Any in-progress operations may be interrupted
        - The factory becomes unusable after this call (attempting to use it will raise errors)

        This method asynchronously delegates to the underlying session manager's close method,
        running it in a separate thread to avoid blocking the event loop. This ensures that
        long-running cleanup operations don't impact the responsiveness of your async application.

        Returns:
            None: This method doesn't return a value. Upon successful completion, the factory
                 is considered closed and should no longer be used.

        Raises:
            SessionError: If terminating the connections fails for any reason, such as network
                         errors or server-side issues during the cleanup process.

        Example:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionFactory

            async def main():
                # Create and authenticate the factory
                factory = CorePlusSessionFactory.from_url("https://example.com/iris/connection.json")
                await factory.password("username", "password")

                # Use the factory...
                session = await factory.connect_to_new_worker()

                # When finished, properly close everything
                await session.close()
                await factory.close()
            ```

        Note:
            - This method does NOT automatically close any sessions created by this factory
            - Each CorePlusSession must be closed separately before closing the factory
            - After calling close(), the factory should be considered unusable
            - For proper resource cleanup, always use this method in a try/finally block or
              with async context managers to ensure it's called even if exceptions occur

        Note:
            After closing the session manager, it cannot be reused. A new instance
            must be created if further connections are needed.
        """
        try:
            _LOGGER.debug(
                "[CorePlusSessionFactory:close] Closing session manager connection"
            )
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug(
                "[CorePlusSessionFactory:close] Successfully closed session manager connection"
            )
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:close] Failed to close session manager: {e}"
            )
            raise SessionError(
                f"Failed to close session manager connections: {e}"
            ) from e

    @staticmethod
    def _get_programming_language(session: pydeephaven.Session) -> str:
        """
        Extract programming language from session object or return default.

        This helper method determines the programming language for a session by:
        1. Accessing the private _session_type attribute on the session object
        2. Falling back to 'python' as the default if _session_type is None or empty

        Note:
            Uses the private attribute _session_type as a temporary workaround.
            See https://deephaven.atlassian.net/browse/DH-19984 for tracking.

        Args:
            session: The raw session object from the enterprise system

        Returns:
            str: The programming language (e.g., "python", "groovy")
        """
        # TODO: the private attribute _session_type is a temporary workaround: See https://deephaven.atlassian.net/browse/DH-19984
        session_type = session._session_type
        # Default to python
        return session_type if session_type else "python"

    async def connect_to_new_worker(
        self: "CorePlusSessionFactory",
        name: str | None = None,
        heap_size_gb: int | None = None,
        server: str | None = None,
        extra_jvm_args: list[str] | None = None,
        extra_environment_vars: list[str] | None = None,
        engine: str = "DeephavenCommunity",
        auto_delete_timeout: int | None = 600,
        admin_groups: list[str] | None = None,
        viewer_groups: list[str] | None = None,
        timeout_seconds: float = 60,
        configuration_transformer: Callable[..., Any] | None = None,
        session_arguments: dict[str, Any] | None = None,
    ) -> CorePlusSession:
        """Create a new worker process and establish a session connection to it.

        This method creates a new Deephaven worker process (implemented as a PersistentQuery)
        and returns a fully initialized session object that can be used to execute queries,
        create tables, and perform other Deephaven operations. The worker operates as an
        isolated execution environment with its own memory space and resources.

        This method asynchronously delegates to the underlying session manager's connect_to_new_worker method,
        running it in a separate thread to avoid blocking the event loop. The returned session is wrapped
        in a CorePlusSession for consistent behavior across the API.

        Args:
            # Worker identification
            name: Optional name for the worker process. If None (default), an auto-generated name based
                on the current timestamp will be used. A descriptive name can make it easier to
                identify your worker in monitoring tools and logs.

            # Resource configuration
            heap_size_gb: JVM heap size in gigabytes (integer only, e.g., 8 for -Xmx8g). Determines the maximum amount of memory available
                to the worker process. Larger values are necessary for processing larger datasets, but
                require more system resources. If None (default), the server's default heap size is used.
            server: Specific server to run the worker on. If None (default), the server will be chosen
                automatically from available resources. Useful for targeting specific hardware configurations.
            extra_jvm_args: Additional JVM arguments to configure the worker's Java Virtual Machine.
                Examples include garbage collection settings ("-XX:+UseG1GC"), memory settings, or
                custom Java properties. If None (default), only standard JVM arguments are used.
            extra_environment_vars: Environment variables to set for the worker process.
                Format as ["NAME=value", ...]. Useful for configuring system properties, paths,
                or feature flags. If None (default), the standard environment is used.
            engine: Engine type that determines the worker's capabilities and behavior.
                Defaults to "DeephavenCommunity". Other options may include enterprise engines
                with additional features depending on your Deephaven installation.

            # Lifecycle management
            auto_delete_timeout: Number of seconds of inactivity before the worker is automatically
                terminated and cleaned up. Defaults to 600 (10 minutes). Set to None to prevent
                auto-deletion (not recommended for production use as it can lead to resource leaks).
                Set to 0 for immediate cleanup when the session disconnects.
            timeout_seconds: Maximum time in seconds to wait for the worker to start before raising
                an exception. Defaults to 60 seconds. Increase for slower environments or when
                creating workers with complex initialization processes.

            # Access controls
            admin_groups: List of user groups that have administrative permissions for this worker.
                Admins can modify, restart, or terminate the worker. If None (default), only the
                creator has admin privileges.
            viewer_groups: List of user groups that have read-only access to this worker.
                Viewers can connect to the worker and view its data but cannot modify it.
                If None (default), only the creator has viewing privileges.

            # Advanced configuration
            configuration_transformer: Optional function that takes and returns a configuration dictionary.
                This allows for advanced customization of the worker configuration beyond what the
                standard parameters provide. The function signature should be:
                `(config: dict) -> dict`. Use with caution as it may override other settings.
            session_arguments: Additional keyword arguments to pass to the pydeephaven.Session constructor.
                These parameters control session behavior rather than worker configuration.
                Common options include `disable_open_table_listener` or `chunk_size`.

        Returns:
            CorePlusSession: A fully initialized session object connected to the new worker. This session
                provides methods for creating and manipulating tables, executing queries, and
                performing other Deephaven operations. Always close this session when finished
                to properly release resources.

        Raises:
            ResourceError: If there are insufficient server resources (memory, CPU, etc.) to create
                the worker, or if resource allocation limits have been reached.
            SessionCreationError: If an error occurs during worker creation or connection establishment,
                such as invalid configuration parameters or initialization failures.
            DeephavenConnectionError: If there is a network or communication problem with the Deephaven
                server during worker creation or connection.
            AuthenticationError: If the current authentication is invalid or has insufficient permissions.
            TimeoutError: If worker creation exceeds the specified timeout_seconds.

        Example - Basic usage:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionFactory

            async def create_basic_worker():
                # Create and authenticate the session manager
                factory = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")
                await factory.password("username", "password")

                # Create a default worker and get a session
                session = await factory.connect_to_new_worker()

                # Create and manipulate tables
                table = session.table([1, 2, 3], columns=["Value"])
                result = table.update("DoubleValue = Value * 2")

                return result
            ```

        Example - Advanced configuration:
            ```python
            async def create_specialized_worker():
                # Create and authenticate the session manager
                factory = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")
                await factory.password("username", "password")

                # Create a high-memory worker with custom settings
                session = await factory.connect_to_new_worker(
                    name="analytics_worker_v2",
                    heap_size_gb=16.0,
                    auto_delete_timeout=1800,  # 30 minutes
                    extra_jvm_args=[
                        "-XX:+UseG1GC",
                        "-XX:MaxGCPauseMillis=200",
                        "-Ddeephaven.io.bufferSize=65536"
                    ],
                    viewer_groups=["data_analysts", "data_scientists"],
                    admin_groups=["data_engineers"],
                    session_arguments={"chunk_size": 100000}
                )

                return session
            ```

        Note:
            - Creating a new worker is resource-intensive. For better resource utilization, consider
              using `connect_to_persistent_query` to connect to an existing worker if one is available.
            - Multiple sessions can connect to the same worker by sharing its name/ID.
            - Worker resources are not released until all connected sessions are closed AND the
              auto_delete_timeout period has elapsed (if configured).
            - Always close sessions when finished to prevent resource leaks.
            - The worker will continue to run in the background until explicitly closed or
              terminated by the auto_delete_timeout mechanism.

        See Also:
            - connect_to_persistent_query: Connect to an existing worker by name or serial
            - controller_client: Property for accessing a client to manage workers directly
            - CorePlusSession.close: Method to disconnect from a worker and release resources
        """
        try:
            _LOGGER.debug(
                "[CorePlusSessionFactory:connect_to_new_worker] Creating new worker and connecting to it"
            )
            session = await asyncio.to_thread(
                self.wrapped.connect_to_new_worker,
                name=name,
                heap_size_gb=heap_size_gb,
                server=server,
                extra_jvm_args=extra_jvm_args,
                extra_environment_vars=extra_environment_vars,
                engine=engine,
                auto_delete_timeout=auto_delete_timeout,
                admin_groups=admin_groups,
                viewer_groups=viewer_groups,
                timeout_seconds=timeout_seconds,
                configuration_transformer=configuration_transformer,
                session_arguments=session_arguments,
            )
            _LOGGER.debug(
                "[CorePlusSessionFactory:connect_to_new_worker] Successfully connected to new worker"
            )
            programming_language = CorePlusSessionFactory._get_programming_language(
                session
            )
            return CorePlusSession(session, programming_language)
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:connect_to_new_worker] Connection error while creating new worker: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error while creating new worker: {e}"
            ) from e
        except ResourceError as e:
            # Re-raise resource exceptions unchanged
            _LOGGER.error(
                f"[CorePlusSessionFactory:connect_to_new_worker] Insufficient resources to create worker: {e}"
            )
            raise
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:connect_to_new_worker] Failed to connect to new worker: {e}"
            )
            raise SessionCreationError(
                f"Failed to create and connect to new worker: {e}"
            ) from e

    async def connect_to_persistent_query(
        self,
        name: str | None = None,
        serial: CorePlusQuerySerial | None = None,
        session_arguments: dict[str, Any] | None = None,
    ) -> CorePlusSession:
        """Connect to an existing persistent query (worker) by name or serial number.

        This method establishes a connection to a running worker process that was previously
        created, allowing you to access its tables and execute queries against it. Connecting
        to existing workers is significantly more efficient than creating new ones and enables
        multiple clients to share and collaborate on the same data and computation environment.

        Common use cases include:
        - Connecting multiple users to the same computation environment
        - Reconnecting to a worker after a client disconnect or network interruption
        - Creating monitoring or administrative tools that connect to existing workers
        - Implementing load balancing by distributing client connections across workers

        The method asynchronously delegates to the underlying session manager's connect_to_persistent_query
        method, running it in a separate thread to avoid blocking the event loop. The returned session
        is wrapped in a CorePlusSession for consistent behavior with other MCP client interfaces.

        Args:
            name: The name of the persistent query (worker) to connect to. This is the human-readable
                identifier specified when the worker was created. Either name or serial must be provided,
                but not both. Names are typically easier to use in interactive scenarios and when the
                worker was created with a custom name.
            serial: The unique serial number of the persistent query to connect to. Serial numbers are
                system-assigned unique identifiers that remain constant throughout a worker's lifetime.
                Either name or serial must be provided, but not both. Using serial is more reliable when
                names might be reused or when precise worker identification is critical.
            session_arguments: A dictionary of additional arguments to pass to the underlying
                pydeephaven.Session constructor. This allows customization of session behavior
                such as setting chunk_size for data transfer or configuring query processing options.
                Common options include `disable_open_table_listener` or `chunk_size`.

        Returns:
            CorePlusSession: A fully initialized session object connected to the existing worker.
                This session provides methods for creating and manipulating tables, executing
                queries, and performing other Deephaven operations. The session shares the same
                computation environment with any other sessions connected to the same worker,
                including access to the same tables and variables.

        Raises:
            ValueError: If neither name nor serial is provided (exactly one is required), or if both
                are provided simultaneously (only one identifier should be used).
            QueryError: If the persistent query cannot be found (doesn't exist, was terminated), is not
                in a valid state to accept connections, or cannot be accessed due to permission issues.
            DeephavenConnectionError: If a network-related issue occurs while connecting to the worker,
                such as connectivity problems or server unavailability.
            SessionCreationError: If there's an error establishing the session connection for any other
                reason, such as version incompatibility or resource constraints.
            AuthenticationError: If the current authentication is invalid or has insufficient permissions
                to connect to the specified worker.

        Example - Connecting by name:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionFactory

            async def connect_to_existing_worker():
                # Create and authenticate the session manager
                factory = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")
                await factory.password("username", "password")

                # Connect to an existing worker by name
                session = await factory.connect_to_persistent_query(name="analytics_worker_v2")

                # Access existing tables from the worker
                my_table = session.get_table("my_table")

                # Close when done
                await factory.close()

                return my_table
            ```

        Example - Connecting by serial number:
            ```python
            async def connect_with_serial():
                factory = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")
                await factory.password("username", "password")

                # Get the serial number from somewhere (e.g., stored from previous session)
                serial_number = CorePlusQuerySerial.from_string("0123456789abcdef0123456789abcdef")

                # Connect to the worker using its serial number
                session = await factory.connect_to_persistent_query(serial=serial_number)

                return session
            ```

        Note:
            - Multiple clients can connect to the same worker simultaneously
            - All connected sessions share the same execution environment and tables
            - Changes made by one session are visible to all other connected sessions
            - Worker resources are only released when all sessions are closed AND the
              auto_delete_timeout has elapsed (if configured)
            - This method is more efficient than creating a new worker with connect_to_new_worker

        See Also:
            - connect_to_new_worker: Create a new worker if one does not already exist
            - CorePlusSession.get_table: Method to access existing tables in the worker
            - CorePlusSession.close: Method to disconnect from a worker and release resources
        """
        try:
            _LOGGER.debug(
                f"[CorePlusSessionFactory:connect_to_persistent_query] Connecting to persistent query (name={name}, serial={serial})"
            )
            session = await asyncio.to_thread(
                self.wrapped.connect_to_persistent_query,
                name=name,
                serial=serial,
                session_arguments=session_arguments,
            )
            _LOGGER.debug(
                "[CorePlusSessionFactory:connect_to_persistent_query] Successfully connected to persistent query"
            )
            programming_language = CorePlusSessionFactory._get_programming_language(
                session
            )
            return CorePlusSession(session, programming_language)
        except ValueError:
            # Re-raise input validation exceptions unchanged
            raise
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:connect_to_persistent_query] Connection error while connecting to persistent query: {e}"
            )
            raise DeephavenConnectionError(
                f"Connection error while connecting to persistent query: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:connect_to_persistent_query] Failed to find persistent query: {e}"
            )
            raise QueryError(f"Persistent query not found: {e}") from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:connect_to_persistent_query] Failed to connect to persistent query: {e}"
            )
            raise SessionCreationError(
                f"Failed to establish connection to persistent query: {e}"
            ) from e

    async def delete_key(self, public_key_text: str) -> None:
        """Delete a previously uploaded public key from the Deephaven server's authentication system.

        This method is used for managing SSH key-based authentication in Deephaven Enterprise.
        It removes a public key that was previously registered to your account, preventing future
        authentication attempts using the corresponding private key. This is useful for revoking
        access when a key might be compromised or is no longer needed.

        Key management best practices include:
        - Regularly rotating authentication keys for security
        - Removing keys that are no longer in use
        - Deleting keys that may have been compromised
        - Limiting the number of authorized keys to those actively needed

        This method asynchronously delegates to the underlying session manager's delete_key method,
        running it in a separate thread to avoid blocking the event loop. The method will attempt
        to find and remove the specified key from the server's key store.

        Args:
            public_key_text: The complete text of the public key to delete, exactly as it was
                uploaded. This should include the full key string including any key type prefix
                (e.g., "ssh-rsa AAAA...") and comment suffix if present. The key text must match
                exactly what was registered in the system for successful deletion.

        Raises:
            ResourceError: If the key cannot be deleted due to issues such as the key not being
                found in the system, insufficient permissions to delete the key, or other
                server-side key storage problems.
            DeephavenConnectionError: If there is a problem connecting to the server during the
                deletion operation, such as network issues or server unavailability.
            AuthenticationError: If the current authentication is invalid or has expired,
                requiring re-authentication before key management operations can be performed.

        Example:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionFactory

            async def revoke_key_access():
                # Create and authenticate the session manager
                factory = CorePlusSessionFactory.from_url("https://example.com/iris/connection.json")
                await factory.password("username", "password")

                # The public key to remove (as previously uploaded)
                public_key = "ssh-rsa AAAAB3NzaC1yc2EAAA...truncated...user@example.com"

                # Delete the key from the server
                await factory.delete_key(public_key)

                # Clean up
                await factory.close()

                print("Key access has been revoked")
            ```

        Note:
            - This operation permanently removes the key and cannot be undone
            - You must be authenticated as the key owner or have admin privileges
            - The key text must match exactly what was previously uploaded
            - If you need to rotate keys, upload the new key before deleting the old one
            - This operation affects only future authentication attempts; existing
              authenticated sessions will not be terminated
        """
        try:
            _LOGGER.debug("[CorePlusSessionFactory:delete_key] Deleting public key")
            await asyncio.to_thread(self.wrapped.delete_key, public_key_text)
            _LOGGER.debug(
                "[CorePlusSessionFactory:delete_key] Successfully deleted public key"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:delete_key] Connection error when deleting key: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to connect while deleting key: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:delete_key] Failed to delete key: {e}"
            )
            raise ResourceError(f"Failed to delete authentication key: {e}") from e

    async def password(
        self, user: str, password: str, effective_user: str | None = None
    ) -> None:
        """Authenticate to the server using username and password credentials.

        This method performs authentication with the Deephaven server using standard username
        and password credentials. It establishes a secure session that can be used for subsequent
        operations like connecting to workers or managing server resources. This is the most
        common authentication method and is suitable for most use cases.

        The authentication process creates and stores session tokens internally, which are
        then used for all subsequent API calls. These tokens are automatically refreshed when
        needed without requiring re-authentication.

        This method asynchronously delegates to the underlying session manager's password method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            user: The username to authenticate with. This must be a valid user registered
                with the Deephaven server. Case sensitivity depends on the server's authentication
                configuration.
            password: The user's password for authentication. Authentication is secure and
                passwords are never stored in memory longer than necessary.
            effective_user: The user to operate as after authentication. Defaults to None, which
                means the authenticated user will be used. This parameter enables authentication
                as one user but performing operations as another (requires appropriate permissions,
                typically admin or impersonation rights).

        Raises:
            AuthenticationError: If authentication fails due to invalid credentials, expired
                passwords, account lockouts, or insufficient permissions.
            DeephavenConnectionError: If there is a problem connecting to the authentication server
                such as network issues or server unavailability.

        Note:
            This method must be called before making any requests that require authentication,
            unless another authentication method like private_key() or saml() is used instead.
            Only one authentication method should be used per session.

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionFactory

            async def authenticate_and_work():
                # Create the session manager
                manager = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")

                # Authenticate with username/password
                await manager.password("username", "my_secure_password")

                # Now we can connect to workers, etc.
                session = await manager.connect_to_new_worker(name="my_worker")

                # Do work with the session
                table = session.empty_table()

                # Close when done
                await manager.close()
            ```

        See Also:
            - private_key: Alternative authentication method using key-based authentication
            - saml: Alternative authentication method using SAML-based single sign-on
        """
        try:
            _LOGGER.debug(
                f"[CorePlusSessionFactory:password] Authenticating as user: {user} (effective user: {effective_user or user})"
            )
            await asyncio.to_thread(
                self.wrapped.password, user, password, effective_user
            )
            _LOGGER.debug(
                "[CorePlusSessionFactory:password] Successfully authenticated"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:password] Failed to connect to authentication server: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to connect to authentication server: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:password] Authentication failed: {e}"
            )
            raise AuthenticationError(f"Failed to authenticate user {user}: {e}") from e

    async def ping(self) -> bool:
        """Send a connectivity check ping to verify the connection to Deephaven services.

        This method tests the connectivity to both the authentication server and the controller
        service by sending lightweight ping requests. It's useful for verifying that the connection
        is still active and that both essential services are responding correctly without making
        any changes to server state.

        Common use cases for this method include:
        - Implementing connection health checks in long-running applications
        - Testing whether authentication tokens are still valid
        - Verifying network connectivity to the Deephaven server
        - Determining if a session needs to be re-authenticated after periods of inactivity

        This method asynchronously delegates to the underlying session manager's ping method,
        running it in a separate thread to avoid blocking the event loop.

        Returns:
            bool: True if both the authentication server and controller successfully responded
                to the ping request, indicating a healthy connection. False if either service
                failed to respond properly, suggesting connection issues or an expired session.

        Raises:
            DeephavenConnectionError: If there is a more serious error connecting to the server,
                such as network failures, DNS resolution problems, or if the ping times out.

        Example:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionFactory

            async def check_connection():
                factory = CorePlusSessionFactory.from_url("https://server.example.com/iris/connection.json")
                await factory.password("username", "password")

                # Check if connection is still active
                is_connected = await factory.ping()
                print(f"Connection status: {'Active' if is_connected else 'Inactive'}")

                # If not connected, re-authenticate
                if not is_connected:
                    await factory.password("username", "password")
            ```

        Note:
            This method is lightweight and suitable for frequent calls. It can be called
            periodically as part of a connection monitoring strategy to detect server
            disconnections early.
        """
        try:
            _LOGGER.debug(
                "[CorePlusSessionFactory:ping] Sending ping to authentication server and controller"
            )
            result = await asyncio.to_thread(self.wrapped.ping)
            _LOGGER.debug(f"[CorePlusSessionFactory:ping] Ping result: {result}")
            return cast(bool, result)
        except Exception as e:
            _LOGGER.error(f"[CorePlusSessionFactory:ping] Ping failed: {e}")
            raise DeephavenConnectionError(f"Failed to ping server: {e}") from e

    async def private_key(self, file: str | io.StringIO) -> None:
        r"""Authenticate to the server using a Deephaven format private key file.

        This method performs certificate-based authentication with the Deephaven server using
        a private key. This authentication method is more secure than password-based authentication
        and is particularly useful for automated systems, CI/CD pipelines, and scripted operations
        where storing passwords is not desirable.

        This method asynchronously delegates to the underlying session manager's private_key method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            file: Either a string containing the path to a file with the private key produced by
                the Deephaven generate-iris-keys tool, or alternatively an io.StringIO instance
                containing the key data directly. If an io.StringIO is provided, it may be closed
                after this method is called as the contents are read fully before returning.

        Raises:
            AuthenticationError: If authentication with the private key fails due to an invalid key,
                expired key, key not being registered with the server, or insufficient permissions.
            DeephavenConnectionError: If there is a problem connecting to the authentication server
                such as network issues or server unavailability.
            IOError: If the private key file cannot be read (when providing a file path).

        Note:
            Private key authentication is an alternative to username/password or SAML authentication.
            Only one authentication method should be used per session.

            Before using this method, you need to:
            1. Generate a key pair using Deephaven's tools
            2. Register the public key with the Deephaven server
            3. Keep the private key secure

            The private key should be stored securely and protected from unauthorized access,
            as anyone with access to the private key can authenticate as the associated user.

        Example with file path:
            ```python
            from deephaven_mcp.client import CorePlusSessionFactory
            import asyncio

            async def use_private_key_auth():
                # Create the session manager
                manager = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")

                # Authenticate using a private key file
                await manager.private_key("/path/to/private_key.pem")

                # Use the authenticated session manager
                session = await manager.connect_to_new_worker()
            ```

        Example with StringIO:
            ```python
            import io

            # Read key from somewhere else (e.g., environment variable, secrets manager)
            key_data = "-----BEGIN RSA PRIVATE KEY-----\n..."
            key_io = io.StringIO(key_data)

            # Authenticate using the in-memory key
            await manager.private_key(key_io)
            ```

        See Also:
            - password: Alternative authentication using username/password
            - saml: Alternative authentication using SAML single sign-on
            - upload_key: Method for uploading the corresponding public key

        External Documentation:
            For details on setting up private keys, see the Deephaven documentation:
            https://docs.deephaven.io/Core+/latest/how-to/connect/connect-from-java/#instructions-for-setting-up-private-keys
        """
        try:
            _LOGGER.debug(
                "[CorePlusSessionFactory:private_key] Authenticating with private key"
            )
            await asyncio.to_thread(self.wrapped.private_key, file)
            _LOGGER.debug(
                "[CorePlusSessionFactory:private_key] Successfully authenticated with private key"
            )
        except FileNotFoundError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:private_key] Private key file not found: {e}"
            )
            raise AuthenticationError(f"Private key file not found: {e}") from e
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:private_key] Failed to connect to authentication server: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to connect to authentication server: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:private_key] Private key authentication failed: {e}"
            )
            raise AuthenticationError(
                f"Failed to authenticate with private key: {e}"
            ) from e

    async def saml(self) -> None:
        """Authenticate asynchronously using SAML-based Single Sign-On (SSO).

        This method initiates SAML-based single sign-on authentication with the Deephaven server,
        which is ideal for enterprise environments that have centralized identity management.
        SAML authentication allows users to authenticate using their organization's existing
        identity provider (IdP) such as Okta, Azure AD, OneLogin, or other SAML 2.0 compliant services.

        The method asynchronously delegates to the underlying session manager's saml method,
        running it in a separate thread to avoid blocking the event loop. The implementation
        handles all SAML protocol details internally based on server configuration.

        Authentication Process Flow:
        1. This method initiates a SAML authentication request
        2. The user will be redirected to their organization's identity provider login page
        3. After successful authentication with the IdP, the user is redirected back to Deephaven
        4. Authentication tokens are established and stored for subsequent API calls

        Raises:
            AuthenticationError: If SAML authentication fails due to configuration issues, incorrect
                               IdP setup, invalid credentials, account issues, or insufficient permissions.
            DeephavenConnectionError: If there is a problem connecting to the authentication server,
                                    SAML provider, or if network issues prevent authentication.
            SessionError: If the session cannot be established after successful authentication.

        Note:
            SAML authentication provides several benefits:
            - Users can authenticate with their existing organizational credentials
            - No need to manage separate passwords for Deephaven
            - Support for advanced security features like multi-factor authentication
            - Centralized user management and access control

            Prerequisites for using SAML authentication:
            - The Deephaven server must be configured with SAML support
            - The organization's IdP must be properly configured to work with Deephaven
            - Network access must be available to both the Deephaven server and the IdP

            This method must be called before making requests that require authentication,
            unless another authentication method is used instead. Only one authentication
            method should be used per session.

            For detailed information about configuring SAML with Deephaven, refer to the
            Deephaven Enterprise documentation at https://docs.deephaven.io.

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionFactory

            async def authenticate_with_saml():
                # Create the session manager
                manager = CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")

                # Authenticate using SAML - this may open a browser window for SSO login
                await manager.saml()

                # Now we can use the authenticated session manager
                session = await manager.connect_to_new_worker()

                # Use the session to work with tables
                table = session.empty_table()
            ```

        See Also:
            - password: Alternative authentication using username/password credentials
            - private_key: Alternative authentication using private key cryptographic authentication
        """
        try:
            _LOGGER.debug(
                "[CorePlusSessionFactory:saml] Starting SAML authentication flow"
            )
            await asyncio.to_thread(self.wrapped.saml)
            _LOGGER.debug(
                "[CorePlusSessionFactory:saml] Successfully authenticated using SAML"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"Failed to connect to authentication server or SAML provider: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to connect to authentication server or SAML provider: {e}"
            ) from e
        except ValueError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:saml] SAML configuration error: {e}"
            )
            raise AuthenticationError(f"SAML configuration error: {e}") from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:saml] SAML authentication failed: {e}"
            )
            raise AuthenticationError(f"Failed to authenticate via SAML: {e}") from e

    async def upload_key(self, public_key_text: str) -> None:
        """Upload a public key to the Deephaven server for certificate-based authentication.

        This method registers a public key with the Deephaven server, associating it with your
        user account. Once uploaded, you can use the corresponding private key to authenticate
        in future sessions without needing a username and password. This is particularly useful
        for automated systems, scripts, and CI/CD pipelines.

        The public key should be in the format generated by Deephaven's key generation tools.
        Typically, this is an RSA public key in PEM format.

        Key-based authentication workflow:
        1. Generate a key pair (typically using Deephaven's tools)
        2. Authenticate to the server using password or SAML
        3. Upload the public key with this method
        4. For future sessions, use private_key() method with the corresponding private key

        This method asynchronously delegates to the underlying session manager's upload_key method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            public_key_text: The full text representation of the public key to upload. This should be
                the complete PEM-encoded public key, including the header and footer lines
                (e.g., "-----BEGIN PUBLIC KEY-----" and "-----END PUBLIC KEY-----").

        Raises:
            ResourceError: If uploading the key fails due to issues such as invalid key format,
                malformed PEM data, server-side key storage problems, or permission issues.
            DeephavenConnectionError: If there is a problem connecting to the authentication server
                such as network issues or server unavailability.
            AuthenticationError: If the current session is not authenticated or lacks the necessary
                permissions to upload keys.

        Note:
            - You must be authenticated before calling this method
            - Each user can have multiple public keys registered
            - Keys are identified by a fingerprint calculated from the key data
            - If a key with the same fingerprint already exists, it will be overwritten
            - There is no automatic expiration for uploaded keys; they remain valid until removed

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionFactory
            import asyncio

            async def register_public_key():
                # Create factory and authenticate
                factory = CorePlusSessionFactory.from_url("https://server.example.com/iris/connection.json")
                await factory.password("username", "password")

                # Read public key from file
                with open("my_public_key.pem", "r") as f:
                    public_key = f.read()

                # Upload the public key
                await factory.upload_key(public_key)
                print("Public key registered successfully")
            ```
        """
        try:
            _LOGGER.debug("[CorePlusSessionFactory:upload_key] Uploading public key")
            await asyncio.to_thread(self.wrapped.upload_key, public_key_text)
            _LOGGER.debug(
                "[CorePlusSessionFactory:upload_key] Successfully uploaded public key"
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:upload_key] Connection error when uploading key: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to connect while uploading key: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                f"[CorePlusSessionFactory:upload_key] Failed to upload key: {e}"
            )
            raise ResourceError(f"Failed to upload authentication key: {e}") from e
