"""Deephaven Core+ Session Manager Wrapper.

This module provides an asynchronous wrapper around the Deephaven Core+ SessionManager
(deephaven_enterprise.client.session_manager.SessionManager) that enhances functionality
while maintaining strict interface compatibility. The wrapper adds comprehensive
documentation, robust logging, and ensures non-blocking operation by running potentially
blocking operations in separate threads.

The CorePlusSessionManager delegates all method calls to the underlying session manager
instance and wraps returned sessions in CorePlusSession objects for consistent behavior.

Example:
    ```python
    import asyncio
    from deephaven_mcp.client import CorePlusSessionManager

    # Create a Core+ session manager connected to a server
    async def main():
        # Create a session manager using the from_url classmethod
        manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")

        # Authenticate
        await manager.password("username", "password")

        # Connect to a new worker
        session = await manager.connect_to_new_worker()

        # Use the session
        table = session.empty_table()

        # Close the manager when done
        await manager.close()

    asyncio.run(main())
    ```

You can also directly instantiate the class with an existing SessionManager:

    ```python
    from deephaven_enterprise.client.session_manager import SessionManager
    from deephaven_mcp.client import CorePlusSessionManager

    # Create and wrap an existing session manager
    session_manager = SessionManager("https://myserver.example.com/iris/connection.json")
    wrapped_manager = CorePlusSessionManager(session_manager)
    ```
"""

# Standard library imports
import asyncio
import io
import logging
from typing import Any

from deephaven_mcp._exceptions import (
    AuthenticationError,
    DeephavenConnectionError,
    InternalError,
    QueryError,
    ResourceError,
    SessionCreationError,
    SessionError,
)

from ._auth_client import CorePlusAuthClient

# Local application imports
from ._base import ClientObjectWrapper, is_enterprise_available
from ._controller_client import CorePlusControllerClient, CorePlusQuerySerial
from ._session import CorePlusSession

# Define the logger for this module
_LOGGER = logging.getLogger(__name__)


class CorePlusSessionManager(
    ClientObjectWrapper["deephaven_enterprise.client.session_manager.SessionManager"]
):
    """Asynchronous wrapper for the Deephaven Core+ SessionManager.

    This class wraps an existing Deephaven Core+ session manager instance, delegating all
    method calls to the underlying instance while providing enhanced documentation and logging.
    The wrapper runs potentially blocking operations in separate threads using asyncio.to_thread
    to prevent blocking the event loop.

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

    def __init__(self, session_manager):
        """Initialize the CorePlusSessionManager wrapper.

        Args:
            session_manager: The SessionManager instance to wrap. Must be an instance
                           of deephaven_enterprise.client.session_manager.SessionManager.
                           This should be a fully initialized session manager that is ready
                           for use (already authenticated if necessary).

        """
        super().__init__(session_manager, is_enterprise=True)
        _LOGGER.info("CorePlusSessionManager initialized")

    @classmethod
    def from_url(cls, url: str) -> "CorePlusSessionManager":
        """Create a CorePlusSessionManager connected to the specified connection URL.

        This convenience method creates a new SessionManager connected to the specified
        connection URL and wraps it in a CorePlusSessionManager for asynchronous use.

        Args:
            url: The connection URL for the Deephaven server. This should point to a
                 connection.json file, typically in the format
                 "https://<server>/iris/connection.json".

        Returns:
            CorePlusSessionManager: A new wrapper instance connected to the specified URL

        Raises:
            InternalError: If Core+ features are not available (deephaven-coreplus-client not installed)
            DeephavenConnectionError: If unable to connect to the specified URL

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionManager

            # Create a session manager connected to the server
            manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")

            # Authenticate (in an async context)
            await manager.password("username", "password")

            # Connect to an existing worker or create a new one
            session = await manager.connect_to_persistent_query("My Query")
            # Or
            session = await manager.connect_to_new_worker(heap_size_gb=1.0)

            # Use the session to work with tables
            table = session.empty_table()

            # Close the session when done
            await manager.close()
            ```

        Note:
            When used within a Deephaven worker's Python environment, the SessionManager can be
            created without a connection URL to connect to the current cluster automatically.
        """
        if not is_enterprise_available:
            raise InternalError(
                "Core+ features are not available (deephaven-coreplus-client not installed)"
            )
        else:
            from deephaven_enterprise.client.session_manager import SessionManager

            try:
                _LOGGER.debug(f"Creating SessionManager with URL: {url}")
                return cls(SessionManager(url))
            except Exception as e:
                _LOGGER.error(f"Failed to create SessionManager with URL {url}: {e}")
                raise DeephavenConnectionError(
                    f"Failed to establish connection to Deephaven at {url}: {e}"
                ) from e

    async def close(self) -> None:
        """Terminate this session manager's connection to the authentication server and controller.

        This asynchronously delegates to the underlying session manager's close method,
        running it in a separate thread to avoid blocking the event loop.

        Returns:
            None: This method doesn't return anything.

        Raises:
            SessionError: If terminating the connections fails for any reason.

        Note:
            After closing the session manager, it cannot be reused. A new instance
            must be created if further connections are needed.
        """
        try:
            _LOGGER.debug("Closing session manager connection")
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug("Session manager connection closed successfully")
        except Exception as e:
            _LOGGER.error(f"Failed to close session manager: {e}")
            raise SessionError(
                f"Failed to close session manager connections: {e}"
            ) from e

    async def connect_to_new_worker(
        self,
        name: str | None = None,
        heap_size_gb: float = None,
        server: str = None,
        extra_jvm_args: list[str] = None,
        extra_environment_vars: list[str] = None,
        engine: str = "DeephavenCommunity",
        auto_delete_timeout: int | None = 600,
        admin_groups: list[str] = None,
        viewer_groups: list[str] = None,
        timeout_seconds: float = 60,
        configuration_transformer=None,
        session_arguments: dict[str, Any] = None,
    ) -> CorePlusSession:
        """Create a new worker (as a temporary PersistentQuery) and establish a session to it.

        This method asynchronously delegates to the underlying session manager's connect_to_new_worker method,
        running it in a separate thread to avoid blocking the event loop. The returned session is wrapped
        in a CorePlusSession for consistent behavior.

        Args:
            # Worker identification
            name: The name of the persistent query. Defaults to None, which means a name based
                on the current time is used.

            # Resource configuration
            heap_size_gb: The heap size of the worker in GB. Larger values allow for processing
                more data but consume more resources.
            server: The server to connect to. Defaults to None, which means the first available server.
            extra_jvm_args: Extra JVM arguments for starting the worker. Defaults to None.
                Useful for configuring JVM behavior like garbage collection.
            extra_environment_vars: Extra environment variables for the worker. Defaults to None.
                Useful for configuring worker behavior through environment.
            engine: Which engine (worker kind) to use for the backend worker.
                Defaults to "DeephavenCommunity".

            # Lifecycle management
            auto_delete_timeout: After how many seconds should the query be automatically deleted after inactivity.
                Defaults to 600 seconds (10 minutes). If None, auto-delete is disabled. If zero, the query
                is deleted immediately after a client connection is lost.
            timeout_seconds: How long to wait for the query to start, in seconds. Defaults to 60 seconds.

            # Access controls
            admin_groups: List of groups that may administer the query. Defaults to None, which means only the
                current user may administer the query.
            viewer_groups: List of groups that may view the query. Defaults to None, which means only the current
                user may view the query.

            # Advanced configuration
            configuration_transformer: A function that can replace (or edit) the automatically generated persistent
                query configuration, enabling you to set more advanced options than the other
                function parameters provide. Defaults to None.
            session_arguments: A dictionary of additional arguments to pass to the pydeephaven.Session
                created and wrapped by a CorePlusSession.

        Returns:
            CorePlusSession: A session connected to a new Interactive Console PQ worker. This session
                provides access to tables and other Deephaven functionality.

        Raises:
            ResourceError: If there are insufficient resources to create the worker.
            SessionCreationError: If an error occurs creating the worker or connecting to it.
            DeephavenConnectionError: If there is a connection error to the Deephaven server.

        Example:
            ```python
            import asyncio
            from deephaven_mcp.client import CorePlusSessionManager

            async def create_custom_worker():
                # Create and authenticate the session manager
                manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")
                await manager.password("username", "password")

                # Create a high-memory worker with a custom name and 10-minute auto-delete
                session = await manager.connect_to_new_worker(
                    name="my_analytics_worker",
                    heap_size_gb=8.0,
                    auto_delete_timeout=600,
                    extra_jvm_args=["-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200"]
                )

                # Work with the session
                table = session.table([1, 2, 3], columns=["A"])

                # When done, close the manager
                await manager.close()

                return table
            ```

        Note:
            Creating a new worker is resource-intensive. Consider using `connect_to_persistent_query`
            to connect to an existing worker if one is available.

        See Also:
            - connect_to_persistent_query: Connect to an existing worker by name or serial
            - create_controller_client: Create a client for managing workers directly
        """
        try:
            _LOGGER.debug("Creating new worker and connecting to it")
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
            _LOGGER.debug("Successfully connected to new worker")
            return CorePlusSession(session)
        except ConnectionError as e:
            _LOGGER.error(f"Connection error while connecting to new worker: {e}")
            raise DeephavenConnectionError(
                f"Connection error while creating worker: {e}"
            ) from e
        except ResourceError as e:
            # Re-raise resource exceptions unchanged
            _LOGGER.error(f"Insufficient resources to create worker: {e}")
            raise
        except Exception as e:
            _LOGGER.error(f"Failed to connect to new worker: {e}")
            raise SessionCreationError(
                f"Failed to create and connect to new worker: {e}"
            ) from e

    async def connect_to_persistent_query(
        self,
        name: str | None = None,
        serial: CorePlusQuerySerial | None = None,
        session_arguments: dict[str, Any] | None = None,
    ) -> CorePlusSession:
        """Connect to an existing persistent query by name or serial number.

        This method asynchronously delegates to the underlying session manager's connect_to_persistent_query method,
        running it in a separate thread to avoid blocking the event loop. The returned session is wrapped
        in a CorePlusSession for consistent behavior.

        Args:
            name: The name of the persistent query to connect to. Either name or serial must be provided,
                but not both.
            serial: The serial number of the persistent query to connect to. Either name or serial must
                be provided, but not both.
            session_arguments: A dictionary of additional arguments to pass to the pydeephaven.Session
                created and wrapped by a CorePlusSession.

        Returns:
            CorePlusSession: A session connected to the persistent query. This session provides access
                to tables and other Deephaven functionality.

        Raises:
            ValueError: If neither name nor serial is provided, or if both are provided.
            QueryError: If the persistent query does not exist or is not running.
            SessionCreationError: If there's an error establishing the session connection.
        """
        try:
            _LOGGER.debug(
                f"Connecting to persistent query (name={name}, serial={serial})"
            )
            session = await asyncio.to_thread(
                self.wrapped.connect_to_persistent_query,
                name=name,
                serial=serial,
                session_arguments=session_arguments,
            )
            _LOGGER.debug("Successfully connected to persistent query")
            return CorePlusSession(session)
        except ValueError:
            # Re-raise input validation exceptions unchanged
            raise
        except ConnectionError as e:
            _LOGGER.error(f"Connection error while connecting to persistent query: {e}")
            raise DeephavenConnectionError(
                f"Connection error while connecting to persistent query: {e}"
            ) from e
        except KeyError as e:
            _LOGGER.error(f"Failed to find persistent query: {e}")
            raise QueryError(f"Persistent query not found: {e}") from e
        except Exception as e:
            _LOGGER.error(f"Failed to connect to persistent query: {e}")
            raise SessionCreationError(
                f"Failed to establish connection to persistent query: {e}"
            ) from e

    async def create_auth_client(
        self, auth_host: str | None = None
    ) -> CorePlusAuthClient:
        """Create the authentication client for this session manager.

        This method asynchronously delegates to the underlying session manager's create_auth_client method,
        running it in a separate thread to avoid blocking the event loop.

        The authentication client is used for authentication-related operations such as
        logging in, obtaining tokens, and verifying credentials.

        Args:
            auth_host: The authentication host to connect to. Defaults to None, which means
                the first host in the JSON config's auth_host list will be used.

        Returns:
            CorePlusAuthClient: The wrapped authentication client instance.

        Raises:
            DeephavenConnectionError: If a connection cannot be established with the authentication service.
            AuthenticationError: If there are authentication-related issues creating the client.
        """
        try:
            _LOGGER.debug(
                f"Creating authentication client for host: {auth_host or 'default'}"
            )
            auth_client = await asyncio.to_thread(
                self.wrapped.create_auth_client, auth_host
            )
            _LOGGER.debug("Authentication client created successfully")
            return CorePlusAuthClient(auth_client)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Failed to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to create authentication client: {e}")
            raise AuthenticationError(
                f"Failed to create authentication client: {e}"
            ) from e

    async def create_controller_client(self) -> CorePlusControllerClient:
        """Create the controller client for this session manager.

        This method asynchronously creates an instance of the underlying controller client
        and wraps it in a CorePlusControllerClient for a fully asynchronous interface.

        The controller client is used for managing persistent queries and other server-side resources.
        It provides methods for creating, listing, and managing queries and workers.

        Returns:
            CorePlusControllerClient: The controller client.

        Raises:
            DeephavenConnectionError: If a connection cannot be established with the controller service.
            SessionError: If there is an error creating the controller client for reasons other than connectivity.
        """
        try:
            _LOGGER.debug("Creating controller client")
            controller_client = await asyncio.to_thread(
                self.wrapped.create_controller_client
            )
            _LOGGER.debug("Controller client created successfully")
            return CorePlusControllerClient(controller_client)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to controller service: {e}")
            raise DeephavenConnectionError(
                f"Failed to connect to controller service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to create controller client: {e}")
            raise SessionError(f"Failed to create controller client: {e}") from e

    async def delete_key(self, public_key_text: str) -> None:
        """Delete the specified public key from the Deephaven server.

        This method asynchronously delegates to the underlying session manager's delete_key method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            public_key_text: The public key text to delete. This is the text representation of the
                public key that was previously uploaded to the server.

        Raises:
            ResourceError: If the key cannot be deleted due to resource management issues such as
                key not found, permissions, or server-side key storage problems.
            DeephavenConnectionError: If there is a problem connecting to the server.

        Note:
            This method is used for key management in Deephaven's authentication system.
            It removes a previously uploaded public key from the server's authorized keys.
        """
        try:
            _LOGGER.debug("Deleting public key")
            await asyncio.to_thread(self.wrapped.delete_key, public_key_text)
            _LOGGER.debug("Public key deleted successfully")
        except ConnectionError as e:
            _LOGGER.error(f"Connection error when deleting key: {e}")
            raise DeephavenConnectionError(
                f"Failed to connect while deleting key: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to delete key: {e}")
            raise ResourceError(f"Failed to delete authentication key: {e}") from e

    async def password(
        self, user: str, password: str, effective_user: str | None = None
    ) -> None:
        """Authenticate to the server using a username and password.

        This method asynchronously delegates to the underlying session manager's password method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            user: The username to authenticate with. This must be a valid user registered
                with the Deephaven server.
            password: The user's password for authentication.
            effective_user: The user to operate as after authentication. Defaults to None, which
                means the authenticated user will be used. This parameter enables authentication
                as one user but performing operations as another (requires appropriate permissions).

        Raises:
            AuthenticationError: If authentication fails due to invalid credentials or permission issues.
            DeephavenConnectionError: If there is a problem connecting to the authentication server.

        Note:
            This method must be called before making any requests that require authentication,
            unless another authentication method like private_key or saml is used instead.

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionManager

            async def authenticate_and_work():
                # Create the session manager
                manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")

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
                f"Authenticating as user: {user} (effective user: {effective_user or user})"
            )
            await asyncio.to_thread(
                self.wrapped.password, user, password, effective_user
            )
            _LOGGER.debug("Authentication successful")
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication server: {e}")
            raise DeephavenConnectionError(
                f"Failed to connect to authentication server: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Authentication failed: {e}")
            raise AuthenticationError(f"Failed to authenticate user {user}: {e}") from e

    async def ping(self) -> bool:
        """Send a ping to the authentication server and controller to verify connectivity.

        This method asynchronously delegates to the underlying session manager's ping method,
        running it in a separate thread to avoid blocking the event loop.

        Returns:
            bool: True if both pings were successfully sent and received, False if either
                ping failed to be sent or acknowledged. This can be used to check if the
                connection to the server is still active.

        Raises:
            DeephavenConnectionError: If there is an error connecting to the server or the ping times out.

        Note:
            This is useful for maintaining or verifying the connection to the Deephaven server.
            It can be called periodically to ensure the connection is still alive.
        """
        try:
            _LOGGER.debug("Sending ping to authentication server and controller")
            result = await asyncio.to_thread(self.wrapped.ping)
            _LOGGER.debug(f"Ping result: {result}")
            return result
        except Exception as e:
            _LOGGER.error(f"Ping failed: {e}")
            raise DeephavenConnectionError(f"Failed to ping server: {e}") from e

    async def private_key(self, file: str | io.StringIO) -> None:
        """Authenticate to the server using a Deephaven format private key file.

        This method asynchronously delegates to the underlying session manager's private_key method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            file: Either a string containing the path to a file with the private key produced by
                generate-iris-keys, or alternatively an io.StringIO instance containing the key data.
                If an io.StringIO is provided, it may be closed after this method is called as the
                contents are read fully before returning.

        Raises:
            AuthenticationError: If authentication with the private key fails due to an invalid key or permissions issues.
            DeephavenConnectionError: If there is a problem connecting to the authentication server.

        Note:
            Private key authentication is an alternative to username/password or SAML authentication.
            This method needs to be called before making requests that require authentication,
            unless another authentication method is used instead.

        Example with file path:
            ```python
            from deephaven_mcp.client import CorePlusSessionManager
            import asyncio

            async def use_private_key_auth():
                # Create the session manager
                manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")

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
            _LOGGER.debug("Authenticating with private key")
            await asyncio.to_thread(self.wrapped.private_key, file)
            _LOGGER.debug("Private key authentication successful")
        except FileNotFoundError as e:
            _LOGGER.error(f"Private key file not found: {e}")
            raise AuthenticationError(f"Private key file not found: {e}") from e
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication server: {e}")
            raise DeephavenConnectionError(
                f"Failed to connect to authentication server: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Private key authentication failed: {e}")
            raise AuthenticationError(
                f"Failed to authenticate with private key: {e}"
            ) from e

    async def saml(self) -> None:
        """Authenticate asynchronously using SAML (Security Assertion Markup Language).

        This method initiates SAML-based single sign-on authentication with the Deephaven server.
        It asynchronously delegates to the underlying session manager's saml method,
        running it in a separate thread to avoid blocking the event loop. The original implementation
        handles the SAML URI internally based on server configuration.

        When called, this method will typically open a browser window or redirect to an identity
        provider's login page where the user can enter their credentials. After successful
        authentication with the identity provider, the user is redirected back to Deephaven with
        the appropriate authentication tokens established.

        Raises:
            AuthenticationError: If SAML authentication fails due to configuration issues, incorrect
                               setup, invalid credentials, or insufficient permissions.
            DeephavenConnectionError: If there is a problem connecting to the authentication server,
                                    SAML provider, or if network issues prevent authentication.

        Note:
            SAML authentication is typically used in enterprise environments for single sign-on (SSO),
            allowing users to authenticate once with their organizational credentials and access
            multiple services without re-authentication.

            The Deephaven server must be properly configured with SAML support for this authentication
            method to work, including correct identity provider settings and certificate configuration.

            This method must be called before making requests that require authentication,
            unless another authentication method (password or private key) is used instead.

            For detailed information about configuring SAML with Deephaven, refer to the
            Deephaven Enterprise documentation at https://docs.deephaven.io.

        Example:
            ```python
            from deephaven_mcp.client import CorePlusSessionManager

            async def authenticate_with_saml():
                # Create the session manager
                manager = CorePlusSessionManager.from_url("https://myserver.example.com/iris/connection.json")

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
            _LOGGER.debug("Starting SAML authentication flow")
            await asyncio.to_thread(self.wrapped.saml)
            _LOGGER.debug("SAML authentication successful")
        except ConnectionError as e:
            _LOGGER.error(
                f"Failed to connect to authentication server or SAML provider: {e}"
            )
            raise DeephavenConnectionError(
                f"Failed to connect to authentication server or SAML provider: {e}"
            ) from e
        except ValueError as e:
            _LOGGER.error(f"SAML configuration error: {e}")
            raise AuthenticationError(f"SAML configuration error: {e}") from e
        except Exception as e:
            _LOGGER.error(f"SAML authentication failed: {e}")
            raise AuthenticationError(f"Failed to authenticate via SAML: {e}") from e

    async def upload_key(self, public_key_text: str) -> None:
        """Upload the provided public key to the Deephaven server for authentication.

        This method asynchronously delegates to the underlying session manager's upload_key method,
        running it in a separate thread to avoid blocking the event loop.

        Args:
            public_key_text: The public key text to upload as a string. This should be the text representation
                of a public key that corresponds to a private key you possess for future authentication.

        Raises:
            ResourceError: If uploading the key fails due to resource management issues such as
                invalid key format, server-side key storage problems, or permission issues.
            DeephavenConnectionError: If there is a problem connecting to the server.

        Note:
            This method is used for setting up key-based authentication. After uploading a public key,
            you can later authenticate using the corresponding private key with the private_key() method.
            The key will be associated with your user account on the server.
        """
        try:
            _LOGGER.debug("Uploading public key")
            await asyncio.to_thread(self.wrapped.upload_key, public_key_text)
            _LOGGER.debug("Public key uploaded successfully")
        except ConnectionError as e:
            _LOGGER.error(f"Connection error when uploading key: {e}")
            raise DeephavenConnectionError(
                f"Failed to connect while uploading key: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Failed to upload key: {e}")
            raise ResourceError(f"Failed to upload authentication key: {e}") from e
