"""Asynchronous wrapper for the Deephaven authentication client.

This module provides a wrapper around the Deephaven AuthClient to enable non-blocking
asynchronous authentication operations in the Deephaven MCP environment. It's used by the
CoreSessionManager and other components that need to authenticate with Deephaven servers.

The wrapper maintains the same interface as the original AuthClient but converts all
blocking operations to asynchronous methods using asyncio.to_thread. This allows client
code to use async/await syntax with the authentication client without blocking the event loop.

Classes:
    CorePlusAuthClient: Async wrapper around deephaven_enterprise.client.auth.AuthClient
"""

import asyncio
import logging

from deephaven_mcp._exceptions import (
    AuthenticationError,
    DeephavenConnectionError,
)

from ._base import ClientObjectWrapper
from ._protobuf import CorePlusToken

_LOGGER = logging.getLogger(__name__)


class CorePlusAuthClient(
    ClientObjectWrapper["deephaven_enterprise.client.auth.AuthClient"]
):
    """Asynchronous wrapper around the AuthClient.

    This class provides an asynchronous interface to the AuthClient, which connects to the
    Deephaven authentication service. It enables authentication and token management for
    accessing Deephaven services.

    All blocking calls are performed in separate threads using asyncio.to_thread to avoid blocking
    the event loop. The wrapper maintains the same interface as the underlying AuthClient
    while making it compatible with asynchronous code.

    This class is typically not instantiated directly but is created by CorePlusSessionManager's
    create_auth_client method. It provides three main authentication methods:
    - authenticate: Username/password authentication
    - authenticate_with_token: Token-based authentication
    - create_token: Create new service tokens

    Examples:
        ```python
        import asyncio
        from deephaven_mcp.client import CorePlusSessionManager

        async def authenticate_example():
            # Create a session manager
            manager = CorePlusSessionManager.from_url("https://myserver.example.com/connection.json")

            # Get auth client and authenticate
            auth_client = await manager.create_auth_client()
            token = await auth_client.authenticate("username", "password")

            # Create a service token
            service_token = await auth_client.create_token("PersistentQueryController",
                                                     duration_seconds=3600)

            # Use the token with controller client
            controller = await manager.create_controller_client()
            await controller.authenticate(service_token)

            # Close connections when done
            await auth_client.close()
        ```
    """

    def __init__(
        self, auth_client: "deephaven_enterprise.client.auth.AuthClient"  # noqa: F821
    ):
        """Initialize the CorePlusAuthClient with an AuthClient instance.

        Args:
            auth_client: The AuthClient instance to wrap.
        """
        super().__init__(auth_client, is_enterprise=True)
        _LOGGER.info("CorePlusAuthClient initialized")

    async def authenticate(
        self, username: str, password: str, timeout: float | None = None
    ) -> CorePlusToken:
        """Authenticate to the auth service using username and password asynchronously.

        This is typically the first authentication step when connecting to a Deephaven server.
        The returned token can be used to authenticate with other Deephaven services such as
        the PersistentQueryController.

        Args:
            username: The username to authenticate with. Must be a valid user registered with
                     the Deephaven authentication service.
            password: The password to authenticate with. Sensitive and never logged.
            timeout: Timeout in seconds for the operation. If None, the client's default timeout is used.
                    Setting an appropriate timeout can prevent indefinite blocking if the server is unresponsive.

        Returns:
            A CorePlusToken object for the authenticated user. This token contains the necessary
            credentials to authenticate with other Deephaven services.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service due to
                                    network issues, server unavailability, or connection problems.
            AuthenticationError: If authentication fails due to invalid credentials, expired accounts,
                               permission issues, or other authentication-related reasons.
        """
        _LOGGER.debug(f"Authenticating user {username}")
        try:
            result = await asyncio.to_thread(
                self.wrapped.authenticate, username, password, timeout
            )
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.warning(f"Authentication failed for user {username}: {e}")
            raise AuthenticationError(f"Authentication failed: {e}") from e

    async def authenticate_with_token(
        self, token: str, timeout: float | None = None
    ) -> CorePlusToken:
        """Authenticate to the auth service using a token string asynchronously.

        This method provides an alternative to username/password authentication by accepting
        an existing token string. This is useful for scenarios where tokens are managed externally
        or when implementing token refresh mechanisms.

        Args:
            token: The token string to authenticate with. This should be a valid, non-expired token
                  that was previously issued by the Deephaven authentication service.
            timeout: Timeout in seconds for the operation. If None, the client's default timeout is used.
                    A reasonable timeout prevents indefinite blocking if the server is unresponsive.

        Returns:
            A CorePlusToken object for the authenticated user. This token encapsulates the authenticated
            user's identity and permissions, allowing access to protected Deephaven services.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service due to
                                    network issues, server unavailability, or connection problems.
            AuthenticationError: If the token is invalid, expired, revoked, or authentication fails
                               for any other reason related to the token itself.
        """
        _LOGGER.debug("Authenticating with token")
        try:
            result = await asyncio.to_thread(
                self.wrapped.authenticate_with_token, token, timeout
            )
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.warning(f"Authentication failed with token: {e}")
            raise AuthenticationError(f"Authentication with token failed: {e}") from e

    async def create_token(
        self,
        service: str,
        username: str = "",
        duration_seconds: int = 3600,
        timeout: float | None = None,
    ) -> CorePlusToken:
        """Create a token for the specified service asynchronously.

        This method creates a service-specific token that can be used to authenticate with
        other Deephaven services. Service tokens have more limited permissions than user tokens
        and are typically used for specific service-to-service authentication scenarios.

        Common service values include:
        - "PersistentQueryController": For controller client authentication
        - "JavaScriptClient": For web client authentication
        - "Console": For console access

        Args:
            service: The service name to create a token for. Must be a valid service name
                   recognized by the Deephaven authentication service.
            username: The username to create a token for. If empty, the currently authenticated
                    user is used. Only users with appropriate permissions can create tokens for
                    other users.
            duration_seconds: The duration of the token in seconds. Default is 3600 (1 hour).
                           Longer durations increase the window of potential token misuse if
                           compromised, while shorter durations require more frequent token refreshes.
            timeout: Timeout in seconds for the operation. If None, the client's default timeout is used.

        Returns:
            A CorePlusToken object for the specified service. This token is specifically scoped
            to the requested service and has permissions appropriate for that service.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service due to
                                    network issues, server unavailability, or connection problems.
            AuthenticationError: If token creation fails due to authentication, insufficient permissions,
                               invalid service name, or other token-related issues.
        """
        _LOGGER.debug(f"Creating token for service {service}")
        try:
            result = await asyncio.to_thread(
                self.wrapped.create_token, service, username, duration_seconds, timeout
            )
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.warning(f"Failed to create token for service {service}: {e}")
            raise AuthenticationError(f"Token creation failed: {e}") from e

    async def close(self) -> None:
        """Close the client connection asynchronously.

        This method gracefully terminates the connection to the authentication service and
        releases any associated resources. After calling this method, no further operations
        should be performed with this client instance.

        It's important to call this method when you're finished with the auth client to
        properly clean up resources and avoid connection leaks. Typically, you would call
        this after you've finished all authentication operations or when shutting down
        your application.

        Raises:
            DeephavenConnectionError: If there is a network or connection error closing the
                                    authentication connection, such as if the network becomes
                                    unavailable during the close operation.
            AuthenticationError: If there is an authentication-related error during the close
                               operation, such as if the server rejects the request due to
                               an invalid or expired session.

        Note:
            Even if an exception is raised, the client should still be considered closed
            and should not be reused. The exceptions are raised primarily for diagnostic
            purposes.
        """
        _LOGGER.debug("Closing auth client")
        try:
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug("Auth client connection closed successfully")
        except ConnectionError as e:
            _LOGGER.error(f"Connection error while closing auth client: {e}")
            raise DeephavenConnectionError(
                f"Connection error while closing authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(f"Error closing auth client connection: {e}")
            raise AuthenticationError(
                f"Failed to close authentication connection: {e}"
            ) from e
