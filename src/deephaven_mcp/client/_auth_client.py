"""
Asynchronous wrapper for the Deephaven authentication client.

This module provides an asynchronous wrapper around the Deephaven AuthClient to enable non-blocking
asynchronous authentication operations in the Deephaven MCP environment. Used by the CoreSessionManager
and other components that need to authenticate with Deephaven servers.

All blocking operations are converted to asynchronous methods using asyncio.to_thread, allowing client
code to use async/await syntax without blocking the event loop.

Logging:
    - All authentication operations log entry, success, and error events at DEBUG or ERROR level using the module logger.
    - Sensitive information (such as passwords and tokens) is never logged.

Classes:
    CorePlusAuthClient: Async wrapper around deephaven_enterprise.client.auth.AuthClient
"""

import asyncio
import logging

from deephaven_mcp._exceptions import AuthenticationError, DeephavenConnectionError
from ._base import ClientObjectWrapper
from ._protobuf import CorePlusToken

_LOGGER = logging.getLogger(__name__)


class CorePlusAuthClient(
    ClientObjectWrapper["deephaven_enterprise.client.auth.AuthClient"]
):
    """
    Asynchronous wrapper around the Deephaven AuthClient, enabling non-blocking authentication and token management for Deephaven services.

    All blocking calls are performed in separate threads using asyncio.to_thread to avoid blocking the event loop.

    This class is typically instantiated by CorePlusSessionManager's create_auth_client method and provides:
        - authenticate: Username/password authentication
        - authenticate_with_token: Token-based authentication
        - create_token: Service token creation

    Logging:
        - Logs entry, success, and error for all authentication operations at DEBUG or ERROR level.
        - Sensitive information is never logged.

    Example:
        import asyncio
        from deephaven_mcp.client import CorePlusSessionManager

        async def authenticate_example():
            manager = CorePlusSessionManager.from_url("https://myserver.example.com/connection.json")
            auth_client = await manager.create_auth_client()
            token = await auth_client.authenticate("username", "password")
            service_token = await auth_client.create_token("PersistentQueryController", duration_seconds=3600)
            controller = await manager.create_controller_client()
            await controller.authenticate(service_token)
            await auth_client.close()
    """

    def __init__(
        self, auth_client: "deephaven_enterprise.client.auth.AuthClient"  # noqa: F821
    ):
        """Initialize the CorePlusAuthClient with an AuthClient instance.

        Args:
            auth_client: The synchronous AuthClient instance to wrap.

        Note:
            This method is used internally by CorePlusSessionManager and should not be called directly.
        """
        super().__init__(auth_client, is_enterprise=True)
        _LOGGER.info("[CorePlusAuthClient] initialized")

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

        Note:
            This method uses asyncio.to_thread to perform the authentication operation in a separate thread,
            allowing for non-blocking asynchronous operation.
        """
        _LOGGER.debug("[CorePlusAuthClient] Authenticating user '%s'...", username)
        try:
            result = await asyncio.to_thread(
                self.wrapped.authenticate, username, password, timeout
            )
            _LOGGER.debug("[CorePlusAuthClient] User '%s' authenticated successfully.", username)
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error("[CorePlusAuthClient] Authentication failed for user '%s': %s", username, e)
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

        Note:
            This method uses asyncio.to_thread to perform the authentication operation in a separate thread,
            allowing for non-blocking asynchronous operation.
        """
        _LOGGER.debug("[CorePlusAuthClient] Authenticating with token (not logged)...")
        try:
            result = await asyncio.to_thread(
                self.wrapped.authenticate_with_token, token, timeout
            )
            _LOGGER.debug("[CorePlusAuthClient] Token authentication succeeded.")
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error("[CorePlusAuthClient] Token authentication failed: %s", e)
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

        Note:
            This method uses asyncio.to_thread to perform the token creation operation in a separate thread,
            allowing for non-blocking asynchronous operation.
        """
        _LOGGER.debug("[CorePlusAuthClient] Creating service token for service '%s' (username='%s', duration=%ds)...", service, username or '[current user]', duration_seconds)
        try:
            result = await asyncio.to_thread(
                self.wrapped.create_token, service, username, duration_seconds, timeout
            )
            _LOGGER.debug("[CorePlusAuthClient] Service token for '%s' created successfully.", service)
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error(f"Failed to connect to authentication service: {e}")
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error("[CorePlusAuthClient] Service token creation failed for '%s': %s", service, e)
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

        Note:
            This method uses asyncio.to_thread to perform the close operation in a separate thread,
            allowing for non-blocking asynchronous operation.
        """
        _LOGGER.debug("[CorePlusAuthClient] Closing authentication client connection...")
        try:
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug("[CorePlusAuthClient] Authentication client connection closed.")
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
