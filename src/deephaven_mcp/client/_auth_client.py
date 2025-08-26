"""
Asynchronous Deephaven authentication client wrapper for MCP.

This module provides an async interface to the Deephaven AuthClient, enabling non-blocking authentication
and token management for Deephaven services. It is primarily used by the CorePlusSessionManager and related
components that require authentication with Deephaven Enterprise servers.

Key Features:
    - Converts all blocking AuthClient operations to async using asyncio.to_thread for event loop safety.
    - Provides async methods for username/password and token-based authentication, as well as service token creation.
    - Ensures sensitive information (passwords, tokens) is never logged; only usernames are logged at DEBUG/INFO levels.
    - Consistent and detailed logging for entry, success, and error events.

Classes:
    CorePlusAuthClient: Main async wrapper for deephaven_enterprise.client.auth.AuthClient.

Example:
    import asyncio
    from deephaven_mcp.client import CorePlusSessionManager

    async def authenticate_example():
        manager = CorePlusSessionManager.from_url("https://myserver.example.com/connection.json")
        auth_client = manager.auth_client
        token = await auth_client.authenticate("username", "password")
        service_token = await auth_client.create_token("PersistentQueryController", duration_seconds=3600)
        controller = await manager.create_controller_client()
        await controller.authenticate(service_token)
        await auth_client.close()
"""

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import deephaven_enterprise.client.auth  # pragma: no cover

from deephaven_mcp._exceptions import AuthenticationError, DeephavenConnectionError

from ._base import ClientObjectWrapper
from ._protobuf import CorePlusToken

_LOGGER = logging.getLogger(__name__)


class CorePlusAuthClient(
    ClientObjectWrapper["deephaven_enterprise.client.auth.AuthClient"]
):
    """
    Asynchronous wrapper for the Deephaven AuthClient, providing non-blocking authentication and token management.

    This class wraps a synchronous Deephaven AuthClient and exposes async methods for authentication
    and token creation. All blocking operations are executed in threads to preserve event loop responsiveness.

    Typical Usage:
        - Instantiate via CorePlusSessionManager (not directly).
        - Authenticate using username/password or an existing token.
        - Create service-specific tokens for downstream authentication.
        - Close the client when finished.

    Logging:
        - Logs entry, success, and error for all authentication operations at DEBUG or ERROR level.
        - Usernames may be logged; passwords and tokens are never logged.

    Example:
        import asyncio
        from deephaven_mcp.client import CorePlusSessionManager

        async def authenticate_example():
            manager = CorePlusSessionManager.from_url("https://myserver.example.com/connection.json")
            auth_client = manager.auth_client
            token = await auth_client.authenticate("username", "password")
            service_token = await auth_client.create_token("PersistentQueryController", duration_seconds=3600)
            controller = await manager.create_controller_client()
            await controller.authenticate(service_token)
            await auth_client.close()
    """

    def __init__(
        self, auth_client: "deephaven_enterprise.client.auth.AuthClient"  # noqa: F821
    ):
        """Initialize CorePlusAuthClient with a synchronous AuthClient instance.

        Args:
            auth_client: The synchronous Deephaven AuthClient instance to wrap.

        Note:
            This constructor is intended for use by CorePlusSessionManager. Users should not instantiate
            this class directly.
        """
        super().__init__(auth_client, is_enterprise=True)
        _LOGGER.info("[CorePlusAuthClient] initialized")

    async def authenticate(
        self, username: str, password: str, timeout: float | None = None
    ) -> CorePlusToken:
        """Authenticate asynchronously using username and password.

        This method is typically the first authentication step when connecting to a Deephaven server.
        It performs authentication in a background thread for event loop safety.

        Args:
            username (str): Deephaven username. Must be registered with the authentication service. Usernames are logged at DEBUG level.
            password (str): Password for authentication. Never logged.
            timeout (float | None): Optional timeout in seconds. If None, uses the client's default. Prevents indefinite blocking.

        Returns:
            CorePlusToken: Token for the authenticated user. Can be used for subsequent service authentication.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service (network/server issues).
            AuthenticationError: If authentication fails (invalid credentials, expired account, permissions, etc).

        Logging:
            - Logs entry, success, and errors at DEBUG or ERROR.
            - Passwords are never logged.

        Note:
            Uses asyncio.to_thread to avoid blocking the event loop.
        """
        _LOGGER.debug("[CorePlusAuthClient] Authenticating user '%s'...", username)
        try:
            result = await asyncio.to_thread(
                self.wrapped.authenticate, username, password, timeout
            )
            _LOGGER.debug(
                "[CorePlusAuthClient] User '%s' authenticated successfully.", username
            )
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error("[CorePlusAuthClient:authenticate] Failed to connect to authentication service: %s", e)
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                "[CorePlusAuthClient] Authentication failed for user '%s': %s",
                username,
                e,
            )
            raise AuthenticationError(f"Authentication failed: {e}") from e

    async def authenticate_with_token(
        self, token: str, timeout: float | None = None
    ) -> CorePlusToken:
        """Authenticate asynchronously using an existing token string.

        This method allows authentication using a previously issued token, which is never logged.
        Useful for token refresh or external token management scenarios.

        Args:
            token (str): Valid, non-expired Deephaven authentication token. Never logged.
            timeout (float | None): Optional timeout in seconds. If None, uses the client's default.

        Returns:
            CorePlusToken: Token for the authenticated user.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service (network/server issues).
            AuthenticationError: If the token is invalid, expired, revoked, or authentication fails for any reason.

        Logging:
            - Entry, success, and error events at DEBUG or ERROR level.
            - Token values are never logged.

        Note:
            Uses asyncio.to_thread to avoid blocking the event loop.
        """
        _LOGGER.debug("[CorePlusAuthClient] Authenticating with token (not logged)...")
        try:
            result = await asyncio.to_thread(
                self.wrapped.authenticate_with_token, token, timeout
            )
            _LOGGER.debug("[CorePlusAuthClient] Token authentication succeeded.")
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error("[CorePlusAuthClient:authenticate_with_token] Failed to connect to authentication service: %s", e)
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
        """Create a service-specific authentication token asynchronously.

        This method generates a token for a specific Deephaven service (e.g., PersistentQueryController, JavaScriptClient, Console).
        Service tokens are typically used for inter-service authentication and have limited permissions.

        Args:
            service (str): Name of the target service. Must be recognized by the Deephaven authentication service.
            username (str, optional): Username for whom to create the token. If empty, uses the currently authenticated user.
            duration_seconds (int, optional): Token validity period in seconds. Default is 3600 (1 hour).
            timeout (float | None, optional): Timeout in seconds. If None, uses the client's default.

        Returns:
            CorePlusToken: Token scoped to the requested service.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service (network/server issues).
            AuthenticationError: If token creation fails (auth errors, permissions, invalid service, etc).

        Logging:
            - Logs entry, success, and errors at DEBUG or ERROR.
            - Sensitive information is never logged.

        Note:
            Uses asyncio.to_thread for non-blocking operation.
        """
        _LOGGER.debug(
            "[CorePlusAuthClient] Creating service token for service '%s' (username='%s', duration=%ds)...",
            service,
            username or "[current user]",
            duration_seconds,
        )
        try:
            result = await asyncio.to_thread(
                self.wrapped.create_token, service, username, duration_seconds, timeout
            )
            _LOGGER.debug(
                "[CorePlusAuthClient] Service token for '%s' created successfully.",
                service,
            )
            return CorePlusToken(result)
        except ConnectionError as e:
            _LOGGER.error("[CorePlusAuthClient:create_token] Failed to connect to authentication service: %s", e)
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                "[CorePlusAuthClient] Service token creation failed for '%s': %s",
                service,
                e,
            )
            raise AuthenticationError(f"Token creation failed: {e}") from e

    async def close(self) -> None:
        """Close the authentication client asynchronously.

        This method gracefully closes the connection to the authentication service and releases associated resources.
        After calling this method, the client should not be used for further operations.

        It's important to close the client to prevent resource leaks, especially in long-running applications.

        Raises:
            DeephavenConnectionError: If a network or connection error occurs during close.
            AuthenticationError: If an authentication-related error occurs during close (e.g., invalid or expired session).

        Logging:
            - Logs entry, success, and errors at DEBUG or ERROR.

        Note:
            Even if an exception is raised, the client is considered closed and should not be reused.
            Uses asyncio.to_thread for non-blocking operation.
        """
        _LOGGER.debug(
            "[CorePlusAuthClient] Closing authentication client connection..."
        )
        try:
            await asyncio.to_thread(self.wrapped.close)
            _LOGGER.debug(
                "[CorePlusAuthClient] Authentication client connection closed."
            )
        except ConnectionError as e:
            _LOGGER.error("[CorePlusAuthClient:close] Connection error while closing auth client: %s", e)
            raise DeephavenConnectionError(
                f"Connection error while closing authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error("[CorePlusAuthClient:close] Error closing auth client connection: %s", e)
            raise AuthenticationError(
                f"Failed to close authentication connection: {e}"
            ) from e
