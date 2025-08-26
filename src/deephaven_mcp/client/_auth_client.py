"""Asynchronous Deephaven authentication client wrapper for MCP.

This module provides an async interface to the Deephaven AuthClient, enabling non-blocking token
management for Deephaven services. It is primarily used by the CorePlusSessionManager and related
components that require authentication with Deephaven Enterprise servers.

Key Features:
    - Converts blocking AuthClient operations to async using asyncio.to_thread for event loop safety.
    - Provides async methods for service token creation.
    - Ensures sensitive information is never logged; only usernames are logged at DEBUG/INFO levels.
    - Consistent and detailed logging for entry, success, and error events.

Classes:
    CorePlusAuthClient: Main async wrapper for deephaven_enterprise.client.auth.AuthClient that provides
        asynchronous token creation capabilities.

Types:
    CorePlusToken: A wrapper around Deephaven's native token objects with additional serialization 
        and property access capabilities for MCP interoperability.

Service Token Usage:
    Service tokens are specialized authentication tokens with limited permissions scoped to specific
    Deephaven service components. Common service types include:
    - "PersistentQueryController": For query API operations
    - "JavaScriptClient": For web client access
    - "Console": For Deephaven console operations

Example:
    import asyncio
    from deephaven_mcp.client import CorePlusSessionManager

    async def token_example():
        manager = CorePlusSessionManager.from_url("https://myserver.example.com/connection.json")
        auth_client = manager.auth_client
        service_token = await auth_client.create_token("PersistentQueryController", duration_seconds=3600)
        controller = await manager.create_controller_client()
        # Use the token with the controller
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
    Asynchronous wrapper for the Deephaven AuthClient, providing non-blocking token management.

    This class wraps a synchronous Deephaven AuthClient and exposes async methods for token creation.
    All blocking operations are executed in threads using asyncio.to_thread to preserve event loop
    responsiveness and prevent I/O operations from blocking the main asyncio event loop.

    Typical Usage:
        - Instantiate via CorePlusSessionManager (not directly).
        - Create service-specific tokens for downstream authentication.
        - Pass tokens to other client components that need authentication.
        - Set appropriate token duration based on expected usage lifetime.
    
    Event Loop Safety:
        - All network and I/O operations are offloaded to threads using asyncio.to_thread.
        - Error handling preserves the original stack trace while converting to MCP-specific exceptions.
        - No synchronous blocking calls are made directly from async contexts.

    Logging:
        - Logs entry, success, and error for all token operations at DEBUG or ERROR level.
        - Usernames may be logged; sensitive information is never logged.
        - Error paths include detailed context to aid troubleshooting.

    Example:
        import asyncio
        from deephaven_mcp.client import CorePlusSessionManager

        async def token_example():
            manager = CorePlusSessionManager.from_url("https://myserver.example.com/connection.json")
            auth_client = manager.auth_client
            service_token = await auth_client.create_token("PersistentQueryController", duration_seconds=3600)
            controller = await manager.create_controller_client()
            # Use the token with the controller
    """

    def __init__(
        self, auth_client: "deephaven_enterprise.client.auth.AuthClient"  # noqa: F821
    ) -> None:
        """Initialize CorePlusAuthClient with a synchronous AuthClient instance.

        Args:
            auth_client (deephaven_enterprise.client.auth.AuthClient): The synchronous Deephaven AuthClient instance to wrap.

        Note:
            This constructor is intended for use by CorePlusSessionManager. Users should not instantiate
            this class directly.
        """
        super().__init__(auth_client, is_enterprise=True)
        _LOGGER.info("[CorePlusAuthClient] initialized")


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
                Valid service types include: "PersistentQueryController", "JavaScriptClient", "Console", "ApiGateway".
            username (str, optional): Username for whom to create the token. If empty, uses the currently authenticated user.
                Default is "" (empty string).
            duration_seconds (int, optional): Token validity period in seconds. Default is 3600 (1 hour).
                Consider shorter durations for security-sensitive operations and longer durations for
                long-running background processes.
            timeout (float | None, optional): Timeout in seconds for the token creation request. If None, 
                uses the client's default timeout. The timeout applies to the entire operation including
                network communication.

        Returns:
            CorePlusToken: Token scoped to the requested service. This is a wrapper around the native
                Deephaven token object with additional properties and serialization capabilities
                for use with other Deephaven Enterprise clients. The token contains the encoded JWT,
                expiration information, and scope details.

        Raises:
            DeephavenConnectionError: If unable to connect to the authentication service due to network issues, 
                server unavailability, TLS/certificate errors, or connection timeouts.
            AuthenticationError: If token creation fails due to authorization issues (invalid credentials),
                insufficient permissions, invalid service name, rate limiting, or internal auth server errors.

        Logging:
            - Logs entry at DEBUG level with service name, username (or [current user]), and duration.
            - Logs success at DEBUG level with service name.
            - Logs errors at ERROR level with service name and error details.
            - Sensitive information like tokens and passwords is never logged.

        Note:
            Uses asyncio.to_thread for non-blocking operation to ensure the main event loop
            remains responsive even during authentication operations.

        Example:
            # Create a token for PersistentQueryController with 24-hour validity
            token = await auth_client.create_token(
                service="PersistentQueryController", 
                duration_seconds=86400
            )
            # Use token with a controller
            controller = await session_manager.create_controller_client()
            await controller.set_auth_token(token)
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

