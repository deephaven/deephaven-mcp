"""Asynchronous Deephaven authentication client wrapper for MCP.

This module provides an async interface to the Deephaven AuthClient, enabling non-blocking token
management for Deephaven services. It is primarily used by the CorePlusSessionFactory and related
components that require authentication with Deephaven Enterprise servers.

Key Features:
    - Converts blocking AuthClient operations to async using asyncio.to_thread for event loop safety.
    - Provides async methods for service token retrieval.
    - Ensures sensitive information (tokens, passwords) is never logged.
    - Consistent and detailed logging for entry, success, and error events.

Classes:
    CorePlusAuthClient: Main async wrapper for deephaven_enterprise.client.auth.AuthClient that provides
        asynchronous service token retrieval capabilities.

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
    from deephaven_mcp.client import CorePlusSessionFactory

    async def token_example():
        factory = await CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")
        await factory.password("username", "password")
        auth_client = factory.auth_client
        service_token = await auth_client.get_token("PersistentQueryController")
        # Use the token with other services
"""

import asyncio
import logging

from deephaven_enterprise.client.auth import AuthClient

from deephaven_mcp._exceptions import AuthenticationError, DeephavenConnectionError

from ._base import ClientObjectWrapper
from ._protobuf import CorePlusToken
from ._timeouts import EnterpriseClientTimeouts

_LOGGER = logging.getLogger(__name__)


class CorePlusAuthClient(ClientObjectWrapper[AuthClient]):
    """
    Asynchronous wrapper for the Deephaven AuthClient, providing non-blocking token management.

    This class wraps a synchronous Deephaven AuthClient and exposes async methods for service
    token retrieval. All blocking operations are executed in threads using asyncio.to_thread to
    preserve event loop responsiveness and prevent I/O operations from blocking the main asyncio
    event loop.

    Typical Usage:
        - Instantiate via CorePlusSessionFactory (not directly).
        - Obtain service-specific tokens for downstream authentication.
        - Pass tokens to other client components that need authentication.

    Event Loop Safety:
        - All network and I/O operations are offloaded to threads using asyncio.to_thread.
        - Error handling preserves the original stack trace while converting to MCP-specific exceptions.
        - No synchronous blocking calls are made directly from async contexts.

    Logging:
        - Logs entry, success, and error for all token operations at DEBUG or ERROR level.
        - Sensitive information (tokens, passwords) is never logged.
        - Error paths include detailed context to aid troubleshooting.

    Example:
        import asyncio
        from deephaven_mcp.client import CorePlusSessionFactory

        async def token_example():
            factory = await CorePlusSessionFactory.from_url("https://myserver.example.com/iris/connection.json")
            await factory.password("username", "password")
            auth_client = factory.auth_client
            service_token = await auth_client.get_token("PersistentQueryController")
            # Use the token with other services
    """

    def __init__(
        self,
        auth_client: AuthClient,
        timeouts: EnterpriseClientTimeouts,
    ) -> None:
        """Initialize CorePlusAuthClient with a synchronous AuthClient instance.

        Args:
            auth_client (deephaven_enterprise.client.auth.AuthClient): The synchronous Deephaven AuthClient instance to wrap.
            timeouts: Operator-tuned timeout schema; ``get_token`` uses
                ``timeouts.client.auth_timeout_seconds`` to bound the upstream RPC.

        Note:
            This constructor is intended for use by CorePlusSessionFactory. Users should not instantiate
            this class directly.
        """
        super().__init__(auth_client)
        self._timeouts = timeouts
        _LOGGER.debug("[CorePlusAuthClient] Initialized")

    async def get_token(self, service: str) -> CorePlusToken:
        """Get a service-specific authentication token asynchronously.

        This method obtains a single-use token for a specific Deephaven service (e.g.,
        PersistentQueryController, JavaScriptClient, Console). Service tokens are typically
        used for inter-service authentication and are consumed by the authentication server
        during the verification process.

        The request is bounded by ``EnterpriseClientTimeouts.auth_timeout_seconds`` via
        :func:`asyncio.wait_for`; if the upstream RPC does not return within that
        budget the call is cancelled and a :class:`DeephavenConnectionError` is
        raised with a hint to tune the operator JSON config.

        Args:
            service (str): Name of the target service. Must be recognized by the Deephaven authentication service.
                Valid service types include: "PersistentQueryController", "JavaScriptClient", "Console", "ApiGateway".

        Returns:
            CorePlusToken: Token scoped to the requested service. This is a wrapper around the native
                Deephaven token object with additional properties and serialization capabilities
                for use with other Deephaven Enterprise clients.

        Raises:
            DeephavenConnectionError: If the call exceeds
                ``timeouts.client.auth_timeout_seconds``, or if a Python-level
                ``ConnectionError`` is raised while dispatching the call
                (uncommon; most upstream gRPC failures are reported as
                ``AuthenticationError`` instead, see below).
            AuthenticationError: If token retrieval fails for any other reason. This is the
                catch-all for upstream ``grpc.RpcError`` (network issues, server unavailability,
                TLS/certificate errors, gRPC timeouts), authorization failures (invalid
                credentials, insufficient permissions, invalid service name), rate limiting,
                and internal auth server errors.

        Logging:
            - Logs entry at DEBUG level with service name.
            - Logs success at DEBUG level with service name.
            - Logs errors at ERROR level with service name and error details.
            - Sensitive information (tokens, passwords) is never logged.

        Note:
            Uses asyncio.to_thread for non-blocking operation to ensure the main event loop
            remains responsive even during authentication operations.

        Example:
            # Get a single-use token for PersistentQueryController
            token = await auth_client.get_token(service="PersistentQueryController")
            # ``token`` is a CorePlusToken; pass it to APIs that accept Deephaven service tokens.
        """
        timeout_seconds = self._timeouts.auth_timeout_seconds
        _LOGGER.debug(
            f"[CorePlusAuthClient:get_token] Getting service token for service='{service}'"
        )
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(self.wrapped.get_token, service),
                timeout=timeout_seconds,
            )
            _LOGGER.debug(
                f"[CorePlusAuthClient:get_token] Service token for '{service}' obtained successfully"
            )
            return CorePlusToken(result)
        except TimeoutError:
            _LOGGER.error(
                f"[CorePlusAuthClient:get_token] Service token retrieval for '{service}' "
                f"timed out after {timeout_seconds}s. "
                f"Increase enterprise/settings.json: timeouts.client.auth_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Service token retrieval for '{service}' timed out after "
                f"{timeout_seconds} seconds. The authentication server may be "
                f"overloaded or unreachable. To allow more time, increase "
                f"enterprise/settings.json: timeouts.client.auth_timeout_seconds "
                f"in the operator config."
            ) from None
        except ConnectionError as e:
            _LOGGER.error(
                "[CorePlusAuthClient:get_token] Failed to connect to authentication "
                "service for %r: %r",
                service,
                e,
                exc_info=True,
            )
            raise DeephavenConnectionError(
                f"Unable to connect to authentication service: {e}"
            ) from e
        except Exception as e:
            _LOGGER.error(
                "[CorePlusAuthClient:get_token] Service token retrieval failed "
                "for %r: %r",
                service,
                e,
                exc_info=True,
            )
            raise AuthenticationError(f"Token retrieval failed: {e}") from e
