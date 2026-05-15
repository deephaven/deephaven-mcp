/**
 * Asynchronous Deephaven authentication client wrapper for MCP.
 *
 * This module provides an async interface to the Deephaven AuthClient, enabling non-blocking token
 * management for Deephaven services. It is primarily used by the CorePlusSessionFactory and related
 * components that require authentication with Deephaven Enterprise servers.
 *
 * Key Features:
 *   - Provides async methods for service token retrieval.
 *   - Ensures sensitive information (tokens, passwords) is never logged.
 *   - Consistent and detailed logging for entry, success, and error events.
 *
 * Classes:
 *   {@link CorePlusAuthClient}: Main async wrapper for DHE auth client that provides
 *     asynchronous service token retrieval capabilities.
 *
 * Types:
 *   {@link CorePlusToken}: A wrapper around Deephaven's native token objects with additional
 *     property access capabilities for MCP interoperability.
 *
 * Service Token Usage:
 *   Service tokens are specialized authentication tokens with limited permissions scoped to specific
 *   Deephaven service components. Common service types include:
 *   - "PersistentQueryController": For query API operations
 *   - "JavaScriptClient": For web client access
 *   - "Console": For Deephaven console operations
 *
 * @example
 * ```typescript
 * import { CorePlusSessionFactory } from "./session-factory.js";
 *
 * async function tokenExample() {
 *   const factory = await CorePlusSessionFactory.fromUrl("https://myserver.example.com/iris/connection.json");
 *   await factory.password("username", "password");
 *   const authClient = factory.authClient;
 *   const serviceToken = await authClient.getToken("PersistentQueryController");
 *   // Use the token with other services
 * }
 * ```
 */

import pino from "pino";
import { AuthenticationError, DeephavenConnectionError } from "../exceptions.js";
import { ClientObjectWrapper } from "./base.js";
import { CorePlusToken } from "./protobuf.js";

const _logger = pino({ name: "deephaven-mcp:client/auth-client" });

/**
 * DHE auth client interface — the DHE JS API auth client contract.
 * The actual object is loaded at runtime from the DHE server.
 */
export interface DheAuthClient {
  getToken(service: string, timeoutSeconds?: number | null): Promise<Record<string, unknown>>;
}

/**
 * Asynchronous wrapper for the Deephaven AuthClient, providing non-blocking token management.
 *
 * This class wraps a DHE auth client and exposes async methods for service
 * token retrieval. All blocking operations are executed asynchronously to
 * preserve event loop responsiveness.
 *
 * Typical Usage:
 *   - Instantiate via CorePlusSessionFactory (not directly).
 *   - Obtain service-specific tokens for downstream authentication.
 *   - Pass tokens to other client components that need authentication.
 *
 * Logging:
 *   - Logs entry, success, and error for all token operations at DEBUG or ERROR level.
 *   - Sensitive information (tokens, passwords) is never logged.
 *   - Error paths include detailed context to aid troubleshooting.
 *
 * @example
 * ```typescript
 * const authClient = factory.authClient;
 * const serviceToken = await authClient.getToken("PersistentQueryController");
 * ```
 */
export class CorePlusAuthClient extends ClientObjectWrapper<DheAuthClient> {
  /**
   * Initialize CorePlusAuthClient with a DHE auth client instance.
   *
   * @param authClient - The DHE auth client instance to wrap.
   */
  constructor(authClient: DheAuthClient) {
    super(authClient, true);
    _logger.debug("[CorePlusAuthClient] Initialized");
  }

  /**
   * Get a service-specific authentication token asynchronously.
   *
   * This method obtains a single-use token for a specific Deephaven service (e.g.,
   * PersistentQueryController, JavaScriptClient, Console). Service tokens are typically
   * used for inter-service authentication.
   *
   * @param service - Name of the target service. Must be recognized by the Deephaven authentication service.
   *   Valid service types include: "PersistentQueryController", "JavaScriptClient", "Console", "ApiGateway".
   * @param timeoutSeconds - Timeout in seconds for the token request.
   *   If `undefined`, uses the client's default timeout. The timeout
   *   applies to the entire operation including network communication.
   * @returns Token scoped to the requested service.
   * @throws {DeephavenConnectionError} If a connection error is raised while dispatching the call.
   * @throws {AuthenticationError} If token retrieval fails for any other reason.
   *
   * @example
   * ```typescript
   * // Get a single-use token for PersistentQueryController
   * const token = await authClient.getToken("PersistentQueryController");
   * ```
   */
  async getToken(
    service: string,
    timeoutSeconds?: number | null,
  ): Promise<CorePlusToken> {
    _logger.debug(
      `[CorePlusAuthClient:getToken] Getting service token for service='${service}' (timeoutSeconds=${timeoutSeconds})`,
    );
    try {
      const result = await this.wrapped.getToken(service, timeoutSeconds ?? undefined);
      _logger.debug(
        `[CorePlusAuthClient:getToken] Service token for '${service}' obtained successfully`,
      );
      return new CorePlusToken(result);
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusAuthClient:getToken] Failed to connect to authentication service for '${service}': ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to authentication service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusAuthClient:getToken] Service token retrieval failed for '${service}': ${err.message}`,
      );
      throw new AuthenticationError(`Token retrieval failed: ${err.message}`);
    }
  }
}
