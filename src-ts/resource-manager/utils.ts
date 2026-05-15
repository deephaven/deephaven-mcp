/**
 * Utility functions for dynamic Deephaven Community session management.
 *
 * This module provides low-level utilities for dynamically launched Deephaven sessions:
 *
 * - **Port allocation**: Find available TCP ports for session binding
 * - **Authentication token generation**: Create secure PSK (pre-shared key) tokens
 *
 * These utilities are primarily used by session launchers but can be imported independently
 * for custom workflows requiring dynamic port assignment or token generation.
 *
 * Note:
 *   These utilities are specific to Community sessions that are launched dynamically.
 *   Static (pre-configured) sessions use ports and tokens from configuration files.
 */

import * as net from "node:net";
import * as crypto from "node:crypto";
import pino from "pino";
import { SessionLaunchError } from "../exceptions.js";

const _logger = pino({ name: "deephaven-mcp:resource-manager/utils" });

/**
 * Find an available TCP port on localhost for session binding.
 *
 * Uses the OS to assign an available port by binding to port 0. The OS automatically
 * selects an available port from the ephemeral port range. This is the recommended
 * approach for avoiding port conflicts in dynamic session creation.
 *
 * @returns An available port number assigned by the OS.
 * @throws {SessionLaunchError} If unable to find an available port due to socket errors
 *   or system resource limitations.
 *
 * @example
 * ```typescript
 * const port = await findAvailablePort();
 * console.log(`Using port: ${port}`);
 * ```
 */
export function findAvailablePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : null;
      server.close(() => {
        if (port === null) {
          const err = new SessionLaunchError("Failed to find available port: no address assigned");
          _logger.error(`[utils:findAvailablePort] ${err.message}`);
          reject(err);
        } else {
          _logger.debug(`[utils:findAvailablePort] Found available port: ${port}`);
          resolve(port);
        }
      });
    });
    server.on("error", (e: Error) => {
      _logger.error(`[utils:findAvailablePort] Failed to find available port: ${e.message}`);
      reject(new SessionLaunchError(`Failed to find available port: ${e.message}`));
    });
  });
}

/**
 * Generate a cryptographically secure authentication token for PSK auth.
 *
 * Uses `crypto.randomBytes()` to generate a 32-character hexadecimal token
 * (16 bytes of randomness). This provides sufficient entropy for secure pre-shared key
 * (PSK) authentication in dynamically launched Deephaven Community sessions.
 *
 * The token is suitable for use with Deephaven's PSK authentication handler and should be
 * kept confidential. Never log this token.
 *
 * @returns A 32-character hexadecimal authentication token (lowercase).
 *   Format: [0-9a-f]{32}
 *
 * @example
 * ```typescript
 * const token = generateAuthToken();
 * // token is 32 hex chars, 128 bits of entropy
 * ```
 */
export function generateAuthToken(): string {
  const token = crypto.randomBytes(16).toString("hex");
  _logger.debug("[utils:generateAuthToken] Generated new auth token");
  return token;
}
