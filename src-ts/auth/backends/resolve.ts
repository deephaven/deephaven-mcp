/**
 * Pure-function credential resolver reusable outside the MCP middleware.
 *
 * The middleware (`AuthenticationMiddleware`) is the normal path used by
 * the streamable-HTTP servers, but some callers have a mapping of headers
 * and no HTTP request: for example a future CLI that talks to the same
 * backends directly, or unit tests that want to exercise a backend chain
 * without spinning up Starlette/Express. Those callers use
 * {@link authenticateAndResolve}, which implements the same
 * "first-backend-wins" logic as the middleware over a plain object of headers.
 */

import { Credentials, Principal } from "../credentials/index.js";
import { AuthBackend, AuthenticationError } from "./base.js";

/**
 * Run `backends` against `headers` and return the first match.
 *
 * Applies the same "first backend to return a {@link Principal} wins"
 * rule as the authentication middleware. If a backend throws
 * {@link AuthenticationError}, that error is re-thrown immediately
 * (remaining backends are not tried); this matches the middleware's
 * short-circuit behavior.
 *
 * `headers` is converted to a lowercase-key mapping before each backend
 * is called, so callers may pass headers with any casing.
 *
 * @param backends - The backends to try, in order.
 * @param headers - The request headers to authenticate.
 * @returns The authenticated principal and the credentials derived from
 *   it by the matching backend.
 * @throws {AuthenticationError} If any backend rejected the request, if
 *   no backend produced a principal (the message lists the backends that
 *   were tried), or if `backends` is empty (indicating a server-side
 *   misconfiguration).
 */
export async function authenticateAndResolve(
  backends: AuthBackend[],
  headers: Record<string, string>,
): Promise<[Principal, Credentials]> {
  if (backends.length === 0) {
    throw new AuthenticationError(
      "No authentication backends are configured on this server.",
    );
  }
  const lowered: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    lowered[k.toLowerCase()] = v;
  }
  for (const backend of backends) {
    const principal = await backend.authenticate(lowered);
    if (principal !== undefined) {
      const credentials = await backend.deriveCredentials(principal, lowered);
      return [principal, credentials];
    }
  }
  const tried = backends.map((b) => b.name).join(", ");
  throw new AuthenticationError(
    `No registered authentication backend accepted the supplied headers (tried: ${tried}).`,
  );
}
