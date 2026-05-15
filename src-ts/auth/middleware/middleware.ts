/**
 * Express/Node.js middleware enforcing {@link AuthBackend}-based authentication.
 *
 * Mounted by the streamable-HTTP MCP servers in front of the FastMCP
 * Express app. For every incoming HTTP request:
 *
 * 1. Lowercase the request headers into a plain `Record<string, string>`.
 * 2. Delegate to {@link authenticateAndResolve}, which walks the registered
 *    backends in order and returns the first {@link Principal} (and derived
 *    {@link Credentials}) produced.
 * 3. On success, attach the principal and credentials to the request object
 *    under the keys {@link SCOPE_KEY_PRINCIPAL} and
 *    {@link SCOPE_KEY_CREDENTIALS}, then pass the request through to the
 *    inner handler.
 * 4. On failure (any backend raised {@link AuthenticationError}, or no
 *    backend produced a principal), short-circuit with a `401 Unauthorized`
 *    response whose `WWW-Authenticate` header advertises every registered
 *    backend's challenge and whose JSON body's `detail` field carries the
 *    resolver's error message.
 *
 * **Bypass paths**: Some routes MUST be reachable without credentials (for
 * example the `/.well-known/oauth-protected-resource` URL defined by the
 * MCP 2025-06-18 auth spec). The `bypassPaths` parameter holds an
 * exact-match set of such paths; requests whose path is in the set are
 * allowed through with no principal attached.
 */

import type { IncomingMessage, ServerResponse } from "node:http";
import { Credentials, Principal } from "../credentials/index.js";
import { AuthBackend, AuthenticationError } from "../backends/base.js";
import { authenticateAndResolve } from "../backends/resolve.js";

export { AuthenticationError };

/** Request context key under which an authenticated {@link Principal} is attached. */
export const SCOPE_KEY_PRINCIPAL = "deephaven_mcp.principal";

/** Request context key under which the backend-derived {@link Credentials} are attached. */
export const SCOPE_KEY_CREDENTIALS = "deephaven_mcp.credentials";

/** Extended Node.js IncomingMessage with MCP auth context. */
export interface AuthenticatedRequest extends IncomingMessage {
  [SCOPE_KEY_PRINCIPAL]?: Principal;
  [SCOPE_KEY_CREDENTIALS]?: Credentials;
}

/** Express-compatible middleware handler signature. */
export type NextFunction = (err?: unknown) => void;

/** Express-compatible middleware function type. */
export type MiddlewareFn = (
  req: IncomingMessage,
  res: ServerResponse,
  next: NextFunction,
) => void | Promise<void>;

/**
 * Express/Node.js middleware that runs registered backends against each request.
 *
 * For usage details see the module docstring.
 */
export class AuthenticationMiddleware {
  /** The registered backends, in order. */
  readonly backends: readonly AuthBackend[];

  /** Exact paths that bypass authentication. */
  readonly bypassPaths: ReadonlySet<string>;

  /**
   * @param backends - Backends to try on each request, in order. Must be non-empty.
   * @param options - Optional configuration.
   * @param options.bypassPaths - Exact paths to allow through without authentication.
   *   Defaults to the empty set.
   * @throws {Error} If `backends` is empty (a middleware with no backends would
   *   reject every request and is always a configuration bug).
   */
  constructor(
    backends: AuthBackend[],
    options?: { bypassPaths?: Set<string> },
  ) {
    if (backends.length === 0) {
      throw new Error("AuthenticationMiddleware requires at least one backend.");
    }
    this.backends = Object.freeze([...backends]);
    this.bypassPaths = options?.bypassPaths ?? new Set<string>();
  }

  /**
   * Returns an Express-compatible middleware function.
   *
   * The returned function authenticates each incoming request. On success,
   * the principal and credentials are attached to the request object under
   * {@link SCOPE_KEY_PRINCIPAL} and {@link SCOPE_KEY_CREDENTIALS} respectively,
   * and `next()` is called. On failure, a `401 Unauthorized` response is sent.
   *
   * @returns An Express-compatible middleware function.
   */
  handler(): MiddlewareFn {
    return async (req: IncomingMessage, res: ServerResponse, next: NextFunction) => {
      const url = (req as { url?: string }).url ?? "";
      const path = url.split("?")[0] ?? "";

      if (this.bypassPaths.has(path)) {
        next();
        return;
      }

      const headers = _lowerHeaders(req.headers as Record<string, string | string[] | undefined>);

      let principal: Principal;
      let credentials: Credentials;
      try {
        [principal, credentials] = await authenticateAndResolve(
          this.backends as AuthBackend[],
          headers,
        );
      } catch (exc) {
        if (exc instanceof AuthenticationError) {
          await _send401(res, this.backends as AuthBackend[], exc.message);
          return;
        }
        next(exc);
        return;
      }

      (req as AuthenticatedRequest)[SCOPE_KEY_PRINCIPAL] = principal;
      (req as AuthenticatedRequest)[SCOPE_KEY_CREDENTIALS] = credentials;
      next();
    };
  }
}

/**
 * Convert Node.js request headers to a lowercase-keyed `Record<string, string>`.
 *
 * Later values for the same header silently overwrite earlier ones; this matches
 * how most authentication headers are treated (and the tiny number of headers
 * the auth layer cares about never legitimately appear twice).
 *
 * @param headers - The raw Node.js headers object.
 * @returns A lowercase-keyed dictionary of header values.
 */
export function _lowerHeaders(
  headers: Record<string, string | string[] | undefined>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    // Node.js may return arrays for multi-value headers; take the last value
    const normalized = Array.isArray(value) ? value[value.length - 1]! : value;
    out[name.toLowerCase()] = normalized;
  }
  return out;
}

/**
 * Emit a compact JSON `401 Unauthorized` response.
 *
 * @param res - The Node.js ServerResponse to write to.
 * @param backends - The backends whose challenges to advertise.
 * @param detail - The error detail message.
 */
export async function _send401(
  res: ServerResponse,
  backends: readonly AuthBackend[],
  detail: string,
): Promise<void> {
  const challenges = backends.map((b) => b.challenge()).join(", ");
  const body = JSON.stringify({ error: "unauthorized", detail });
  const bodyBytes = Buffer.from(body, "utf-8");

  res.writeHead(401, {
    "content-type": "application/json",
    "www-authenticate": challenges,
    "content-length": String(bodyBytes.length),
  });
  res.end(bodyBytes);
}
