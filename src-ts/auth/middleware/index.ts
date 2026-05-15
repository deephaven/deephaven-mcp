/**
 * Node.js middleware integration for the `auth` framework.
 *
 * This subpackage adapts the pure-function backend chain
 * ({@link module:auth/backends}) to Express/Node.js by exposing two
 * middlewares that the MCP servers compose in front of the FastMCP HTTP app:
 *
 * - {@link AuthenticationMiddleware} reads HTTP headers, runs the configured
 *   backend chain, and on success attaches the resulting {@link Principal} and
 *   {@link Credentials} to the request object. On failure it short-circuits
 *   with `401 Unauthorized`.
 * - {@link TlsEnforcementMiddleware} decides whether the request's *transport*
 *   is acceptable to carry the auth headers' secrets. It rejects cleartext
 *   non-loopback traffic with `426 Upgrade Required`.
 *
 * In production the servers mount the TLS layer **outermost** and the auth
 * layer immediately inside it.
 */

export {
  SCOPE_KEY_PRINCIPAL,
  SCOPE_KEY_CREDENTIALS,
  AuthenticationMiddleware,
  AuthenticationError,
  _lowerHeaders,
  _send401,
} from "./middleware.js";

export type { AuthenticatedRequest, MiddlewareFn, NextFunction } from "./middleware.js";

export {
  TlsEnforcementMiddleware,
  parseForwardedAllowIps,
  createTransportSecurityPolicy,
  _extractPeerIp,
  _isLoopback,
  _peerInAllowlist,
  _ipInCidr,
  _lastForwardedProto,
  _send426,
} from "./tls.js";

export type { TransportSecurityPolicy } from "./tls.js";
