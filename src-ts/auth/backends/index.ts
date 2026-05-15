/**
 * Authentication backend abstract base class, concrete backends, and chain runner.
 *
 * This subpackage contains the **mechanism layer** of the `auth` framework:
 *
 * - {@link AuthBackend} — the abstract base class every backend inherits from,
 *   plus the {@link AuthenticationError} raised on invalid credentials.
 * - {@link authenticateAndResolve} — the pure-function chain runner used by the
 *   middleware and by non-HTTP callers (e.g. a future CLI, tests).
 * - Concrete backends ({@link PSKBackend}, {@link PasswordBackend},
 *   {@link PrivateKeyBackend}) — pluggable implementations registered by each
 *   MCP server at startup.
 * - `HEADER_*` constants — the lowercase HTTP header names that make up the MCP
 *   auth wire protocol. External consumers (client SDKs, integration tests, CLI
 *   tools) should import these rather than hard-coding the strings.
 *
 * Depends on `auth/credentials` for the types it produces; never imports from
 * `auth/middleware`.
 */

export { AuthBackend, AuthenticationError } from "./base.js";
export {
  HEADER_EFFECTIVE_USER,
  HEADER_PASSWORD,
  HEADER_PRIVATE_KEY,
  HEADER_PSK,
  HEADER_USERNAME,
} from "./headers.js";
export { PasswordBackend } from "./password.js";
export { PrivateKeyBackend } from "./private-key.js";
export { PSKBackend } from "./psk.js";
export { authenticateAndResolve } from "./resolve.js";
