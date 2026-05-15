/**
 * HTTP header names that make up the MCP auth wire protocol.
 *
 * Single source of truth for every custom `X-Deephaven-*` header the
 * auth backends recognise on incoming requests. These constants are
 * **public**: the server advertises the same names back to clients in
 * the `WWW-Authenticate` challenge on `401` responses, so they are
 * part of the external contract, not an internal implementation detail.
 *
 * External consumers (MCP client SDKs, integration tests, CLI tools)
 * should import these constants from `auth/backends` rather than
 * hard-coding the strings, so that any future rename happens at exactly
 * one site. The module file itself stays leading-underscore
 * (`_headers.py` / `headers.ts`) to match the package's convention of
 * private submodules with publicly re-exported symbols.
 *
 * All values are lowercase because the ASGI middleware lowercases header
 * names before dispatching to backends. The `.toLowerCase()` happens at
 * the edge; backend code compares against the lowercase forms defined here.
 * Clients are free to send any case on the wire (HTTP header names are
 * case-insensitive per RFC 7230).
 *
 * This module contains no imports from sibling modules, no logic, and no
 * classes — only string constants — so it can be imported freely from
 * any backend module without introducing circular dependencies.
 */

/** Lowercase header name carrying the username. Shared by backends that authenticate per-user. */
export const HEADER_USERNAME = "x-deephaven-username";

/** Lowercase header name carrying the password (used by PasswordBackend). */
export const HEADER_PASSWORD = "x-deephaven-password";

/** Lowercase header name carrying the optional "operate as" username (used by PasswordBackend when allowEffectiveUser=true). */
export const HEADER_EFFECTIVE_USER = "x-deephaven-effective-user";

/** Lowercase header name carrying the base64-encoded private-key file (used by PrivateKeyBackend). */
export const HEADER_PRIVATE_KEY = "x-deephaven-private-key";

/** Lowercase header name carrying the pre-shared key (used by PSKBackend). */
export const HEADER_PSK = "x-deephaven-psk";
