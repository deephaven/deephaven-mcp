/**
 * Single source of truth for the liveness/readiness probe path.
 *
 * Two MCP servers register a GET handler at {@link HEALTH_PATH}:
 *
 * - The systems server. Because this server mounts TLS and authentication
 *   middleware, its startup code also lists {@link HEALTH_PATH} in bypass paths
 *   so that probes succeed regardless of peer, scheme, or credentials.
 * - The docs server. This server does not mount any auth/TLS middleware,
 *   so no bypass list is needed.
 *
 * Defining the constant here — outside of any middleware module — keeps
 * the auth/middleware layer free of application-route knowledge while
 * still letting both servers (and their tests) import a single canonical value.
 */

/**
 * Canonical liveness/readiness probe path (`string`).
 *
 * The string includes a single leading slash and no trailing slash so it
 * matches exact-match path routing. Treat as immutable.
 */
export const HEALTH_PATH: string = "/health";
