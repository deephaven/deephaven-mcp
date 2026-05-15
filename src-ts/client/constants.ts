/**
 * Timeout constants for the Deephaven client API.
 *
 * Most timeout values are in seconds (number). Each constant can be overridden
 * at process startup by setting the corresponding environment variable.
 * The environment variable must be parseable as the constant's declared type;
 * invalid values throw at module load time.
 */

import { envFloat, envInt } from "../env.js";

/**
 * Timeout (seconds) for establishing the initial connection to a Deephaven server.
 *
 * Default for the three initial-connection entry points:
 * - `CorePlusSessionFactory.fromUrl` — TCP/TLS handshake plus connection.json retrieval.
 * - `CorePlusSessionFactory.fromConfig` — constructor and controller subscription.
 * - `CoreSession.fromConfig` — pydeephaven.Session constructor.
 *
 * Environment variable override: DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS
 */
export const SESSION_CONNECT_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS",
  60.0,
);

/**
 * Timeout (seconds) for subscribing to controller state updates.
 *
 * Covers the time for the controller to deliver its initial PQ state snapshot.
 * Increase this if the controller manages a very large number of persistent queries.
 * Environment variable override: DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS
 */
export const SUBSCRIBE_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS",
  30.0,
);

/**
 * Timeout (seconds) for opening a session to a running persistent query worker.
 *
 * Distinct from SESSION_CONNECT_TIMEOUT_SECONDS, which covers the initial server
 * connection; this covers the worker-level connection after the PQ is already running.
 * Environment variable override: DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS
 */
export const PQ_CONNECTION_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS",
  60.0,
);

/**
 * Timeout (seconds) for provisioning and connecting to a new on-demand worker.
 *
 * Covers JVM startup plus the initial connection handshake. Increase this on
 * systems where worker startup is slow or resources are contended.
 * Environment variable override: DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS
 */
export const WORKER_CREATION_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS",
  60.0,
);

/**
 * Timeout (seconds) for standard authentication operations (password, private_key).
 *
 * Covers credential exchange with the server. See SAML_AUTH_TIMEOUT_SECONDS for
 * the longer timeout used when browser interaction is required.
 * Environment variable override: DH_MCP_AUTH_TIMEOUT_SECONDS
 */
export const AUTH_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_AUTH_TIMEOUT_SECONDS",
  60.0,
);

/**
 * Timeout (seconds) for SAML authentication.
 *
 * Longer than AUTH_TIMEOUT_SECONDS to accommodate the browser redirect roundtrip
 * that SAML requires before the server can complete the handshake.
 * Environment variable override: DH_MCP_SAML_AUTH_TIMEOUT_SECONDS
 */
export const SAML_AUTH_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_SAML_AUTH_TIMEOUT_SECONDS",
  120.0,
);

/**
 * Timeout (seconds) for persistent query management operations (add, delete, modify).
 *
 * Default for `ControllerClient.addQuery` / `deleteQuery` / `modifyQuery`;
 * covers the controller round-trip to register, remove, or update a PQ definition.
 * Does not cover waiting for a worker to reach a target state.
 * Environment variable override: DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS
 */
export const PQ_MANAGEMENT_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS",
  60.0,
);

/**
 * Timeout (seconds) for lightweight network round-trips (ping, key management).
 *
 * Kept short (5s default) because these calls should complete near-instantly;
 * a timeout here typically indicates a connectivity problem rather than slow work.
 * Environment variable override: DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS
 */
export const QUICK_OPERATION_TIMEOUT_SECONDS: number = envFloat(
  "DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS",
  5.0,
);

/**
 * Timeout (integer seconds) for waiting on persistent query state transitions.
 *
 * Covers the time from issuing a start or restart to the worker reaching its target
 * state (e.g. RUNNING). Increase this for PQs with large heaps or slow init scripts.
 *
 * This constant is an integer because it is forwarded verbatim to
 * `ControllerClient.start_and_wait` / `stop_and_wait`, whose typed stubs
 * declare `timeout_seconds: int`. Fractional env-var values
 * (e.g. `DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS=120.5`) throw at module load time.
 *
 * Environment variable override: DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS
 */
export const PQ_STATE_CHANGE_TIMEOUT_SECONDS: number = envInt(
  "DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS",
  120,
);

/**
 * Sentinel value (0s, number) for subscription-map lookups that should not block.
 *
 * Default for `ControllerClient.get` and `ControllerClient.getSerialForName`,
 * which look up persistent queries in the controller's local subscription map.
 * A value of `0` instructs those methods to raise immediately if the requested
 * query is not already present in the map.
 * Environment variable override: DH_MCP_NO_WAIT_SECONDS
 */
export const NO_WAIT_SECONDS: number = envFloat("DH_MCP_NO_WAIT_SECONDS", 0.0);
