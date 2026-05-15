/**
 * Asynchronous wrapper for the Deephaven ControllerClient.
 *
 * This module provides an asynchronous wrapper around the Deephaven ControllerClient, enabling
 * non-blocking operations with the Persistent Query Controller in the Deephaven MCP environment.
 * It manages persistent queries and their state changes while maintaining the same interface as the
 * original ControllerClient.
 *
 * The Persistent Query Controller is a core component of Deephaven Enterprise responsible for:
 * - Creating and managing long-running query processes (workers)
 * - Monitoring query lifecycle and state changes
 * - Resource allocation and management for queries
 * - Query replication and fault tolerance
 *
 * Key features of this asynchronous wrapper:
 * 1. Full compatibility with modern async/await programming paradigms
 * 2. Non-blocking operations that won't stall the Node.js event loop
 * 3. Enhanced error handling with specific exception types for better diagnostics
 * 4. Consistent logging for operations and error conditions
 *
 * The controller client requires subscription initialization via subscribe() before query state
 * operations. When created through CorePlusSessionFactory, subscription is handled automatically
 * during factory initialization.
 *
 * Typical usage flow:
 * 1. Create query configurations and add queries
 * 2. Start queries and wait for them to reach the running state
 * 3. Monitor query status and handle state changes
 * 4. Stop, restart, or delete queries as needed
 *
 * Classes:
 *   {@link CorePlusControllerClient}: Async wrapper around the DHE JS API ControllerClient
 *
 * @example
 * ```typescript
 * import { CorePlusSessionFactory } from "./session-factory.js";
 *
 * async function controllerExample() {
 *   const factory = await CorePlusSessionFactory.fromUrl("https://myserver.example.com/iris/connection.json");
 *   await factory.password("username", "password");
 *   const controller = factory.controllerClient;
 *
 *   const config = await controller.makePqConfig("my-worker", 2);
 *   const serial = await controller.addQuery(config);
 *   await controller.startAndWait(serial);
 * }
 * ```
 */

import pino from "pino";
import {
  DeephavenConnectionError,
  InternalError,
  QueryError,
  ResourceError,
} from "../exceptions.js";
import { ClientObjectWrapper } from "./base.js";
import {
  CorePlusQueryConfig,
  CorePlusQueryInfo,
  CorePlusQuerySerial,
} from "./protobuf.js";
import {
  NO_WAIT_SECONDS,
  PQ_MANAGEMENT_TIMEOUT_SECONDS,
  PQ_STATE_CHANGE_TIMEOUT_SECONDS,
  QUICK_OPERATION_TIMEOUT_SECONDS,
  SUBSCRIBE_TIMEOUT_SECONDS,
} from "./constants.js";

const _logger = pino({ name: "deephaven-mcp:client/controller-client" });

/**
 * DHE controller client interface — the DHE JS API controller client contract.
 * The actual object is loaded at runtime from the DHE server.
 */
export interface DheControllerClient {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  ping(): Promise<boolean> | boolean;
  subscribe(): Promise<void> | void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  map(): Promise<Map<number, Record<string, any>>> | Map<number, Record<string, any>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  mapAndVersion(): Promise<[Map<number, Record<string, any>>, number]> | [Map<number, Record<string, any>>, number];
  getSerialForName(name: string, timeoutSeconds: number): Promise<number> | number;
  waitForChange(timeoutSeconds: number): Promise<void> | void;
  waitForChangeFromVersion(mapVersion: number, timeoutSeconds: number): Promise<boolean> | boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  get(serial: number, timeoutSeconds: number): Promise<Record<string, any>> | Record<string, any>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  addQuery(queryConfig: Record<string, any>): Promise<number> | number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  makeTemporaryConfig(
    name: string,
    heapSizeGb: number,
    server: string | null | undefined,
    extraJvmArgs: string[] | null | undefined,
    extraEnvironmentVars: string[] | null | undefined,
    engine: string,
    autoDeleteTimeout: number,
    adminGroups: string[] | null | undefined,
    viewerGroups: string[] | null | undefined,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  ): Promise<Record<string, any>> | Record<string, any>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  deleteQuery(serial: number): Promise<void> | void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  modifyQuery(config: Record<string, any>, restart: boolean): Promise<void> | void;
  restartQuery(serials: number | number[], timeoutSeconds: number | null | undefined): Promise<void> | void;
  startAndWait(serial: number, timeoutSeconds: number): Promise<void> | void;
  stopQuery(serials: number | number[], timeoutSeconds: number | null | undefined): Promise<void> | void;
  stopAndWait(serial: number, timeoutSeconds: number): Promise<void> | void;
}

/**
 * Validate a timeoutSeconds value. `undefined`/`null` are accepted; negatives are rejected.
 *
 * @param timeoutSeconds - Timeout value to validate.
 * @throws {ValueError} If timeoutSeconds is not null/undefined and is negative.
 */
export function _validateTimeout(timeoutSeconds: number | null | undefined): void {
  if (timeoutSeconds != null && timeoutSeconds < 0) {
    throw new RangeError(
      `timeout_seconds must be non-negative, got ${JSON.stringify(timeoutSeconds)}`,
    );
  }
}

/**
 * Default scheduling entries applied by {@link CorePlusControllerClient.makePqConfig} to
 * a *permanent* PQ (`autoDeleteTimeout=undefined`) when the caller passes `schedule=undefined`.
 * Produces a continuous scheduler that auto-starts the PQ after the controller accepts it.
 */
export const _DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING: readonly string[] = [
  "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerContinuous",
  "StartTime=00:00:00",
  "TimeZone=America/New_York",
  "DailyRestart=false",
  "StopTimeDisabled=true",
  "RestartErrorCount=0",
  "RestartErrorDelay=0",
  "RestartWhenRunning=Yes",
  "SchedulingDisabled=false",
];

/**
 * Asynchronous wrapper around the ControllerClient for managing persistent queries.
 *
 * This class provides an asynchronous interface to the ControllerClient, which connects to the
 * Deephaven PersistentQueryController process. It enables management of persistent queries,
 * including creation, modification, and deletion of those queries.
 *
 * All blocking calls are performed via Promise-based async/await to avoid blocking the event loop.
 * The wrapper maintains the same interface as the underlying ControllerClient while making it
 * compatible with asynchronous code.
 *
 * Error handling is enhanced with specific exception types that provide more context and clarity
 * than the underlying gRPC errors surfaced from the Java controller server. Network issues
 * typically result in DeephavenConnectionError and query-related issues in QueryError.
 *
 * @example
 * ```typescript
 * // Create a controller client from an authenticated session factory
 * const sessionFactory = await CorePlusSessionFactory.fromUrl(
 *   "https://deephaven.example.com:10000/iris/connection.json"
 * );
 * await sessionFactory.password("username", "password");
 * const controllerClient = sessionFactory.controllerClient;
 *
 * // Create a query configuration and add it
 * const config = await controllerClient.makePqConfig("my-worker", 2);
 * const serial = await controllerClient.addQuery(config);
 *
 * // Start the query and wait for it to initialize
 * await controllerClient.startAndWait(serial);
 *
 * // Clean up when done
 * await controllerClient.stopQuery(serial);
 * await controllerClient.deleteQuery(serial);
 * ```
 */
export class CorePlusControllerClient extends ClientObjectWrapper<DheControllerClient> {
  private _subscribed = false;

  /**
   * Initialize the CorePlusControllerClient with a ControllerClient instance.
   *
   * @param controllerClient - The ControllerClient instance to wrap.
   */
  constructor(controllerClient: DheControllerClient) {
    super(controllerClient, true);
    _logger.debug("[CorePlusControllerClient] Initialized");
  }

  // ===========================================================================
  // Initialization & Connection Management
  // ===========================================================================

  /**
   * Ping the controller and refresh the cookie asynchronously.
   *
   * This method sends a lightweight ping request to the controller service to verify
   * connectivity and refresh the authentication cookie. It's useful for:
   * 1. Verifying that the controller service is reachable and responsive
   * 2. Keeping the authentication session active by refreshing the cookie
   * 3. Detecting network or server issues early
   *
   * @param timeoutSeconds - Maximum time in seconds to wait for the ping.
   *   Defaults to QUICK_OPERATION_TIMEOUT_SECONDS.
   * @returns True if the ping was sent successfully and the cookie was refreshed,
   *   False if there was no cookie to refresh.
   * @throws {DeephavenConnectionError} If the connection to the server fails, times out,
   *   or there are communication errors.
   */
  async ping(timeoutSeconds: number = QUICK_OPERATION_TIMEOUT_SECONDS): Promise<boolean> {
    _logger.debug("[CorePlusControllerClient:ping] Sending ping to controller");
    try {
      const result = await Promise.race([
        Promise.resolve(this.wrapped.ping()),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`__timeout__:${timeoutSeconds}`)), timeoutSeconds * 1000),
        ),
      ]);
      return Boolean(result);
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusControllerClient:ping] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Ping timed out after ${timeoutSeconds} seconds.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:ping] Failed to ping controller: ${err.message}`,
        );
        throw new DeephavenConnectionError(`Failed to ping controller: ${err.message}`);
      }
      _logger.error(
        `[CorePlusControllerClient:ping] Unexpected error during ping: ${err.message}`,
      );
      throw new DeephavenConnectionError(`Connection error during ping: ${err.message}`);
    }
  }

  /**
   * Subscribe to persistent query state updates asynchronously.
   *
   * This method establishes a subscription to the controller's persistent query state
   * and waits for the initial query state snapshot to be populated. It MUST be called
   * before using state query methods like map(), get(), and waitForChange().
   *
   * This method is idempotent - calling it multiple times is safe and will only
   * subscribe once. Subsequent calls will return immediately without error.
   *
   * @param timeoutSeconds - Maximum time in seconds to wait for subscription to complete.
   *   Defaults to SUBSCRIBE_TIMEOUT_SECONDS.
   * @throws {DeephavenConnectionError} If not authenticated, if unable to connect to the
   *   controller service, or if subscription times out.
   * @throws {QueryError} If the subscription fails due to invalid state or permission issues.
   */
  async subscribe(timeoutSeconds: number = SUBSCRIBE_TIMEOUT_SECONDS): Promise<void> {
    if (this._subscribed) {
      _logger.debug(
        "[CorePlusControllerClient:subscribe] Already subscribed, skipping",
      );
      return;
    }
    _logger.debug(
      `[CorePlusControllerClient:subscribe] Subscribing to query state (timeoutSeconds=${timeoutSeconds})`,
    );
    try {
      await Promise.race([
        Promise.resolve(this.wrapped.subscribe()),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`__timeout__:${timeoutSeconds}`)), timeoutSeconds * 1000),
        ),
      ]);
      this._subscribed = true;
      _logger.debug(
        "[CorePlusControllerClient:subscribe] Successfully subscribed to query state",
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusControllerClient:subscribe] Subscription timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Controller subscription timed out after ${timeoutSeconds} seconds. ` +
          `The server may be overloaded or unreachable.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:subscribe] Connection error during subscription: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:subscribe] Failed to subscribe to query state: ${err.message}`,
      );
      throw new QueryError(
        `Failed to subscribe to persistent query state: ${err.message}`,
      );
    }
  }

  // ===========================================================================
  // Query State Management
  // ===========================================================================

  /**
   * Retrieve a copy of the current persistent query state asynchronously.
   *
   * A successful call to subscribe() should have happened before this call.
   *
   * @returns A Map mapping query serial numbers to CorePlusQueryInfo objects.
   * @throws {InternalError} If subscribe() was not called before this method.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {QueryError} If the subscription state is invalid.
   */
  async map(): Promise<Map<CorePlusQuerySerial, CorePlusQueryInfo>> {
    if (!this._subscribed) {
      _logger.error(
        "[CorePlusControllerClient:map] subscribe() must be called before map(). " +
        "This indicates a programming bug - the controller client was not properly initialized.",
      );
      throw new InternalError(
        "subscribe() must be called before map(). This indicates a programming bug - " +
        "the controller client was not properly initialized.",
      );
    }
    _logger.debug("[CorePlusControllerClient:map] Retrieving query map");
    try {
      const rawMap = await Promise.resolve(this.wrapped.map());
      const result = new Map<CorePlusQuerySerial, CorePlusQueryInfo>();
      for (const [k, v] of rawMap) {
        result.set(k as CorePlusQuerySerial, new CorePlusQueryInfo(v));
      }
      return result;
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:map] Connection error while retrieving query map: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:map] Failed to retrieve query map: ${err.message}`,
      );
      throw new QueryError(`Failed to retrieve query state: ${err.message}`);
    }
  }

  /**
   * Retrieve query state with version number for synchronization.
   *
   * @returns A tuple of [queryMap, version] where queryMap maps serial numbers to
   *   CorePlusQueryInfo objects and version is a monotonically increasing integer.
   * @throws {InternalError} If subscribe() was not called before this method.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {QueryError} If the subscription state is invalid.
   */
  async mapAndVersion(): Promise<[Map<CorePlusQuerySerial, CorePlusQueryInfo>, number]> {
    if (!this._subscribed) {
      _logger.error(
        "[CorePlusControllerClient:mapAndVersion] subscribe() must be called before mapAndVersion(). " +
        "This indicates a programming bug - the controller client was not properly initialized.",
      );
      throw new InternalError(
        "subscribe() must be called before mapAndVersion(). This indicates a programming bug - " +
        "the controller client was not properly initialized.",
      );
    }
    _logger.debug("[CorePlusControllerClient:mapAndVersion] Retrieving query map with version");
    try {
      const [rawMap, version] = await Promise.resolve(this.wrapped.mapAndVersion());
      const queryMap = new Map<CorePlusQuerySerial, CorePlusQueryInfo>();
      for (const [k, v] of rawMap) {
        queryMap.set(k as CorePlusQuerySerial, new CorePlusQueryInfo(v));
      }
      _logger.debug(
        `[CorePlusControllerClient:mapAndVersion] Retrieved ${queryMap.size} queries, version=${version}`,
      );
      return [queryMap, version];
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:mapAndVersion] Connection error: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:mapAndVersion] Failed to retrieve query map: ${err.message}`,
      );
      throw new QueryError(`Failed to retrieve query state with version: ${err.message}`);
    }
  }

  /**
   * Retrieve the serial number for a given query name asynchronously.
   *
   * @param name - The name of the query to find.
   * @param timeoutSeconds - How long to wait for the query to be found, in seconds.
   *   Default is NO_WAIT_SECONDS (0, meaning no wait).
   * @returns The serial number for the query with the given name.
   * @throws {InternalError} If subscribe() was not called before this method.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {QueryError} If no query with the given name is found within the timeout period.
   */
  async getSerialForName(
    name: string,
    timeoutSeconds: number = NO_WAIT_SECONDS,
  ): Promise<CorePlusQuerySerial> {
    if (!this._subscribed) {
      _logger.error(
        "[CorePlusControllerClient:getSerialForName] subscribe() must be called before getSerialForName(). " +
        "This indicates a programming bug - the controller client was not properly initialized.",
      );
      throw new InternalError(
        "subscribe() must be called before getSerialForName(). This indicates a programming bug - " +
        "the controller client was not properly initialized.",
      );
    }
    _logger.debug(
      `[CorePlusControllerClient:getSerialForName] Looking up serial for query name='${name}'`,
    );
    try {
      const result = await Promise.resolve(
        this.wrapped.getSerialForName(name, timeoutSeconds),
      );
      return result as CorePlusQuerySerial;
    } catch (e) {
      const err = e as Error;
      // Re-raise native exceptions unchanged
      if (err instanceof RangeError || err instanceof TypeError) {
        throw err;
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:getSerialForName] Connection error while retrieving serial for query '${name}': ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:getSerialForName] Failed to get serial for query name '${name}': ${err.message}`,
      );
      throw new QueryError(`Failed to find query with name '${name}': ${err.message}`);
    }
  }

  /**
   * Wait for a change in the query map to occur asynchronously.
   *
   * A normal return means the wait ended; this method does not distinguish "a change was
   * observed" from "the wait ended for any other reason". If that distinction matters,
   * use waitForChangeFromVersion(), which returns a bool.
   *
   * @param timeoutSeconds - How long to wait for a change, in seconds.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {QueryError} If there is an issue with the query state or subscription.
   */
  async waitForChange(timeoutSeconds: number): Promise<void> {
    _logger.debug(
      `[CorePlusControllerClient:waitForChange] Waiting for query state change, timeout=${timeoutSeconds}`,
    );
    try {
      await Promise.resolve(this.wrapped.waitForChange(timeoutSeconds));
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:waitForChange] Connection error while waiting for query state change: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:waitForChange] Failed to wait for change: ${err.message}`,
      );
      throw new QueryError(`Failed to wait for query state change: ${err.message}`);
    }
  }

  /**
   * Wait for query map version to increment beyond specified version.
   *
   * This is a long-poll API. This wrapper rejects `timeoutSeconds <= 0` with a RangeError.
   *
   * @param mapVersion - The version number to wait to exceed.
   * @param timeoutSeconds - Maximum time to wait for version change, in seconds.
   *   Must be strictly positive.
   * @returns True if version changed (version > mapVersion), False if timeout occurred.
   * @throws {RangeError} If timeoutSeconds is not strictly positive.
   * @throws {DeephavenConnectionError} If unable to connect to controller service.
   * @throws {QueryError} If subscription state is invalid.
   */
  async waitForChangeFromVersion(mapVersion: number, timeoutSeconds: number): Promise<boolean> {
    if (timeoutSeconds <= 0) {
      throw new RangeError(
        `timeout_seconds must be a positive value, got ${JSON.stringify(timeoutSeconds)}`,
      );
    }
    _logger.debug(
      `[CorePlusControllerClient:waitForChangeFromVersion] ` +
      `Waiting for version > ${mapVersion}, timeout=${timeoutSeconds}s`,
    );
    try {
      const result = await Promise.resolve(
        this.wrapped.waitForChangeFromVersion(mapVersion, timeoutSeconds),
      );
      const changed = Boolean(result);
      _logger.debug(
        `[CorePlusControllerClient:waitForChangeFromVersion] ` +
        `Returned: ${changed} (version ${changed ? "changed" : "unchanged"})`,
      );
      return changed;
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:waitForChangeFromVersion] Connection error: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:waitForChangeFromVersion] Failed: ${err.message}`,
      );
      throw new QueryError(
        `Failed to wait for version change from ${mapVersion}: ${err.message}`,
      );
    }
  }

  /**
   * Get a specific query's information from the subscription map asynchronously.
   *
   * A successful call to subscribe() should have happened before this call.
   *
   * @param serial - The serial number of the query to get.
   * @param timeoutSeconds - How long to wait for the query to exist, in seconds.
   *   Default is NO_WAIT_SECONDS (0, meaning no wait).
   * @returns The CorePlusQueryInfo associated with the serial number.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {QueryError} If the query does not exist within the timeout period, or if the
   *   subscription state is invalid.
   */
  async get(
    serial: CorePlusQuerySerial,
    timeoutSeconds: number = NO_WAIT_SECONDS,
  ): Promise<CorePlusQueryInfo> {
    _logger.debug(
      `[CorePlusControllerClient:get] Retrieving query info for serial=${serial}, timeout=${timeoutSeconds}`,
    );
    try {
      const result = await Promise.resolve(this.wrapped.get(serial as number, timeoutSeconds));
      return new CorePlusQueryInfo(result);
    } catch (e) {
      const err = e as Error;
      // KeyError analog: RangeError when query not found
      if (err.name === "KeyError" || (err instanceof RangeError && err.message.includes(String(serial)))) {
        _logger.error(
          `[CorePlusControllerClient:get] Query ${serial} does not exist: ${err.message}`,
        );
        throw new QueryError(`Query with serial ${serial} does not exist`);
      }
      // Re-raise native exceptions unchanged
      if (err instanceof RangeError) {
        throw err;
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:get] Connection error while retrieving query ${serial}: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusControllerClient:get] Failed to get query ${serial}: ${err.message}`,
      );
      throw new QueryError(`Failed to retrieve query ${serial}: ${err.message}`);
    }
  }

  // ===========================================================================
  // Query Creation & Configuration
  // ===========================================================================

  /**
   * Add a persistent query asynchronously.
   *
   * Creates a new persistent query in the Deephaven controller based on the provided
   * configuration. After adding, you typically need to call startAndWait() to ensure
   * the query transitions to the RUNNING state.
   *
   * @param queryConfig - The query configuration to add.
   * @param timeoutSeconds - Maximum time in seconds to wait for the operation.
   *   Defaults to PQ_MANAGEMENT_TIMEOUT_SECONDS.
   * @returns The serial number of the newly added query.
   * @throws {DeephavenConnectionError} If not authenticated or unable to connect, or if
   *   the operation times out.
   * @throws {QueryError} If the query creation fails for any other reason.
   */
  async addQuery(
    queryConfig: CorePlusQueryConfig,
    timeoutSeconds: number = PQ_MANAGEMENT_TIMEOUT_SECONDS,
  ): Promise<CorePlusQuerySerial> {
    const pb = queryConfig.pb;
    _logger.debug(
      `[CorePlusControllerClient:addQuery] Adding query: ` +
      `name='${pb["name"]}', heapSizeGb=${pb["heapSizeGb"]}, ` +
      `scriptLanguage=${JSON.stringify(pb["scriptLanguage"])}, configurationType=${JSON.stringify(pb["configurationType"])}, ` +
      `enabled=${pb["enabled"]}, ` +
      `script_body=${pb["scriptCode"] ? "<set>" : null}, scriptPath=${JSON.stringify(pb["scriptPath"])}, ` +
      `serverName=${JSON.stringify(pb["serverName"])}, workerKind=${JSON.stringify(pb["workerKind"])}`,
    );
    try {
      const result = await Promise.race([
        Promise.resolve(this.wrapped.addQuery(pb)),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`__timeout__:${timeoutSeconds}`)), timeoutSeconds * 1000),
        ),
      ]);
      return result as CorePlusQuerySerial;
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusControllerClient:addQuery] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Query creation timed out after ${timeoutSeconds} seconds.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:addQuery] Failed to connect to controller when adding query: ${err.message}`,
        );
        throw new DeephavenConnectionError(`Unable to connect to controller: ${err.message}`);
      }
      if (err instanceof RangeError || err instanceof ResourceError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:addQuery] Failed to create query: ${err.message}`,
      );
      throw new QueryError(`Failed to create query: ${err.message}`);
    }
  }

  /**
   * Apply scheduling entries to a config with three-way semantics.
   *
   * - `undefined`: leave `config.scheduling` untouched.
   * - `[]`: clear `config.scheduling` (caller explicitly wants no schedule).
   * - `[...]`: replace `config.scheduling` wholesale with the supplied entries.
   *
   * @param config - The config object whose `scheduling` field will be updated.
   * @param schedule - Scheduling entries to apply, or `undefined` to skip.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE config objects have arbitrary shape
  _applyScheduleConfig(config: Record<string, any>, schedule: string[] | undefined): void {
    if (schedule !== undefined) {
      config["scheduling"] = [...schedule];
    }
  }

  /**
   * Apply caller-supplied configuration parameters to a config object in place.
   *
   * Every field except `schedule` follows a "undefined means skip" rule. See `schedule` below.
   *
   * @param config - The config object to modify.
   * @param programmingLanguage - Programming language ("Python" or "Groovy"), or undefined.
   * @param scriptBody - Inline script code, or undefined.
   * @param scriptPath - Path to script file, or undefined.
   * @param configurationType - Query type ("Script", "RunAndDone", etc.), or undefined.
   * @param enabled - Whether query is enabled, or undefined.
   * @param restartUsers - Restart permission setting, or undefined.
   * @param extraClassPath - Additional classpath entries, or undefined.
   * @param schedule - Scheduling entries. undefined leaves existing untouched; [] clears; [...] replaces.
   * @param initTimeoutNanos - Initialization timeout in nanoseconds, or undefined.
   * @param jvmProfile - Named JVM profile, or undefined.
   * @param pythonVirtualEnvironment - Named Python venv, or undefined.
   */
  _applyPqConfigParameters( // eslint-disable-line @typescript-eslint/explicit-module-boundary-types
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE config objects have arbitrary shape
    config: Record<string, any>,
    programmingLanguage: string | undefined,
    scriptBody: string | undefined,
    scriptPath: string | undefined,
    configurationType: string | undefined,
    enabled: boolean | undefined,
    restartUsers: string | undefined,
    extraClassPath: string[] | undefined,
    schedule: string[] | undefined,
    initTimeoutNanos: number | undefined,
    jvmProfile: string | undefined,
    pythonVirtualEnvironment: string | undefined,
  ): void {
    if (programmingLanguage !== undefined) {
      config["scriptLanguage"] = programmingLanguage;
    }
    if (scriptBody !== undefined) {
      config["scriptCode"] = scriptBody;
    }
    if (scriptPath !== undefined) {
      config["scriptPath"] = scriptPath;
    }
    if (configurationType !== undefined) {
      config["configurationType"] = configurationType;
    }
    if (enabled !== undefined) {
      config["enabled"] = enabled;
    }
    if (restartUsers !== undefined) {
      config["restartUsers"] = restartUsers;
    }
    if (extraClassPath && extraClassPath.length > 0) {
      config["extraClassPath"] = [...(config["extraClassPath"] ?? []), ...extraClassPath];
    }
    this._applyScheduleConfig(config, schedule);
    if (initTimeoutNanos !== undefined) {
      config["initTimeoutNanos"] = initTimeoutNanos;
    }
    if (jvmProfile !== undefined) {
      config["jvmProfile"] = jvmProfile;
    }
    if (pythonVirtualEnvironment !== undefined) {
      config["pythonVirtualEnvironment"] = pythonVirtualEnvironment;
    }
  }

  /**
   * Create a persistent query configuration.
   *
   * Creates an in-memory PQ configuration object that can be customized with script content,
   * scheduling, resource settings, and access controls. The configuration is not persisted
   * until passed to addQuery().
   *
   * Scheduler semantics (three-way, based on `schedule`):
   * - `schedule=undefined` (default): install the default scheduler and make no further changes.
   *   For a permanent PQ, installs a continuous scheduler. For a temporary PQ, uses whatever
   *   makeTemporaryConfig installs.
   * - `schedule=[]`: explicitly clear scheduling. The scheduling list is cleared before return.
   * - `schedule=[...]` (non-empty): the caller-supplied list **replaces** the scheduling block.
   *
   * @param name - The name of the persistent query.
   * @param heapSizeGb - The heap size of the worker in gigabytes (e.g., 8 or 2.5).
   * @param scriptBody - The inline script code to execute. Mutually exclusive with scriptPath.
   * @param scriptPath - Path to script file in Git repository. Mutually exclusive with scriptBody.
   * @param programmingLanguage - Script language - "Python" or "Groovy". undefined uses default.
   * @param configurationType - Query type - "Script", "RunAndDone", etc. undefined uses default.
   * @param enabled - Whether the query is enabled. undefined uses controller default.
   * @param schedule - Scheduling config as list of "Key=Value" strings, or undefined for default.
   * @param server - The specific server to run the worker on. undefined lets controller choose.
   * @param engine - The engine to use. Defaults to "DeephavenCommunity".
   * @param jvmProfile - Named JVM profile (e.g., "large-memory"), or undefined.
   * @param extraJvmArgs - Extra JVM arguments, or undefined.
   * @param extraClassPath - Additional classpath entries, or undefined.
   * @param pythonVirtualEnvironment - Named Python venv for Core+ workers, or undefined.
   * @param extraEnvironmentVars - Extra environment variables, or undefined.
   * @param initTimeoutNanos - Initialization timeout in nanoseconds, or undefined.
   * @param autoDeleteTimeout - Timeout in seconds for auto-deletion; undefined for permanent.
   * @param adminGroups - User groups with admin access, or undefined.
   * @param viewerGroups - User groups with viewer access, or undefined.
   * @param restartUsers - Who can restart the query, or undefined.
   * @returns The configuration object for the persistent query.
   * @throws {RangeError} If both scriptBody and scriptPath are provided.
   * @throws {DeephavenConnectionError} If not authenticated or unable to communicate.
   * @throws {QueryError} If configuration creation fails for any other reason.
   */
  async makePqConfig(
    name: string,
    heapSizeGb: number,
    scriptBody?: string,
    scriptPath?: string,
    programmingLanguage?: string,
    configurationType?: string,
    enabled?: boolean,
    schedule?: string[],
    server?: string,
    engine: string = "DeephavenCommunity",
    jvmProfile?: string,
    extraJvmArgs?: string[],
    extraClassPath?: string[],
    pythonVirtualEnvironment?: string,
    extraEnvironmentVars?: string[],
    initTimeoutNanos?: number,
    autoDeleteTimeout?: number,
    adminGroups?: string[],
    viewerGroups?: string[],
    restartUsers?: string,
  ): Promise<CorePlusQueryConfig> {
    _logger.debug(
      `[CorePlusControllerClient:makePqConfig] Creating PQ config: ` +
      `name='${name}', heapSizeGb=${heapSizeGb}, server=${JSON.stringify(server)}, engine=${JSON.stringify(engine)}, ` +
      `autoDeleteTimeout=${autoDeleteTimeout}, programmingLanguage=${JSON.stringify(programmingLanguage)}, ` +
      `configurationType=${JSON.stringify(configurationType)}, enabled=${enabled}, ` +
      `script_body=${scriptBody ? "<set>" : null}, scriptPath=${JSON.stringify(scriptPath)}, ` +
      `schedule=${JSON.stringify(schedule)}, jvmProfile=${JSON.stringify(jvmProfile)}, ` +
      `pythonVirtualEnvironment=${JSON.stringify(pythonVirtualEnvironment)}, ` +
      `adminGroups=${JSON.stringify(adminGroups)}, viewerGroups=${JSON.stringify(viewerGroups)}, restartUsers=${JSON.stringify(restartUsers)}`,
    );

    if (scriptBody !== undefined && scriptPath !== undefined) {
      throw new RangeError(
        "script_body and script_path are mutually exclusive - specify only one",
      );
    }

    try {
      // Step 1: call makeTemporaryConfig to produce a baseline config.
      const isPermanent = autoDeleteTimeout === undefined;
      const effectiveTimeout = isPermanent ? 600 : autoDeleteTimeout!;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE config objects have arbitrary shape
      const config: Record<string, any> = await Promise.resolve(
        this.wrapped.makeTemporaryConfig(
          name,
          heapSizeGb,
          server,
          extraJvmArgs,
          extraEnvironmentVars,
          engine,
          effectiveTimeout,
          adminGroups,
          viewerGroups,
        ),
      );

      // Step 2: install the default scheduler for permanent queries when the
      // caller did not supply a schedule.
      if (isPermanent && schedule === undefined) {
        config["scheduling"] = [..._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING];
        _logger.debug(
          `[CorePlusControllerClient:makePqConfig] ` +
          `Installed default continuous scheduler for permanent query '${name}'`,
        );
      }

      // Step 3: apply all caller-supplied parameters.
      this._applyPqConfigParameters(
        config,
        programmingLanguage,
        scriptBody,
        scriptPath,
        configurationType,
        enabled,
        restartUsers,
        extraClassPath,
        schedule,
        initTimeoutNanos,
        jvmProfile,
        pythonVirtualEnvironment,
      );

      _logger.debug(
        `[CorePlusControllerClient:makePqConfig] Successfully created config for '${name}'`,
      );
      return new CorePlusQueryConfig(config);
    } catch (e) {
      _logger.error(
        `[CorePlusControllerClient:makePqConfig] Failed to create config for '${name}': ${(e as Error).message}`,
      );
      throw e;
    }
  }

  // ===========================================================================
  // Query Lifecycle Management
  // ===========================================================================

  /**
   * Delete a query asynchronously.
   *
   * Permanently removes a query from the controller. The serial number becomes invalid.
   *
   * @param serial - The serial number of the query to delete.
   * @param timeoutSeconds - Maximum time in seconds to wait for the operation.
   *   Defaults to PQ_MANAGEMENT_TIMEOUT_SECONDS.
   * @throws {DeephavenConnectionError} If not authenticated, unable to connect, or times out.
   * @throws {QueryError} If the query deletion fails for any other reason.
   */
  async deleteQuery(
    serial: CorePlusQuerySerial,
    timeoutSeconds: number = PQ_MANAGEMENT_TIMEOUT_SECONDS,
  ): Promise<void> {
    _logger.debug(
      `[CorePlusControllerClient:deleteQuery] Starting query deletion for serial=${serial}`,
    );
    try {
      await Promise.race([
        Promise.resolve(this.wrapped.deleteQuery(serial as number)),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`__timeout__:${timeoutSeconds}`)), timeoutSeconds * 1000),
        ),
      ]);
      _logger.debug(
        `[CorePlusControllerClient:deleteQuery] Query ${serial} deleted successfully`,
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusControllerClient:deleteQuery] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Query deletion timed out after ${timeoutSeconds} seconds.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:deleteQuery] Connection error while deleting query ${serial}: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:deleteQuery] Failed to delete query ${serial}: ${err.message}`,
      );
      throw new QueryError(`Failed to delete query ${serial}: ${err.message}`);
    }
  }

  /**
   * Modify a persistent query configuration asynchronously.
   *
   * @param updatedConfig - The complete updated configuration for the query.
   * @param restart - Whether to restart the query after modifying the configuration.
   *   Defaults to false.
   * @param timeoutSeconds - Maximum time in seconds to wait for the operation.
   *   Defaults to PQ_MANAGEMENT_TIMEOUT_SECONDS.
   * @throws {DeephavenConnectionError} If not authenticated, unable to connect, or times out.
   * @throws {QueryError} If the query modification fails for any other reason.
   */
  async modifyQuery(
    updatedConfig: CorePlusQueryConfig,
    restart: boolean = false,
    timeoutSeconds: number = PQ_MANAGEMENT_TIMEOUT_SECONDS,
  ): Promise<void> {
    const pb = updatedConfig.pb;
    _logger.debug(
      `[CorePlusControllerClient:modifyQuery] Modifying query: ` +
      `serial=${pb["serial"]}, name='${pb["name"]}', restart=${restart}`,
    );
    try {
      await Promise.race([
        Promise.resolve(this.wrapped.modifyQuery(pb, restart)),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`__timeout__:${timeoutSeconds}`)), timeoutSeconds * 1000),
        ),
      ]);
      _logger.debug(
        `[CorePlusControllerClient:modifyQuery] Query ${pb["serial"]} modified successfully`,
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusControllerClient:modifyQuery] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Query modification timed out after ${timeoutSeconds} seconds.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:modifyQuery] Connection error while modifying query ${pb["serial"]}: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:modifyQuery] Failed to modify query ${pb["serial"]}: ${err.message}`,
      );
      throw new QueryError(`Failed to modify query ${pb["serial"]}: ${err.message}`);
    }
  }

  /**
   * Restart one or more queries asynchronously.
   *
   * @param serials - A query serial number, or an array of serial numbers.
   * @param timeoutSeconds - Timeout in seconds for the operation. undefined uses client default.
   * @throws {DeephavenConnectionError} If not authenticated or unable to connect.
   * @throws {QueryError} If the query restart fails for any other reason.
   */
  async restartQuery(
    serials: CorePlusQuerySerial | CorePlusQuerySerial[],
    timeoutSeconds?: number,
  ): Promise<void> {
    _logger.debug("[CorePlusControllerClient:restartQuery] Starting query restart");
    try {
      await Promise.resolve(
        this.wrapped.restartQuery(
          Array.isArray(serials) ? (serials as number[]) : (serials as number),
          timeoutSeconds,
        ),
      );
      _logger.debug(
        "[CorePlusControllerClient:restartQuery] Query restart completed successfully",
      );
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:restartQuery] Connection error while restarting query(s): ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:restartQuery] Failed to restart query(s): ${err.message}`,
      );
      throw new QueryError(`Failed to restart query(s): ${err.message}`);
    }
  }

  /**
   * Start the given query and wait for it to become running asynchronously.
   *
   * @param serial - The serial number of the query to start.
   * @param timeoutSeconds - Maximum time in integer seconds to wait.
   *   Defaults to PQ_STATE_CHANGE_TIMEOUT_SECONDS.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {RangeError} If timeoutSeconds is negative.
   * @throws {QueryError} If the query fails to reach the RUNNING state, or for any other error.
   */
  async startAndWait(
    serial: CorePlusQuerySerial,
    timeoutSeconds: number = PQ_STATE_CHANGE_TIMEOUT_SECONDS,
  ): Promise<void> {
    _validateTimeout(timeoutSeconds);
    _logger.debug(
      `[CorePlusControllerClient:startAndWait] Starting query and waiting for serial=${serial}`,
    );
    try {
      await Promise.resolve(this.wrapped.startAndWait(serial as number, timeoutSeconds));
      _logger.debug(
        `[CorePlusControllerClient:startAndWait] Query ${serial} started successfully`,
      );
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:startAndWait] Connection error while starting query ${serial}: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:startAndWait] Query ${serial} failed to start: ${err.message}`,
      );
      throw new QueryError(`Failed to start query ${serial}: ${err.message}`);
    }
  }

  /**
   * Stop one or more queries asynchronously.
   *
   * @param serials - A query serial number, or an array of serial numbers.
   * @param timeoutSeconds - Timeout in integer seconds. undefined uses client default.
   * @throws {DeephavenConnectionError} If not authenticated or unable to connect.
   * @throws {RangeError} If timeoutSeconds is negative.
   * @throws {QueryError} If the query stop fails for any other reason.
   */
  async stopQuery(
    serials: CorePlusQuerySerial | CorePlusQuerySerial[],
    timeoutSeconds?: number,
  ): Promise<void> {
    _validateTimeout(timeoutSeconds);
    _logger.debug("[CorePlusControllerClient:stopQuery] Starting query stop");
    try {
      await Promise.resolve(
        this.wrapped.stopQuery(
          Array.isArray(serials) ? (serials as number[]) : (serials as number),
          timeoutSeconds,
        ),
      );
      _logger.debug(
        "[CorePlusControllerClient:stopQuery] Query stop completed successfully",
      );
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:stopQuery] Connection error while stopping query(s): ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:stopQuery] Failed to stop query(s): ${err.message}`,
      );
      throw new QueryError(`Failed to stop query(s): ${err.message}`);
    }
  }

  /**
   * Stop the given query and wait for it to become terminal asynchronously.
   *
   * @param serial - The serial number of the query to stop.
   * @param timeoutSeconds - Maximum time in integer seconds to wait.
   *   Defaults to PQ_STATE_CHANGE_TIMEOUT_SECONDS.
   * @throws {DeephavenConnectionError} If unable to connect to the controller service.
   * @throws {RangeError} If timeoutSeconds is negative.
   * @throws {QueryError} If the query fails to reach a terminal state, or for any other error.
   */
  async stopAndWait(
    serial: CorePlusQuerySerial,
    timeoutSeconds: number = PQ_STATE_CHANGE_TIMEOUT_SECONDS,
  ): Promise<void> {
    _validateTimeout(timeoutSeconds);
    _logger.debug(
      `[CorePlusControllerClient:stopAndWait] Stopping query and waiting for serial=${serial}`,
    );
    try {
      await Promise.resolve(this.wrapped.stopAndWait(serial as number, timeoutSeconds));
      _logger.debug(
        `[CorePlusControllerClient:stopAndWait] Query ${serial} stopped successfully`,
      );
    } catch (e) {
      const err = e as Error;
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusControllerClient:stopAndWait] Connection error while stopping query ${serial}: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Unable to connect to controller service: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        throw err;
      }
      _logger.error(
        `[CorePlusControllerClient:stopAndWait] Failed to stop query ${serial}: ${err.message}`,
      );
      throw new QueryError(`Failed to stop query ${serial}: ${err.message}`);
    }
  }
}
