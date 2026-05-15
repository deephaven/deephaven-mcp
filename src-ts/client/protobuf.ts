/**
 * Wrapper types for DHE query objects used by the Deephaven client module.
 *
 * In the TypeScript port, the Python protobuf-based wrappers are replaced with
 * JavaScript-native wrappers around the DHE JS API objects. The DHE JS API is
 * loaded at runtime from the DHE server via `@deephaven/jsapi-nodejs`.
 *
 * The architecture follows a consistent pattern where each wrapper class:
 * - Wraps a specific DHE JS API object type
 * - Inherits from the {@link DheWrapper} base class
 * - Provides specialized methods relevant to the wrapped object's domain
 * - Maintains access to the underlying DHE object when needed
 *
 * Classes:
 *   {@link DheWrapper}: Base class providing common functionality for DHE object wrappers.
 *   {@link CorePlusQueryStatus}: Wrapper for query status values with convenience methods.
 *   {@link CorePlusToken}: Wrapper for authentication token objects.
 *   {@link CorePlusQueryConfig}: Wrapper for query configuration objects.
 *   {@link CorePlusQueryState}: Wrapper for query state objects.
 *   {@link CorePlusQueryInfo}: Wrapper for comprehensive query information objects.
 *
 * Type Definitions:
 *   {@link CorePlusQuerySerial}: Type representing the serial number of a query.
 */

/**
 * Maps each PQ state string to its lifecycle category.
 *
 * Categories:
 * - ACTIVE: PQ is processing data; session_id is present (RUNNING, EXECUTING)
 * - TRANSITIONAL: PQ is between stable states; do not branch on a specific value
 * - TERMINAL: state will not change without user action; STOPPED and FAILED can be restarted
 * - INVALID: UNSPECIFIED is a protobuf zero-value sentinel; should not appear at runtime
 */
export const PQ_STATES: Record<string, string> = {
  RUNNING: "ACTIVE",
  EXECUTING: "ACTIVE",
  UNINITIALIZED: "TRANSITIONAL",
  CONNECTING: "TRANSITIONAL",
  AUTHENTICATING: "TRANSITIONAL",
  ACQUIRING_WORKER: "TRANSITIONAL",
  INITIALIZING: "TRANSITIONAL",
  STOPPING: "TRANSITIONAL",
  DISCONNECTED: "TRANSITIONAL",
  STOPPED: "TERMINAL",
  FAILED: "TERMINAL",
  KILLED: "TERMINAL",
  COMPLETED: "TERMINAL",
  ERROR: "TERMINAL",
  UNSPECIFIED: "INVALID",
};

/**
 * Type representing the serial number of a persistent query.
 *
 * A query serial is a unique identifier assigned to each persistent query in the Deephaven system.
 * It is used to reference, lookup, and manage specific query instances. Query serials are
 * numbers assigned incrementally by the controller service when queries are created.
 *
 * Unlike query names (which are optional and user-defined), serials are guaranteed to be unique
 * within a Deephaven server instance and are the primary key for query identification in the API.
 *
 * @example
 * ```typescript
 * const querySerial: CorePlusQuerySerial = 12345 as CorePlusQuerySerial;
 * const info = await controllerClient.get(querySerial);
 * ```
 */
export type CorePlusQuerySerial = number & { readonly _brand: unique symbol };

/**
 * A wrapper for a DHE object that provides convenience methods.
 *
 * This base class provides common functionality for all DHE object wrapper classes.
 * It enforces non-null objects and provides a consistent interface for accessing
 * the underlying DHE object.
 *
 * @example
 * ```typescript
 * const wrapper = new DheWrapper(dheObject);
 * const dict = wrapper.toDict();
 * const json = wrapper.toJson();
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
export class DheWrapper<T extends Record<string, any> = Record<string, any>> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  protected readonly _pb: T;

  /**
   * Initialize with a DHE object.
   *
   * @param pb - The DHE object to wrap. Must not be null/undefined.
   * @throws {Error} If the provided object is null/undefined.
   */
  constructor(pb: T) {
    if (pb === null || pb === undefined) {
      throw new Error("Protobuf message cannot be None");
    }
    this._pb = pb;
  }

  toString(): string {
    return `<${this.constructor.name} wrapping ${Object.prototype.toString.call(this._pb)}>`;
  }

  /**
   * The underlying DHE object.
   */
  get pb(): T {
    return this._pb;
  }

  /**
   * Return the DHE object as a plain dictionary.
   *
   * @returns A dictionary representation of the DHE object.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  toDict(): Record<string, any> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
    return { ...(this._pb as Record<string, any>) };
  }

  /**
   * Return the DHE object as a JSON string.
   *
   * @returns A JSON string representation of the DHE object.
   */
  toJson(): string {
    return JSON.stringify(this._pb);
  }
}

/**
 * Wrapper for a query status value providing status checking functionality.
 *
 * This class wraps a query status object from the DHE JS API, which represents
 * the current lifecycle state of a query or worker process in the Deephaven system.
 * It provides utility methods and properties for checking status conditions,
 * simplifying status-based decision making.
 *
 * Common status values include:
 * - UNINITIALIZED: Initial state before query execution begins
 * - INITIALIZING: Query is being set up but not yet running
 * - RUNNING: Query is actively executing and processing data
 * - STOPPING: Query is in the process of shutting down gracefully
 * - STOPPED: Query has been gracefully terminated
 * - COMPLETED: Query has finished execution successfully
 * - FAILED: Query encountered an error and terminated abnormally
 * - KILLED: Query was forcibly terminated
 *
 * @example
 * ```typescript
 * const status = new CorePlusQueryStatus({ name: "RUNNING" });
 * if (status.isRunning) {
 *   console.log(`Query is running with status: ${status}`);
 * }
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE status objects have arbitrary shape
export class CorePlusQueryStatus extends DheWrapper<Record<string, any>> {
  /**
   * Initialize with a DHE query status object.
   *
   * @param status - The DHE status object. Must have a `name` property.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE status objects have arbitrary shape
  constructor(status: Record<string, any>) {
    super(status);
  }

  toString(): string {
    return this.name;
  }

  equals(other: CorePlusQueryStatus | string | unknown): boolean {
    if (other instanceof CorePlusQueryStatus) {
      return this.pb === other.pb || this.name === other.name;
    }
    if (typeof other === "string") {
      return this.name === other;
    }
    return this.pb === other;
  }

  /**
   * The string name of the status, with the DHE `PQS_` prefix stripped.
   *
   * The underlying library may return prefixed status names (e.g., `"PQS_RUNNING"`);
   * this property returns the short form (e.g., `"RUNNING"`) for logging,
   * display, and string comparisons.
   *
   * @returns The stripped status name (e.g., `"RUNNING"`, `"COMPLETED"`, `"FAILED"`).
   */
  get name(): string {
    const raw: string = String(this._pb["name"] ?? this._pb["status"] ?? "UNSPECIFIED");
    return raw.startsWith("PQS_") ? raw.slice("PQS_".length) : raw;
  }

  /**
   * Check if the query status is running.
   *
   * A running query is actively processing data and executing its defined operations.
   *
   * @returns `true` if the query is in a running state, `false` otherwise.
   */
  get isRunning(): boolean {
    return PQ_STATES[this.name] === "ACTIVE";
  }

  /**
   * Check if the query status is completed.
   *
   * A completed query has finished its execution successfully.
   *
   * @returns `true` if the query has completed successfully, `false` otherwise.
   */
  get isCompleted(): boolean {
    return this.name === "COMPLETED";
  }

  /**
   * Check if the query status is in a terminal state.
   *
   * Terminal states represent the end of a query's lifecycle. No further state transitions
   * will occur once a query reaches a terminal state.
   *
   * @returns `true` if the query is in a terminal state, `false` otherwise.
   */
  get isTerminal(): boolean {
    return PQ_STATES[this.name] === "TERMINAL";
  }

  /**
   * Check if the query status is uninitialized.
   *
   * @returns `true` if the query is in the uninitialized state, `false` otherwise.
   */
  get isUninitialized(): boolean {
    return this.name === "UNINITIALIZED";
  }

  /**
   * Check if the query status is initializing.
   *
   * @returns `true` if the query is in the initializing state, `false` otherwise.
   */
  get isInitializing(): boolean {
    return this.name === "INITIALIZING";
  }
}

/**
 * Wrapper for authentication token objects in the Deephaven authentication system.
 *
 * This class wraps a DHE JS API token object to provide a more convenient interface
 * for accessing token information such as service name, issuer, and expiration time.
 * Tokens are central to Deephaven's authentication and authorization system.
 *
 * @example
 * ```typescript
 * const token = new CorePlusToken(dheToken);
 * const tokenDict = token.toDict();
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE token objects have arbitrary shape
export class CorePlusToken extends DheWrapper<Record<string, any>> {
  /**
   * Initialize with a DHE token object.
   *
   * @param token - The DHE token object to wrap. Must not be null/undefined.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE token objects have arbitrary shape
  constructor(token: Record<string, any>) {
    super(token);
  }
}

/**
 * Wrapper for a query configuration object defining how a query should be executed.
 *
 * Provides a more convenient interface to the query configuration from the DHE JS API.
 *
 * @example
 * ```typescript
 * const config = new CorePlusQueryConfig(dheConfig);
 * const configDict = config.toDict();
 * console.log(`Query name: ${configDict.name}`);
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE config objects have arbitrary shape
export class CorePlusQueryConfig extends DheWrapper<Record<string, any>> {
  /**
   * Initialize with a DHE query configuration object.
   *
   * @param config - The DHE configuration object to wrap. Must not be null/undefined.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE config objects have arbitrary shape
  constructor(config: Record<string, any>) {
    super(config);
  }
}

/**
 * Wrapper for a query state object.
 *
 * This class wraps the DHE JS API state object for persistent queries to provide
 * a more convenient interface for accessing state information such as query status.
 *
 * @example
 * ```typescript
 * const state = new CorePlusQueryState(dheState);
 * const status = state.status;
 * if (status.isRunning) {
 *   console.log("Query is running");
 * }
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE state objects have arbitrary shape
export class CorePlusQueryState extends DheWrapper<Record<string, any>> {
  /**
   * Initialize with a DHE query state object.
   *
   * @param state - The DHE state object to wrap. Must not be null/undefined.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE state objects have arbitrary shape
  constructor(state: Record<string, any>) {
    super(state);
  }

  /**
   * The query's current lifecycle status.
   *
   * @returns Wrapper for the query's current status value.
   */
  get status(): CorePlusQueryStatus {
    return new CorePlusQueryStatus(this._pb["status"] ?? { name: "UNSPECIFIED" });
  }
}

/**
 * Wrapper for a comprehensive query information object.
 *
 * Provides a more convenient interface to the query info by wrapping the
 * nested config and state objects into their respective wrapper classes.
 *
 * @example
 * ```typescript
 * const info = new CorePlusQueryInfo(dheQueryInfo);
 * const config = info.config;
 * const state = info.state;
 * if (state && state.status.isRunning) {
 *   console.log(`Query is running with ${info.replicas.length} replicas`);
 * }
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE query info objects have arbitrary shape
export class CorePlusQueryInfo extends DheWrapper<Record<string, any>> {
  private readonly _config: CorePlusQueryConfig;
  private readonly _state: CorePlusQueryState | undefined;
  private readonly _replicas: CorePlusQueryState[];
  private readonly _spares: CorePlusQueryState[];

  /**
   * Initialize with a DHE query info object.
   *
   * @param info - The DHE query info object to wrap. Must not be null/undefined.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE query info objects have arbitrary shape
  constructor(info: Record<string, any>) {
    super(info);
    this._config = new CorePlusQueryConfig(info["config"] ?? {});
    this._state = info["state"] ? new CorePlusQueryState(info["state"]) : undefined;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
    this._replicas = (info["replicas"] ?? []).map((r: Record<string, any>) => new CorePlusQueryState(r));
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
    this._spares = (info["spares"] ?? []).map((s: Record<string, any>) => new CorePlusQueryState(s));
  }

  /**
   * The wrapped configuration of the query.
   *
   * @returns Wrapper for the query's configuration settings.
   */
  get config(): CorePlusQueryConfig {
    return this._config;
  }

  /**
   * The wrapped state of the query, if present.
   *
   * @returns Wrapper for the query's primary state information, or `undefined` if no state exists.
   */
  get state(): CorePlusQueryState | undefined {
    return this._state;
  }

  /**
   * A list of wrapped replica states for the query.
   *
   * @returns A list of state wrappers for all active replicas. Empty if no replicas exist.
   */
  get replicas(): CorePlusQueryState[] {
    return this._replicas;
  }

  /**
   * A list of wrapped spare states for the query.
   *
   * @returns A list of state wrappers for all spare instances. Empty if no spares exist.
   */
  get spares(): CorePlusQueryState[] {
    return this._spares;
  }
}

// Backwards-compatible alias: the base class used to be called ProtobufWrapper
export { DheWrapper as ProtobufWrapper };
