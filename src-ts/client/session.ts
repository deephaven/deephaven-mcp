/**
 * Async wrappers for Deephaven standard and enterprise sessions.
 *
 * This module provides asynchronous wrappers for Deephaven session classes, ensuring all blocking
 * operations are executed asynchronously. It supports both standard and enterprise (Core+) sessions,
 * exposing a unified async API for table creation, data import, querying, and advanced enterprise features.
 *
 * Classes:
 *   {@link BaseSession}: Abstract base class for all asynchronous session wrappers with common functionality.
 *   {@link CoreSession}: Async wrapper for basic DHC session, supporting standard table operations.
 *   {@link CorePlusSession}: Async wrapper for enterprise DHE session, extending BaseSession with persistent
 *     query, historical data, and catalog features.
 *
 * Key Features:
 *   - Non-blocking API: All operations that interact with the server are asynchronous
 *   - Unified interface: Common API across standard and enterprise sessions
 *   - Robust error handling: Consistent exception translation with detailed error messages
 *   - Comprehensive logging: Detailed logs for debugging and monitoring
 *
 * @example
 * ```typescript
 * import { CommunitySessionManager } from "../resource-manager/index.js";
 *
 * async function main() {
 *   const manager = new CommunitySessionManager("localhost", 10000);
 *   const session = await manager.getSession();
 *   const table = await session.timeTable("PT1S");
 *   await session.close();
 * }
 * ```
 */

import pino from "pino";
import type { Table } from "@deephaven/jsapi-types";
import { ClientObjectWrapper } from "./base.js";
import { SESSION_CONNECT_TIMEOUT_SECONDS } from "./constants.js";
import { CorePlusQueryInfo } from "./protobuf.js";
import {
  DeephavenConnectionError,
  QueryError,
  ResourceError,
  SessionCreationError,
  SessionError,
} from "../exceptions.js";
import {
  ConfigurationError,
  redactCommunitySessionConfig,
  resolveSecretField,
  validateCommunitySessionConfig,
} from "../config/index.js";
import { loadBytes } from "../io.js";

const _logger = pino({ name: "deephaven-mcp:client/session" });

// ---------------------------------------------------------------------------
// DHC session interface
// ---------------------------------------------------------------------------

/**
 * Minimal interface for the underlying DHC/DHE session object.
 * The actual session is loaded at runtime from the Deephaven server.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHC/DHE session objects have arbitrary shape
export interface DhcSession extends Record<string, any> {
  close(): void;
  readonly isAlive: boolean;
  readonly tables: string[];
  emptyTable(size: number): Table;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Arrow table from apache-arrow
  importTable(data: any): Table;
  timeTable(period: number | string, startTime?: number | string | null, blinkTable?: boolean): Table;
  mergeTables(tables: Table[], orderBy?: string | null): Table;
  query(table: Table): unknown;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- InputTable from DHC
  inputTable(schema?: any, initTable?: Table | null, keyCols?: string | string[] | null, blinkTable?: boolean): any;
  openTable(name: string): Table;
  bindTable(name: string, table: Table): void;
  runScript(script: string, systemic?: boolean | null): void;
}

/**
 * Minimal interface for a DHE DndSession (enterprise session).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE session objects have arbitrary shape
export interface DheSession extends DhcSession {
  pqinfo(): unknown;
  historicalTable(namespace: string, tableName: string): Table;
  liveTable(namespace: string, tableName: string): Table;
  catalogTable(): Table;
}

// ---------------------------------------------------------------------------
// Base session configuration for fromConfig
// ---------------------------------------------------------------------------

/** Configuration for creating a Core (community) session. */
export interface CommunitySessionConfig extends Record<string, unknown> {
  host?: string | null;
  port?: number | null;
  auth_type?: string;
  auth_token?: string;
  auth_token_env_var?: string;
  never_timeout?: boolean;
  session_type?: string;
  use_tls?: boolean;
  tls_root_certs?: string | null | Buffer | Uint8Array;
  client_cert_chain?: string | null | Buffer | Uint8Array;
  client_private_key?: string | null | Buffer | Uint8Array;
}

/**
 * Base class for asynchronous Deephaven session wrappers.
 *
 * Provides a unified async interface for all Deephaven session types (standard and enterprise).
 * Intended for subclassing by {@link CoreSession} (standard) and {@link CorePlusSession} (enterprise).
 *
 * @typeParam T - The underlying session type being wrapped.
 */
export class BaseSession<T extends DhcSession> extends ClientObjectWrapper<T> {
  protected readonly _programmingLanguage: string;

  /**
   * @param session - An initialized session object to wrap.
   * @param isEnterprise - Set `true` for enterprise (Core+) sessions, `false` for standard sessions.
   * @param programmingLanguage - The programming language associated with this session (e.g., "python").
   */
  constructor(session: T, isEnterprise: boolean, programmingLanguage: string) {
    super(session, isEnterprise);
    this._programmingLanguage = programmingLanguage;
  }

  /** The programming language associated with this session (e.g., `"python"`, `"groovy"`). */
  get programmingLanguage(): string {
    return this._programmingLanguage;
  }

  toString(): string {
    return String(this.wrapped);
  }

  toRepr(): string {
    return Object.prototype.toString.call(this.wrapped);
  }

  /**
   * Asynchronously creates an empty table with the specified number of rows on the server.
   *
   * @param size - The number of rows to include in the empty table. Must be a non-negative integer.
   * @returns A Table object representing the newly created empty table.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async emptyTable(size: number): Promise<Table> {
    _logger.debug(`[CoreSession:empty_table] Called with size=${size}`);
    try {
      return this.wrapped.emptyTable(size);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:empty_table] Connection error creating empty table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error creating empty table: ${err.message}`);
      }
      _logger.error(`[CoreSession:empty_table] Failed to create empty table: ${err.message}`);
      throw new QueryError(`Failed to create empty table: ${err.message}`);
    }
  }

  /**
   * Asynchronously creates a time table on the server.
   *
   * @param period - The interval at which the time table ticks (in nanoseconds or a time interval string, e.g. "PT1S").
   * @param startTime - The start time in nanoseconds or as a date-time formatted string; default is `null` (meaning now).
   * @param blinkTable - If `true`, creates a blink table. If `false` (default), creates an append-only time table.
   * @returns A Table object representing the time table.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async timeTable(
    period: number | string,
    startTime?: number | string | null,
    blinkTable = false,
  ): Promise<Table> {
    _logger.debug("[CoreSession:time_table] Called");
    try {
      return this.wrapped.timeTable(period, startTime ?? null, blinkTable);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:time_table] Connection error creating time table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error creating time table: ${err.message}`);
      }
      _logger.error(`[CoreSession:time_table] Failed to create time table: ${err.message}`);
      throw new QueryError(`Failed to create time table: ${err.message}`);
    }
  }

  /**
   * Asynchronously imports an Apache Arrow table as a new Deephaven table on the server.
   *
   * @param data - An Apache Arrow Table object to import into Deephaven.
   * @returns A Deephaven Table object representing the imported data.
   * @throws {DeephavenConnectionError} If there is a network or connection error during import.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Arrow table from apache-arrow
  async importTable(data: any): Promise<Table> {
    _logger.debug("[CoreSession:import_table] Called");
    try {
      return this.wrapped.importTable(data);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:import_table] Connection error importing table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error importing table: ${err.message}`);
      }
      _logger.error(`[CoreSession:import_table] Failed to import table: ${err.message}`);
      throw new QueryError(`Failed to import table: ${err.message}`);
    }
  }

  /**
   * Asynchronously merges several tables into one table on the server.
   *
   * @param tables - The list of Table objects to merge.
   * @param orderBy - If specified, the resultant table will be sorted on this column.
   * @returns A Table object.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async mergeTables(tables: Table[], orderBy?: string | null): Promise<Table> {
    _logger.debug(`[CoreSession:merge_tables] Called with ${tables.length} tables`);
    try {
      return this.wrapped.mergeTables(tables, orderBy ?? null);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:merge_tables] Connection error merging tables: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error merging tables: ${err.message}`);
      }
      _logger.error(`[CoreSession:merge_tables] Failed to merge tables: ${err.message}`);
      throw new QueryError(`Failed to merge tables: ${err.message}`);
    }
  }

  /**
   * Asynchronously creates a Query object to define a sequence of operations on a Deephaven table.
   *
   * @param table - A Table object to use as the starting point for the query.
   * @returns A Query-like object that can be used to chain operations.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async query(table: Table): Promise<unknown> {
    _logger.debug("[CoreSession:query] Called");
    try {
      return this.wrapped.query(table);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:query] Connection error creating query: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error creating query: ${err.message}`);
      }
      _logger.error(`[CoreSession:query] Failed to create query: ${err.message}`);
      throw new QueryError(`Failed to create query: ${err.message}`);
    }
  }

  /**
   * Asynchronously create an InputTable on the server using an Arrow schema or an existing Table.
   *
   * @param schema - Arrow schema for the input table. Required if `initTable` is not provided.
   * @param initTable - Existing Table to use as the initial state. Required if `schema` is not provided.
   * @param keyCols - Column(s) to use as unique key for keyed tables.
   * @param blinkTable - If `true`, creates a blink table.
   * @returns An InputTable object.
   * @throws {TypeError} If neither schema nor initTable is provided, or if parameters are invalid.
   * @throws {DeephavenConnectionError} If a network or connection error occurs.
   * @throws {QueryError} If the operation fails due to query or server error.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- InputTable from DHC
  async inputTable(schema?: any, initTable?: Table | null, keyCols?: string | string[] | null, blinkTable = false): Promise<any> {
    _logger.debug("[CoreSession:input_table] Called");
    try {
      return this.wrapped.inputTable(schema, initTable ?? null, keyCols ?? null, blinkTable);
    } catch (e) {
      const err = e as Error;
      if (err instanceof TypeError || err.name === "ValueError") {
        throw err;
      }
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:input_table] Connection error creating input table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error creating input table: ${err.message}`);
      }
      _logger.error(`[CoreSession:input_table] Failed to create input table: ${err.message}`);
      throw new QueryError(`Failed to create input table: ${err.message}`);
    }
  }

  /**
   * Asynchronously open a global table by name from the server.
   *
   * @param name - Name of the table to open. Must exist in the global namespace.
   * @returns The opened Table object.
   * @throws {ResourceError} If no table exists with the given name.
   * @throws {DeephavenConnectionError} If a network or connection error occurs.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async openTable(name: string): Promise<Table> {
    _logger.debug(`[CoreSession:open_table] Called with name=${name}`);
    try {
      return this.wrapped.openTable(name);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:open_table] Connection error opening table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error opening table: ${err.message}`);
      }
      if (err instanceof RangeError || err.name === "KeyError" || err.constructor?.name === "KeyError") {
        _logger.error(`[CoreSession:open_table] Table not found: ${err.message}`);
        throw new ResourceError(`Table not found: ${name}`);
      }
      _logger.error(`[CoreSession:open_table] Failed to open table: ${err.message}`);
      throw new QueryError(`Failed to open table: ${err.message}`);
    }
  }

  /**
   * Asynchronously bind a Table object to a global name on the server.
   *
   * @param name - Name to assign to the table in the global namespace.
   * @param table - The Table object to bind.
   * @throws {DeephavenConnectionError} If a network or connection error occurs.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async bindTable(name: string, table: Table): Promise<void> {
    _logger.debug(`[CoreSession:bind_table] Called with name=${name}`);
    try {
      this.wrapped.bindTable(name, table);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:bind_table] Connection error binding table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error binding table: ${err.message}`);
      }
      _logger.error(`[CoreSession:bind_table] Failed to bind table: ${err.message}`);
      throw new QueryError(`Failed to bind table: ${err.message}`);
    }
  }

  /**
   * Asynchronously close the session and release all associated server resources.
   *
   * @throws {DeephavenConnectionError} If a network or connection error occurs during close.
   * @throws {SessionError} If the session cannot be closed for non-connection reasons.
   */
  async close(): Promise<void> {
    _logger.debug("[CoreSession:close] Called");
    try {
      this.wrapped.close();
      _logger.debug("[CoreSession:close] Session closed successfully");
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:close] Connection error closing session: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error closing session: ${err.message}`);
      }
      _logger.error(`[CoreSession:close] Failed to close session: ${err.message}`);
      throw new SessionError(`Failed to close session: ${err.message}`);
    }
  }

  /**
   * Asynchronously execute a script on the server in the context of this session.
   *
   * @param script - The script code to execute.
   * @param systemic - If `true`, treat the script as systemically important.
   * @throws {DeephavenConnectionError} If a network or connection error occurs.
   * @throws {QueryError} If the script cannot be run or encounters an error during execution.
   */
  async runScript(script: string, systemic?: boolean | null): Promise<void> {
    _logger.debug("[CoreSession:run_script] Called");
    try {
      this.wrapped.runScript(script, systemic ?? undefined);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:run_script] Connection error running script: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error running script: ${err.message}`);
      }
      _logger.error(`[CoreSession:run_script] Failed to run script: ${err.message}`);
      throw new QueryError(`Failed to run script: ${err.message}`);
    }
  }

  /**
   * Asynchronously retrieve the names of all global tables available on the server.
   *
   * @returns List of table names currently registered in the global namespace.
   * @throws {DeephavenConnectionError} If a network or connection error occurs.
   * @throws {QueryError} If the operation fails due to a query-related error.
   */
  async tables(): Promise<string[]> {
    _logger.debug("[CoreSession:tables] Called");
    try {
      return this.wrapped.tables;
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:tables] Connection error listing tables: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error listing tables: ${err.message}`);
      }
      _logger.error(`[CoreSession:tables] Failed to list tables: ${err.message}`);
      throw new QueryError(`Failed to list tables: ${err.message}`);
    }
  }

  /**
   * Asynchronously check if the session is still alive.
   *
   * @returns `true` if the session is alive, `false` otherwise.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {SessionError} If there's an error checking session status.
   */
  async isAlive(): Promise<boolean> {
    _logger.debug("[CoreSession:is_alive] Called");
    try {
      return this.wrapped.isAlive;
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CoreSession:is_alive] Connection error checking session status: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error checking session status: ${err.message}`);
      }
      _logger.error(`[CoreSession:is_alive] Failed to check session status: ${err.message}`);
      throw new SessionError(`Failed to check session status: ${err.message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// CoreSession (community / DHC)
// ---------------------------------------------------------------------------

/**
 * An asynchronous wrapper around the standard Deephaven Community (DHC) session.
 *
 * Provides a fully asynchronous interface for interacting with standard Deephaven servers.
 *
 * @example
 * ```typescript
 * import { CommunitySessionManager } from "../resource-manager/index.js";
 *
 * async function main() {
 *   const manager = new CommunitySessionManager("localhost", 10000);
 *   const session = await manager.getSession();
 *   const table = await session.timeTable("PT1S");
 * }
 * ```
 */
export class CoreSession extends BaseSession<DhcSession> {
  /**
   * @param session - A DHC session instance to wrap.
   * @param programmingLanguage - The programming language associated with this session.
   */
  constructor(session: DhcSession, programmingLanguage: string) {
    super(session, false, programmingLanguage);
  }

  /**
   * Resolve auth token from config dictionary or environment variable.
   *
   * Supports the project-wide `<field>` / `<field>_env_var` schema convention.
   * Either `auth_token` is set inline, or `auth_token_env_var` names an environment
   * variable holding the token. If neither is set, returns the empty string.
   *
   * @param workerCfg - Configuration dictionary that may contain `auth_token` and/or `auth_token_env_var` keys.
   * @param logPrefix - Prefix for log messages.
   * @returns The resolved authentication token string, or `""` if neither field is set.
   * @throws {ConfigurationError} If `auth_token_env_var` names an environment variable that is unset or empty.
   */
  static _resolveAuthToken(
    workerCfg: Record<string, unknown>,
    logPrefix = "[CoreSession:from_config]",
  ): string {
    const resolved = resolveSecretField({
      config: workerCfg,
      inlineField: "auth_token",
      envVarField: "auth_token_env_var",
      context: "community session config",
    });
    if (resolved !== null && resolved !== undefined) {
      _logger.info(`${logPrefix} Resolved auth token from configuration.`);
      return resolved;
    }
    return "";
  }

  /**
   * Log documented guidance for specific known session creation errors.
   *
   * @param exception - The exception that occurred during session creation.
   */
  static _logSessionCreationErrorDetails(exception: Error): void {
    const errorMsg = exception.message.toLowerCase();

    if (errorMsg.includes("failed to get the configuration constants")) {
      _logger.error("[CoreSession:from_config] This error indicates a connection issue when trying to connect to the server.");
      _logger.error("[CoreSession:from_config] Verify that: 1) Server address and port are correct, 2) Deephaven server is running and accessible, 3) Network connectivity is available");
    } else if (
      ["certificate", "ssl", "tls", "handshake", "pkix path building failed", "cert_authority_invalid", "cert_common_name_invalid"]
        .some((p) => errorMsg.includes(p))
    ) {
      _logger.error("[CoreSession:from_config] This error indicates a TLS/SSL certificate issue.");
      _logger.error("[CoreSession:from_config] Verify that: 1) Server certificate is valid and not expired, 2) Certificate hostname matches connection URL, 3) CA certificate is trusted by the client");
    } else if (
      ["authentication failed", "unauthorized", "invalid credentials", "invalid token", "token expired", "access denied"]
        .some((p) => errorMsg.includes(p))
    ) {
      _logger.error("[CoreSession:from_config] This error indicates an authentication issue.");
      _logger.error("[CoreSession:from_config] Verify that: 1) Authentication credentials are correct, 2) Token is valid and not expired, 3) User has proper permissions, 4) Authentication service is running");
    } else if (
      ["timeout", "connection refused", "connection reset", "network unreachable"]
        .some((p) => errorMsg.includes(p))
    ) {
      _logger.error("[CoreSession:from_config] This error indicates a network connectivity issue.");
      _logger.error("[CoreSession:from_config] Verify that: 1) Server is running and accessible, 2) Network connectivity is available, 3) Firewall is not blocking the connection, 4) Port is correct and open");
    } else if (
      ["address already in use", "bind failed", "port already in use"]
        .some((p) => errorMsg.includes(p))
    ) {
      _logger.error("[CoreSession:from_config] This error indicates a port binding issue.");
      _logger.error("[CoreSession:from_config] Verify that: 1) Port is not already in use by another process, 2) You have permission to bind to the port, 3) Try a different port number");
    } else if (
      ["name resolution failed", "host not found", "nodename nor servname provided"]
        .some((p) => errorMsg.includes(p))
    ) {
      _logger.error("[CoreSession:from_config] This error indicates a DNS resolution issue.");
      _logger.error("[CoreSession:from_config] Verify that: 1) Hostname is correct and resolvable, 2) DNS server is accessible, 3) Network connectivity is available, 4) Try using IP address instead of hostname");
    }
  }

  /**
   * Asynchronously create a CoreSession from a community (core) session configuration dictionary.
   *
   * This method first validates the configuration using `validateCommunitySessionConfig`.
   * It then prepares all session parameters (including TLS and auth logic),
   * creates the underlying DHC session, and returns a CoreSession instance.
   * Sensitive fields in the config are redacted before logging.
   *
   * @param workerCfg - The worker's community session configuration.
   * @param timeoutSeconds - Maximum time in seconds to wait for connection.
   *   Defaults to `SESSION_CONNECT_TIMEOUT_SECONDS`.
   * @param sessionFactory - Optional factory for creating the session (for testing/injection).
   * @returns A new CoreSession instance.
   * @throws {ConfigurationError} If the configuration is invalid.
   * @throws {DeephavenConnectionError} If connection times out.
   * @throws {SessionCreationError} If session creation fails for any reason.
   */
  static async fromConfig(
    workerCfg: Record<string, unknown>,
    timeoutSeconds = SESSION_CONNECT_TIMEOUT_SECONDS,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- session factory is arbitrary at runtime
    sessionFactory?: (config: Record<string, unknown>) => any,
  ): Promise<CoreSession> {
    try {
      validateCommunitySessionConfig("from_config", workerCfg);
    } catch (e) {
      if (e instanceof ConfigurationError) {
        _logger.error(`[CoreSession:from_config] Invalid community session config: ${(e as Error).message}`);
      }
      throw e;
    }

    const redact = (cfg: Record<string, unknown>): Record<string, unknown> => {
      return "auth_token" in cfg || "client_private_key" in cfg
        ? redactCommunitySessionConfig(cfg)
        : cfg;
    };

    // Prepare session parameters
    const logCfg = redact(workerCfg);
    _logger.info(`[CoreSession:from_config] Community session configuration: ${JSON.stringify(logCfg)}`);

    const host: unknown = workerCfg["host"] ?? null;
    const port: unknown = workerCfg["port"] ?? null;
    const authType: string = (workerCfg["auth_type"] as string) ?? "Anonymous";
    const authToken: string = CoreSession._resolveAuthToken(workerCfg);
    const neverTimeout: boolean = (workerCfg["never_timeout"] as boolean) ?? false;
    const sessionType: string = (workerCfg["session_type"] as string) ?? "python";
    const programmingLanguage = sessionType;
    const useTls: boolean = (workerCfg["use_tls"] as boolean) ?? false;
    let tlsRootCerts: unknown = workerCfg["tls_root_certs"] ?? null;
    let clientCertChain: unknown = workerCfg["client_cert_chain"] ?? null;
    let clientPrivateKey: unknown = workerCfg["client_private_key"] ?? null;

    if (tlsRootCerts) {
      _logger.info(`[CoreSession:from_config] Loading TLS root certs from: ${workerCfg["tls_root_certs"]}`);
      tlsRootCerts = await loadBytes(tlsRootCerts as string);
      _logger.info("[CoreSession:from_config] Loaded TLS root certs successfully.");
    } else {
      _logger.debug("[CoreSession:from_config] No TLS root certs provided for community session.");
    }
    if (clientCertChain) {
      _logger.info(`[CoreSession:from_config] Loading client cert chain from: ${workerCfg["client_cert_chain"]}`);
      clientCertChain = await loadBytes(clientCertChain as string);
      _logger.info("[CoreSession:from_config] Loaded client cert chain successfully.");
    } else {
      _logger.debug("[CoreSession:from_config] No client cert chain provided for community session.");
    }
    if (clientPrivateKey) {
      _logger.info(`[CoreSession:from_config] Loading client private key from: ${workerCfg["client_private_key"]}`);
      clientPrivateKey = await loadBytes(clientPrivateKey as string);
      _logger.info("[CoreSession:from_config] Loaded client private key successfully.");
    } else {
      _logger.debug("[CoreSession:from_config] No client private key provided for community session.");
    }

    const sessionConfig: Record<string, unknown> = {
      host,
      port,
      auth_type: authType,
      auth_token: authToken,
      never_timeout: neverTimeout,
      session_type: sessionType,
      use_tls: useTls,
      tls_root_certs: tlsRootCerts,
      client_cert_chain: clientCertChain,
      client_private_key: clientPrivateKey,
    };

    const logCfg2 = redact(sessionConfig);
    _logger.info(`[CoreSession:from_config] Prepared Deephaven Community (Core) Session config: ${JSON.stringify(logCfg2)}`);

    try {
      _logger.info(`[CoreSession:from_config] Creating new Deephaven Community (Core) Session with config: ${JSON.stringify(logCfg2)}`);

      const createSession = sessionFactory ?? ((cfg: Record<string, unknown>) => {
        // Default: try to create via @deephaven/jsapi-nodejs (runtime)
        // eslint-disable-next-line @typescript-eslint/no-require-imports -- dynamic load at runtime
        const { Session } = require("@deephaven/jsapi-nodejs");
        return new Session(cfg);
      });

      const session = await Promise.race<DhcSession>([
        Promise.resolve(createSession(sessionConfig)),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error("TIMEOUT")), timeoutSeconds * 1000)),
      ]);

      _logger.info(`[CoreSession:from_config] Successfully created Deephaven Community (Core) Session: ${session}`);
      return new CoreSession(session, programmingLanguage);
    } catch (e) {
      const err = e as Error;
      if (err.message === "TIMEOUT") {
        _logger.error(`[CoreSession:from_config] Connection timed out after ${timeoutSeconds}s`);
        throw new DeephavenConnectionError(
          `Connection to Deephaven Community server timed out after ${timeoutSeconds} seconds.`,
        );
      }
      _logger.warn(`[CoreSession:from_config] Failed to create Deephaven Community (Core) Session with config: ${JSON.stringify(logCfg2)}: ${err.message}`);
      CoreSession._logSessionCreationErrorDetails(err);
      throw new SessionCreationError(
        `Failed to create Deephaven Community (Core) Session with config: ${JSON.stringify(logCfg2)}: ${err.message}`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// CorePlusSession (enterprise / DHE)
// ---------------------------------------------------------------------------

/**
 * A wrapper around the enterprise DHE DndSession class.
 *
 * This class provides access to enterprise-specific functionality like persistent queries,
 * historical data access, and catalog operations while maintaining compatibility with
 * the standard session interface.
 *
 * @example
 * ```typescript
 * import { CorePlusSessionFactory } from "./session-factory.js";
 *
 * async function work() {
 *   const factory = await CorePlusSessionFactory.fromUrl("https://myserver.example.com/iris/connection.json");
 *   await factory.password("username", "password");
 *   const session = await factory.connectToNewWorker({ heapSizeGb: 4 });
 *   const queryInfo = await session.pqinfo();
 * }
 * ```
 */
export class CorePlusSession extends BaseSession<DheSession> {
  /**
   * @param session - A DHE DndSession instance to wrap.
   * @param programmingLanguage - The programming language associated with this session.
   */
  constructor(session: DheSession, programmingLanguage: string) {
    super(session, true, programmingLanguage);
  }

  /**
   * Asynchronously retrieve the persistent query information for this session.
   *
   * @returns A {@link CorePlusQueryInfo} wrapper around the persistent query info.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {QueryError} If the persistent query information cannot be retrieved.
   */
  async pqinfo(): Promise<CorePlusQueryInfo> {
    _logger.debug("[CorePlusSession:pqinfo] Called");
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- runtime DHE object
      const protobufObj: Record<string, any> = this.wrapped.pqinfo() as Record<string, any>;
      return new CorePlusQueryInfo(protobufObj);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CorePlusSession:pqinfo] Connection error retrieving persistent query information: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error retrieving persistent query information: ${err.message}`);
      }
      _logger.error(`[CorePlusSession:pqinfo] Failed to retrieve persistent query information: ${err.message}`);
      throw new QueryError(`Failed to retrieve persistent query information: ${err.message}`);
    }
  }

  /**
   * Asynchronously fetches a historical table from the database on the server.
   *
   * @param namespace - The namespace of the table (e.g., `"market_data"`).
   * @param tableName - The name of the table within the specified namespace.
   * @returns A Table object representing the requested historical table.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {ResourceError} If the table cannot be found in the specified namespace.
   * @throws {QueryError} If the table exists but cannot be accessed.
   */
  async historicalTable(namespace: string, tableName: string): Promise<Table> {
    _logger.debug(`[CorePlusSession:historical_table] Called with namespace=${namespace}, table_name=${tableName}`);
    try {
      return this.wrapped.historicalTable(namespace, tableName);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CorePlusSession:historical_table] Connection error fetching historical table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error fetching historical table: ${err.message}`);
      }
      if (_isKeyError(err)) {
        _logger.error(`[CorePlusSession:historical_table] Historical table not found: ${err.message}`);
        throw new ResourceError(`Historical table not found: ${namespace}.${tableName}`);
      }
      _logger.error(`[CorePlusSession:historical_table] Failed to fetch historical table: ${err.message}`);
      throw new QueryError(`Failed to fetch historical table: ${err.message}`);
    }
  }

  /**
   * Asynchronously fetches a live table from the database on the server.
   *
   * @param namespace - The namespace of the table.
   * @param tableName - The name of the table within the specified namespace.
   * @returns A Table object representing the requested live table.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {ResourceError} If the table cannot be found in the specified namespace.
   * @throws {QueryError} If the table exists but cannot be accessed.
   */
  async liveTable(namespace: string, tableName: string): Promise<Table> {
    _logger.debug(`[CorePlusSession:live_table] Called with namespace=${namespace}, table_name=${tableName}`);
    try {
      return this.wrapped.liveTable(namespace, tableName);
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CorePlusSession:live_table] Connection error fetching live table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error fetching live table: ${err.message}`);
      }
      if (_isKeyError(err)) {
        _logger.error(`[CorePlusSession:live_table] Live table not found: ${err.message}`);
        throw new ResourceError(`Live table not found: ${namespace}.${tableName}`);
      }
      _logger.error(`[CorePlusSession:live_table] Failed to fetch live table: ${err.message}`);
      throw new QueryError(`Failed to fetch live table: ${err.message}`);
    }
  }

  /**
   * Asynchronously retrieves the catalog table, which contains metadata about available tables.
   *
   * @returns A Table object representing the catalog of available tables.
   * @throws {DeephavenConnectionError} If there is a network or connection error.
   * @throws {QueryError} If the catalog table cannot be retrieved.
   */
  async catalogTable(): Promise<Table> {
    _logger.debug("[CorePlusSession:catalog_table] Called");
    try {
      return this.wrapped.catalogTable();
    } catch (e) {
      const err = e as Error;
      if (_isConnectionError(err)) {
        _logger.error(`[CorePlusSession:catalog_table] Connection error fetching catalog table: ${err.message}`);
        throw new DeephavenConnectionError(`Connection error fetching catalog table: ${err.message}`);
      }
      _logger.error(`[CorePlusSession:catalog_table] Failed to fetch catalog table: ${err.message}`);
      throw new QueryError(`Failed to fetch catalog table: ${err.message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/** Check if an error represents a connection error. */
export function _isConnectionError(err: Error): boolean {
  return (
    err.name === "ConnectionError" ||
    err.constructor?.name === "ConnectionError" ||
    err.message?.toLowerCase().includes("connection")
  );
}

/** Check if an error represents a key not found error. */
export function _isKeyError(err: Error): boolean {
  return (
    err.name === "KeyError" ||
    err.constructor?.name === "KeyError"
  );
}
