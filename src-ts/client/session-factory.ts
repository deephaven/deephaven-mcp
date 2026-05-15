/**
 * Deephaven Core+ Session Manager Wrapper for asynchronous interaction with Deephaven Core+.
 *
 * This module provides an asynchronous wrapper around the Deephaven Core+ SessionManager
 * that enhances functionality while maintaining strict interface compatibility. The wrapper adds
 * comprehensive documentation, robust logging, and ensures non-blocking operation.
 *
 * The CorePlusSessionFactory delegates all method calls to the underlying session manager
 * instance and wraps returned sessions in CorePlusSession objects for consistent behavior.
 * It provides methods for:
 *   - Authentication (password, privateKey, saml)
 *   - Worker management (connectToNewWorker, connectToPersistentQuery)
 *   - Connection verification (ping)
 *   - Key management (uploadKey, deleteKey)
 *
 * Classes:
 *   {@link CorePlusSessionFactory}: Main async wrapper for the DHE SessionManager.
 *
 * @example
 * ```typescript
 * import { CorePlusSessionFactory } from "./session-factory.js";
 *
 * async function main() {
 *   const factory = await CorePlusSessionFactory.fromUrl("https://myserver.example.com/iris/connection.json");
 *   await factory.password("username", "password");
 *   const session = await factory.connectToNewWorker(4);
 *   const table = await session.emptyTable(10);
 *   await factory.close();
 * }
 * ```
 */

import pino from "pino";
import {
  AuthenticationError,
  DeephavenConnectionError,
  InternalError,
  QueryError,
  ResourceError,
  SessionCreationError,
  SessionError,
} from "../exceptions.js";
import { Credentials, PasswordCredentials, PrivateKeyCredentials } from "../auth/credentials/credentials.js";
import { validateEnterpriseConfig } from "../config/enterprise.js";
import { ConfigurationError } from "../exceptions.js";
import { ClientObjectWrapper, isEnterpriseAvailable } from "./base.js";
import {
  AUTH_TIMEOUT_SECONDS,
  PQ_CONNECTION_TIMEOUT_SECONDS,
  QUICK_OPERATION_TIMEOUT_SECONDS,
  SAML_AUTH_TIMEOUT_SECONDS,
  SESSION_CONNECT_TIMEOUT_SECONDS,
  WORKER_CREATION_TIMEOUT_SECONDS,
} from "./constants.js";
import { CorePlusAuthClient } from "./auth-client.js";
import { CorePlusControllerClient } from "./controller-client.js";
import { CorePlusQuerySerial } from "./protobuf.js";
import { CorePlusSession } from "./session.js";

const _logger = pino({ name: "deephaven-mcp:client/session-factory" });

/**
 * DHE session manager interface — the DHE JS API session manager contract.
 * The actual object is loaded at runtime from the DHE server.
 */
export interface DheSessionManager {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  readonly controller_client: Record<string, any>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE objects have arbitrary shape
  readonly auth_client: Record<string, any>;
  close(): Promise<void> | void;
  password(user: string, password: string, effectiveUser?: string | null): Promise<void> | void;
  private_key(file: string): Promise<void> | void;
  saml(): Promise<void> | void;
  ping(): Promise<boolean> | boolean;
  upload_key(publicKeyText: string): Promise<void> | void;
  delete_key(publicKeyText: string): Promise<void> | void;
  connect_to_new_worker(options: {
    name?: string | null;
    heap_size_gb: number;
    server?: string | null;
    extra_jvm_args?: string[] | null;
    extra_environment_vars?: string[] | null;
    engine?: string;
    auto_delete_timeout?: number | null;
    admin_groups?: string[] | null;
    viewer_groups?: string[] | null;
    timeout_seconds?: number;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- arbitrary configuration
    configuration_transformer?: ((config: any) => any) | null;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- arbitrary session arguments
    session_arguments?: Record<string, any> | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE session objects have arbitrary shape
  }): Promise<Record<string, any>> | Record<string, any>;
  connect_to_persistent_query(options: {
    name?: string | null;
    serial?: number | null;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- arbitrary session arguments
    session_arguments?: Record<string, any> | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE session objects have arbitrary shape
  }): Promise<Record<string, any>> | Record<string, any>;
}

/**
 * Asynchronous wrapper for the Deephaven Core+ SessionManager providing non-blocking operations.
 *
 * This class wraps an existing Deephaven Core+ session manager instance, delegating all
 * method calls to the underlying instance while providing enhanced error handling, and
 * comprehensive logging.
 *
 * @example
 * ```typescript
 * // Create a session factory using the fromUrl factory method
 * const factory = await CorePlusSessionFactory.fromUrl("https://example.com/iris/connection.json");
 * await factory.password("username", "password");
 *
 * // Connect to a new worker
 * const session = await factory.connectToNewWorker(4);
 * const table = await session.emptyTable(10);
 *
 * // Clean up
 * await factory.close();
 * ```
 */
export class CorePlusSessionFactory extends ClientObjectWrapper<DheSessionManager> {
  private readonly _controllerClient: CorePlusControllerClient;
  private readonly _authClient: CorePlusAuthClient;

  /**
   * Initialize the CorePlusSessionFactory wrapper with an existing session manager.
   *
   * Automatically initializes both `controllerClient` and `authClient` by accessing
   * the corresponding properties from the wrapped session manager.
   *
   * In most cases, prefer the class factory methods over direct instantiation:
   * - Use `fromUrl()` when you have a connection URL to the Deephaven server.
   * - Use `fromCredentials()` when you have a configuration dictionary and credentials.
   *
   * @param sessionManager - The SessionManager instance to wrap.
   * @throws {SessionError} If there was an error initializing the controllerClient property.
   * @throws {AuthenticationError} If there was an error initializing the authClient property.
   */
  constructor(sessionManager: DheSessionManager) {
    super(sessionManager, true);
    _logger.info(
      "[CorePlusSessionFactory:constructor] Successfully initialized CorePlusSessionFactory",
    );

    // Initialize controller client in constructor
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE controller objects have arbitrary shape
      const controllerClient = sessionManager.controller_client as Record<string, any>;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE controller client interface
      this._controllerClient = new CorePlusControllerClient(controllerClient as any);
      _logger.debug(
        "[CorePlusSessionFactory:constructor] Successfully initialized controller client",
      );
    } catch (e) {
      const err = e as Error;
      _logger.error(
        `[CorePlusSessionFactory:constructor] Failed to initialize controller client: ${err.message}`,
      );
      throw new SessionError(`Failed to initialize controller client: ${err.message}`);
    }

    // Initialize auth client in constructor
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE auth objects have arbitrary shape
      const authClient = sessionManager.auth_client as Record<string, any>;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE auth client interface
      this._authClient = new CorePlusAuthClient(authClient as any);
      _logger.debug(
        "[CorePlusSessionFactory:constructor] Successfully initialized auth client",
      );
    } catch (e) {
      const err = e as Error;
      _logger.error(
        `[CorePlusSessionFactory:constructor] Failed to initialize auth client: ${err.message}`,
      );
      throw new AuthenticationError(`Failed to initialize authentication client: ${err.message}`);
    }
  }

  /**
   * Create a CorePlusSessionFactory connected to a Deephaven server specified by URL.
   *
   * This is the recommended way to create a CorePlusSessionFactory when you know the
   * connection.json URL. After creating the factory, you must authenticate before using
   * other methods.
   *
   * @param url - The URL to the Deephaven server's connection.json file.
   * @param timeoutSeconds - Maximum time in seconds to wait for connection.
   *   Defaults to SESSION_CONNECT_TIMEOUT_SECONDS.
   * @returns A new factory instance connected to the specified server, ready for authentication.
   * @throws {InternalError} If the deephaven enterprise package is not available.
   * @throws {DeephavenConnectionError} If unable to connect to the server at the specified URL.
   */
  static async fromUrl(
    url: string,
    timeoutSeconds: number = SESSION_CONNECT_TIMEOUT_SECONDS,
  ): Promise<CorePlusSessionFactory> {
    if (!isEnterpriseAvailable) {
      throw new InternalError(
        "Deephaven enterprise features are not available. Please report this issue.",
      );
    }

    // eslint-disable-next-line @typescript-eslint/no-require-imports -- runtime import of optional enterprise package
    const { SessionManager } = require("@deephaven/jsapi-nodejs");
    _logger.debug(
      `[CorePlusSessionFactory:fromUrl] Creating SessionManager for URL: ${url}`,
    );
    const startTime = Date.now();
    let manager: DheSessionManager;
    try {
      manager = await Promise.race([
        Promise.resolve(new SessionManager(url)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:fromUrl] Connection to ${url} timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Connection to Deephaven at ${url} timed out after ${timeoutSeconds} seconds. ` +
          `The server may be unreachable.`,
        );
      }
      const elapsed = (Date.now() - startTime) / 1000;
      _logger.error(
        `[CorePlusSessionFactory:fromUrl] Failed to create SessionManager with URL ${url} after ${elapsed.toFixed(2)}s: ${err.message}`,
      );
      throw new DeephavenConnectionError(
        `Failed to establish connection to Deephaven at ${url} after ${elapsed.toFixed(2)}s: ${err.message}`,
      );
    }

    const elapsed = (Date.now() - startTime) / 1000;
    _logger.info(
      `[CorePlusSessionFactory:fromUrl] Successfully created SessionManager for URL ${url} in ${elapsed.toFixed(2)}s`,
    );
    const instance = new CorePlusSessionFactory(manager);

    // Subscribe to controller client for persistent query operations
    await instance._controllerClient.subscribe(timeoutSeconds);

    return instance;
  }

  /**
   * Create and authenticate a factory using mechanism-only credentials.
   *
   * This is the per-request authentication path used by the enterprise MCP server: the
   * server's config supplies only the connection target (`connection_json_url`) and
   * operational defaults; the actual credentials are supplied by the caller.
   *
   * @param config - Validated enterprise configuration dictionary.
   * @param creds - Mechanism-only credentials. Only PasswordCredentials and PrivateKeyCredentials
   *   are supported; any other type raises AuthenticationError.
   * @param timeoutSeconds - Maximum time to wait for the SessionManager and controller subscription.
   *   Defaults to SESSION_CONNECT_TIMEOUT_SECONDS.
   * @returns A fully authenticated factory.
   * @throws {InternalError} If the deephaven enterprise package is not available.
   * @throws {ConfigurationError} If config fails enterprise schema validation.
   * @throws {DeephavenConnectionError} If the SessionManager cannot be constructed.
   * @throws {AuthenticationError} If creds is a type this factory does not support, or if
   *   the upstream authentication call rejects it.
   */
  static async fromCredentials(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is arbitrary enterprise config dict
    config: Record<string, any>,
    creds: Credentials,
    timeoutSeconds: number = SESSION_CONNECT_TIMEOUT_SECONDS,
  ): Promise<CorePlusSessionFactory> {
    if (!isEnterpriseAvailable) {
      throw new InternalError(
        "Deephaven enterprise features are not available. Please report this issue.",
      );
    }

    try {
      validateEnterpriseConfig(config);
    } catch (e) {
      if (e instanceof ConfigurationError) {
        _logger.error(
          `[CorePlusSessionFactory:fromCredentials] Invalid enterprise system config: ${(e as Error).message}`,
        );
        throw e;
      }
      throw e;
    }

    const url: string = config["connection_json_url"];
    const credsType = creds.constructor.name;
    _logger.debug(
      `[CorePlusSessionFactory:fromCredentials] Creating SessionManager: url=${url}, creds=${credsType}`,
    );

    // eslint-disable-next-line @typescript-eslint/no-require-imports -- runtime import of optional enterprise package
    const { SessionManager } = require("@deephaven/jsapi-nodejs");

    const startTime = Date.now();
    let manager: DheSessionManager;
    try {
      manager = await Promise.race([
        Promise.resolve(new SessionManager(url)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:fromCredentials] Connection to ${url} timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Connection to Deephaven at ${url} timed out after ${timeoutSeconds} seconds. ` +
          `The server may be unreachable.`,
        );
      }
      const elapsed = (Date.now() - startTime) / 1000;
      _logger.error(
        `[CorePlusSessionFactory:fromCredentials] Failed to create SessionManager with URL ${url} after ${elapsed.toFixed(2)}s: ${err.message}`,
      );
      throw new DeephavenConnectionError(
        `Failed to establish connection to Deephaven at ${url} after ${elapsed.toFixed(2)}s: ${err.message}`,
      );
    }

    const elapsed = (Date.now() - startTime) / 1000;
    _logger.info(
      `[CorePlusSessionFactory:fromCredentials] Successfully created SessionManager (url=${url}) in ${elapsed.toFixed(2)}s`,
    );

    const instance = new CorePlusSessionFactory(manager);

    if (creds instanceof PasswordCredentials) {
      await instance.password(creds.username, creds.password, creds.effectiveUser ?? undefined);
    } else if (creds instanceof PrivateKeyCredentials) {
      await instance.privateKey(creds.keyText);
    } else {
      throw new AuthenticationError(
        `Unsupported credentials type ${JSON.stringify(credsType)} for enterprise authentication.`,
      );
    }

    _logger.info(
      `[CorePlusSessionFactory:fromCredentials] Subscribing to controller (creds=${credsType})`,
    );
    const subscribeStart = Date.now();
    await instance._controllerClient.subscribe(timeoutSeconds);
    const subscribeElapsed = (Date.now() - subscribeStart) / 1000;
    _logger.info(
      `[CorePlusSessionFactory:fromCredentials] Controller subscription completed in ${subscribeElapsed.toFixed(2)}s`,
    );

    return instance;
  }

  /**
   * Authentication client for direct interaction with the Deephaven authentication service.
   *
   * @returns A client for interacting with the Deephaven authentication service.
   */
  get authClient(): CorePlusAuthClient {
    return this._authClient;
  }

  /**
   * Controller client for direct management of server-side resources and workers.
   *
   * @returns A client for interacting with the Deephaven controller service.
   */
  get controllerClient(): CorePlusControllerClient {
    return this._controllerClient;
  }

  /**
   * Terminate this factory's connections to the authentication server and controller.
   *
   * @throws {SessionError} If terminating the connections fails.
   */
  async close(): Promise<void> {
    try {
      _logger.debug("[CorePlusSessionFactory:close] Closing session manager connection");
      await Promise.resolve(this.wrapped.close());
      _logger.debug(
        "[CorePlusSessionFactory:close] Successfully closed session manager connection",
      );
    } catch (e) {
      const err = e as Error;
      _logger.error(
        `[CorePlusSessionFactory:close] Failed to close session manager: ${err.message}`,
      );
      throw new SessionError(`Failed to close session manager connections: ${err.message}`);
    }
  }

  /**
   * Extract programming language from session object or return default.
   *
   * This helper method determines the programming language for a session by accessing the
   * session's type information. Defaults to "python" if no type is available.
   *
   * @param session - The raw session object from the enterprise system.
   * @returns The programming language string (e.g., "python", "groovy"). Defaults to "python".
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE session objects have arbitrary shape
  static _getProgrammingLanguage(session: Record<string, any>): string {
    // TODO: the private attribute _session_type is a temporary workaround:
    // See https://deephaven.atlassian.net/browse/DH-19984
    const sessionType: string | undefined = session["_session_type"] ?? session["sessionType"];
    return sessionType ?? "python";
  }

  /**
   * Create a new worker process and establish a session connection to it.
   *
   * @param heapSizeGb - JVM heap size in gigabytes (e.g., 8 or 2.5).
   * @param name - Optional name for the worker process.
   * @param server - Specific server to run the worker on. undefined lets controller choose.
   * @param extraJvmArgs - Additional JVM arguments.
   * @param extraEnvironmentVars - Environment variables to set for the worker process.
   * @param engine - Engine type. Defaults to "DeephavenCommunity".
   * @param autoDeleteTimeout - Seconds of inactivity before auto-deletion. Defaults to 600.
   *   null prevents auto-deletion.
   * @param adminGroups - User groups with administrative permissions.
   * @param viewerGroups - User groups with read-only access.
   * @param timeoutSeconds - Maximum time to wait for the worker to start.
   *   Defaults to WORKER_CREATION_TIMEOUT_SECONDS.
   * @param configurationTransformer - Optional function to transform the internal config.
   * @param sessionArguments - Additional keyword arguments for the session constructor.
   * @returns A fully initialized session object connected to the new worker.
   * @throws {ResourceError} If there are insufficient server resources.
   * @throws {SessionCreationError} If an error occurs during worker creation.
   * @throws {DeephavenConnectionError} If there is a network or timeout problem.
   */
  async connectToNewWorker(
    heapSizeGb: number,
    name?: string | null,
    server?: string | null,
    extraJvmArgs?: string[] | null,
    extraEnvironmentVars?: string[] | null,
    engine: string = "DeephavenCommunity",
    autoDeleteTimeout: number | null = 600,
    adminGroups?: string[] | null,
    viewerGroups?: string[] | null,
    timeoutSeconds: number = WORKER_CREATION_TIMEOUT_SECONDS,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- arbitrary configuration transformer
    configurationTransformer?: ((config: any) => any) | null,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- arbitrary session arguments
    sessionArguments?: Record<string, any> | null,
  ): Promise<CorePlusSession> {
    try {
      _logger.debug(
        "[CorePlusSessionFactory:connectToNewWorker] Creating new worker and connecting to it",
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- build options object dynamically to avoid exactOptionalPropertyTypes issues
      const workerOptions: any = {
        heap_size_gb: heapSizeGb,
        engine,
        auto_delete_timeout: autoDeleteTimeout,
        timeout_seconds: timeoutSeconds,
      };
      if (name !== undefined) workerOptions["name"] = name;
      if (server !== undefined) workerOptions["server"] = server;
      if (extraJvmArgs !== undefined) workerOptions["extra_jvm_args"] = extraJvmArgs;
      if (extraEnvironmentVars !== undefined) workerOptions["extra_environment_vars"] = extraEnvironmentVars;
      if (adminGroups !== undefined) workerOptions["admin_groups"] = adminGroups;
      if (viewerGroups !== undefined) workerOptions["viewer_groups"] = viewerGroups;
      if (configurationTransformer !== undefined) workerOptions["configuration_transformer"] = configurationTransformer;
      if (sessionArguments !== undefined) workerOptions["session_arguments"] = sessionArguments;
      const session = await Promise.resolve(this.wrapped.connect_to_new_worker(workerOptions));
      _logger.debug(
        "[CorePlusSessionFactory:connectToNewWorker] Successfully connected to new worker",
      );
      const programmingLanguage = CorePlusSessionFactory._getProgrammingLanguage(session);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE session objects satisfy DheSession at runtime
      return new CorePlusSession(session as any, programmingLanguage);
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:connectToNewWorker] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Worker creation timed out after ${timeoutSeconds} seconds. ` +
          `The server may be overloaded or unreachable.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:connectToNewWorker] Connection error while creating new worker: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Connection error while creating new worker: ${err.message}`,
        );
      }
      if (err instanceof ResourceError) {
        _logger.error(
          `[CorePlusSessionFactory:connectToNewWorker] Insufficient resources to create worker: ${err.message}`,
        );
        throw err;
      }
      _logger.error(
        `[CorePlusSessionFactory:connectToNewWorker] Failed to connect to new worker: ${err.message}`,
      );
      throw new SessionCreationError(`Failed to create and connect to new worker: ${err.message}`);
    }
  }

  /**
   * Connect to an existing persistent query (worker) by name or serial number.
   *
   * @param name - The name of the persistent query to connect to. Either name or serial must
   *   be provided, but not both.
   * @param serial - The unique serial number of the persistent query. Either name or serial must
   *   be provided, but not both.
   * @param sessionArguments - Additional arguments to pass to the session constructor.
   * @param timeoutSeconds - Maximum time in seconds to wait for connection.
   *   Defaults to PQ_CONNECTION_TIMEOUT_SECONDS.
   * @returns A fully initialized session object connected to the existing worker.
   * @throws {RangeError} If neither name nor serial is provided, or if both are provided.
   * @throws {QueryError} If the persistent query cannot be found or is not in a valid state.
   * @throws {DeephavenConnectionError} If a network-related issue occurs.
   * @throws {SessionCreationError} If there's an error establishing the session for any other reason.
   */
  async connectToPersistentQuery(
    name?: string | null,
    serial?: CorePlusQuerySerial | null,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- arbitrary session arguments
    sessionArguments?: Record<string, any> | null,
    timeoutSeconds: number = PQ_CONNECTION_TIMEOUT_SECONDS,
  ): Promise<CorePlusSession> {
    try {
      _logger.debug(
        `[CorePlusSessionFactory:connectToPersistentQuery] Connecting to persistent query (name=${name}, serial=${serial})`,
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- build options object dynamically to avoid exactOptionalPropertyTypes issues
      const pqOptions: any = {};
      if (name !== undefined) pqOptions["name"] = name;
      if (serial !== undefined) pqOptions["serial"] = serial as number | null;
      if (sessionArguments !== undefined) pqOptions["session_arguments"] = sessionArguments;
      const session = await Promise.race([
        Promise.resolve(this.wrapped.connect_to_persistent_query(pqOptions)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug(
        "[CorePlusSessionFactory:connectToPersistentQuery] Successfully connected to persistent query",
      );
      const programmingLanguage = CorePlusSessionFactory._getProgrammingLanguage(session);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DHE session objects satisfy DheSession at runtime
      return new CorePlusSession(session as any, programmingLanguage);
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:connectToPersistentQuery] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Connection to persistent query timed out after ${timeoutSeconds} seconds. ` +
          `The server may be overloaded or unreachable.`,
        );
      }
      if (err instanceof RangeError || err instanceof TypeError) {
        throw err;
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:connectToPersistentQuery] Connection error while connecting to persistent query: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Connection error while connecting to persistent query: ${err.message}`,
        );
      }
      // KeyError analog: key not found
      if (err.name === "KeyError" || err.message?.includes("not found")) {
        _logger.error(
          `[CorePlusSessionFactory:connectToPersistentQuery] Failed to find persistent query: ${err.message}`,
        );
        throw new QueryError(`Persistent query not found: ${err.message}`);
      }
      _logger.error(
        `[CorePlusSessionFactory:connectToPersistentQuery] Failed to connect to persistent query: ${err.message}`,
      );
      throw new SessionCreationError(
        `Failed to establish connection to persistent query: ${err.message}`,
      );
    }
  }

  /**
   * Delete a previously uploaded public key from the Deephaven server.
   *
   * @param publicKeyText - The complete text of the public key to delete.
   * @param timeoutSeconds - Maximum time in seconds to wait for the operation.
   *   Defaults to QUICK_OPERATION_TIMEOUT_SECONDS.
   * @throws {ResourceError} If the key cannot be deleted.
   * @throws {DeephavenConnectionError} If there is a network problem or timeout.
   */
  async deleteKey(
    publicKeyText: string,
    timeoutSeconds: number = QUICK_OPERATION_TIMEOUT_SECONDS,
  ): Promise<void> {
    try {
      _logger.debug("[CorePlusSessionFactory:deleteKey] Deleting public key");
      await Promise.race([
        Promise.resolve(this.wrapped.delete_key(publicKeyText)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug(
        "[CorePlusSessionFactory:deleteKey] Successfully deleted public key",
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:deleteKey] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Key deletion timed out after ${timeoutSeconds} seconds.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:deleteKey] Connection error when deleting key: ${err.message}`,
        );
        throw new DeephavenConnectionError(`Failed to connect while deleting key: ${err.message}`);
      }
      _logger.error(
        `[CorePlusSessionFactory:deleteKey] Failed to delete key: ${err.message}`,
      );
      throw new ResourceError(`Failed to delete authentication key: ${err.message}`);
    }
  }

  /**
   * Authenticate to the server using username and password credentials.
   *
   * @param user - The username to authenticate with.
   * @param password - The user's password. Never logged.
   * @param effectiveUser - The user to operate as after authentication. undefined uses the
   *   authenticated user.
   * @param timeoutSeconds - Maximum time in seconds to wait for authentication.
   *   Defaults to AUTH_TIMEOUT_SECONDS.
   * @throws {AuthenticationError} If authentication fails.
   * @throws {DeephavenConnectionError} If there is a problem connecting to the authentication server.
   */
  async password(
    user: string,
    password: string,
    effectiveUser?: string | null,
    timeoutSeconds: number = AUTH_TIMEOUT_SECONDS,
  ): Promise<void> {
    try {
      _logger.debug(
        `[CorePlusSessionFactory:password] Authenticating as user: ${user} (effective user: ${effectiveUser ?? user})`,
      );
      await Promise.race([
        Promise.resolve(this.wrapped.password(user, password, effectiveUser)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug("[CorePlusSessionFactory:password] Successfully authenticated");
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:password] Authentication timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Authentication timed out after ${timeoutSeconds} seconds. ` +
          `The server may be overloaded or unreachable.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:password] Failed to connect to authentication server: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Failed to connect to authentication server: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusSessionFactory:password] Authentication failed: ${err.message}`,
      );
      throw new AuthenticationError(`Failed to authenticate user ${user}: ${err.message}`);
    }
  }

  /**
   * Send a connectivity check ping to verify the connection to Deephaven services.
   *
   * @param timeoutSeconds - Maximum time in seconds to wait for the ping.
   *   Defaults to QUICK_OPERATION_TIMEOUT_SECONDS.
   * @returns True if both the authentication server and controller responded, False otherwise.
   * @throws {DeephavenConnectionError} If there is a more serious connection error or timeout.
   */
  async ping(timeoutSeconds: number = QUICK_OPERATION_TIMEOUT_SECONDS): Promise<boolean> {
    try {
      _logger.debug(
        "[CorePlusSessionFactory:ping] Sending ping to authentication server and controller",
      );
      const result = await Promise.race([
        Promise.resolve(this.wrapped.ping()),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug(`[CorePlusSessionFactory:ping] Ping result: ${result}`);
      return Boolean(result);
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:ping] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Ping timed out after ${timeoutSeconds} seconds.`,
        );
      }
      _logger.error(`[CorePlusSessionFactory:ping] Ping failed: ${err.message}`);
      throw new DeephavenConnectionError(`Failed to ping server: ${err.message}`);
    }
  }

  /**
   * Authenticate to the server using a Deephaven format private key file or key text.
   *
   * @param file - A string containing the path to the private key file, or the raw key text.
   * @param timeoutSeconds - Maximum time in seconds to wait for authentication.
   *   Defaults to AUTH_TIMEOUT_SECONDS.
   * @throws {AuthenticationError} If authentication with the private key fails.
   * @throws {DeephavenConnectionError} If there is a problem connecting to the authentication server.
   */
  async privateKey(
    file: string,
    timeoutSeconds: number = AUTH_TIMEOUT_SECONDS,
  ): Promise<void> {
    try {
      _logger.debug(
        "[CorePlusSessionFactory:privateKey] Authenticating with private key",
      );
      await Promise.race([
        Promise.resolve(this.wrapped.private_key(file)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug(
        "[CorePlusSessionFactory:privateKey] Successfully authenticated with private key",
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:privateKey] Authentication timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Authentication timed out after ${timeoutSeconds} seconds. ` +
          `The server may be overloaded or unreachable.`,
        );
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- NodeJS Error has .code on ENOENT
      if ((err as any).code === "ENOENT" || err.message?.includes("no such file")) {
        _logger.error(
          `[CorePlusSessionFactory:privateKey] Private key file not found: ${err.message}`,
        );
        throw new AuthenticationError(`Private key file not found: ${err.message}`);
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:privateKey] Failed to connect to authentication server: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Failed to connect to authentication server: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusSessionFactory:privateKey] Private key authentication failed: ${err.message}`,
      );
      throw new AuthenticationError(`Failed to authenticate with private key: ${err.message}`);
    }
  }

  /**
   * Authenticate asynchronously using SAML-based Single Sign-On (SSO).
   *
   * @param timeoutSeconds - Maximum time in seconds to wait for SAML authentication.
   *   Defaults to SAML_AUTH_TIMEOUT_SECONDS.
   * @throws {AuthenticationError} If SAML authentication fails.
   * @throws {DeephavenConnectionError} If there is a connection problem.
   */
  async saml(timeoutSeconds: number = SAML_AUTH_TIMEOUT_SECONDS): Promise<void> {
    try {
      _logger.debug("[CorePlusSessionFactory:saml] Starting SAML authentication flow");
      await Promise.race([
        Promise.resolve(this.wrapped.saml()),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug(
        "[CorePlusSessionFactory:saml] Successfully authenticated using SAML",
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:saml] SAML authentication timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `SAML authentication timed out after ${timeoutSeconds} seconds. ` +
          `The authentication flow may have been abandoned or the server is unreachable.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:saml] Failed to connect to authentication server or SAML provider: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Failed to connect to authentication server or SAML provider: ${err.message}`,
        );
      }
      if (err instanceof RangeError) {
        _logger.error(
          `[CorePlusSessionFactory:saml] SAML configuration error: ${err.message}`,
        );
        throw new AuthenticationError(`SAML configuration error: ${err.message}`);
      }
      _logger.error(
        `[CorePlusSessionFactory:saml] SAML authentication failed: ${err.message}`,
      );
      throw new AuthenticationError(`Failed to authenticate via SAML: ${err.message}`);
    }
  }

  /**
   * Upload a public key to the Deephaven server for certificate-based authentication.
   *
   * @param publicKeyText - The full text representation of the public key to upload.
   * @param timeoutSeconds - Maximum time in seconds to wait for the operation.
   *   Defaults to QUICK_OPERATION_TIMEOUT_SECONDS.
   * @throws {ResourceError} If uploading the key fails.
   * @throws {DeephavenConnectionError} If there is a connection problem or timeout.
   */
  async uploadKey(
    publicKeyText: string,
    timeoutSeconds: number = QUICK_OPERATION_TIMEOUT_SECONDS,
  ): Promise<void> {
    try {
      _logger.debug("[CorePlusSessionFactory:uploadKey] Uploading public key");
      await Promise.race([
        Promise.resolve(this.wrapped.upload_key(publicKeyText)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`__timeout__:${timeoutSeconds}`)),
            timeoutSeconds * 1000,
          ),
        ),
      ]);
      _logger.debug(
        "[CorePlusSessionFactory:uploadKey] Successfully uploaded public key",
      );
    } catch (e) {
      const err = e as Error;
      if (err.message?.startsWith("__timeout__:")) {
        _logger.error(
          `[CorePlusSessionFactory:uploadKey] Timed out after ${timeoutSeconds}s`,
        );
        throw new DeephavenConnectionError(
          `Key upload timed out after ${timeoutSeconds} seconds.`,
        );
      }
      if (err.name === "ConnectionError" || err.constructor?.name === "ConnectionError") {
        _logger.error(
          `[CorePlusSessionFactory:uploadKey] Connection error when uploading key: ${err.message}`,
        );
        throw new DeephavenConnectionError(
          `Failed to connect while uploading key: ${err.message}`,
        );
      }
      _logger.error(
        `[CorePlusSessionFactory:uploadKey] Failed to upload key: ${err.message}`,
      );
      throw new ResourceError(`Failed to upload authentication key: ${err.message}`);
    }
  }
}
