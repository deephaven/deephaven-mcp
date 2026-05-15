/**
 * Custom exception types for Deephaven MCP.
 *
 * Defines specialized exception hierarchies related to various subsystems including session
 * management, client operations, authentication, and resource handling. These exceptions provide
 * fine-grained error reporting and enable more specific exception handling strategies.
 *
 * All exception classes in this module should be used consistently throughout the Deephaven MCP
 * system to signal recoverable or expected problems, allowing callers to implement appropriate
 * recovery or reporting strategies.
 *
 * Exception Hierarchy:
 *   - Base exceptions: McpError (base for all MCP exceptions), InternalError (extends McpError),
 *     UnsupportedOperationError (extends McpError), MissingEnterprisePackageError (extends InternalError)
 *   - Session exceptions: SessionError (extends McpError), SessionCreationError (extends SessionError),
 *     SessionLaunchError (extends SessionCreationError), InvalidSessionNameError (extends SessionError)
 *   - Authentication exceptions: AuthenticationError (extends McpError)
 *   - Query exceptions: QueryError (extends McpError)
 *   - Connection exceptions: DeephavenConnectionError (extends McpError)
 *   - Resource exceptions: ResourceError (extends McpError), RegistryItemNotFoundError (extends ResourceError)
 *   - Configuration exceptions: ConfigurationError (extends McpError)
 *
 * @example
 * ```typescript
 * import { SessionError, DeephavenConnectionError } from "./exceptions.js";
 *
 * function connectToSession(config: unknown) {
 *   try {
 *     return createSession(config);
 *   } catch (e) {
 *     if (e instanceof DeephavenConnectionError) {
 *       // Network or connection problems
 *       throw e;
 *     }
 *     if (e instanceof SessionError) {
 *       // Other session-related problems
 *       throw e;
 *     }
 *   }
 * }
 * ```
 */

// ─── Base Exceptions ─────────────────────────────────────────────────────────

/**
 * Base exception for all Deephaven MCP errors.
 *
 * This serves as the common base class for all MCP-related exceptions,
 * allowing callers to catch all MCP errors with a single catch block
 * while still maintaining specific exception types for detailed error handling.
 *
 * All MCP exceptions should inherit from this class either directly or
 * through one of the more specific base classes (SessionError, ConfigurationError, etc.).
 *
 * @example
 * ```typescript
 * try {
 *   // MCP operations
 * } catch (e) {
 *   if (e instanceof McpError) {
 *     // Handle any MCP-related error
 *   }
 * }
 * ```
 */
export class McpError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "McpError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Internal errors indicating bugs in the MCP implementation.
 *
 * InternalError should be raised when:
 * - Unexpected internal state is encountered
 * - Programming assumptions are violated
 * - System invariants are broken
 * - Unrecoverable implementation bugs occur
 *
 * @remarks In Python this also extends RuntimeError (multiple inheritance). In TypeScript,
 * we extend McpError only and document the dropped RuntimeError base.
 *
 * @example
 * ```typescript
 * if (unexpectedInternalState) {
 *   throw new InternalError("Unexpected state in registry");
 * }
 * ```
 */
export class InternalError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "InternalError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Exception raised when deephaven-coreplus-client package is not installed.
 *
 * This exception provides prominent error messaging to help users quickly identify
 * and resolve the missing package issue when attempting to use Deephaven Enterprise
 * (DHE) features.
 *
 * The exception formats the error message to be highly visible and actionable,
 * with clear instructions on how to resolve the issue.
 *
 * @example
 * ```typescript
 * if (!isEnterpriseAvailable) {
 *   throw new MissingEnterprisePackageError();
 * }
 * ```
 */
export class MissingEnterprisePackageError extends InternalError {
  /** The package-specific message before banner formatting. */
  packageMessage: string;

  /**
   * @param message - Optional custom message. If not provided, uses a default
   *   message about the missing deephaven-coreplus-client package.
   */
  constructor(message?: string) {
    const packageMessage =
      message ??
      "Core+ features are not available (deephaven-coreplus-client Python package not installed)";
    const separator = "=".repeat(80);
    const formattedMessage = `
${separator}
ERROR: Core+ features are not available
${separator}

The Python package 'deephaven-coreplus-client' is not installed.

This package is required to use Deephaven Enterprise (DHE) features.

To resolve this issue:
  1. Obtain the deephaven-coreplus-client wheel file from your
     Deephaven Enterprise administrator
  2. Install it using pip:

     pip install /path/to/deephaven_coreplus_client-X.Y.Z-py3-none-any.whl

For more information, see the installation documentation.

${separator}
`;
    super(formattedMessage);
    this.packageMessage = packageMessage;
    this.name = "MissingEnterprisePackageError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Exception raised when an operation is not supported in the current context.
 *
 * Common scenarios include:
 *   - Python-specific operations attempted on Groovy sessions
 *   - Enterprise (Core+) features attempted on Community sessions
 *   - Language-specific operations (e.g., pip packages) on non-Python sessions
 *   - Operations requiring specific capabilities not available in current environment
 *   - Features not yet implemented for certain session types
 *   - Platform-specific operations attempted on unsupported platforms
 *
 * @example
 * ```typescript
 * if (session.programmingLanguage !== "python") {
 *   throw new UnsupportedOperationError(
 *     `This operation requires a Python session, but session uses ${session.programmingLanguage}`
 *   );
 * }
 * ```
 *
 * @remarks This is distinct from built-in errors, which indicate planned but unimplemented
 * features. UnsupportedOperationError indicates operations that are fundamentally
 * incompatible with the current context.
 */
export class UnsupportedOperationError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "UnsupportedOperationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Session Exceptions ───────────────────────────────────────────────────────

/**
 * Base exception for all session-related errors.
 *
 * Use SessionError for errors with existing, already-initialized sessions, such as:
 * - Session connections cannot be closed properly
 * - Session enters an invalid or unexpected state
 * - Session operations timeout
 * - Session resource allocation fails after initialization
 *
 * For session initialization and creation failures, use SessionCreationError instead.
 * For session name parsing failures, use InvalidSessionNameError instead.
 *
 * @example
 * ```typescript
 * try {
 *   await session.close();
 * } catch (e) {
 *   if (e instanceof SessionError) {
 *     // Implement cleanup or recovery logic
 *   }
 * }
 * ```
 */
export class SessionError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "SessionError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Exception raised when a Deephaven session cannot be created or initialized.
 *
 * This exception is raised during the session creation and initialization phase, before
 * the session is fully operational. It indicates that a new session could not be instantiated
 * due to configuration errors, resource issues, authentication failures, or other problems
 * that prevent successful session startup.
 *
 * Common causes include:
 * - Failed to create a new worker for a session
 * - Unable to connect to a persistent query
 * - Failed to establish initial session connection
 * - Missing required session parameters
 * - Session initialization script failed
 * - Authentication failed during session startup
 *
 * For dynamic session launch failures (Docker/Python process startup), use the more
 * specific SessionLaunchError subclass instead.
 *
 * @example
 * ```typescript
 * try {
 *   const session = await sessionManager.connectToNewWorker({ heapSizeGb: 4 });
 * } catch (e) {
 *   if (e instanceof SessionCreationError) {
 *     // Implement fallback or retry logic
 *   }
 * }
 * ```
 */
export class SessionCreationError extends SessionError {
  constructor(message?: string) {
    super(message);
    this.name = "SessionCreationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Exception raised when launching a Deephaven Community session fails.
 *
 * This exception is raised during the launch phase of dynamically created community sessions
 * (via Docker or Python). It represents failures in the actual process/container startup,
 * port allocation, health checking, or session readiness verification.
 *
 * Examples:
 *   - Docker container failed to start
 *   - Python-launched Deephaven process failed to start
 *   - Unable to find available port for session
 *   - Session health check failed or timed out
 *   - Container/process startup returned non-zero exit code
 *   - Failed to stop running container/process
 *
 * @example
 * ```typescript
 * try {
 *   const session = await launcher.launch(sessionName, port, config);
 * } catch (e) {
 *   if (e instanceof SessionLaunchError) {
 *     // Implement cleanup or retry logic
 *   }
 * }
 * ```
 */
export class SessionLaunchError extends SessionCreationError {
  constructor(message?: string) {
    super(message);
    this.name = "SessionLaunchError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Exception raised when a session name cannot be parsed or is malformed.
 *
 * This exception is raised when attempting to parse a session name that does not
 * follow the expected format: `system_type:source:name` (e.g., "enterprise:factory1:session1"
 * or "community:local:worker1").
 *
 * @remarks In Python this also extends ValueError (multiple inheritance). In TypeScript,
 * we extend SessionError only and document the dropped ValueError base.
 *
 * Common causes include:
 * - Session name missing required colons (separators)
 * - Session name with too few or too many components
 * - Session name with empty components (e.g., "enterprise::session1")
 * - Session name with invalid system type
 *
 * @example
 * ```typescript
 * try {
 *   const [systemType, source, name] = BaseItemManager.parseFullName(sessionId);
 * } catch (e) {
 *   if (e instanceof InvalidSessionNameError) {
 *     // Handle malformed session name gracefully
 *   }
 * }
 * ```
 */
export class InvalidSessionNameError extends SessionError {
  constructor(message?: string) {
    super(message);
    this.name = "InvalidSessionNameError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Authentication Exceptions ────────────────────────────────────────────────

/**
 * Exception raised when authentication fails.
 *
 * Examples:
 *   - Invalid username or password
 *   - Expired authentication token
 *   - Invalid or corrupted private key
 *   - Authentication service unavailable
 *   - Insufficient permissions for requested operation
 *   - Failed SAML authentication
 *
 * @example
 * ```typescript
 * try {
 *   await sessionManager.password("username", "password");
 * } catch (e) {
 *   if (e instanceof AuthenticationError) {
 *     // Implement authentication retry or fallback
 *   }
 * }
 * ```
 */
export class AuthenticationError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "AuthenticationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Query Exceptions ─────────────────────────────────────────────────────────

/**
 * Exception raised when a query operation fails.
 *
 * Examples:
 *   - Query syntax errors
 *   - Failed table creation or manipulation
 *   - Invalid query parameters
 *   - Query execution timeout
 *   - Script execution failures
 *   - Table binding errors
 *
 * @example
 * ```typescript
 * try {
 *   const result = await session.query(table).updateView(["Value = x + 1"]).toTable();
 * } catch (e) {
 *   if (e instanceof QueryError) {
 *     // Handle the query failure
 *   }
 * }
 * ```
 */
export class QueryError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "QueryError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Connection Exceptions ────────────────────────────────────────────────────

/**
 * Exception raised when connection to a Deephaven service fails.
 *
 * This exception represents failures to establish or maintain connections to
 * Deephaven services. It wraps lower-level network errors and provides a consistent
 * interface for connection-related failures across the Deephaven MCP codebase.
 *
 * Common causes include:
 * - Network connectivity issues
 * - Server not responding or unreachable
 * - Connection timeout
 * - Connection reset or terminated unexpectedly
 * - TLS/SSL connection failures
 * - DNS resolution failures
 *
 * @example
 * ```typescript
 * try {
 *   const manager = CorePlusSessionManager.fromUrl("https://example.com/iris/connection.json");
 *   await manager.ping();
 * } catch (e) {
 *   if (e instanceof DeephavenConnectionError) {
 *     // Implement connection retry or fallback logic
 *   }
 * }
 * ```
 */
export class DeephavenConnectionError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "DeephavenConnectionError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Resource Exceptions ──────────────────────────────────────────────────────

/**
 * Exception raised when resource management operations fail.
 *
 * Examples:
 *   - Table not found
 *   - Key not found or cannot be deleted
 *   - Insufficient server resources to create a worker
 *   - Memory allocation limits exceeded
 *   - Resource quota exceeded
 *   - Historical or live table not found in namespace
 *
 * @example
 * ```typescript
 * try {
 *   const table = await session.openTable("non_existent_table");
 * } catch (e) {
 *   if (e instanceof ResourceError) {
 *     // Create resource or use alternative
 *   }
 * }
 * ```
 */
export class ResourceError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "ResourceError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Exception raised when an item is not found in a registry.
 *
 * This exception is raised by registry `get()` methods when attempting to retrieve
 * an item by name that does not exist in the registry.
 *
 * @remarks In Python this also extends KeyError (multiple inheritance). In TypeScript,
 * we extend ResourceError only and document the dropped KeyError base.
 *
 * Common causes include:
 * - Item removed from configuration file
 * - Item name misspelled or incorrectly formatted
 * - Item not yet discovered during initialization
 * - Factory or session offline and removed from active registry
 * - Stale reference to previously-existing item
 *
 * @example
 * ```typescript
 * try {
 *   const factory = await registry.get(factoryName);
 * } catch (e) {
 *   if (e instanceof RegistryItemNotFoundError) {
 *     // Handle missing item gracefully (e.g., skip, use default, retry)
 *   }
 * }
 * ```
 */
export class RegistryItemNotFoundError extends ResourceError {
  constructor(message?: string) {
    super(message);
    this.name = "RegistryItemNotFoundError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Configuration Exceptions ─────────────────────────────────────────────────

/**
 * Base class for all Deephaven MCP configuration errors.
 *
 * Key Distinction:
 *   ConfigurationError indicates problems with user-provided configuration data
 *   (files, environment variables) that can be corrected by the user. This is
 *   distinct from InternalError, which indicates bugs in the MCP code itself.
 *
 * Common causes include:
 *   - Invalid JSON syntax in configuration files
 *   - Missing required configuration fields
 *   - Invalid configuration field values or types
 *   - Conflicting configuration settings
 *   - Configuration referencing unavailable features
 *   - Environment variables not set or incorrectly formatted
 *
 * @example
 * ```typescript
 * try {
 *   const config = loadConfiguration(configFile);
 * } catch (e) {
 *   if (e instanceof ConfigurationError) {
 *     // Provide guidance to user on fixing configuration
 *   }
 * }
 * ```
 */
export class ConfigurationError extends McpError {
  constructor(message?: string) {
    super(message);
    this.name = "ConfigurationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
