/**
 * Base classes and utilities for the Deephaven client interface.
 *
 * This module provides common base classes and utility functions used throughout the
 * Deephaven client package. It contains functionality that is shared between both
 * standard and enterprise client components.
 *
 * The primary purpose of this module is to establish a consistent wrapping pattern
 * for JavaScript client objects, providing them with enhanced async interfaces and
 * error handling capabilities. It handles feature detection for enterprise components
 * and provides appropriate error handling when required features are not available.
 *
 * The wrapping pattern implemented here enables several key benefits:
 * 1. Transparent conversion of blocking calls to non-blocking async calls
 * 2. Enhanced error handling with more descriptive exceptions
 * 3. Consistent logging across all client components
 * 4. Type safety through generic typing
 *
 * Attributes:
 *   {@link isEnterpriseAvailable} - Flag indicating if enterprise features are available
 *     in the current environment. This is determined by attempting to load the
 *     DHE JSAPI bundle.
 */

import pino from "pino";
import { InternalError } from "../exceptions.js";

const _logger = pino({ name: "deephaven-mcp:client/base" });

/**
 * Flag indicating if enterprise features are available in the current environment.
 *
 * In the TypeScript port, enterprise features use `@deephaven/jsapi-nodejs` connecting
 * to a DHE server which serves its own JSAPI bundle. This flag is `true` by default
 * since the package is always available; runtime availability is determined when
 * actually connecting to a DHE server.
 */
export let isEnterpriseAvailable = false;
try {
  // Attempt dynamic require of the DHE jsapi-nodejs package
  // eslint-disable-next-line @typescript-eslint/no-require-imports -- dynamic availability check
  require("@deephaven/jsapi-nodejs");
  isEnterpriseAvailable = true;
  _logger.debug("Enterprise features available");
} catch {
  _logger.debug("Enterprise features not available");
}

/**
 * Base class for client wrappers with generic type support.
 *
 * This class serves as a foundation for wrappers around Deephaven client objects.
 * It provides common functionality for all client wrappers, such as access to the
 * underlying wrapped client object. The generic type parameter T represents the
 * type of the wrapped object, ensuring type safety throughout inheritance.
 *
 * Purpose:
 *   1. Provide a consistent pattern for wrapping client objects with enhanced interfaces
 *   2. Enable non-blocking asynchronous access to potentially blocking operations
 *   3. Ensure proper detection and handling of enterprise feature requirements
 *   4. Establish a consistent error handling pattern across client components
 *
 * Usage Pattern:
 *   When extending this class, implementers should:
 *   1. Initialize with the object to be wrapped and specify whether it requires enterprise features
 *   2. Create async wrapper methods that delegate to the underlying wrapped client object
 *   3. Add enhanced error handling by catching errors and translating them to MCP exceptions
 *   4. Implement consistent logging patterns for method entry, success, and error conditions
 */
export class ClientObjectWrapper<T> {
  private readonly _wrapped: T;

  /**
   * Initialize a wrapper for a client object.
   *
   * @param wrapped - The client object to wrap. Must not be null/undefined.
   * @param isEnterprise - Specifies whether the wrapped object requires enterprise features.
   *   Must be `true` for enterprise objects and `false` for non-enterprise objects.
   *   When `true`, availability of enterprise features will be verified using
   *   the module-level {@link isEnterpriseAvailable} flag.
   * @throws {Error} If the wrapped object is null/undefined.
   * @throws {InternalError} If `isEnterprise=true` but enterprise features are not available.
   *   This typically indicates a programming error in the library, as enterprise
   *   wrappers should only be created in environments where enterprise features are available.
   */
  constructor(wrapped: T, isEnterprise: boolean) {
    if (wrapped === null || wrapped === undefined) {
      _logger.error("ClientObjectWrapper constructor called with None");
      throw new Error("Cannot wrap None");
    }

    this._wrapped = wrapped;

    if (isEnterprise && !isEnterpriseAvailable) {
      _logger.error(
        "[ClientObjectWrapper] Constructor called with enterprise=True when enterprise features are not available. " +
          "Please report this issue.",
      );
      throw new InternalError(
        "ClientObjectWrapper constructor called with enterprise=True when enterprise features are not available. Please report this issue.",
      );
    }
  }

  /**
   * Access the underlying wrapped client object.
   *
   * This property provides direct access to the wrapped client object when
   * needed. In most cases, consumers should use the wrapper's methods instead
   * of directly accessing the wrapped client.
   *
   * @returns The wrapped client object of type T.
   */
  get wrapped(): T {
    return this._wrapped;
  }
}
