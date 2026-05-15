/**
 * Exception handling utilities for Deephaven MCP servers.
 *
 * This module provides comprehensive structured logging for unhandled exceptions,
 * implementing Google Cloud Platform (GCP) Cloud Logging optimized for production environments.
 *
 * Key Features:
 * - Client disconnect detection and graceful handling
 * - Google Cloud Logging integration for native GCP log aggregation
 * - Structured exception metadata for filtering and alerting
 * - Defensive error handling to prevent logging failures from masking exceptions
 * - DEBUG-level logging for expected client disconnects vs ERROR-level for server errors
 *
 * @remarks In Python, this module patches Uvicorn's RequestResponseCycle for ASGI exception handling.
 * In TypeScript/Node.js, there is no Uvicorn equivalent. This module provides equivalent
 * utilities using pino logger for structured logging in Express/Fastify/other HTTP servers.
 */

import pino from "pino";
import type { Logger } from "pino";

/**
 * Structured logger interface for GCP-compatible logging.
 */
export interface GcpLogger {
  error(msg: string, extra?: Record<string, unknown>): void;
  debug(msg: string, extra?: Record<string, unknown>): void;
}

// Lazy initialization - logger created only when needed
let _gcpLogger: GcpLogger | null = null;

/**
 * Configure Google Cloud Logging for ASGI exception handling.
 *
 * Creates a pino logger named 'gcp_asgi_errors' configured for structured log output.
 * In production, this can be configured to send to Google Cloud Logging.
 *
 * @returns Configured logger instance.
 */
export function _setupGcpLogger(): GcpLogger {
  const pinoLogger = pino({ name: "gcp_asgi_errors" });
  return {
    error(msg: string, extra?: Record<string, unknown>): void {
      if (extra) {
        pinoLogger.error(extra, msg);
      } else {
        pinoLogger.error(msg);
      }
    },
    debug(msg: string, extra?: Record<string, unknown>): void {
      if (extra) {
        pinoLogger.debug(extra, msg);
      } else {
        pinoLogger.debug(msg);
      }
    },
  };
}

/**
 * Get or create the GCP logger using lazy initialization.
 *
 * This prevents early initialization issues by only creating the GCP logger
 * when it's actually needed, rather than at module import time.
 *
 * @returns The GCP logger instance.
 */
export function _getGcpLogger(): GcpLogger {
  if (_gcpLogger === null) {
    _gcpLogger = _setupGcpLogger();
  }
  return _gcpLogger;
}

/**
 * Reset the GCP logger to null (for testing).
 * @internal For testing only.
 */
export function _resetGcpLogger(): void {
  _gcpLogger = null;
}

/**
 * Check if an error indicates a client disconnect rather than a server error.
 *
 * This function recursively examines errors to detect ClientDisconnectError
 * (equivalent to Python's anyio.ClosedResourceError), which typically indicates
 * that a client has disconnected during request processing.
 * It handles direct errors, AggregateErrors, and nested errors via `cause` attribute.
 *
 * @param err - The error to examine for client disconnect indicators.
 * @returns `true` if the error indicates a client disconnect, `false` otherwise.
 *
 * @remarks Client disconnects are expected behavior and should be logged at DEBUG level
 * rather than ERROR level to reduce noise in production logs.
 */
export function _isClientDisconnectError(err: unknown): boolean {
  if (!(err instanceof Error)) {
    return false;
  }

  // Direct ClientDisconnectError (equivalent to anyio.ClosedResourceError)
  if (err.name === "ClientDisconnectError") {
    return true;
  }

  // AggregateError containing ClientDisconnectError (equivalent to Python's ExceptionGroup)
  if (err instanceof AggregateError && Array.isArray(err.errors)) {
    for (const subErr of err.errors) {
      if (_isClientDisconnectError(subErr)) {
        return true;
      }
    }
  }

  // Check nested cause
  if ((err as Error & { cause?: unknown }).cause) {
    if (_isClientDisconnectError((err as Error & { cause?: unknown }).cause)) {
      return true;
    }
  }

  return false;
}

/**
 * Wrap an async request handler with comprehensive ASGI exception logging.
 *
 * This function addresses limitations in default exception handling by wrapping
 * async application execution with structured logging optimized for
 * Google Cloud Platform (GCP) Cloud Run environments. It distinguishes between
 * client disconnects and actual server errors for appropriate log severity.
 *
 * Exception Handling:
 *   - Client disconnects (ClientDisconnectError): Logged at DEBUG level
 *   - Server errors: Logged at ERROR level with full structured metadata
 *   - Recursive detection of nested exceptions and AggregateErrors
 *   - Defensive error handling to prevent logging failures
 *
 * @param handler - The async request handler to wrap.
 * @returns A wrapped handler with exception logging.
 *
 * @remarks In Python, this is applied as a monkey-patch to Uvicorn's RequestResponseCycle.
 * In TypeScript, this is applied as a middleware wrapper function.
 */
export function wrapWithExceptionLogging<TArgs extends unknown[], TReturn>(
  handler: (...args: TArgs) => Promise<TReturn>
): (...args: TArgs) => Promise<TReturn> {
  const logger = pino({ name: "_monkeypatch" });
  logger.warn(
    "[_monkeypatch:wrapWithExceptionLogging] Wrapping handler with exception logging."
  );

  return async (...args: TArgs): Promise<TReturn> => {
    try {
      return await handler(...args);
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      const stackTrace = err.stack ?? "";

      if (_isClientDisconnectError(err)) {
        try {
          _getGcpLogger().debug(
            `Unhandled client disconnect detected in ASGI application: ${err.name}: ${err.message}`,
            {
              event_type: "client_disconnect",
              exception_type: err.name,
              exception_message: err.message,
              stack_trace: stackTrace,
            }
          );
        } catch (disconnectLogErr) {
          logger.error(
            `[_monkeypatch:wrapped_app] Client disconnect logging failed: ${disconnectLogErr}`
          );
        }
        // Return gracefully without re-raising - let connection close naturally
        return undefined as unknown as TReturn;
      }

      // GCP Cloud Logging: primary logging strategy for native GCP integration
      try {
        _getGcpLogger().error(
          `Unhandled exception in ASGI application (GCP Cloud Logging): ${err.name}: ${err.message}`,
          {
            exception_type: err.name,
            exception_message: err.message,
            stack_trace: stackTrace,
          }
        );
      } catch (gcpErr) {
        logger.error(`[_monkeypatch:wrapped_app] GCP Logging failed: ${gcpErr}`);
      }

      // Re-raise the original exception to maintain normal error flow
      throw e;
    }
  };
}
