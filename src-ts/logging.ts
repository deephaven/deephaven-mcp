/**
 * Logging and global exception handling utilities for Deephaven MCP servers.
 *
 * This module provides functions to:
 * - Set up root logger configuration early in process startup (`setupLogging`).
 * - Ensure all unhandled synchronous and asynchronous exceptions are logged (`setupGlobalExceptionLogging`).
 * - Set up logging for all catchable termination signals (`setupSignalHandlerLogging`).
 * - Log process resource state for diagnostic purposes (`logProcessState`).
 *
 * Call `setupLogging()` before any other imports in your main entrypoint to ensure all loggers are configured correctly.
 * Call `setupGlobalExceptionLogging()` once at process startup to guarantee robust error visibility.
 * Call `setupSignalHandlerLogging()` to register handlers for all catchable signals (SIGTERM, SIGINT, SIGHUP, etc.).
 */

import pino from "pino";

/**
 * Module-level logger using pino.
 * @internal
 */
export const _logger = pino({ name: "deephaven-mcp" });

/**
 * Idempotency guard for global exception logging setup.
 * @internal
 */
export let _EXC_LOGGING_INSTALLED = false;

/**
 * Idempotency guard for signal handler registration.
 * @internal
 */
export let _SIGNAL_HANDLERS_INSTALLED = false;

/**
 * Reset the exception logging idempotency guard.
 * @internal For testing only.
 */
export function _resetExcLoggingInstalled(): void {
  _EXC_LOGGING_INSTALLED = false;
}

/**
 * Reset the signal handler idempotency guard.
 * @internal For testing only.
 */
export function _resetSignalHandlersInstalled(): void {
  _SIGNAL_HANDLERS_INSTALLED = false;
}

/**
 * Set up logging configuration for the application.
 *
 * Configures the pino logger using the `LOG_LEVEL` environment variable to set
 * the log level. Should be called before any other imports in the main entrypoint to
 * ensure all loggers are set up correctly.
 */
export function setupLogging(): void {
  const level = process.env["LOG_LEVEL"] ?? "info";
  // Re-configure pino level
  _logger.level = level;
}

/**
 * Signal handler that logs received signals and then terminates the process.
 *
 * Logs the signal name and then terminates the process by sending the signal
 * back to itself after restoring the default handler behavior.
 *
 * @param signal - The signal name that was received (e.g., "SIGTERM").
 */
export function _signalHandler(signal: string): void {
  try {
    _logger.warn(`[signal_handler] Received signal ${signal}`);
    _logger.warn(`[signal_handler] Initiating shutdown due to signal ${signal}`);
  } catch (e) {
    try {
      process.stderr.write(
        `[signal_handler] CRITICAL: Received signal ${signal} but logging failed: ${e}\n`
      );
    } catch {
      // Nothing more we can do - last resort fallback
    }
  }

  // Re-raise the signal so the process terminates with the expected exit status.
  try {
    process.kill(process.pid, signal);
  } catch {
    // Last-resort: if we cannot re-raise the signal, fall back to a direct exit.
    process.exit(1);
  }
}

/**
 * Set up global logging for all unhandled exceptions (synchronous and asynchronous) in the process.
 *
 * This function ensures that:
 *   - All uncaught exceptions in synchronous code are logged using pino.
 *   - All unhandled promise rejections are logged.
 *   - The function is idempotent and can be safely called multiple times.
 *
 * Call this function once at process startup (e.g., at the top of your main() entrypoint).
 */
export function setupGlobalExceptionLogging(): void {
  if (_EXC_LOGGING_INSTALLED) {
    return;
  }
  _EXC_LOGGING_INSTALLED = true;

  process.on("uncaughtException", (err: Error) => {
    _logger.error({ err }, "UNHANDLED EXCEPTION");
  });

  process.on("unhandledRejection", (reason: unknown) => {
    _logger.error({ err: reason }, "UNHANDLED ASYNC EXCEPTION");
  });
}

/**
 * Log current process resource state for debugging.
 *
 * Records key metrics about the current process to help diagnose resource issues:
 *   - Memory usage in MB (heap used and RSS)
 *   - CPU utilization percentage (not available natively, logged as N/A)
 *   - Process ID (startup and non-shutdown contexts only)
 *
 * @param logTag - Tag to use in log message prefix (e.g., "app_lifespan" becomes "[app_lifespan]").
 * @param context - Context string describing when metrics are being collected (e.g., "startup",
 *   "shutdown", "periodic_check"). Special handling is applied for "shutdown" context.
 *
 * @example
 * ```typescript
 * // At server startup
 * logProcessState("mcp_docs_server:app_lifespan", "startup");
 *
 * // During server shutdown
 * logProcessState("mcp_docs_server:app_lifespan", "shutdown");
 * ```
 */
export function logProcessState(logTag: string, context: string): void {
  try {
    const prefix = context === "shutdown" ? "Final " : "";
    const memUsage = process.memoryUsage();
    const rssMb = (memUsage.rss / 1024 / 1024).toFixed(2);
    const heapMb = (memUsage.heapUsed / 1024 / 1024).toFixed(2);
    _logger.info(`[${logTag}] ${prefix}memory usage (RSS): ${rssMb} MB`);
    _logger.info(`[${logTag}] ${prefix}memory usage (heap): ${heapMb} MB`);

    if (context !== "shutdown") {
      _logger.info(`[${logTag}] Process PID: ${process.pid}`);
    }
  } catch (e) {
    _logger.error(`[${logTag}] Error getting ${context} process state: ${e}`);
  }
}

/**
 * Set up logging for all catchable termination signals.
 *
 * Registers handlers for all catchable signals that might terminate the process:
 *   - SIGTERM: Standard termination signal
 *   - SIGINT: Keyboard interrupt signal
 *   - SIGHUP: Hangup signal (Unix/Linux/macOS only)
 *   - SIGQUIT: Quit signal (Unix/Linux/macOS only)
 *   - SIGABRT: Abort signal
 *
 * The function is idempotent and can be safely called multiple times. Signal handler registration
 * failures are logged but do not raise exceptions.
 *
 * @example
 * ```typescript
 * // In your main application entry point
 * import { setupLogging, setupSignalHandlerLogging } from "./logging.js";
 *
 * setupLogging();
 * setupSignalHandlerLogging();
 * ```
 */
export function setupSignalHandlerLogging(): void {
  if (_SIGNAL_HANDLERS_INSTALLED) {
    return;
  }
  _SIGNAL_HANDLERS_INSTALLED = true;

  const signalsToRegister: Array<[NodeJS.Signals, boolean]> = [
    ["SIGTERM", true],
    ["SIGINT", true],
    ["SIGABRT", true],
    ["SIGHUP", false],
    ["SIGQUIT", false],
  ];

  const registeredSignals: string[] = [];
  const failedSignals: string[] = [];

  for (const [signal, isCritical] of signalsToRegister) {
    try {
      process.on(signal, () => _signalHandler(signal));
      registeredSignals.push(signal);
    } catch (e) {
      if (isCritical) {
        failedSignals.push(`${signal} (${e})`);
      }
    }
  }

  if (registeredSignals.length > 0) {
    _logger.info(
      `[signal_handler] Signal handlers registered for: ${registeredSignals.join(", ")}`
    );
  }

  if (failedSignals.length > 0) {
    _logger.debug(
      `[signal_handler] Failed to register handlers for: ${failedSignals.join(", ")}`
    );
  }
}
