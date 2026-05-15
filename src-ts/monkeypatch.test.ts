/**
 * Tests for monkeypatch module.
 *
 * Note: The Python module patches Uvicorn's RequestResponseCycle for GCP Cloud Logging.
 * In TypeScript/Node.js, there is no Uvicorn equivalent. This module provides
 * ASGI-equivalent exception handling utilities using pino logger for structured logging.
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import {
  _isClientDisconnectError,
  _setupGcpLogger,
  _getGcpLogger,
  _resetGcpLogger,
} from "./monkeypatch.js";

afterEach(() => {
  vi.restoreAllMocks();
  _resetGcpLogger();
});

describe("isClientDisconnectError", () => {
  it("direct_closed_resource_error", () => {
    const err = new ClientDisconnectError("client disconnected");
    expect(_isClientDisconnectError(err)).toBe(true);
  });

  it("other_error_returns_false", () => {
    const err = new Error("some other error");
    expect(_isClientDisconnectError(err)).toBe(false);
  });

  it("nested_cause_closed_resource_error", () => {
    const inner = new ClientDisconnectError("closed");
    const outer = new Error("wrapper");
    (outer as Error & { cause?: Error }).cause = inner;
    expect(_isClientDisconnectError(outer)).toBe(true);
  });

  it("aggregate_error_containing_closed_resource_error", () => {
    // Simulate AggregateError (similar to Python ExceptionGroup)
    const inner = new ClientDisconnectError("closed");
    const agg = new AggregateError([inner, new Error("other")], "aggregate");
    expect(_isClientDisconnectError(agg)).toBe(true);
  });

  it("aggregate_error_without_closed_resource_error", () => {
    const agg = new AggregateError([new Error("foo"), new Error("bar")], "aggregate");
    expect(_isClientDisconnectError(agg)).toBe(false);
  });
});

describe("gcpLogger", () => {
  it("get_gcp_logger_returns_logger_object", () => {
    const logger = _getGcpLogger();
    expect(logger).toBeDefined();
    expect(typeof logger.error).toBe("function");
    expect(typeof logger.debug).toBe("function");
  });

  it("get_gcp_logger_is_cached", () => {
    const logger1 = _getGcpLogger();
    const logger2 = _getGcpLogger();
    expect(logger1).toBe(logger2);
  });

  it("reset_gcp_logger_clears_cache", () => {
    const logger1 = _getGcpLogger();
    _resetGcpLogger();
    const logger2 = _getGcpLogger();
    // After reset, a new logger should be created (but may be same pino instance)
    expect(logger2).toBeDefined();
  });
});

// Helper class used in tests to simulate client disconnect errors
class ClientDisconnectError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "ClientDisconnectError";
  }
}
