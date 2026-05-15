/**
 * Tests for custom exception types for Deephaven MCP.
 */
import { describe, it, expect } from "vitest";
import {
  McpError,
  InternalError,
  UnsupportedOperationError,
  MissingEnterprisePackageError,
  SessionError,
  SessionCreationError,
  SessionLaunchError,
  InvalidSessionNameError,
  AuthenticationError,
  QueryError,
  DeephavenConnectionError,
  ResourceError,
  RegistryItemNotFoundError,
  ConfigurationError,
} from "./exceptions.js";

describe("BaseExceptions", () => {
  it("mcp_error", () => {
    const message = "base MCP error";
    const err = new McpError(message);
    expect(() => { throw err; }).toThrow(McpError);
    expect(err.message).toBe(message);
    expect(err instanceof Error).toBe(true);
  });

  it("internal_error", () => {
    const message = "internal MCP error";
    const err = new InternalError(message);
    expect(() => { throw err; }).toThrow(InternalError);
    expect(err.message).toBe(message);
    expect(err instanceof McpError).toBe(true);
    expect(err instanceof Error).toBe(true);
  });

  it("internal_error_inheritance", () => {
    const message = "internal error with multiple inheritance";
    const err = new InternalError(message);
    expect(err instanceof McpError).toBe(true);
    expect(err instanceof InternalError).toBe(true);
    expect(err.message).toBe(message);
  });

  it("missing_enterprise_package_error_default", () => {
    const err = new MissingEnterprisePackageError();
    expect(() => { throw err; }).toThrow(MissingEnterprisePackageError);
    const msg = err.message;
    expect(msg).toContain("deephaven-coreplus-client");
    expect(msg).toContain("ERROR: Core+ features are not available");
    expect(msg).toContain("pip install");
    expect(err instanceof InternalError).toBe(true);
    expect(err instanceof McpError).toBe(true);
  });

  it("unsupported_operation_error", () => {
    const message = "operation not supported";
    const err = new UnsupportedOperationError(message);
    expect(() => { throw err; }).toThrow(UnsupportedOperationError);
    expect(err.message).toBe(message);
    expect(err instanceof McpError).toBe(true);
    expect(err instanceof Error).toBe(true);
  });
});

describe("ExceptionParameterized", () => {
  it.each([
    [SessionError, "session error"],
    [AuthenticationError, "authentication error"],
    [QueryError, "query error"],
    [DeephavenConnectionError, "connection error"],
    [ResourceError, "resource error"],
    [ConfigurationError, "configuration error"],
    [UnsupportedOperationError, "unsupported operation error"],
    [SessionCreationError, "session creation error"],
    [SessionLaunchError, "session launch error"],
  ])("exception_basics %s", (ExceptionClass, message) => {
    const err = new (ExceptionClass as new (msg: string) => McpError)(message);
    expect(() => { throw err; }).toThrow(ExceptionClass);
    expect(err.message).toBe(message);
    expect(err instanceof McpError).toBe(true);
    expect(err instanceof Error).toBe(true);
  });

  it("exception_basics MissingEnterprisePackageError inheritance", () => {
    const message = "Core+ features are not available (deephaven-coreplus-client Python package not installed)";
    const err = new MissingEnterprisePackageError(message);
    expect(err instanceof InternalError).toBe(true);
    expect(err instanceof McpError).toBe(true);
    expect(err instanceof Error).toBe(true);
  });

  it("session_creation_error inherits from session_error", () => {
    const err = new SessionCreationError("session creation error");
    expect(err instanceof SessionError).toBe(true);
    expect(err instanceof McpError).toBe(true);
  });

  it("session_launch_error inherits from session_creation_error", () => {
    const err = new SessionLaunchError("session launch error");
    expect(err instanceof SessionCreationError).toBe(true);
    expect(err instanceof SessionError).toBe(true);
    expect(err instanceof McpError).toBe(true);
  });
});

describe("ExceptionModule", () => {
  it("all_exceptions_exported", () => {
    // All expected exception classes should be importable and be Error subclasses
    const classes = [
      McpError,
      InternalError,
      UnsupportedOperationError,
      MissingEnterprisePackageError,
      SessionError,
      SessionCreationError,
      SessionLaunchError,
      InvalidSessionNameError,
      AuthenticationError,
      QueryError,
      DeephavenConnectionError,
      ResourceError,
      RegistryItemNotFoundError,
      ConfigurationError,
    ];
    for (const cls of classes) {
      expect(cls).toBeDefined();
      const instance = new (cls as new () => Error)();
      expect(instance instanceof Error).toBe(true);
    }
  });
});
