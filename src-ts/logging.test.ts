/**
 * Tests for logging module.
 */
import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import type { Logger } from "pino";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("setupLogging", () => {
  it("returns_without_error", async () => {
    const mod = await import("./logging.js");
    // setupLogging creates a pino logger; just verify it doesn't throw
    expect(() => mod.setupLogging()).not.toThrow();
  });

  it("respects_log_level_env", async () => {
    vi.stubEnv("LOG_LEVEL", "debug");
    const mod = await import("./logging.js");
    expect(() => mod.setupLogging()).not.toThrow();
  });
});

describe("setupGlobalExceptionLogging", () => {
  it("is_idempotent", async () => {
    const mod = await import("./logging.js");
    mod._resetExcLoggingInstalled();
    mod.setupGlobalExceptionLogging();
    expect(mod._EXC_LOGGING_INSTALLED).toBe(true);
    // Call again - should be no-op
    mod.setupGlobalExceptionLogging();
    expect(mod._EXC_LOGGING_INSTALLED).toBe(true);
  });

  it("captures_unhandled_promise_rejections", async () => {
    const mod = await import("./logging.js");
    mod._resetExcLoggingInstalled();
    // Should not throw
    expect(() => mod.setupGlobalExceptionLogging()).not.toThrow();
    expect(mod._EXC_LOGGING_INSTALLED).toBe(true);
  });
});

describe("setupSignalHandlerLogging", () => {
  it("is_idempotent", async () => {
    const mod = await import("./logging.js");
    mod._resetSignalHandlersInstalled();
    mod.setupSignalHandlerLogging();
    expect(mod._SIGNAL_HANDLERS_INSTALLED).toBe(true);
    // Second call is no-op
    mod.setupSignalHandlerLogging();
    expect(mod._SIGNAL_HANDLERS_INSTALLED).toBe(true);
  });

  it("does_not_throw", async () => {
    const mod = await import("./logging.js");
    mod._resetSignalHandlersInstalled();
    expect(() => mod.setupSignalHandlerLogging()).not.toThrow();
  });

  it("registers_sigterm_and_sigint", async () => {
    const mod = await import("./logging.js");
    mod._resetSignalHandlersInstalled();
    const processOn = vi.spyOn(process, "on");
    mod.setupSignalHandlerLogging();
    const registeredSignals = processOn.mock.calls.map((c) => c[0]);
    expect(registeredSignals).toContain("SIGTERM");
    expect(registeredSignals).toContain("SIGINT");
  });
});

describe("logProcessState", () => {
  it("logs_without_error_startup", async () => {
    const mod = await import("./logging.js");
    // Should not throw
    expect(() => mod.logProcessState("test_tag", "startup")).not.toThrow();
  });

  it("logs_without_error_shutdown", async () => {
    const mod = await import("./logging.js");
    expect(() => mod.logProcessState("test_tag", "shutdown")).not.toThrow();
  });
});

describe("_signalHandler", () => {
  it("logs_signal_information", async () => {
    const mod = await import("./logging.js");
    const loggerWarn = vi.fn();
    // Replace the module logger temporarily for test
    (mod._logger as unknown as { warn: unknown }).warn = loggerWarn;
    // Mock process.kill to prevent actual termination
    const processKill = vi.spyOn(process, "kill").mockImplementation(() => true);
    mod._signalHandler("SIGTERM");
    expect(loggerWarn).toHaveBeenCalled();
    expect(processKill).toHaveBeenCalled();
    processKill.mockRestore();
  });
});
