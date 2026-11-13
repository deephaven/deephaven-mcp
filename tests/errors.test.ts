import { expect, test } from "bun:test";
import {
  cancellationError,
  cleanupError,
  commandUsageError,
  DhError,
  describe,
  interruptedError,
  reasonOf,
} from "../src/errors.ts";

test("describe: one short line", () => {
  expect(describe(new Error("a\n  b"))).toBe("a b");
  expect(describe({ detail: "from GWT" })).toBe("from GWT");
  expect(describe("x".repeat(500)).length).toBe(201);
});

test("reasonOf: drops reasons that say nothing", () => {
  expect(reasonOf(new Event("error"))).toBeUndefined();
  expect(reasonOf("")).toBeUndefined();
  expect(reasonOf("Login failed, verify credentials.")).toBe(
    "Login failed, verify credentials.",
  );
});

test("DhError: the detail follows the message", () => {
  const e = new DhError(
    "auth_failed",
    "x rejected the credentials",
    "hint",
    "why",
  );
  expect(e.message).toBe("x rejected the credentials: why");
  expect(e.detail).toBe("why");
  expect(new DhError("usage", "plain").message).toBe("plain");
});

test("cancellationError: preserves a useful abort reason as detail", () => {
  expect(cancellationError(new Error("SIGTERM")).detail).toBe("SIGTERM");
  expect(cancellationError().message).toBe("Cancelled");
});

test("interruptedError: retains cancellation errors and formats other reasons", () => {
  const cancellation = cancellationError("SIGTERM");
  expect(interruptedError(cancellation)).toBe(cancellation);
  expect(interruptedError("Interrupted by SIGINT")).toMatchObject({
    code: "cancelled",
    headline: "Interrupted",
    detail: "Interrupted by SIGINT",
  });
});

test("commandUsageError: normalizes parser errors and supplies help", () => {
  expect(
    commandUsageError("error: unknown option '--bad'", "dh auth"),
  ).toMatchObject({
    code: "usage",
    headline: "unknown option '--bad'",
    hint: "Run `dh auth --help` for usage.",
  });
});

test("cleanupError: retains the primary failure and separates cleanup diagnostics", () => {
  const primary = new DhError(
    "auth_failed",
    "Sign-in failed",
    "Retry",
    "Rejected",
  );
  const failure = cleanupError(
    [new Error("Close failed"), "Unlock failed"],
    primary,
  );
  expect(failure).toMatchObject({
    code: "auth_failed",
    headline: "Sign-in failed",
    hint: "Retry",
    detail: "Rejected",
    cleanupDetail: "Close failed; Unlock failed",
  });
  expect(primary.detail).toBe("Rejected");
  expect(cleanupError([new Error("Close failed")])).toMatchObject({
    code: "internal",
    headline: "Command cleanup failed",
    detail: "Close failed",
  });
});
