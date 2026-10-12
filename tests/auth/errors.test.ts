import { expect, test } from "bun:test";
import { DhError, describe, reasonOf } from "../../src/auth/errors.ts";

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
