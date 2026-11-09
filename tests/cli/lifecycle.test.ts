import { expect, spyOn, test } from "bun:test";
import {
  CommandLifecycle,
  runLifecycleAction,
} from "../../src/cli/lifecycle.ts";
import type { FailureReport } from "../../src/errors.ts";

test("lifecycle: cleanup runs once in reverse order and collects failures", async () => {
  const lifecycle = new CommandLifecycle();
  const calls: string[] = [];
  const failure = new Error("close failed");
  lifecycle.onCleanup(() => {
    calls.push("first");
  });
  lifecycle.onCleanup(() => {
    calls.push("second");
    throw failure;
  });

  const result = await lifecycle.cleanup();

  expect(await lifecycle.cleanup()).toBe(result);
  expect(calls).toEqual(["second", "first"]);
  expect(result).toEqual({ failures: [failure], timedOut: false });
  expect(() => lifecycle.onCleanup(() => {})).toThrow(
    "Cannot register cleanup after cleanup has started",
  );
});

test("lifecycle: exposes one abort signal and its reason", () => {
  const lifecycle = new CommandLifecycle();
  const reason = new Error("SIGINT");

  lifecycle.abort(reason);

  expect(lifecycle.signal.aborted).toBe(true);
  expect(lifecycle.signal.reason).toBe(reason);
});

test("lifecycle: identifies a repeated termination signal", () => {
  const lifecycle = new CommandLifecycle();

  expect(lifecycle.receiveSignal("SIGINT")).toBe(true);
  expect(lifecycle.receiveSignal("SIGINT")).toBe(false);
  expect(lifecycle.signal.aborted).toBe(true);
});

test("lifecycle: actions restore their caller without leaking into prompts", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.setFailureReport({ command: "auth login", profile: "prod" });
  await lifecycle.runLifecycleAction(
    {
      operation: "profile save",
      target: "https://example.com",
      progress: "Signed in.",
      recoveryCommand: ["dh", "auth"],
      outcome: "unknown",
    },
    async () => {
      await lifecycle.runLifecycleAction(
        { operation: "SSO sign-in", phase: "signing_in" },
        async () => {
          expect(lifecycle.failureReport).toEqual({
            command: "auth login",
            profile: "prod",
            operation: "SSO sign-in",
            phase: "signing_in",
            elapsedSeconds: 0,
          });
        },
      );
      expect(lifecycle.failureReport.operation).toBe("profile save");
    },
  );
  lifecycle.receiveSignal("SIGINT");
  expect(lifecycle.failureReport).toEqual({
    command: "auth login",
    profile: "prod",
  });
});

test("lifecycle: interruption freezes operation timing and observation age before cleanup", async () => {
  const lifecycle = new CommandLifecycle();
  const clock = spyOn(performance, "now").mockReturnValue(1_000);
  const wallClock = spyOn(Date, "now").mockReturnValue(
    Date.parse("2026-01-01T12:00:00Z"),
  );
  try {
    await expect(
      lifecycle.runLifecycleAction(
        {
          operation: "console startup",
          observation: {
            state: "Initializing",
            observedAt: "2026-01-01T12:00:40Z",
            ageSeconds: 0,
          },
        },
        async () => {
          clock.mockReturnValue(46_000);
          wallClock.mockReturnValue(Date.parse("2026-01-01T12:00:45Z"));
          lifecycle.receiveSignal("SIGTERM");
        },
      ),
    ).rejects.toMatchObject({ code: "cancelled" });
    const report = lifecycle.failureReport;
    expect(report.elapsedSeconds).toBe(45);
    expect(report.observation?.ageSeconds).toBe(5);
    clock.mockReturnValue(100_000);
    wallClock.mockReturnValue(Date.parse("2026-01-01T12:01:40Z"));
    lifecycle.setFailureReport({ target: "too late" });
    await lifecycle.cleanup();
    expect(lifecycle.failureReport).toEqual(report);
  } finally {
    clock.mockRestore();
    wallClock.mockRestore();
  }
});

test("lifecycle: cleanup freezes failure context without requiring cancellation", async () => {
  const lifecycle = new CommandLifecycle();
  const clock = spyOn(performance, "now").mockReturnValue(1_000);
  try {
    await expect(
      lifecycle.runLifecycleAction(
        { operation: "connection", target: "https://example.com" },
        async () => {
          clock.mockReturnValue(6_000);
          throw new Error("Connection failed");
        },
      ),
    ).rejects.toThrow("Connection failed");
    lifecycle.onCleanup(() => {
      clock.mockReturnValue(10_000);
    });
    await lifecycle.cleanup();
    expect(lifecycle.failureReport).toMatchObject({
      operation: "connection",
      target: "https://example.com",
      elapsedSeconds: 5,
    });
  } finally {
    clock.mockRestore();
  }
});

test("lifecycle: actions update plain facts and interruption captures an independent snapshot", async () => {
  const lifecycle = new CommandLifecycle();
  const report: FailureReport = {
    operation: "console startup",
    outcome: "not_started",
    human: { summary: "Cancelled before creation.", details: [] },
  };
  await expect(
    lifecycle.runLifecycleAction(report, async () => {
      report.outcome = "unknown";
      report.continuation = "continues";
      report.target = "abc123";
      report.human = {
        summary: "Cancelled while waiting for console abc123 to start.",
        details: ["Startup continues on the server."],
      };
      lifecycle.receiveSignal("SIGINT");
      report.outcome = "succeeded";
      report.human.details?.push("Too late.");
    }),
  ).rejects.toMatchObject({ code: "cancelled" });
  expect(lifecycle.failureReport).toMatchObject({
    target: "abc123",
    outcome: "unknown",
    continuation: "continues",
    human: {
      summary: "Cancelled while waiting for console abc123 to start.",
      details: ["Startup continues on the server."],
    },
  });
});

test("lifecycle: a handled action failure does not leak after its caller succeeds", async () => {
  const lifecycle = new CommandLifecycle();
  await lifecycle.runLifecycleAction({ operation: "migration" }, async () => {
    await expect(
      lifecycle.runLifecycleAction({ operation: "sign-in" }, async () => {
        throw new Error("Rejected");
      }),
    ).rejects.toThrow("Rejected");
  });
  expect(lifecycle.failureReport).toEqual({});
});

test("lifecycle: action helpers work without a CLI lifecycle and refuse cancelled work", async () => {
  expect(await runLifecycleAction(undefined, {}, async () => 42)).toBe(42);
  const lifecycle = new CommandLifecycle();
  lifecycle.receiveSignal("SIGINT");
  let started = false;
  await expect(
    runLifecycleAction(lifecycle, {}, async () => {
      started = true;
    }),
  ).rejects.toMatchObject({ code: "cancelled" });
  expect(started).toBe(false);
});

test("lifecycle: action helpers forward arguments and preserve defaults", async () => {
  async function work(target: string, count = 2) {
    return { target, count };
  }
  for (const lifecycle of [undefined, new CommandLifecycle()]) {
    expect(await runLifecycleAction(lifecycle, {}, work, "server")).toEqual({
      target: "server",
      count: 2,
    });
    expect(await runLifecycleAction(lifecycle, {}, work, "server", 3)).toEqual({
      target: "server",
      count: 3,
    });
  }
});

test("lifecycle: named helpers update their scoped report and retain failures", async () => {
  const lifecycle = new CommandLifecycle();
  const report = { operation: "saving", progress: "" };
  const failure = new Error("Cleanup failed");
  async function work(current: typeof report, context: CommandLifecycle) {
    current.progress = "Profile saved.";
    expect(context.failureReport.progress).toBe("Profile saved.");
    throw failure;
  }
  await expect(
    runLifecycleAction(lifecycle, report, work, report, lifecycle),
  ).rejects.toBe(failure);
  expect(lifecycle.failureReport).toMatchObject({
    operation: "saving",
    progress: "Profile saved.",
  });
  lifecycle.receiveSignal("SIGINT");
  let started = false;
  async function cancelledWork(target: string) {
    started = true;
    return target;
  }
  await expect(
    runLifecycleAction(lifecycle, {}, cancelledWork, "server"),
  ).rejects.toMatchObject({ code: "cancelled" });
  expect(started).toBe(false);
});

test("lifecycle: stops waiting at the cleanup deadline", async () => {
  const lifecycle = new CommandLifecycle();
  const calls: string[] = [];
  let finish!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  lifecycle.onCleanup(() => {
    calls.push("after timeout");
  });
  lifecycle.onCleanup(() => pending);

  const started = Date.now();
  const result = await lifecycle.cleanup();
  finish();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(Date.now() - started).toBeLessThan(2_000);
  expect(result.timedOut).toBe(true);
  expect(result.failures).toHaveLength(1);
  expect((result.failures[0] as Error).message).toBe("Cleanup timed out");
  expect(calls).toEqual([]);
});
