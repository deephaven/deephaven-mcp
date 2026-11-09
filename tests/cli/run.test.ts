import { expect, test } from "bun:test";
import { Command } from "commander";
import { defineCommand, registerCommand } from "../../src/cli/command.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { runWithLifecycle } from "../../src/cli/run.ts";
import { DhError } from "../../src/errors.ts";

test("runner: cleans up success and failure and removes signal listeners", async () => {
  for (const fails of [false, true]) {
    const lifecycle = new CommandLifecycle();
    const before = ["SIGINT", "SIGTERM"].map((signal) =>
      process.listenerCount(signal),
    );
    const failure = new DhError("usage", "Bad input");
    let cleaned = 0;
    lifecycle.onCleanup(() => {
      cleaned++;
    });
    const running = runWithLifecycle(lifecycle, async () => {
      if (fails) throw failure;
    });
    if (fails) await expect(running).rejects.toBe(failure);
    else await running;

    expect(cleaned).toBe(1);
    expect(
      ["SIGINT", "SIGTERM"].map((signal) => process.listenerCount(signal)),
    ).toEqual(before);
  }
});

test("runner: preserves the primary error when cleanup fails", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.onCleanup(() => {
    throw new Error("Close failed");
  });
  await expect(
    runWithLifecycle(lifecycle, async () => {
      throw new DhError("auth_failed", "Rejected", "Retry", "Bad credentials");
    }),
  ).rejects.toMatchObject({
    code: "auth_failed",
    headline: "Rejected",
    hint: "Retry",
    detail: "Bad credentials",
    cleanupDetail: "Close failed",
  });
});

test("runner: reports cleanup failure after an otherwise successful command", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.onCleanup(() => {
    throw new Error("Close failed");
  });
  await expect(
    runWithLifecycle(lifecycle, async () => {}),
  ).rejects.toMatchObject({
    code: "internal",
    headline: "Command cleanup failed",
    detail: "Close failed",
  });
});

test("runner: keeps signal handling active during cleanup", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.onCleanup(() => {
    process.emit("SIGTERM", "SIGTERM");
  });
  await expect(
    runWithLifecycle(lifecycle, async () => {}),
  ).rejects.toMatchObject({
    code: "cancelled",
    headline: "Interrupted",
    detail: "Interrupted by SIGTERM",
  });
});

test("runner: does not start an already cancelled command", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.abort();
  let started = false;
  await expect(
    runWithLifecycle(lifecycle, async () => {
      started = true;
    }),
  ).rejects.toMatchObject({ code: "cancelled" });
  expect(started).toBe(false);
});

test("runner: interruption during failed cleanup preserves its reason", async () => {
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    const lifecycle = new CommandLifecycle();
    lifecycle.onCleanup(() => {
      process.emit(signal, signal);
      throw new Error("Close failed");
    });
    await expect(
      runWithLifecycle(lifecycle, async () => {}),
    ).rejects.toMatchObject({
      code: "cancelled",
      headline: "Interrupted",
      detail: `Interrupted by ${signal}`,
      cleanupDetail: "Close failed",
    });
  }
});

test("runner: an earlier command failure takes precedence over cleanup interruption", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.onCleanup(() => {
    process.emit("SIGTERM", "SIGTERM");
    throw new Error("Close failed");
  });

  await expect(
    runWithLifecycle(lifecycle, async () => {
      throw new DhError("auth_failed", "Rejected");
    }),
  ).rejects.toMatchObject({
    code: "auth_failed",
    headline: "Rejected",
    cleanupDetail: "Close failed",
  });
});

test("runner: interruption during a cleanup timeout remains primary", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.onCleanup(() => {
    process.emit("SIGINT", "SIGINT");
    return new Promise(() => {});
  });
  await expect(
    runWithLifecycle(lifecycle, async () => {}),
  ).rejects.toMatchObject({
    code: "cancelled",
    detail: "Interrupted by SIGINT",
    cleanupDetail: "Cleanup timed out",
  });
});

test("command registration: separate invocations do not share lifecycles", async () => {
  const first = new CommandLifecycle();
  const second = new CommandLifecycle();
  const seen: CommandLifecycle[] = [];
  const spec = defineCommand({
    name: "test",
    description: "Test invocation isolation",
    async run(ctx) {
      seen.push(ctx.lifecycle);
      expect(ctx.signal).toBe(ctx.lifecycle.signal);
      return { empty: true, human: [], json: null };
    },
  });
  for (const lifecycle of [first, second]) {
    const root = registerCommand(new Command("test"), spec, lifecycle);
    await runWithLifecycle(lifecycle, () =>
      root.parseAsync([], { from: "user" }),
    );
  }
  first.abort();
  first.setFailureReport({ target: "first" });
  expect(seen).toEqual([first, second]);
  expect(second.signal.aborted).toBe(false);
  expect(second.failureReport.target).toBeUndefined();
});

test.skipIf(process.platform === "win32")(
  "runner: a second interrupt exits immediately during cleanup",
  async () => {
    const script = `
      import { CommandLifecycle } from ${JSON.stringify(new URL("../../src/cli/lifecycle.ts", import.meta.url).href)};
      import { runWithLifecycle } from ${JSON.stringify(new URL("../../src/cli/run.ts", import.meta.url).href)};
      const lifecycle = new CommandLifecycle();
      lifecycle.onCleanup(() => {
        process.stdout.write("cleanup\\n");
        return new Promise(() => {});
      });
      await runWithLifecycle(lifecycle, async () => {
        process.stdout.write("ready\\n");
        await new Promise(() => {});
      });
    `;
    const proc = Bun.spawn([process.execPath, "-e", script], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const reader = proc.stdout.getReader();
    try {
      expect(new TextDecoder().decode((await reader.read()).value)).toBe(
        "ready\n",
      );
      proc.kill("SIGINT");
      expect(new TextDecoder().decode((await reader.read()).value)).toBe(
        "cleanup\n",
      );
      const started = Date.now();
      proc.kill("SIGINT");
      expect(await proc.exited).toBe(3);
      expect(Date.now() - started).toBeLessThan(500);
    } finally {
      if (proc.exitCode === null) proc.kill("SIGKILL");
      await proc.exited;
      reader.releaseLock();
    }
  },
);
