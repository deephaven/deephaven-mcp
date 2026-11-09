import { asDhError, cleanupError, interruptedError } from "../errors.ts";
import type { CommandLifecycle } from "./lifecycle.ts";

/** Runs one invocation with signal handling and cleanup on every exit path. */
export async function runWithLifecycle(
  lifecycle: CommandLifecycle,
  run: () => Promise<unknown>,
): Promise<void> {
  let onAbort = () => {};
  const interrupted = new Promise<never>((_, reject) => {
    onAbort = () => reject(interruptedError(lifecycle.signal.reason));
    lifecycle.signal.addEventListener("abort", onAbort, { once: true });
    if (lifecycle.signal.aborted) onAbort();
  });
  const onSignal = (signal: NodeJS.Signals) => {
    if (!lifecycle.receiveSignal(signal)) process.exit(3);
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);

  try {
    let failed = false;
    let failure: unknown;
    try {
      await Promise.race([
        Promise.resolve().then(() => {
          if (lifecycle.signal.aborted) {
            throw interruptedError(lifecycle.signal.reason);
          }
          return run();
        }),
        interrupted,
      ]);
    } catch (error) {
      failed = true;
      failure = error;
    }

    const cleanup = await lifecycle.cleanup();
    if (cleanup.failures.length > 0) {
      throw cleanupError(
        cleanup.failures,
        failed ? asDhError(failure) : undefined,
      );
    }
    if (failed) throw failure;
    if (lifecycle.signal.aborted)
      throw interruptedError(lifecycle.signal.reason);
  } finally {
    lifecycle.signal.removeEventListener("abort", onAbort);
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
  }
}
