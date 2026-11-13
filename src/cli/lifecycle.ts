import { type FailureReport, interruptedError } from "../errors.ts";

export type Cleanup = () => void | Promise<void>;

export interface CleanupResult {
  failures: unknown[];
  timedOut: boolean;
}

const CLEANUP_TIMEOUT_MS = 1_800;

interface ActiveOperation {
  report: FailureReport;
  startedAt: number;
}

/** Owns one command's cancellation signal and its once-only cleanup stack. */
export class CommandLifecycle {
  readonly #controller = new AbortController();
  readonly #cleanups: Cleanup[] = [];
  #cleanupResult?: Promise<CleanupResult>;
  #signalCount = 0;
  #signalDelegate?: (signal: NodeJS.Signals) => void;
  #failureReport: FailureReport = {};
  #operation?: ActiveOperation;
  #reportFrozen = false;

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  get failureReport(): FailureReport {
    if (this.#reportFrozen) return this.#failureReport;
    return reportWithTiming(this.#failureReport, this.#operation);
  }

  setFailureReport(report: FailureReport): void {
    if (this.#reportFrozen) return;
    this.#failureReport = { ...this.#failureReport, ...report };
  }

  /** Captures this action's report on failure; restores its caller on success. */
  async runLifecycleAction<T>(
    report: FailureReport,
    run: () => Promise<T>,
  ): Promise<T> {
    this.signal.throwIfAborted();
    const previous = this.#operation;
    if (!this.#reportFrozen) {
      this.#operation = { report, startedAt: performance.now() };
    }
    const result = await run();
    if (!this.#reportFrozen) this.#operation = previous;
    this.signal.throwIfAborted();
    return result;
  }

  private freezeReport(): void {
    this.#failureReport = structuredClone(this.failureReport);
    this.#operation = undefined;
    this.#reportFrozen = true;
  }

  setSignalDelegate(delegate?: (signal: NodeJS.Signals) => void): void {
    this.#signalDelegate = delegate;
  }

  /** Returns false for a repeated signal so the caller can exit immediately. */
  receiveSignal(signal: NodeJS.Signals): boolean {
    this.#signalCount++;
    if (this.#signalDelegate) {
      this.#signalDelegate(signal);
      return this.#signalCount === 1;
    }
    if (this.#signalCount > 1) return false;
    this.abort(interruptedError(`Interrupted by ${signal}`));
    return true;
  }

  abort(reason?: unknown): void {
    if (this.signal.aborted) return;
    this.freezeReport();
    this.#controller.abort(reason);
  }

  /** Registers cleanup to run in reverse order when {@link cleanup} is called. */
  onCleanup(cleanup: Cleanup): void {
    if (this.#cleanupResult) {
      throw new Error("Cannot register cleanup after cleanup has started");
    }
    this.#cleanups.push(cleanup);
  }

  /** Runs cleanup once, collecting failures and stopping at the shared deadline. */
  cleanup(): Promise<CleanupResult> {
    if (this.#cleanupResult) return this.#cleanupResult;

    this.freezeReport();
    const cleanups = this.#cleanups.splice(0).reverse();
    this.#cleanupResult = runCleanupsBeforeDeadline(cleanups);
    return this.#cleanupResult;
  }
}

/** Copy the current report, updating operation duration and observation age. */
function reportWithTiming(
  current: FailureReport,
  operation?: ActiveOperation,
): FailureReport {
  const report = { ...current };
  if (operation) {
    Object.assign(report, operation.report, {
      elapsedSeconds: Math.floor(
        (performance.now() - operation.startedAt) / 1000,
      ),
    });
  }
  if (report.observation) {
    report.observation = {
      ...report.observation,
      ageSeconds: Math.max(
        0,
        Math.floor(
          (Date.now() - Date.parse(report.observation.observedAt)) / 1000,
        ),
      ),
    };
  }
  return report;
}

/**
 * Run cleanups in the supplied order, collecting failures within one deadline.
 * The deadline stops waiting and skips remaining cleanups; it cannot cancel
 * a cleanup already running.
 */
async function runCleanupsBeforeDeadline(
  cleanups: Cleanup[],
): Promise<CleanupResult> {
  const failures: unknown[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  const work = (async () => {
    for (const cleanup of cleanups) {
      if (timedOut) return;
      try {
        await cleanup();
      } catch (error) {
        failures.push(error);
      }
    }
  })();

  try {
    await Promise.race([
      work,
      new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          timedOut = true;
          resolve();
        }, CLEANUP_TIMEOUT_MS);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }

  if (timedOut) failures.push(new Error("Cleanup timed out"));
  return { failures, timedOut };
}

/** Forward interruption signals to the child and return a function to stop forwarding. */
export function forwardSignalsToChild(
  child: Pick<Bun.Subprocess, "kill">,
  lifecycle?: CommandLifecycle,
): () => void {
  const forwardSignal = (signal: NodeJS.Signals) => {
    child.kill(signal);
  };
  if (lifecycle) {
    lifecycle.setSignalDelegate(forwardSignal);
    return () => lifecycle.setSignalDelegate(undefined);
  }
  const forwardInterrupt = () => forwardSignal("SIGINT");
  const forwardTermination = () => forwardSignal("SIGTERM");
  process.on("SIGINT", forwardInterrupt);
  process.on("SIGTERM", forwardTermination);
  return () => {
    process.off("SIGINT", forwardInterrupt);
    process.off("SIGTERM", forwardTermination);
  };
}

export function runLifecycleAction<Args extends unknown[], T>(
  lifecycle: CommandLifecycle | undefined,
  report: FailureReport,
  run: (...args: Args) => Promise<T>,
  ...args: Args
): Promise<T> {
  return lifecycle
    ? lifecycle.runLifecycleAction(report, () => run(...args))
    : run(...args);
}
