/** Shared PASS/FAIL reporting so every check prints the same way. */
export const runtime = Deno.build.standalone ? "compiled" : "deno run";

/** Per-check limit; jsapi calls can hang forever when a server misbehaves. */
export const TIMEOUT_MS = Number(Deno.env.get("DH_TIMEOUT") ?? 30) * 1000;

let failed = false;

export async function check<T>(
  name: string,
  fn: () => T | Promise<T>,
  describe: (value: T) => unknown = (value) => value,
  timeoutMs = TIMEOUT_MS,
): Promise<T | undefined> {
  const start = performance.now();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const value = await Promise.race([
      Promise.resolve().then(fn),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out after ${timeoutMs} ms`)),
          timeoutMs,
        );
      }),
    ]);
    const ms = Math.round(performance.now() - start);
    const shown = describe(value);
    console.log(
      `PASS ${name} (${ms} ms)${shown == null ? "" : `: ${fmt(shown)}`}`,
    );
    return value;
  } catch (e) {
    failed = true;
    const ms = Math.round(performance.now() - start);
    const message = e instanceof Error ? e.stack ?? e.message : String(e);
    console.log(`FAIL ${name} (${ms} ms): ${message}`);
    return undefined;
  } finally {
    clearTimeout(timer);
  }
}

export function done(): never {
  console.log(failed ? "RESULT: FAIL" : "RESULT: PASS");
  // jsapi keeps sockets open; don't wait for the event loop to drain.
  Deno.exit(failed ? 1 : 0);
}

function fmt(value: unknown): string {
  return typeof value === "string" ? value : JSON.stringify(value);
}
