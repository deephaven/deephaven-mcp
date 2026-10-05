import { DhError, ERROR_CODES } from "./auth/errors.ts";

export type Format = "human" | "json";

const encoder = new TextEncoder();
let format: Format = "human";

export function setFormat(value: Format): void {
  format = value;
}

export function isJson(): boolean {
  return format === "json";
}

// Not console: it is silenced while the Deephaven client libraries run.
export function out(text = ""): void {
  Deno.stdout.writeSync(encoder.encode(text + "\n"));
}

export function err(text: string): void {
  Deno.stderr.writeSync(encoder.encode(text + "\n"));
}

/** Progress and notices; never mixed into JSON stdout. */
export function note(text: string): void {
  if (isJson()) err(text);
  else out(text);
}

export function result(human: () => string | string[], json: unknown): void {
  if (isJson()) {
    out(JSON.stringify(json));
    return;
  }
  const text = human();
  for (const line of Array.isArray(text) ? text : [text]) out(line);
}

/** Prints `e` and returns the process exit code. */
export function reportError(e: unknown): number {
  const error = e instanceof DhError
    ? e
    : new DhError("internal", e instanceof Error ? e.message : String(e));
  if (
    error.code === "internal" && Deno.env.get("DH_DEBUG") && e instanceof Error
  ) {
    err(e.stack ?? e.message);
  }
  if (isJson()) {
    err(JSON.stringify({
      error: error.message,
      code: error.code,
      exit: error.exit,
      ...(error.hint ? { hint: error.hint } : {}),
    }));
  } else {
    err(`✖ ${error.message}`);
    if (error.hint) err(`  ${error.hint}`);
  }
  return ERROR_CODES[error.code].exit;
}
