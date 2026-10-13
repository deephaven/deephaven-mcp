import { writeSync } from "node:fs";
import { styleText } from "node:util";
import { DhError, ERROR_CODES } from "./errors.ts";

/** `-o human` (default) or `-o json`. */
export type Format = "human" | "json";

let format: Format = "human";

export function setFormat(value: Format): void {
  format = value;
}

export function isJson(): boolean {
  return format === "json";
}

// Not console: it is silenced while the Deephaven client libraries run.
// Synchronous, so nothing is lost when the process exits right after.
export function out(text = ""): void {
  writeSync(1, `${text}\n`);
}

export function err(text: string): void {
  writeSync(2, `${text}\n`);
}

/** Progress and notices; never mixed into JSON stdout. */
export function note(text: string): void {
  if (isJson()) err(styled(text, process.stderr));
  else out(styled(text, process.stdout));
}

type Style = Parameters<typeof styleText>[0];

/** The leading marks of result, error and notice lines. */
const MARKS: Record<string, Style> = {
  "✔": "green",
  "✖": "red",
  "!": "yellow",
};

/**
 * `line` colored for `stream`, plain unless it's a terminal: its leading mark,
 * and `commands` in cyan. A `…` progress line is dimmed whole.
 */
function styled(line: string, stream: NodeJS.WriteStream): string {
  const paint = (style: Style, s: string) => styleText(style, s, { stream });
  const [, indent = "", mark = ""] = /^(\s*)([✔✖!…]) /.exec(line) ?? [];
  if (mark === "…") return paint("dim", line);
  const text = line.replace(/`[^`\n]+`/g, (c) => paint("cyan", c));
  const style = MARKS[mark];
  if (!style) return text;
  return indent + paint(style, mark) + text.slice(indent.length + 1);
}

/** Prints a command's result: `json` with `-o json`, else the lines from `human`. */
export function result(human: () => string | string[], json: unknown): void {
  if (isJson()) {
    out(JSON.stringify(json));
    return;
  }
  const text = human();
  for (const line of Array.isArray(text) ? text : [text]) {
    out(styled(line, process.stdout));
  }
}

/** Prints `e` and returns the process exit code. */
export function reportError(e: unknown): number {
  const message = e instanceof Error ? e.message : String(e);
  const error = e instanceof DhError ? e : new DhError("internal", message);
  if (error.code === "internal" && process.env.DH_DEBUG && e instanceof Error) {
    err(e.stack ?? e.message);
  }
  if (isJson()) {
    err(
      JSON.stringify({
        error: error.message,
        code: error.code,
        exit: error.exit,
        ...(error.hint ? { hint: error.hint } : {}),
      }),
    );
  } else {
    err(styled(`✖ ${error.message}`, process.stderr));
    if (error.hint) err(styled(`  ${error.hint}`, process.stderr));
  }
  return ERROR_CODES[error.code].exit;
}
