import { writeSync } from "node:fs";
import { styleText } from "node:util";
import { asDhError, type DhError, type FailureReport } from "./errors.ts";

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
 * `text` colored for `stream`, plain unless it's a terminal: a leading mark,
 * `commands` in cyan, and a URL alone on its line. A `…` progress line is
 * dimmed whole.
 */
function styled(text: string, stream: NodeJS.WriteStream): string {
  const paint = (style: Style, s: string) => styleText(style, s, { stream });
  const styleLine = (line: string) => {
    if (/^\s*https?:\/\/\S+$/.test(line)) {
      return line.replace(/\S+/, (url) => paint(["cyan", "underline"], url));
    }
    const [, indent = "", mark = ""] = /^(\s*)([✔✖!…]) /.exec(line) ?? [];
    if (mark === "…") return paint("dim", line);
    const plain = line.replace(/`[^`]+`/g, (c) => paint("cyan", c));
    const style = MARKS[mark];
    if (!style) return plain;
    return indent + paint(style, mark) + plain.slice(indent.length + 1);
  };
  return text.split("\n").map(styleLine).join("\n");
}

/** Prints a command's result: `json` with `-o json`, else the lines from `human`. */
export function result(
  human: () => string | string[],
  json: unknown,
  empty = false,
): void {
  if (empty) return;
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
export function reportError(e: unknown, report: FailureReport = {}): number {
  const error = asDhError(e);
  if (error.code === "internal" && process.env.DH_DEBUG && e instanceof Error) {
    err(e.stack ?? e.message);
  }
  if (isJson()) {
    const { command, human: _human, ...fields } = report;
    err(
      JSON.stringify({
        type: "error",
        error: error.headline,
        code: error.code,
        exit: error.exit,
        ...(command ? { command } : {}),
        ...(error.detail ? { detail: error.detail } : {}),
        ...(error.hint ? { hint: error.hint } : {}),
        ...(error.cleanupDetail ? { cleanupDetail: error.cleanupDetail } : {}),
        ...fields,
      }),
    );
  } else {
    const presentation = report.human;
    const headline =
      error.code === "cancelled" &&
      /^(Cancelled|Interrupted)\.?$/.test(error.headline)
        ? "Cancelled."
        : error.headline;
    err(
      styled(
        `✖ ${error.code === "cancelled" ? (presentation?.summary ?? headline) : headline}`,
        process.stderr,
      ),
    );
    const lines = [...(presentation ? (presentation.details ?? []) : [])];
    if (presentation && error.code !== "cancelled" && presentation.context) {
      lines.unshift(presentation.context);
    }
    if (presentation && (report.elapsedSeconds ?? 0) >= 5) {
      lines.push(`Waited ${report.elapsedSeconds}s.`);
    }
    if (error.detail && !isSignalDetail(error)) {
      lines.push(error.detail);
    }
    if (error.hint) lines.push(error.hint);
    if (report.recoveryCommand) {
      lines.push(
        `${presentation?.recoveryLabel ?? "Follow up"}: ${report.recoveryCommand.map(quoteShellArg).join(" ")}`,
      );
    }
    if (error.cleanupDetail)
      lines.push(`Cleanup failed: ${error.cleanupDetail}`);
    for (const line of lines.flatMap((text) => text.split("\n"))) {
      err(styled(`  ${line}`, process.stderr));
    }
  }
  return error.exit;
}

function isSignalDetail(error: DhError): boolean {
  return (
    error.code === "cancelled" &&
    /^Interrupted by SIG(INT|TERM)$/.test(error.detail ?? "")
  );
}

function quoteShellArg(arg: string): string {
  if (/^[A-Za-z0-9_./:-]+$/.test(arg)) return arg;
  if (process.platform === "win32") return `'${arg.replaceAll("'", "''")}'`;
  return `'${arg.replaceAll("'", "'\\''")}'`;
}
