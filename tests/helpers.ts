import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

export const WINDOWS = process.platform === "win32";
export const MAIN = resolve(import.meta.dir, "../src/main.ts");

export interface Run {
  code: number;
  stdout: string;
  stderr: string;
}

/** Runs a process to completion; `env` extends this one's, and `undefined` unsets. */
export async function run(
  cmd: string[],
  env: Record<string, string | undefined> = {},
  stdin?: string,
): Promise<Run> {
  const merged = Object.entries({ ...process.env, ...env }).filter(
    (entry): entry is [string, string] => entry[1] !== undefined,
  );
  const proc = Bun.spawn(cmd, {
    env: Object.fromEntries(merged),
    stdin: stdin === undefined ? "ignore" : new TextEncoder().encode(stdin),
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { code, stdout, stderr };
}

export function tempDir(prefix: string): Promise<string> {
  return mkdtemp(join(tmpdir(), prefix));
}
