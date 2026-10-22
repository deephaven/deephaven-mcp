import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { RUNTIME_FLAGS } from "../scripts/build.ts";

export const WINDOWS = process.platform === "win32";
export const MAIN = resolve(import.meta.dir, "../src/main.ts");
/** Runs `dh` from source the way the compiled binary runs. */
export const CLI = [process.execPath, ...RUNTIME_FLAGS, MAIN];
const TLS = resolve(import.meta.dir, "fixtures/tls");
/** Test-only CA that signed the fixture's `localhost` certificate. */
export const TEST_CA = join(TLS, "ca.pem");

/** An HTTP/2-only https server on localhost that looks like Enterprise to `dh`. */
export async function http2OnlyEnterprise() {
  return Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    tls: {
      key: await readFile(join(TLS, "localhost.key"), "utf8"),
      cert: await readFile(join(TLS, "localhost.pem"), "utf8"),
    },
    http2: true,
    http1: false,
    fetch: (req) =>
      new URL(req.url).pathname === "/iris/connection.json"
        ? new Response("{}")
        : new Response(null, { status: 404 }),
  });
}

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
