import { rmSync, writeSync } from "node:fs";
import { mkdir, mkdtemp, realpath, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { DhError, describe } from "./errors.ts";
import { isLocal } from "./resolve.ts";

// biome-ignore lint/suspicious/noExplicitAny: the jsapi is untyped.
export type Api = any;

const MAX_REDIRECTS = 5;

let quieted = false;

/** The client libraries log to console and leave stray rejections; keep both out of our output. */
function quietLibraries(libraryDir: string): void {
  if (quieted) return;
  quieted = true;
  const debug = process.env.DH_DEBUG;
  if (!debug) {
    for (const m of [
      "log",
      "info",
      "warn",
      "error",
      "debug",
      "trace",
    ] as const) {
      console[m] = () => {};
    }
  }
  process.on("unhandledRejection", (reason) => {
    const fromLibrary =
      !(reason instanceof Error) || String(reason.stack).includes(libraryDir);
    if (!fromLibrary) throw reason;
    if (debug) writeSync(2, `jsapi rejection: ${describe(reason)}\n`);
  });
}

/** The jsapi is code from the server, so only https or a local server may supply it. */
export function assertTrusted(url: URL): void {
  if (url.protocol === "https:" || isLocal(url)) return;
  throw new DhError(
    "server_unsupported",
    `Refusing to load client code over plain HTTP from ${url.origin}`,
    "Use https://, or a local server.",
  );
}

async function download(
  url: URL,
  dest: string,
  timeout: number,
  optional = false,
): Promise<void> {
  const signal = AbortSignal.timeout(timeout);
  let res: Response;
  // Redirects are followed by hand so every hop must pass `assertTrusted`.
  for (let hop = 0; ; hop++) {
    assertTrusted(url);
    try {
      res = await fetch(url, { signal, redirect: "manual" });
    } catch (e) {
      throw new DhError(
        "server_unreachable",
        `Could not fetch ${url}`,
        describe(e),
      );
    }
    const location = res.headers.get("location");
    if (res.status < 300 || res.status >= 400 || !location) break;
    await res.body?.cancel();
    if (hop === MAX_REDIRECTS) {
      throw new DhError("server_unsupported", `Too many redirects for ${url}`);
    }
    url = new URL(location, url);
  }
  if (optional && res.status === 404) {
    await res.body?.cancel();
    await writeFile(dest, "");
    return;
  }
  if (!res.ok) {
    await res.body?.cancel();
    throw new DhError(
      "server_unsupported",
      `HTTP ${res.status} fetching ${url}`,
    );
  }
  await writeFile(dest, await res.text());
}

const runDirs = new Map<string, Promise<string>>();

/** This process's own folder under `cacheRoot`, removed on exit, so concurrent runs never share files. */
function runDir(cacheRoot: string): Promise<string> {
  let dir = runDirs.get(cacheRoot);
  if (!dir) {
    dir = (async () => {
      await mkdir(cacheRoot, { recursive: true });
      const created = await mkdtemp(join(cacheRoot, "run-"));
      process.on("exit", () => {
        try {
          rmSync(created, { recursive: true, force: true });
        } catch {
          // Best effort.
        }
      });
      // Compiled Bun binaries can't import through a symlink (macOS's /var is one).
      return await realpath(created);
    })();
    runDirs.set(cacheRoot, dir);
  }
  return dir;
}

async function originDir(cacheRoot: string, origin: URL): Promise<string> {
  // Reversible, so two origins can never share a directory (or a cached module).
  const dir = join(await runDir(cacheRoot), encodeURIComponent(origin.origin));
  await mkdir(dir, { recursive: true });
  return dir;
}

function polyfillBrowserGlobals(origin: URL): void {
  // biome-ignore lint/suspicious/noExplicitAny: browser globals the jsapi expects.
  const g = globalThis as any;
  g.self = globalThis;
  g.window = globalThis;
  g.window.location ??= new URL(
    `${origin.protocol}//deephaven-polyfill.localhost/`,
  );
}

/** Downloads and imports the Community jsapi (`dh`) from `origin`. */
export async function loadCommunityApi(
  origin: URL,
  cacheRoot: string,
  timeout: number,
): Promise<Api> {
  assertTrusted(origin);
  const dir = await originDir(cacheRoot, origin);
  await download(
    new URL("/jsapi/dh-core.js", origin),
    join(dir, "dh-core.js"),
    timeout,
  );
  // Removed from newer servers; the core module still imports it.
  await download(
    new URL("/jsapi/dh-internal.js", origin),
    join(dir, "dh-internal.js"),
    timeout,
    true,
  );
  polyfillBrowserGlobals(origin);
  quietLibraries(await runDir(cacheRoot));
  const mod = await import(pathToFileURL(join(dir, "dh-core.js")).href);
  return mod.default ?? mod;
}

/** Downloads and imports the Enterprise jsapi (`iris`) from `origin`. */
export async function loadEnterpriseApi(
  origin: URL,
  cacheRoot: string,
  timeout: number,
): Promise<Api> {
  assertTrusted(origin);
  const dir = await originDir(cacheRoot, origin);
  const file = join(dir, "irisapi.nocache.js");
  await download(new URL("/irisapi/irisapi.nocache.js", origin), file, timeout);
  polyfillBrowserGlobals(origin);
  quietLibraries(await runDir(cacheRoot));
  await import(pathToFileURL(file).href);
  // biome-ignore lint/suspicious/noExplicitAny: set by the loaded script.
  return (globalThis as any).iris;
}
