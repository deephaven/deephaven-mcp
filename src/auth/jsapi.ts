import { join, toFileUrl } from "@std/path";
import { describe, DhError } from "./errors.ts";
import { isLocal } from "./resolve.ts";

// deno-lint-ignore no-explicit-any
export type Api = any;

const MAX_REDIRECTS = 5;

let quieted = false;

/** The client libraries log to console and leave stray rejections; keep both out of our output. */
function quietLibraries(cacheRoot: string): void {
  if (quieted) return;
  quieted = true;
  const debug = Deno.env.get("DH_DEBUG");
  if (!debug) {
    for (
      const m of ["log", "info", "warn", "error", "debug", "trace"] as const
    ) {
      console[m] = () => {};
    }
  }
  globalThis.addEventListener("unhandledrejection", (e) => {
    const reason = e.reason;
    const fromLibrary = !(reason instanceof Error) ||
      String(reason.stack).includes(cacheRoot);
    if (!fromLibrary) return;
    e.preventDefault();
    if (debug) {
      Deno.stderr.writeSync(
        new TextEncoder().encode(`jsapi rejection: ${describe(reason)}\n`),
      );
    }
  });
}

function assertTrusted(url: URL): void {
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
  for (let hop = 0;; hop++) {
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
    await Deno.writeTextFile(dest, "");
    return;
  }
  if (!res.ok) {
    await res.body?.cancel();
    throw new DhError(
      "server_unsupported",
      `HTTP ${res.status} fetching ${url}`,
    );
  }
  await Deno.writeTextFile(dest, await res.text());
}

const runDirs = new Map<string, Promise<string>>();

/** This process's own folder under `cacheRoot`, removed on exit, so concurrent runs never share files. */
function runDir(cacheRoot: string): Promise<string> {
  let dir = runDirs.get(cacheRoot);
  if (!dir) {
    dir = (async () => {
      await Deno.mkdir(cacheRoot, { recursive: true });
      const created = await Deno.makeTempDir({
        dir: cacheRoot,
        prefix: "run-",
      });
      globalThis.addEventListener("unload", () => {
        try {
          Deno.removeSync(created, { recursive: true });
        } catch {
          // Best effort.
        }
      });
      return created;
    })();
    runDirs.set(cacheRoot, dir);
  }
  return dir;
}

async function originDir(cacheRoot: string, origin: URL): Promise<string> {
  // Reversible, so two origins can never share a directory (or a cached module).
  const dir = join(await runDir(cacheRoot), encodeURIComponent(origin.origin));
  await Deno.mkdir(dir, { recursive: true });
  return dir;
}

function polyfillBrowserGlobals(origin: URL): void {
  // deno-lint-ignore no-explicit-any
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
  quietLibraries(cacheRoot);
  const mod = await import(toFileUrl(join(dir, "dh-core.js")).href);
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
  quietLibraries(cacheRoot);
  await import(toFileUrl(file).href);
  // deno-lint-ignore no-explicit-any
  return (globalThis as any).iris;
}
