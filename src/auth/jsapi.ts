import { join, toFileUrl } from "@std/path";
import { describe, DhError } from "./errors.ts";
import { isLocal } from "./resolve.ts";

// deno-lint-ignore no-explicit-any
export type Api = any;

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

function assertTrusted(origin: URL): void {
  if (origin.protocol === "https:" || isLocal(origin)) return;
  throw new DhError(
    "server_unsupported",
    `Refusing to load client code over plain HTTP from ${origin.origin}`,
    "Use https://, or a local server.",
  );
}

async function download(
  url: URL,
  dest: string,
  timeout: number,
  optional = false,
): Promise<void> {
  let res: Response;
  try {
    res = await fetch(url, { signal: AbortSignal.timeout(timeout) });
  } catch (e) {
    throw new DhError(
      "server_unreachable",
      `Could not fetch ${url}`,
      describe(e),
    );
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

async function originDir(cacheRoot: string, origin: URL): Promise<string> {
  const dir = join(cacheRoot, origin.origin.replace(/[^a-z0-9.-]+/gi, "_"));
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
