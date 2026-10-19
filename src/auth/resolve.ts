import { DhError, describe } from "../errors.ts";
import type { Kind } from "./store.ts";

/** A server address normalized to its origin, and which product answered there. */
export interface ResolvedServer {
  origin: string;
  kind: Kind;
}

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
const HTTPS_PORTS = [443, 8000, 8123];
const HTTP_PORTS = [10000, 8000, 8123, 80];
const PROBE_TIMEOUT_MS = 2_000;
/** A leading `scheme://`; without one, the input is a bare host. */
const SCHEME = /^[a-z][a-z0-9+.-]*:\/\//i;

export function isLocal(url: URL): boolean {
  return LOCAL_HOSTS.has(url.hostname);
}

const DEFAULT_PORTS: Record<string, string> = {
  "https:": "443",
  "http:": "80",
};

/** `origin` with its port spelled out, so {@link candidates} tries only it. */
export function withPort(origin: string): string {
  const url = new URL(origin);
  return `${url.protocol}//${url.hostname}:${url.port || DEFAULT_PORTS[url.protocol]}`;
}

/** Origins to probe for `input`, in preference order. */
export function candidates(input: string): URL[] {
  const text = input.trim();
  const hasScheme = SCHEME.test(text);
  let url: URL;
  try {
    url = new URL(hasScheme ? text : `https://${text}`);
  } catch {
    throw new DhError("usage", `Not a server address: ${input}`);
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw new DhError("usage", `Unsupported scheme: ${url.protocol}`);
  }
  if (!hasScheme && isLocal(url)) url.protocol = "http:";
  // http(s) URLs drop a default port (`:443`, `:80`); a custom scheme keeps it.
  const typed = new URL(
    hasScheme ? text.replace(SCHEME, "dh://") : `dh://${text}`,
  );
  if (typed.port) {
    // Reapplied: the scheme switch above loses a port that was https's default.
    url.port = typed.port;
    return [new URL(url.origin)];
  }
  const ports = url.protocol === "https:" ? HTTPS_PORTS : HTTP_PORTS;
  return ports.map((port) => {
    const candidate = new URL(url.origin);
    candidate.port = String(port);
    return candidate;
  });
}

/** True if `url` answers 2xx (redirects don't count). */
async function answers(url: URL, signal: AbortSignal): Promise<boolean> {
  const res = await fetch(url, { signal, redirect: "manual" });
  await res.body?.cancel();
  return res.ok;
}

/** Which kind of Deephaven server `origin` is, by the files it serves; null if neither. */
async function detect(origin: URL, timeoutMs: number): Promise<Kind | null> {
  const signal = AbortSignal.timeout(timeoutMs);
  const probe = (path: string) => answers(new URL(path, origin), signal);
  const [dhe, dhc] = await Promise.allSettled([
    probe("/iris/connection.json"),
    probe("/jsapi/dh-core.js"),
  ]);
  if (dhe.status === "fulfilled" && dhe.value) return "enterprise";
  if (dhc.status === "fulfilled" && dhc.value) return "community";
  // Neither matched; a failed probe says why better than "not found".
  const failed = [dhe, dhc].find((r) => r.status === "rejected");
  if (failed) throw failed.reason;
  return null;
}

/** Normalizes `input` and finds the first origin that answers as Deephaven. */
export async function resolveServer(
  input: string,
  timeoutMs = PROBE_TIMEOUT_MS,
): Promise<ResolvedServer> {
  const urls = candidates(input);
  const errors: string[] = [];
  const kinds = await Promise.all(
    urls.map((url) =>
      detect(url, timeoutMs).catch((e) => {
        errors.push(`${url.origin}: ${describe(e)}`);
        return null;
      }),
    ),
  );
  const i = kinds.findIndex((kind) => kind !== null);
  const url = urls[i];
  const kind = kinds[i];
  if (url && kind) return { origin: url.origin, kind };
  throw new DhError(
    "server_unreachable",
    `No Deephaven server found at ${input}`,
    errors.length === urls.length && urls.length === 1
      ? errors[0]
      : `Tried:${urls.map((u) => `\n  ${u.origin}`).join("")}`,
  );
}
