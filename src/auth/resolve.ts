import { describe, DhError } from "./errors.ts";
import type { Kind } from "./store.ts";

export interface ResolvedServer {
  origin: string;
  kind: Kind;
}

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
const HTTPS_PORTS = [443, 8000, 8123];
const HTTP_PORTS = [10000, 8000, 8123, 80];
const PROBE_TIMEOUT_MS = 2_000;

export function isLocal(url: URL): boolean {
  return LOCAL_HOSTS.has(url.hostname);
}

/** Origins to probe for `input`, in preference order. */
export function candidates(input: string): URL[] {
  const text = input.trim();
  const hasScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(text);
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
  if (url.port) return [new URL(url.origin)];
  const ports = url.protocol === "https:" ? HTTPS_PORTS : HTTP_PORTS;
  return ports.map((port) => {
    const candidate = new URL(url.origin);
    candidate.port = String(port);
    return candidate;
  });
}

async function answers(url: URL, signal: AbortSignal): Promise<boolean> {
  const res = await fetch(url, { signal, redirect: "manual" });
  await res.body?.cancel();
  return res.ok;
}

async function detect(origin: URL, timeoutMs: number): Promise<Kind | null> {
  const signal = AbortSignal.timeout(timeoutMs);
  const probe = (path: string) => answers(new URL(path, origin), signal);
  const [dhe, dhc] = await Promise.all([
    probe("/iris/connection.json"),
    probe("/jsapi/dh-core.js"),
  ]);
  if (dhe) return "enterprise";
  if (dhc) return "community";
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
      })
    ),
  );
  const i = kinds.findIndex((kind) => kind !== null);
  if (i >= 0) return { origin: urls[i].origin, kind: kinds[i]! };
  throw new DhError(
    "server_unreachable",
    `No Deephaven server found at ${input}`,
    errors.length === urls.length && urls.length === 1
      ? errors[0]
      : `Tried: ${urls.map((u) => u.origin).join(", ")}`,
  );
}
