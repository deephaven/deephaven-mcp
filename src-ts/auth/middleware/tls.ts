/**
 * Express/Node.js middleware enforcing transport-layer security on auth-bearing requests.
 *
 * The MCP servers consume credentials via HTTP headers
 * (`X-Deephaven-Password`, `X-Deephaven-Private-Key`, `X-Deephaven-PSK`).
 * Those headers carry secrets in cleartext on the wire and must therefore
 * travel over TLS. {@link TlsEnforcementMiddleware} is mounted ahead of
 * {@link AuthenticationMiddleware} and rejects requests that cannot be
 * shown to be transport-encrypted.
 *
 * **Decision algorithm** (per HTTP request, evaluated top-to-bottom):
 *
 * 1. If the path is in `bypassPaths` — pass. Callers typically include their
 *    liveness/readiness probe path here so probes succeed regardless of peer.
 * 2. If `req.socket?.encrypted` (TLS at the socket level) — pass.
 * 3. If the immediate peer is loopback (`127.0.0.0/8` / `::1`) — pass.
 *    Loopback traffic never leaves the kernel.
 * 4. If `trustForwardedProto` is set AND the immediate peer is in the
 *    `forwardedAllowIps` allowlist AND the request carries
 *    `X-Forwarded-Proto: https` — pass.
 * 5. If `allowCleartext` is set — pass, with a throttled WARNING.
 * 6. Otherwise reject with `426 Upgrade Required`.
 *
 * Non-HTTP scopes pass through unchanged. This middleware never reads,
 * inspects, or validates the `X-Deephaven-*` auth headers; that is the
 * responsibility of {@link AuthenticationMiddleware}.
 */

import * as net from "node:net";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { MiddlewareFn } from "./middleware.js";

/** Minimum milliseconds between successive cleartext-allowed WARNING log lines. */
const _CLEARTEXT_WARNING_INTERVAL_MS = 60_000;

// ---------------------------------------------------------------------------
// TransportSecurityPolicy
// ---------------------------------------------------------------------------

/**
 * Immutable transport-security policy decided once at server startup.
 */
export interface TransportSecurityPolicy {
  /** Whether to honor the `X-Forwarded-Proto` header from a fronting reverse proxy. */
  readonly trustForwardedProto: boolean;
  /**
   * Peer-IP allowlist (CIDR-aware) for trusting `X-Forwarded-Proto`.
   * Each entry is an IP address string or CIDR notation string.
   * Ignored when `trustForwardedProto` is `false`.
   */
  readonly forwardedAllowIps: readonly string[];
  /** When `true`, any peer is treated as in the allowlist (set when operator passed `"*"`). */
  readonly allowAnyForwardedIp: boolean;
  /** When `true`, accept cleartext non-loopback traffic with a throttled WARNING. */
  readonly allowCleartext: boolean;
  /** Exact-match request paths that skip TLS enforcement entirely. */
  readonly bypassPaths: ReadonlySet<string>;
}

/**
 * Create a {@link TransportSecurityPolicy} with defaults.
 *
 * @param options - Policy configuration.
 * @returns An immutable policy object.
 */
export function createTransportSecurityPolicy(
  options?: Partial<TransportSecurityPolicy>,
): TransportSecurityPolicy {
  return Object.freeze({
    trustForwardedProto: options?.trustForwardedProto ?? false,
    forwardedAllowIps: options?.forwardedAllowIps ?? [],
    allowAnyForwardedIp: options?.allowAnyForwardedIp ?? false,
    allowCleartext: options?.allowCleartext ?? false,
    bypassPaths: options?.bypassPaths ?? new Set<string>(),
  });
}

// ---------------------------------------------------------------------------
// parseForwardedAllowIps
// ---------------------------------------------------------------------------

/**
 * Parse a comma-separated forwarded-IP allowlist.
 *
 * Mirrors uvicorn's `--forwarded-allow-ips` flag. Accepts:
 * - A single IP or CIDR notation string.
 * - A comma-separated list.
 * - The wildcard `"*"` (any peer trusted).
 *
 * @param raw - The raw flag value.
 * @returns `[networks, allowAny]`. `networks` is the parsed list of IP/CIDR strings
 *   (empty when `allowAny` is `true`). `allowAny` is `true` iff the input contained `"*"`.
 * @throws {Error} If `raw` is empty/whitespace, contains an empty entry, or
 *   contains an unparseable address/network.
 */
export function parseForwardedAllowIps(
  raw: string,
): [string[], boolean] {
  if (!raw || !raw.trim()) {
    throw new Error(
      "--forwarded-allow-ips must not be empty (use '*' to allow any peer).",
    );
  }
  const networks: string[] = [];
  let allowAny = false;
  for (const entry of raw.split(",")) {
    const token = entry.trim();
    if (!token) {
      throw new Error(
        `--forwarded-allow-ips contains an empty entry: ${JSON.stringify(raw)}`,
      );
    }
    if (token === "*") {
      allowAny = true;
      continue;
    }
    // Validate the IP/CIDR format
    const cidrParts = token.split("/");
    const ipPart = cidrParts[0]!;
    if (!net.isIP(ipPart)) {
      throw new Error(
        `--forwarded-allow-ips entry ${JSON.stringify(token)} is not a valid IP address or CIDR network`,
      );
    }
    if (cidrParts.length > 2) {
      throw new Error(
        `--forwarded-allow-ips entry ${JSON.stringify(token)} is not a valid IP address or CIDR network`,
      );
    }
    if (cidrParts.length === 2) {
      const prefix = parseInt(cidrParts[1]!, 10);
      const maxPrefix = net.isIPv4(ipPart) ? 32 : 128;
      if (isNaN(prefix) || prefix < 0 || prefix > maxPrefix) {
        throw new Error(
          `--forwarded-allow-ips entry ${JSON.stringify(token)} is not a valid IP address or CIDR network`,
        );
      }
    }
    networks.push(token);
  }
  if (allowAny) {
    return [[], true];
  }
  return [networks, false];
}

// ---------------------------------------------------------------------------
// TlsEnforcementMiddleware
// ---------------------------------------------------------------------------

/**
 * Express/Node.js middleware that rejects cleartext non-loopback HTTP traffic.
 *
 * See module docstring for the full decision algorithm.
 */
export class TlsEnforcementMiddleware {
  /** The transport-security policy. */
  readonly policy: TransportSecurityPolicy;

  private _lastCleartextWarningMonotonic: number = 0;
  private _suppressedCleartextWarnings: number = 0;

  /**
   * @param policy - Frozen transport-security policy. In production this is
   *   returned by the server startup validation; tests construct it directly.
   */
  constructor(policy: TransportSecurityPolicy) {
    this.policy = policy;
  }

  /**
   * Returns an Express-compatible middleware function.
   *
   * @returns An Express-compatible middleware function.
   */
  handler(): MiddlewareFn {
    return async (req: IncomingMessage, res: ServerResponse, next: (err?: unknown) => void) => {
      const url = (req as { url?: string }).url ?? "";
      const path = url.split("?")[0] ?? "";

      if (this.policy.bypassPaths.has(path)) {
        next();
        return;
      }

      // Check if TLS at socket level (req.socket is a TLSSocket)
      const socket = (req as { socket?: { encrypted?: boolean } }).socket;
      if (socket?.encrypted) {
        next();
        return;
      }

      const peerIp = _extractPeerIp(req);
      if (peerIp !== null && _isLoopback(peerIp)) {
        next();
        return;
      }

      if (this.policy.trustForwardedProto && peerIp !== null) {
        if (_peerInAllowlist(peerIp, this.policy)) {
          if (_lastForwardedProto(req.headers) === "https") {
            next();
            return;
          }
        }
      }

      if (this.policy.allowCleartext) {
        this._maybeWarnCleartext(path, peerIp);
        next();
        return;
      }

      await _send426(res);
    };
  }

  private _maybeWarnCleartext(
    path: string,
    peerIp: string | null,
  ): void {
    const now = Date.now();
    if (now - this._lastCleartextWarningMonotonic < _CLEARTEXT_WARNING_INTERVAL_MS) {
      this._suppressedCleartextWarnings += 1;
      return;
    }
    const suppressed = this._suppressedCleartextWarnings;
    this._suppressedCleartextWarnings = 0;
    this._lastCleartextWarningMonotonic = now;
    const suppressedSuffix =
      suppressed > 0
        ? ` (suppressed ${suppressed} similar warning${suppressed === 1 ? "" : "s"} in the last ${Math.round(_CLEARTEXT_WARNING_INTERVAL_MS / 1000)}s)`
        : "";
    // Use console.warn for Node.js since pino logger would be set up separately
    console.warn(
      `[TlsEnforcementMiddleware] Accepting cleartext request to ${JSON.stringify(path)} from peer=${peerIp} because allow_cleartext=True. Auth headers are traveling unencrypted on the wire.${suppressedSuffix}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Extract the immediate peer's IP from a Node.js request.
 *
 * @param req - The Node.js IncomingMessage.
 * @returns The peer IP string, or `null` if absent or unparseable.
 */
export function _extractPeerIp(req: IncomingMessage): string | null {
  const remoteAddress = (req.socket as { remoteAddress?: string } | undefined)?.remoteAddress;
  if (!remoteAddress) return null;
  return remoteAddress;
}

/**
 * Check if an IP address is loopback.
 *
 * @param ip - The IP address string.
 * @returns `true` if the IP is a loopback address.
 */
export function _isLoopback(ip: string): boolean {
  // Handle IPv6-mapped IPv4 addresses like "::ffff:127.0.0.1"
  const normalized = ip.replace(/^::ffff:/i, "");
  if (net.isIPv4(normalized)) {
    // 127.0.0.0/8
    return normalized.startsWith("127.");
  }
  if (net.isIPv6(ip)) {
    return ip === "::1" || ip.toLowerCase() === "::1";
  }
  return false;
}

/**
 * Check if a peer IP is in the forwarded-allow-ips allowlist.
 *
 * @param peerIp - The peer IP string.
 * @param policy - The transport-security policy.
 * @returns `true` if the peer is trusted.
 */
export function _peerInAllowlist(
  peerIp: string,
  policy: TransportSecurityPolicy,
): boolean {
  if (policy.allowAnyForwardedIp) {
    return true;
  }
  const peerAddr = peerIp.replace(/^::ffff:/i, "");
  for (const network of policy.forwardedAllowIps) {
    if (_ipInCidr(peerAddr, network)) {
      return true;
    }
  }
  return false;
}

/**
 * Check if an IP address is in a CIDR range.
 *
 * @param ip - The IP address to check.
 * @param cidr - The CIDR range (e.g., "10.0.0.0/8" or "10.0.0.5").
 * @returns `true` if the IP is in the CIDR range.
 */
export function _ipInCidr(ip: string, cidr: string): boolean {
  if (cidr === ip) return true;
  const parts = cidr.split("/");
  const networkIp = parts[0]!;
  const prefix = parts[1] !== undefined ? parseInt(parts[1], 10) : undefined;

  if (prefix === undefined) {
    return ip === networkIp;
  }

  // Only support IPv4 CIDR matching for now
  if (!net.isIPv4(ip) || !net.isIPv4(networkIp)) {
    // For IPv6, do exact match only
    return ip === networkIp;
  }

  const ipNum = _ipv4ToNumber(ip);
  const networkNum = _ipv4ToNumber(networkIp);
  const mask = prefix === 0 ? 0 : (~0 << (32 - prefix)) >>> 0;
  return (ipNum & mask) === (networkNum & mask);
}

function _ipv4ToNumber(ip: string): number {
  return ip.split(".").reduce((acc, octet) => (acc << 8) | parseInt(octet, 10), 0) >>> 0;
}

/**
 * Return the effective `X-Forwarded-Proto` value, lowercased.
 *
 * Walks the raw headers to defend against header-stuffing on misbehaving proxies.
 * The convention every well-behaved proxy follows when adding the header is to
 * **append**, so the LAST occurrence is the one written by the most-recent (most-trusted) hop.
 *
 * @param headers - The Node.js request headers.
 * @returns The effective `X-Forwarded-Proto` value, or `null` if absent.
 */
export function _lastForwardedProto(
  headers: IncomingMessage["headers"],
): string | null {
  const raw = headers["x-forwarded-proto"];
  if (raw === undefined) return null;
  // Node.js may return array for duplicate headers; take the last
  const lastValue = Array.isArray(raw) ? raw[raw.length - 1] : raw;
  if (!lastValue) return null;
  // Take the LAST comma token: the entry the trusted proxy wrote
  const lastToken = lastValue.split(",").pop()!;
  return lastToken.trim().toLowerCase();
}

/**
 * Emit a compact JSON `426 Upgrade Required` response.
 *
 * @param res - The Node.js ServerResponse to write to.
 */
export async function _send426(res: ServerResponse): Promise<void> {
  const body = JSON.stringify({
    error: "tls_required",
    detail:
      "This endpoint accepts authenticated requests only over TLS. " +
      "Connect via https://, or — if a TLS-terminating proxy is in " +
      "front of this server — start the server with " +
      "--trust-forwarded-proto and ensure the proxy sets " +
      "X-Forwarded-Proto: https.",
  });
  const bodyBytes = Buffer.from(body, "utf-8");

  res.writeHead(426, {
    "content-type": "application/json",
    upgrade: "TLS/1.2, HTTP/1.1",
    connection: "Upgrade",
    "content-length": String(bodyBytes.length),
  });
  res.end(bodyBytes);
}
