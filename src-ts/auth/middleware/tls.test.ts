/**
 * Tests for auth/middleware/tls module.
 */
import { describe, it, expect, vi } from "vitest";
import type { IncomingMessage, ServerResponse } from "node:http";
import {
  TlsEnforcementMiddleware,
  TransportSecurityPolicy,
  createTransportSecurityPolicy,
  parseForwardedAllowIps,
  _extractPeerIp,
  _isLoopback,
  _peerInAllowlist,
  _lastForwardedProto,
  _ipInCidr,
} from "./tls.js";

// ---------------------------------------------------------------------------
// parseForwardedAllowIps
// ---------------------------------------------------------------------------

describe("parseForwardedAllowIps", () => {
  it("single_ipv4", () => {
    const [nets, allowAny] = parseForwardedAllowIps("10.0.0.5");
    expect(allowAny).toBe(false);
    expect(nets.length).toBe(1);
    expect(_ipInCidr("10.0.0.5", nets[0]!)).toBe(true);
    expect(_ipInCidr("10.0.0.6", nets[0]!)).toBe(false);
  });

  it("single_ipv6", () => {
    const [nets, allowAny] = parseForwardedAllowIps("::1");
    expect(allowAny).toBe(false);
    expect(_ipInCidr("::1", nets[0]!)).toBe(true);
  });

  it("cidr", () => {
    const [nets, allowAny] = parseForwardedAllowIps("10.0.0.0/8");
    expect(allowAny).toBe(false);
    expect(_ipInCidr("10.255.255.1", nets[0]!)).toBe(true);
    expect(_ipInCidr("11.0.0.1", nets[0]!)).toBe(false);
  });

  it("comma_list", () => {
    const [nets, allowAny] = parseForwardedAllowIps("10.0.0.5,192.168.1.0/24, 172.16.0.1");
    expect(allowAny).toBe(false);
    expect(nets.length).toBe(3);
    expect(_ipInCidr("10.0.0.5", nets[0]!)).toBe(true);
    expect(_ipInCidr("192.168.1.42", nets[1]!)).toBe(true);
    expect(_ipInCidr("172.16.0.1", nets[2]!)).toBe(true);
  });

  it("wildcard", () => {
    const [nets, allowAny] = parseForwardedAllowIps("*");
    expect(allowAny).toBe(true);
    expect(nets.length).toBe(0);
  });

  it("wildcard_with_other_entries_subsumes_them", () => {
    const [nets, allowAny] = parseForwardedAllowIps("10.0.0.0/8,*");
    expect(allowAny).toBe(true);
    expect(nets.length).toBe(0);
  });

  it("empty_raises", () => {
    expect(() => parseForwardedAllowIps("")).toThrow(/must not be empty/);
    expect(() => parseForwardedAllowIps("   ")).toThrow(/must not be empty/);
  });

  it("empty_entry_raises", () => {
    expect(() => parseForwardedAllowIps("10.0.0.1,,192.168.1.1")).toThrow(/empty entry/);
  });

  it("invalid_ip_raises", () => {
    expect(() => parseForwardedAllowIps("not-an-ip")).toThrow(/not a valid IP/);
  });
});

// ---------------------------------------------------------------------------
// _isLoopback
// ---------------------------------------------------------------------------

describe("_isLoopback", () => {
  it("ipv4_loopback", () => {
    expect(_isLoopback("127.0.0.1")).toBe(true);
    expect(_isLoopback("127.0.0.2")).toBe(true);
    expect(_isLoopback("127.255.255.255")).toBe(true);
  });

  it("ipv4_non_loopback", () => {
    expect(_isLoopback("192.168.1.1")).toBe(false);
    expect(_isLoopback("10.0.0.1")).toBe(false);
  });

  it("ipv6_loopback", () => {
    expect(_isLoopback("::1")).toBe(true);
  });

  it("ipv6_non_loopback", () => {
    expect(_isLoopback("2001:db8::1")).toBe(false);
  });

  it("ipv6_mapped_ipv4_loopback", () => {
    expect(_isLoopback("::ffff:127.0.0.1")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// _peerInAllowlist
// ---------------------------------------------------------------------------

describe("_peerInAllowlist", () => {
  it("allow_any_returns_true", () => {
    const policy = createTransportSecurityPolicy({ allowAnyForwardedIp: true });
    expect(_peerInAllowlist("203.0.113.5", policy)).toBe(true);
  });

  it("ip_in_cidr_returns_true", () => {
    const policy = createTransportSecurityPolicy({
      allowAnyForwardedIp: false,
      forwardedAllowIps: ["10.0.0.0/8"],
    });
    expect(_peerInAllowlist("10.5.5.5", policy)).toBe(true);
    expect(_peerInAllowlist("11.0.0.1", policy)).toBe(false);
  });

  it("ip_not_in_list_returns_false", () => {
    const policy = createTransportSecurityPolicy({
      allowAnyForwardedIp: false,
      forwardedAllowIps: ["10.0.0.5"],
    });
    expect(_peerInAllowlist("10.0.0.6", policy)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// _lastForwardedProto
// ---------------------------------------------------------------------------

describe("_lastForwardedProto", () => {
  it("returns_null_when_absent", () => {
    expect(_lastForwardedProto({})).toBeNull();
  });

  it("returns_lowercased_value", () => {
    expect(_lastForwardedProto({ "x-forwarded-proto": "HTTPS" })).toBe("https");
  });

  it("takes_last_comma_token", () => {
    expect(_lastForwardedProto({ "x-forwarded-proto": "https,http" })).toBe("http");
    expect(_lastForwardedProto({ "x-forwarded-proto": "http,https" })).toBe("https");
  });

  it("takes_last_of_array", () => {
    expect(_lastForwardedProto({ "x-forwarded-proto": ["http", "https"] })).toBe("https");
  });
});

// ---------------------------------------------------------------------------
// TlsEnforcementMiddleware
// ---------------------------------------------------------------------------

function makeReq(options: {
  path?: string;
  encrypted?: boolean;
  remoteAddress?: string | null;
  headers?: Record<string, string | string[]>;
}): IncomingMessage {
  return {
    url: options.path ?? "/mcp",
    socket: options.remoteAddress !== null ? {
      encrypted: options.encrypted,
      remoteAddress: options.remoteAddress ?? "203.0.113.5",
    } : null,
    headers: options.headers ?? {},
  } as unknown as IncomingMessage;
}

function makeRes(): { res: ServerResponse; getStatus: () => number; getHeaders: () => Record<string, string>; getBody: () => Buffer } {
  let capturedStatus = 200;
  let capturedHeaders: Record<string, string> = {};
  let capturedBody: Buffer = Buffer.alloc(0);

  const res = {
    writeHead: vi.fn((status: number, hdrs: Record<string, string>) => {
      capturedStatus = status;
      Object.assign(capturedHeaders, hdrs);
    }),
    end: vi.fn((data?: Buffer | string) => {
      if (data) capturedBody = Buffer.from(data);
    }),
  } as unknown as ServerResponse;

  return {
    res,
    getStatus: () => capturedStatus,
    getHeaders: () => capturedHeaders,
    getBody: () => capturedBody,
  };
}

describe("TlsEnforcementMiddleware", () => {
  it("https_scope_passes_through", async () => {
    const policy = createTransportSecurityPolicy();
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ encrypted: true });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
    expect(res.writeHead).not.toHaveBeenCalled();
  });

  it("loopback_ipv4_passes_through", async () => {
    const policy = createTransportSecurityPolicy();
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ remoteAddress: "127.0.0.1" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
  });

  it("loopback_ipv6_passes_through", async () => {
    const policy = createTransportSecurityPolicy();
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ remoteAddress: "::1" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
  });

  it("non_loopback_cleartext_rejected_with_426", async () => {
    const policy = createTransportSecurityPolicy();
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ remoteAddress: "203.0.113.5" });
    const { res, getStatus } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(getStatus()).toBe(426);
  });

  it("trusted_proxy_with_https_forwarded_proto_passes", async () => {
    const policy = createTransportSecurityPolicy({
      trustForwardedProto: true,
      forwardedAllowIps: ["10.0.0.1"],
    });
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({
      remoteAddress: "10.0.0.1",
      headers: { "x-forwarded-proto": "https" },
    });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
  });

  it("trusted_proxy_with_http_forwarded_proto_rejected", async () => {
    const policy = createTransportSecurityPolicy({
      trustForwardedProto: true,
      forwardedAllowIps: ["10.0.0.1"],
    });
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({
      remoteAddress: "10.0.0.1",
      headers: { "x-forwarded-proto": "http" },
    });
    const { res, getStatus } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(getStatus()).toBe(426);
  });

  it("allow_cleartext_permits_non_loopback", async () => {
    const policy = createTransportSecurityPolicy({ allowCleartext: true });
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ remoteAddress: "203.0.113.5" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
  });

  it("bypass_path_passes_through", async () => {
    const policy = createTransportSecurityPolicy({
      bypassPaths: new Set(["/livez"]),
    });
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ path: "/livez", remoteAddress: "203.0.113.5" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
  });

  it("bypass_path_exact_match_only", async () => {
    const policy = createTransportSecurityPolicy({
      bypassPaths: new Set(["/health"]),
    });
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ path: "/healthz", remoteAddress: "203.0.113.5" });
    const { res, getStatus } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(getStatus()).toBe(426);
  });

  it("426_response_body_contains_tls_required", async () => {
    const policy = createTransportSecurityPolicy();
    const mw = new TlsEnforcementMiddleware(policy);
    const req = makeReq({ remoteAddress: "203.0.113.5" });
    const { res, getBody } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    const body = JSON.parse(getBody().toString());
    expect(body["error"]).toBe("tls_required");
    expect(body["detail"]).toContain("TLS");
  });
});
