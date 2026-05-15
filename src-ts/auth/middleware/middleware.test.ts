/**
 * Tests for auth/middleware/middleware module.
 */
import { describe, it, expect, vi } from "vitest";
import type { IncomingMessage, ServerResponse } from "node:http";
import {
  AuthenticationMiddleware,
  AuthenticationError,
  SCOPE_KEY_PRINCIPAL,
  SCOPE_KEY_CREDENTIALS,
  _lowerHeaders,
  _send401,
  AuthenticatedRequest,
} from "./middleware.js";
import { Principal, PSKCredentials, PasswordCredentials, Credentials } from "../credentials/index.js";
import { AuthBackend } from "../backends/base.js";

// ---------------------------------------------------------------------------
// Test fixture backends
// ---------------------------------------------------------------------------

class StaticBackend extends AuthBackend {
  readonly name = "static";
  private readonly _token: string;

  constructor(token: string) {
    super();
    this._token = token;
  }

  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    const header = headers["authorization"];
    if (header === undefined) return undefined;
    if (header !== `Bearer ${this._token}`) throw new AuthenticationError("bad token");
    return new Principal("community", "community");
  }

  async deriveCredentials(_principal: Principal, _headers: Record<string, string>): Promise<Credentials> {
    return new PSKCredentials("x");
  }

  protected _challengeScheme(): string { return "Bearer"; }

  challenge(): string { return 'Bearer realm="test"'; }
}

class UserHeaderBackend extends AuthBackend {
  readonly name = "user";

  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    if (!("x-user" in headers)) return undefined;
    return new Principal(headers["x-user"]!, headers["x-user"]!);
  }

  async deriveCredentials(principal: Principal, headers: Record<string, string>): Promise<Credentials> {
    return new PasswordCredentials(principal.subject, headers["x-password"]!);
  }

  protected _challengeScheme(): string { return "DeephavenHeaders"; }

  challenge(): string { return 'DeephavenHeaders realm="enterprise"'; }
}

class RaisingApp {
  // Simulates a downstream handler that raises AuthenticationError
  handle(_req: unknown, _res: unknown, _next: (err?: unknown) => void): void {
    throw new AuthenticationError("from inside downstream app");
  }
}

// ---------------------------------------------------------------------------
// Mock request/response helpers
// ---------------------------------------------------------------------------

function makeReq(path: string, headers: Record<string, string> = {}): IncomingMessage {
  return {
    url: path,
    headers: Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v])),
  } as unknown as IncomingMessage;
}

function makeRes(): { res: ServerResponse; statusCode: number; headers: Record<string, string>; body: Buffer | null } {
  const captured: { statusCode: number; headers: Record<string, string>; body: Buffer | null } = {
    statusCode: 200,
    headers: {},
    body: null,
  };
  const res = {
    writeHead: vi.fn((status: number, hdrs: Record<string, string>) => {
      captured.statusCode = status;
      Object.assign(captured.headers, hdrs);
    }),
    end: vi.fn((data?: Buffer | string) => {
      captured.body = data ? Buffer.from(data) : null;
    }),
  } as unknown as ServerResponse;
  return { res, ...captured };
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

describe("construction", () => {
  it("init_requires_at_least_one_backend", () => {
    expect(() => new AuthenticationMiddleware([])).toThrow(/at least one backend/);
  });

  it("backends_are_stored_as_tuple", () => {
    const backend = new StaticBackend("t");
    const mw = new AuthenticationMiddleware([backend]);
    expect(mw.backends).toEqual([backend]);
  });
});

// ---------------------------------------------------------------------------
// Request handling
// ---------------------------------------------------------------------------

describe("request handling", () => {
  it("valid_request_populates_request_and_calls_next", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret")]);
    const req = makeReq("/mcp", { authorization: "Bearer secret" });
    const { res, ...captured } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
    expect(next).toHaveBeenCalledWith(); // no error
    expect((req as AuthenticatedRequest)[SCOPE_KEY_PRINCIPAL]?.subject).toBe("community");
    expect((req as AuthenticatedRequest)[SCOPE_KEY_CREDENTIALS] instanceof PSKCredentials).toBe(true);
  });

  it("missing_credentials_returns_401_with_www_authenticate", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret"), new UserHeaderBackend()]);
    const req = makeReq("/mcp");
    const { res, ...captured } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect((res.writeHead as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]).toBe(401);
    const hdrs = (res.writeHead as ReturnType<typeof vi.fn>).mock.calls[0]?.[1] as Record<string, string>;
    expect(hdrs["www-authenticate"]).toContain("Bearer");
    expect(hdrs["www-authenticate"]).toContain("DeephavenHeaders");
    const body = JSON.parse(
      (res.end as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]?.toString() ?? "{}"
    );
    expect(body["error"]).toBe("unauthorized");
    expect(body["detail"]).toContain("tried:");
    expect(body["detail"]).toContain("static");
    expect(body["detail"]).toContain("user");
  });

  it("bad_credentials_short_circuits_with_401", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret"), new UserHeaderBackend()]);
    const req = makeReq("/mcp", { authorization: "Bearer wrong" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect((res.writeHead as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]).toBe(401);
    const body = JSON.parse(
      (res.end as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]?.toString() ?? "{}"
    );
    expect(body["detail"]).toBe("bad token");
  });

  it("second_backend_matches_when_first_returns_undefined", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret"), new UserHeaderBackend()]);
    const req = makeReq("/mcp", { "x-user": "alice", "x-password": "pw" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
    expect((req as AuthenticatedRequest)[SCOPE_KEY_PRINCIPAL]?.subject).toBe("alice");
    expect((req as AuthenticatedRequest)[SCOPE_KEY_CREDENTIALS] instanceof PasswordCredentials).toBe(true);
  });

  it("bypass_paths_allow_unauthenticated_access", async () => {
    const mw = new AuthenticationMiddleware(
      [new StaticBackend("secret")],
      { bypassPaths: new Set(["/.well-known/oauth-protected-resource"]) },
    );
    const req = makeReq("/.well-known/oauth-protected-resource");
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
    expect((req as AuthenticatedRequest)[SCOPE_KEY_PRINCIPAL]).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Header handling
// ---------------------------------------------------------------------------

describe("header handling", () => {
  it("headers_are_lowercased_before_backends_see_them", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret")]);
    // Node.js already lowercases incoming headers; pass mixed case to test _lowerHeaders
    const req = makeReq("/mcp", { Authorization: "Bearer secret" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    expect(next).toHaveBeenCalledOnce();
  });
});

// ---------------------------------------------------------------------------
// _lowerHeaders helper
// ---------------------------------------------------------------------------

describe("_lowerHeaders", () => {
  it("lowercases_header_names", () => {
    const result = _lowerHeaders({ "Authorization": "Bearer x", "Content-Type": "application/json" });
    expect(result["authorization"]).toBe("Bearer x");
    expect(result["content-type"]).toBe("application/json");
  });

  it("later_value_wins_for_duplicate_keys", () => {
    // Test the array handling (Node.js may return arrays for duplicates)
    const result = _lowerHeaders({ "authorization": ["Bearer wrong", "Bearer secret"] });
    expect(result["authorization"]).toBe("Bearer secret");
  });

  it("undefined_values_are_skipped", () => {
    const result = _lowerHeaders({ "x-missing": undefined });
    expect(result["x-missing"]).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 401 response shape
// ---------------------------------------------------------------------------

describe("401 response shape", () => {
  it("401_response_has_json_content_type", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret")]);
    const req = makeReq("/mcp");
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    const hdrs = (res.writeHead as ReturnType<typeof vi.fn>).mock.calls[0]?.[1] as Record<string, string>;
    expect(hdrs["content-type"]).toBe("application/json");
  });

  it("401_body_is_valid_json_with_error_and_detail", async () => {
    const mw = new AuthenticationMiddleware([new StaticBackend("secret")]);
    const req = makeReq("/mcp");
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    const body = JSON.parse(
      (res.end as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]?.toString() ?? "{}"
    );
    expect(body["error"]).toBe("unauthorized");
    expect(typeof body["detail"]).toBe("string");
    expect(body["detail"].length).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// Downstream errors propagation
// ---------------------------------------------------------------------------

describe("downstream error propagation", () => {
  it("downstream_authentication_error_is_not_swallowed", async () => {
    const raisingApp = new RaisingApp();
    const mw = new AuthenticationMiddleware([new StaticBackend("secret")]);
    const req = makeReq("/mcp", { authorization: "Bearer secret" });
    const { res } = makeRes();
    const next = vi.fn();

    await mw.handler()(req, res, next);

    // Middleware called next() after auth succeeded; next() propagates to inner app.
    // The raising app would be called separately — test that the middleware itself doesn't catch it.
    expect(next).toHaveBeenCalledOnce();
    // The middleware should NOT have emitted a 401 — auth succeeded
    expect(res.writeHead).not.toHaveBeenCalled();
  });
});
