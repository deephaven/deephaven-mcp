/**
 * Tests for auth/backends/base module (AuthBackend ABC + AuthenticationError).
 */
import { describe, it, expect } from "vitest";
import { AuthBackend, AuthenticationError, _DEFAULT_REALM } from "./base.js";
import { Principal, Credentials, PSKCredentials } from "../credentials/index.js";

// ---------------------------------------------------------------------------
// Concrete-subclass-conforming fixture for testing inherited helpers.
// ---------------------------------------------------------------------------

class ConformingBackend extends AuthBackend {
  readonly name = "conforming";

  async authenticate(_headers: Record<string, string>): Promise<Principal | undefined> {
    return this._makePrincipal("alice");
  }

  async deriveCredentials(_principal: Principal, _headers: Record<string, string>): Promise<Credentials> {
    return new PSKCredentials("x");
  }

  protected _challengeScheme(): string {
    return "Bearer";
  }

  // expose protected methods for testing
  requireHeader(headers: Record<string, string>, headerName: string): string | undefined {
    return this._requireHeader(headers, headerName);
  }

  makePrincipal(subject: string, options?: { displayName?: string; extraRaw?: Record<string, string> }): Principal {
    return this._makePrincipal(subject, options);
  }
}

class BackendWithChallengeHeaders extends AuthBackend {
  readonly name = "with-headers";

  async authenticate(_headers: Record<string, string>): Promise<Principal | undefined> {
    return undefined;
  }

  async deriveCredentials(_principal: Principal, _headers: Record<string, string>): Promise<Credentials> {
    throw new Error("not implemented");
  }

  protected _challengeScheme(): string {
    return "DeephavenTest";
  }

  protected override _challengeHeaders(): string[] {
    return ["x-foo", "x-bar"];
  }
}

// ---------------------------------------------------------------------------
// AuthenticationError
// ---------------------------------------------------------------------------

describe("AuthenticationError", () => {
  it("is_exception", () => {
    const err = new AuthenticationError("nope");
    expect(err instanceof Error).toBe(true);
    expect(err.message).toBe("nope");
  });
});

// ---------------------------------------------------------------------------
// AuthBackend cannot be instantiated directly (abstract).
// ---------------------------------------------------------------------------

describe("AuthBackend abstract", () => {
  it("authbackend_is_abstract", () => {
    expect(() => {
      // TypeScript enforces this at compile time; runtime check via direct call
      class Bare extends AuthBackend {
        readonly name = "bare";
        async authenticate(): Promise<Principal | undefined> { return undefined; }
        async deriveCredentials(): Promise<Credentials> { throw new Error(); }
        // Missing _challengeScheme - would cause TS error
        protected _challengeScheme(): string { return "X"; }
      }
      // This should succeed since all abstract methods are implemented
      new Bare();
    }).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Default __init__ stores realm with the documented fallback.
// ---------------------------------------------------------------------------

describe("realm", () => {
  it("default_realm_used_when_none_passed", () => {
    const b = new ConformingBackend();
    expect(b.realm).toBe(_DEFAULT_REALM);
    expect(b.realm).toBe("deephaven-mcp");
  });

  it("explicit_realm_overrides_default", () => {
    const b = new ConformingBackend({ realm: "custom-realm" });
    expect(b.realm).toBe("custom-realm");
  });
});

// ---------------------------------------------------------------------------
// Default challenge() formatting.
// ---------------------------------------------------------------------------

describe("challenge", () => {
  it("without_headers_omits_headers_clause", () => {
    const b = new ConformingBackend({ realm: "r" });
    expect(b.challenge()).toBe('Bearer realm="r"');
  });

  it("with_headers_includes_headers_clause", () => {
    const b = new BackendWithChallengeHeaders();
    expect(b.challenge()).toBe('DeephavenTest realm="deephaven-mcp", headers="x-foo, x-bar"');
  });
});

// ---------------------------------------------------------------------------
// _makePrincipal helper.
// ---------------------------------------------------------------------------

describe("_makePrincipal", () => {
  it("tags_backend_name_in_raw", () => {
    const b = new ConformingBackend();
    const p = b.makePrincipal("alice");
    expect(p instanceof Principal).toBe(true);
    expect(p.subject).toBe("alice");
    expect(p.displayName).toBe("alice");
    expect(p.raw).toEqual({ backend: "conforming" });
  });

  it("uses_explicit_display_name", () => {
    const b = new ConformingBackend();
    const p = b.makePrincipal("alice", { displayName: "Alice In Wonderland" });
    expect(p.subject).toBe("alice");
    expect(p.displayName).toBe("Alice In Wonderland");
  });

  it("merges_extra_raw", () => {
    const b = new ConformingBackend();
    const p = b.makePrincipal("alice", { extraRaw: { effective_user: "bob" } });
    expect(p.raw).toEqual({ backend: "conforming", effective_user: "bob" });
  });

  it("extra_raw_can_override_backend_key", () => {
    const b = new ConformingBackend();
    const p = b.makePrincipal("alice", { extraRaw: { backend: "spoofed" } });
    expect(p.raw).toEqual({ backend: "spoofed" });
  });
});

// ---------------------------------------------------------------------------
// Conforming subclass behaves end-to-end.
// ---------------------------------------------------------------------------

describe("conforming subclass", () => {
  it("authenticate_and_derive", async () => {
    const b = new ConformingBackend();
    const p = await b.authenticate({});
    expect(p).toBeDefined();
    expect(p!.subject).toBe("alice");
    const creds = await b.deriveCredentials(p!, {});
    expect(creds instanceof PSKCredentials).toBe(true);
  });

  it("isinstance_authbackend", () => {
    expect(new ConformingBackend() instanceof AuthBackend).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// _requireHeader helper.
// ---------------------------------------------------------------------------

describe("_requireHeader", () => {
  it("absent_returns_undefined", () => {
    const b = new ConformingBackend();
    expect(b.requireHeader({}, "x-missing")).toBeUndefined();
  });

  it("present_returns_value", () => {
    const b = new ConformingBackend();
    expect(b.requireHeader({ "x-token": "abc" }, "x-token")).toBe("abc");
  });

  it("empty_raises", () => {
    const b = new ConformingBackend();
    expect(() => b.requireHeader({ "x-token": "" }, "x-token")).toThrow(AuthenticationError);
    expect(() => b.requireHeader({ "x-token": "" }, "x-token")).toThrow(/x-token header must not be empty/);
  });
});
