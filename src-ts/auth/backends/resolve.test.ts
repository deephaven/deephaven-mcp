/**
 * Tests for auth/backends/resolve module.
 */
import { it, expect } from "vitest";
import { authenticateAndResolve } from "./resolve.js";
import { AuthenticationError, AuthBackend } from "./base.js";
import { Credentials, PasswordCredentials, Principal, PSKCredentials } from "../credentials/index.js";

// ---------------------------------------------------------------------------
// Test fixture backends
// ---------------------------------------------------------------------------

class PasswordHeaderBackend extends AuthBackend {
  readonly name = "password";

  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    if ("x-user" in headers) {
      return new Principal(headers["x-user"]!, headers["x-user"]!);
    }
    return undefined;
  }

  async deriveCredentials(principal: Principal, headers: Record<string, string>): Promise<Credentials> {
    return new PasswordCredentials(principal.subject, headers["x-password"]!);
  }

  protected _challengeScheme(): string {
    return "DeephavenHeaders";
  }
}

class TokenBackend extends AuthBackend {
  readonly name = "token";
  private readonly _expected: string;

  constructor(expected: string) {
    super();
    this._expected = expected;
  }

  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    const header = headers["authorization"];
    if (header === undefined) {
      return undefined;
    }
    if (header !== `Bearer ${this._expected}`) {
      throw new AuthenticationError("bad token");
    }
    return new Principal("community", "community");
  }

  async deriveCredentials(_principal: Principal, _headers: Record<string, string>): Promise<Credentials> {
    return new PSKCredentials("a");
  }

  protected _challengeScheme(): string {
    return "Bearer";
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

it("first_matching_backend_wins", async () => {
  const first = new TokenBackend("a");
  const second = new PasswordHeaderBackend();
  const [principal, creds] = await authenticateAndResolve(
    [first, second],
    { Authorization: "Bearer a" },
  );
  expect(principal.subject).toBe("community");
  expect(creds instanceof PSKCredentials).toBe(true);
});

it("headers_are_lowercased_before_backend_call", async () => {
  const backend = new PasswordHeaderBackend();
  const [, creds] = await authenticateAndResolve(
    [backend],
    { "X-User": "alice", "X-Password": "pw" },
  );
  expect(creds instanceof PasswordCredentials).toBe(true);
  expect((creds as PasswordCredentials).username).toBe("alice");
  expect((creds as PasswordCredentials).password).toBe("pw");
});

it("authentication_error_is_raised_immediately", async () => {
  const backend = new TokenBackend("expected");
  await expect(
    authenticateAndResolve([backend], { Authorization: "Bearer wrong" }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    authenticateAndResolve([backend], { Authorization: "Bearer wrong" }),
  ).rejects.toThrow(/bad token/);
});

it("no_backend_matches_raises", async () => {
  const backend = new TokenBackend("expected");
  await expect(
    authenticateAndResolve([backend], {}),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    authenticateAndResolve([backend], {}),
  ).rejects.toThrow(/No registered/);
});

it("no_backend_matches_error_lists_tried_backend_names", async () => {
  const first = new TokenBackend("expected");
  const second = new PasswordHeaderBackend();
  try {
    await authenticateAndResolve([first, second], {});
    expect.fail("Should have thrown");
  } catch (e) {
    const message = (e as AuthenticationError).message;
    expect(message).toContain("tried:");
    expect(message).toContain("token");
    expect(message).toContain("password");
  }
});

it("empty_backend_chain_raises_misconfiguration_error", async () => {
  await expect(
    authenticateAndResolve([], { Authorization: "Bearer anything" }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    authenticateAndResolve([], { Authorization: "Bearer anything" }),
  ).rejects.toThrow(/No authentication backends/);
});

it("later_backends_are_tried_when_earlier_returns_undefined", async () => {
  const first = new TokenBackend("t");
  const second = new PasswordHeaderBackend();
  const [principal, creds] = await authenticateAndResolve(
    [first, second],
    { "X-User": "bob", "X-Password": "pw" },
  );
  expect(principal.subject).toBe("bob");
  expect(creds instanceof PasswordCredentials).toBe(true);
});
