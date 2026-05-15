/**
 * Tests for auth/backends/password module.
 */
import { it, expect, describe } from "vitest";
import { PasswordBackend } from "./password.js";
import { AuthBackend, AuthenticationError } from "./base.js";
import { PasswordCredentials, Principal } from "../credentials/index.js";

it("conforms_to_auth_backend_protocol", () => {
  expect(new PasswordBackend() instanceof AuthBackend).toBe(true);
});

it("name_is_stable", () => {
  expect(new PasswordBackend().name).toBe("password");
});

it("default_disallows_effective_user", () => {
  expect(new PasswordBackend().allowEffectiveUser).toBe(false);
});

it("default_realm", () => {
  expect(new PasswordBackend().realm).toBe("deephaven-mcp");
});

it("realm_is_overridable", () => {
  expect(new PasswordBackend({ realm: "custom" }).realm).toBe("custom");
});

it("missing_password_header_returns_undefined", async () => {
  const backend = new PasswordBackend();
  expect(await backend.authenticate({})).toBeUndefined();
});

it("empty_password_raises", async () => {
  const backend = new PasswordBackend();
  await expect(
    backend.authenticate({ "x-deephaven-username": "alice", "x-deephaven-password": "" }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({ "x-deephaven-username": "alice", "x-deephaven-password": "" }),
  ).rejects.toThrow(/must not be empty/);
});

it("missing_username_raises", async () => {
  const backend = new PasswordBackend();
  await expect(
    backend.authenticate({ "x-deephaven-password": "pw" }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({ "x-deephaven-password": "pw" }),
  ).rejects.toThrow(/x-deephaven-username/);
});

it("empty_username_raises", async () => {
  const backend = new PasswordBackend();
  await expect(
    backend.authenticate({ "x-deephaven-username": "", "x-deephaven-password": "pw" }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({ "x-deephaven-username": "", "x-deephaven-password": "pw" }),
  ).rejects.toThrow(/x-deephaven-username/);
});

it("valid_password_returns_principal", async () => {
  const backend = new PasswordBackend();
  const result = await backend.authenticate({
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
  });
  expect(result instanceof Principal).toBe(true);
  expect(result!.subject).toBe("alice");
  expect(result!.displayName).toBe("alice");
  expect(result!.raw).toEqual({ backend: "password" });
});

it("effective_user_disallowed_raises", async () => {
  const backend = new PasswordBackend({ allowEffectiveUser: false });
  await expect(
    backend.authenticate({
      "x-deephaven-username": "alice",
      "x-deephaven-password": "pw",
      "x-deephaven-effective-user": "bob",
    }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({
      "x-deephaven-username": "alice",
      "x-deephaven-password": "pw",
      "x-deephaven-effective-user": "bob",
    }),
  ).rejects.toThrow(/not permitted/);
});

it("effective_user_ignored_when_empty_even_if_disallowed", async () => {
  const backend = new PasswordBackend({ allowEffectiveUser: false });
  const result = await backend.authenticate({
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
    "x-deephaven-effective-user": "",
  });
  expect(result instanceof Principal).toBe(true);
  expect(result!.raw["effective_user"]).toBeUndefined();
});

it("effective_user_allowed_captured_on_principal", async () => {
  const backend = new PasswordBackend({ allowEffectiveUser: true });
  const result = await backend.authenticate({
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
    "x-deephaven-effective-user": "bob",
  });
  expect(result instanceof Principal).toBe(true);
  expect(result!.raw["effective_user"]).toBe("bob");
});

it("derive_credentials_returns_password_creds", async () => {
  const backend = new PasswordBackend();
  const principal = new Principal("alice", "alice");
  const creds = await backend.deriveCredentials(principal, {
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
  });
  expect(creds instanceof PasswordCredentials).toBe(true);
  expect((creds as PasswordCredentials).username).toBe("alice");
  expect((creds as PasswordCredentials).password).toBe("pw");
  expect((creds as PasswordCredentials).effectiveUser).toBeUndefined();
});

it("derive_credentials_with_effective_user", async () => {
  const backend = new PasswordBackend({ allowEffectiveUser: true });
  const principal = new Principal("alice", "alice");
  const creds = await backend.deriveCredentials(principal, {
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
    "x-deephaven-effective-user": "bob",
  });
  expect((creds as PasswordCredentials).effectiveUser).toBe("bob");
});

it("derive_credentials_ignores_effective_user_when_disallowed", async () => {
  const backend = new PasswordBackend({ allowEffectiveUser: false });
  const principal = new Principal("alice", "alice");
  const creds = await backend.deriveCredentials(principal, {
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
    "x-deephaven-effective-user": "bob",
  });
  expect((creds as PasswordCredentials).effectiveUser).toBeUndefined();
});

it("derive_credentials_empty_effective_user_when_allowed", async () => {
  const backend = new PasswordBackend({ allowEffectiveUser: true });
  const principal = new Principal("alice", "alice");
  const creds = await backend.deriveCredentials(principal, {
    "x-deephaven-username": "alice",
    "x-deephaven-password": "pw",
    "x-deephaven-effective-user": "",
  });
  expect((creds as PasswordCredentials).effectiveUser).toBeUndefined();
});

describe("challenge", () => {
  it("mentions_expected_headers_with_default_realm", () => {
    const challenge = new PasswordBackend().challenge();
    expect(challenge).toContain('realm="deephaven-mcp"');
    expect(challenge).toContain("x-deephaven-username");
    expect(challenge).toContain("x-deephaven-password");
  });

  it("uses_configured_realm", () => {
    const challenge = new PasswordBackend({ realm: "custom-realm" }).challenge();
    expect(challenge).toContain('realm="custom-realm"');
  });
});
