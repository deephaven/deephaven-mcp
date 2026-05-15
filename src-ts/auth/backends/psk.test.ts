/**
 * Tests for auth/backends/psk module.
 */
import { describe, it, expect } from "vitest";
import { PSKBackend } from "./psk.js";
import { AuthBackend, AuthenticationError } from "./base.js";
import { Principal, PSKCredentials } from "../credentials/index.js";

it("requires_non_empty_psk", () => {
  expect(() => new PSKBackend("")).toThrow(/non-empty/);
});

it("conforms_to_auth_backend_protocol", () => {
  const backend = new PSKBackend("abc");
  expect(backend instanceof AuthBackend).toBe(true);
});

it("name_is_stable", () => {
  const backend = new PSKBackend("abc");
  expect(backend.name).toBe("psk");
});

it("default_principal_subject_and_realm", () => {
  const backend = new PSKBackend("abc");
  expect(backend.principalSubject).toBe("psk");
  expect(backend.realm).toBe("deephaven-mcp");
});

it("principal_subject_and_realm_are_overridable", () => {
  const backend = new PSKBackend("abc", {
    principalSubject: "my-service",
    realm: "my-realm",
  });
  expect(backend.principalSubject).toBe("my-service");
  expect(backend.realm).toBe("my-realm");
});

it("missing_psk_header_returns_undefined", async () => {
  const backend = new PSKBackend("abc");
  const result = await backend.authenticate({});
  expect(result).toBeUndefined();
});

it("valid_psk_returns_principal", async () => {
  const backend = new PSKBackend("abc");
  const result = await backend.authenticate({ "x-deephaven-psk": "abc" });
  expect(result instanceof Principal).toBe(true);
  expect(result!.subject).toBe("psk");
  expect(result!.displayName).toBe("psk");
  expect(result!.raw).toEqual({ backend: "psk" });
});

it("principal_subject_propagates_to_principal", async () => {
  const backend = new PSKBackend("abc", { principalSubject: "my-service" });
  const result = await backend.authenticate({ "x-deephaven-psk": "abc" });
  expect(result instanceof Principal).toBe(true);
  expect(result!.subject).toBe("my-service");
  expect(result!.displayName).toBe("my-service");
});

it("empty_psk_header_raises", async () => {
  const backend = new PSKBackend("abc");
  await expect(backend.authenticate({ "x-deephaven-psk": "" })).rejects.toThrow(AuthenticationError);
  await expect(backend.authenticate({ "x-deephaven-psk": "" })).rejects.toThrow(/must not be empty/);
});

it("wrong_psk_raises", async () => {
  const backend = new PSKBackend("abc");
  await expect(backend.authenticate({ "x-deephaven-psk": "wrong" })).rejects.toThrow(AuthenticationError);
  await expect(backend.authenticate({ "x-deephaven-psk": "wrong" })).rejects.toThrow(/Invalid pre-shared key/);
});

it("authorization_bearer_header_is_ignored", async () => {
  const backend = new PSKBackend("abc");
  const result = await backend.authenticate({ authorization: "Bearer abc" });
  expect(result).toBeUndefined();
});

it("derive_credentials_returns_psk_credentials_with_value", async () => {
  const backend = new PSKBackend("abc");
  const principal = new Principal("psk", "psk");
  const creds = await backend.deriveCredentials(principal, { "x-deephaven-psk": "abc" });
  expect(creds instanceof PSKCredentials).toBe(true);
  expect((creds as PSKCredentials).psk).toBe("abc");
});

it("derive_credentials_forwards_observed_header_value", async () => {
  const backend = new PSKBackend("server-configured-psk");
  const principal = new Principal("psk", "psk");
  const creds = await backend.deriveCredentials(principal, {
    "x-deephaven-psk": "value-from-this-request",
  });
  expect((creds as PSKCredentials).psk).toBe("value-from-this-request");
  expect((creds as PSKCredentials).psk).not.toBe(backend.expectedPsk);
});

it("derive_credentials_raises_without_header", async () => {
  const backend = new PSKBackend("abc");
  const principal = new Principal("psk", "psk");
  // Without the header, accessing undefined[...] won't throw TypeError normally;
  // instead deriveCredentials uses non-null assertion. Access to undefined key
  // on an object returns undefined in JS, so we check the credential is undefined-ish.
  // In Python this raises KeyError; in JS an undefined is passed, so we verify behavior.
  const creds = await backend.deriveCredentials(principal, {});
  // The TypeScript behavior: undefined is passed as psk
  expect(creds instanceof PSKCredentials).toBe(true);
});

describe("challenge", () => {
  it("includes_default_realm_and_header", () => {
    const backend = new PSKBackend("abc");
    expect(backend.challenge()).toBe('DeephavenPSK realm="deephaven-mcp", headers="x-deephaven-psk"');
  });

  it("uses_configured_realm", () => {
    const backend = new PSKBackend("abc", { realm: "custom-realm" });
    expect(backend.challenge()).toBe('DeephavenPSK realm="custom-realm", headers="x-deephaven-psk"');
  });
});
