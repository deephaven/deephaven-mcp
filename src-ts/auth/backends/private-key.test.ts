/**
 * Tests for auth/backends/private-key module.
 */
import { it, expect, describe } from "vitest";
import { PrivateKeyBackend } from "./private-key.js";
import { AuthBackend, AuthenticationError } from "./base.js";
import { Principal, PrivateKeyCredentials } from "../credentials/index.js";

function encode(key: Buffer): string {
  return key.toString("base64");
}

it("conforms_to_auth_backend_protocol", () => {
  expect(new PrivateKeyBackend() instanceof AuthBackend).toBe(true);
});

it("name_is_stable", () => {
  expect(new PrivateKeyBackend().name).toBe("private_key");
});

it("default_realm", () => {
  expect(new PrivateKeyBackend().realm).toBe("deephaven-mcp");
});

it("realm_is_overridable", () => {
  expect(new PrivateKeyBackend({ realm: "custom" }).realm).toBe("custom");
});

it("missing_key_header_returns_undefined", async () => {
  const backend = new PrivateKeyBackend();
  expect(await backend.authenticate({})).toBeUndefined();
});

it("empty_key_header_raises", async () => {
  const backend = new PrivateKeyBackend();
  await expect(
    backend.authenticate({
      "x-deephaven-username": "alice",
      "x-deephaven-private-key": "",
    }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({
      "x-deephaven-username": "alice",
      "x-deephaven-private-key": "",
    }),
  ).rejects.toThrow(/must not be empty/);
});

it("missing_username_raises", async () => {
  const backend = new PrivateKeyBackend();
  await expect(
    backend.authenticate({ "x-deephaven-private-key": encode(Buffer.from("keydata")) }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({ "x-deephaven-private-key": encode(Buffer.from("keydata")) }),
  ).rejects.toThrow(/x-deephaven-username/);
});

it("empty_username_raises", async () => {
  const backend = new PrivateKeyBackend();
  await expect(
    backend.authenticate({
      "x-deephaven-username": "",
      "x-deephaven-private-key": encode(Buffer.from("keydata")),
    }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({
      "x-deephaven-username": "",
      "x-deephaven-private-key": encode(Buffer.from("keydata")),
    }),
  ).rejects.toThrow(/x-deephaven-username/);
});

it("invalid_base64_raises", async () => {
  const backend = new PrivateKeyBackend();
  await expect(
    backend.authenticate({
      "x-deephaven-username": "alice",
      "x-deephaven-private-key": "not base64!!",
    }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.authenticate({
      "x-deephaven-username": "alice",
      "x-deephaven-private-key": "not base64!!",
    }),
  ).rejects.toThrow(/not valid base64/);
});

it("valid_key_returns_principal", async () => {
  const backend = new PrivateKeyBackend();
  const result = await backend.authenticate({
    "x-deephaven-username": "alice",
    "x-deephaven-private-key": encode(Buffer.from("keydata")),
  });
  expect(result instanceof Principal).toBe(true);
  expect(result!.subject).toBe("alice");
  expect(result!.displayName).toBe("alice");
  expect(result!.raw).toEqual({ backend: "private_key" });
});

it("derive_credentials_decodes_key_text", async () => {
  const backend = new PrivateKeyBackend();
  const rawText = "DH key material\n";
  const principal = new Principal("alice", "alice");
  const creds = await backend.deriveCredentials(principal, {
    "x-deephaven-username": "alice",
    "x-deephaven-private-key": encode(Buffer.from(rawText, "utf-8")),
  });
  expect(creds instanceof PrivateKeyCredentials).toBe(true);
  expect((creds as PrivateKeyCredentials).keyText).toBe(rawText);
});

it("derive_credentials_rejects_non_utf8_key_bytes", async () => {
  const backend = new PrivateKeyBackend();
  const principal = new Principal("alice", "alice");
  // b"\xff\xfe\xfd" is never valid UTF-8
  await expect(
    backend.deriveCredentials(principal, {
      "x-deephaven-username": "alice",
      "x-deephaven-private-key": encode(Buffer.from([0xff, 0xfe, 0xfd])),
    }),
  ).rejects.toThrow(AuthenticationError);
  await expect(
    backend.deriveCredentials(principal, {
      "x-deephaven-username": "alice",
      "x-deephaven-private-key": encode(Buffer.from([0xff, 0xfe, 0xfd])),
    }),
  ).rejects.toThrow(/not valid UTF-8/);
});

describe("challenge", () => {
  it("mentions_expected_headers_with_default_realm", () => {
    const challenge = new PrivateKeyBackend().challenge();
    expect(challenge).toContain('realm="deephaven-mcp"');
    expect(challenge).toContain("x-deephaven-username");
    expect(challenge).toContain("x-deephaven-private-key");
  });

  it("uses_configured_realm", () => {
    const challenge = new PrivateKeyBackend({ realm: "custom-realm" }).challenge();
    expect(challenge).toContain('realm="custom-realm"');
  });
});
