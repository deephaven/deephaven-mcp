/**
 * Smoke tests for auth/backends public re-export surface.
 *
 * Verifies that all symbols declared in the index are importable and
 * have the expected identity (re-exports are the canonical objects, not shadows).
 * Behavior of each symbol is tested in its own dedicated test module.
 */
import { it, expect } from "vitest";
import {
  HEADER_EFFECTIVE_USER,
  HEADER_PASSWORD,
  HEADER_PRIVATE_KEY,
  HEADER_PSK,
  HEADER_USERNAME,
  AuthBackend,
  AuthenticationError,
  PasswordBackend,
  PrivateKeyBackend,
  PSKBackend,
  authenticateAndResolve,
} from "./index.js";
import { AuthBackend as CanonicalAuthBackend, AuthenticationError as CanonicalAuthenticationError } from "./base.js";
import { HEADER_EFFECTIVE_USER as CANON_EFFECTIVE_USER, HEADER_PASSWORD as CANON_PASSWORD, HEADER_PRIVATE_KEY as CANON_PRIVATE_KEY, HEADER_PSK as CANON_PSK, HEADER_USERNAME as CANON_USERNAME } from "./headers.js";
import { PasswordBackend as CanonicalPassword } from "./password.js";
import { PrivateKeyBackend as CanonicalPrivateKey } from "./private-key.js";
import { PSKBackend as CanonicalPSK } from "./psk.js";
import { authenticateAndResolve as canonicalResolve } from "./resolve.js";

it("auth_backend_is_canonical", () => {
  expect(AuthBackend).toBe(CanonicalAuthBackend);
});

it("authentication_error_is_canonical", () => {
  expect(AuthenticationError).toBe(CanonicalAuthenticationError);
});

it("psk_backend_is_canonical", () => {
  expect(PSKBackend).toBe(CanonicalPSK);
  // issubclass equivalent: prototype chain
  expect(Object.getPrototypeOf(PSKBackend.prototype) instanceof AuthBackend).toBe(false);
  expect(new PSKBackend("x") instanceof AuthBackend).toBe(true);
});

it("password_backend_is_canonical", () => {
  expect(PasswordBackend).toBe(CanonicalPassword);
  expect(new PasswordBackend() instanceof AuthBackend).toBe(true);
});

it("private_key_backend_is_canonical", () => {
  expect(PrivateKeyBackend).toBe(CanonicalPrivateKey);
  expect(new PrivateKeyBackend() instanceof AuthBackend).toBe(true);
});

it("authenticate_and_resolve_is_canonical", () => {
  expect(authenticateAndResolve).toBe(canonicalResolve);
});

it("header_constants_are_canonical", () => {
  expect(HEADER_EFFECTIVE_USER).toBe(CANON_EFFECTIVE_USER);
  expect(HEADER_PASSWORD).toBe(CANON_PASSWORD);
  expect(HEADER_PRIVATE_KEY).toBe(CANON_PRIVATE_KEY);
  expect(HEADER_PSK).toBe(CANON_PSK);
  expect(HEADER_USERNAME).toBe(CANON_USERNAME);
});

it("all_surface_importable", () => {
  // Verify all expected exports exist and are defined
  expect(AuthBackend).toBeDefined();
  expect(AuthenticationError).toBeDefined();
  expect(HEADER_EFFECTIVE_USER).toBeDefined();
  expect(HEADER_PASSWORD).toBeDefined();
  expect(HEADER_PRIVATE_KEY).toBeDefined();
  expect(HEADER_PSK).toBeDefined();
  expect(HEADER_USERNAME).toBeDefined();
  expect(PasswordBackend).toBeDefined();
  expect(PSKBackend).toBeDefined();
  expect(PrivateKeyBackend).toBeDefined();
  expect(authenticateAndResolve).toBeDefined();
});
