/**
 * Smoke tests for auth/middleware public re-export surface.
 *
 * Verifies that all symbols in the index are importable and have the
 * expected identity (re-exports are the canonical objects, not shadows).
 */
import { it, expect } from "vitest";
import {
  SCOPE_KEY_PRINCIPAL,
  SCOPE_KEY_CREDENTIALS,
  AuthenticationMiddleware,
  TlsEnforcementMiddleware,
  parseForwardedAllowIps,
  createTransportSecurityPolicy,
} from "./index.js";
import {
  SCOPE_KEY_PRINCIPAL as CanonicalScopeKeyPrincipal,
  SCOPE_KEY_CREDENTIALS as CanonicalScopeKeyCredentials,
  AuthenticationMiddleware as CanonicalAuthenticationMiddleware,
} from "./middleware.js";
import {
  TlsEnforcementMiddleware as CanonicalTlsEnforcementMiddleware,
  parseForwardedAllowIps as canonicalParseForwardedAllowIps,
  createTransportSecurityPolicy as canonicalCreatePolicy,
} from "./tls.js";

it("authentication_middleware_is_canonical", () => {
  expect(AuthenticationMiddleware).toBe(CanonicalAuthenticationMiddleware);
});

it("scope_key_principal_is_canonical", () => {
  expect(SCOPE_KEY_PRINCIPAL).toBe(CanonicalScopeKeyPrincipal);
});

it("scope_key_credentials_is_canonical", () => {
  expect(SCOPE_KEY_CREDENTIALS).toBe(CanonicalScopeKeyCredentials);
});

it("tls_enforcement_middleware_is_canonical", () => {
  expect(TlsEnforcementMiddleware).toBe(CanonicalTlsEnforcementMiddleware);
});

it("parse_forwarded_allow_ips_is_canonical", () => {
  expect(parseForwardedAllowIps).toBe(canonicalParseForwardedAllowIps);
});

it("create_transport_security_policy_is_canonical", () => {
  expect(createTransportSecurityPolicy).toBe(canonicalCreatePolicy);
});

it("all_surface_importable", () => {
  expect(SCOPE_KEY_PRINCIPAL).toBeDefined();
  expect(SCOPE_KEY_CREDENTIALS).toBeDefined();
  expect(AuthenticationMiddleware).toBeDefined();
  expect(TlsEnforcementMiddleware).toBeDefined();
  expect(parseForwardedAllowIps).toBeDefined();
  expect(createTransportSecurityPolicy).toBeDefined();
});
