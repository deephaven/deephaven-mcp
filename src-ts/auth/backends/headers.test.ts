/**
 * Tests for auth/backends/headers module.
 *
 * This module contains only string constants. The tests below assert the
 * exact wire values (because any change to these strings is a backwards-
 * incompatible protocol change) and check that all values are lowercase
 * (because the middleware lowercases incoming header names before
 * dispatching to backends).
 */
import { it, expect } from "vitest";
import {
  HEADER_USERNAME,
  HEADER_PASSWORD,
  HEADER_EFFECTIVE_USER,
  HEADER_PRIVATE_KEY,
  HEADER_PSK,
} from "./headers.js";
import * as headers from "./headers.js";
import * as backendsIndex from "./index.js";

it("header_username_value", () => {
  expect(HEADER_USERNAME).toBe("x-deephaven-username");
});

it("header_password_value", () => {
  expect(HEADER_PASSWORD).toBe("x-deephaven-password");
});

it("header_effective_user_value", () => {
  expect(HEADER_EFFECTIVE_USER).toBe("x-deephaven-effective-user");
});

it("header_private_key_value", () => {
  expect(HEADER_PRIVATE_KEY).toBe("x-deephaven-private-key");
});

it("header_psk_value", () => {
  expect(HEADER_PSK).toBe("x-deephaven-psk");
});

it("all_header_values_are_lowercase", () => {
  const allHeaders = [HEADER_USERNAME, HEADER_PASSWORD, HEADER_EFFECTIVE_USER, HEADER_PRIVATE_KEY, HEADER_PSK];
  for (const v of allHeaders) {
    expect(v).toBe(v.toLowerCase());
  }
});

it("all_header_values_use_x_deephaven_prefix", () => {
  const allHeaders = [HEADER_USERNAME, HEADER_PASSWORD, HEADER_EFFECTIVE_USER, HEADER_PRIVATE_KEY, HEADER_PSK];
  for (const v of allHeaders) {
    expect(v.startsWith("x-deephaven-")).toBe(true);
  }
});

it("all_header_values_are_unique", () => {
  const allHeaders = [HEADER_USERNAME, HEADER_PASSWORD, HEADER_EFFECTIVE_USER, HEADER_PRIVATE_KEY, HEADER_PSK];
  expect(allHeaders.length).toBe(new Set(allHeaders).size);
});

it("constants_are_re_exported_from_backends_index", () => {
  expect(backendsIndex.HEADER_USERNAME).toBe(headers.HEADER_USERNAME);
  expect(backendsIndex.HEADER_PASSWORD).toBe(headers.HEADER_PASSWORD);
  expect(backendsIndex.HEADER_EFFECTIVE_USER).toBe(headers.HEADER_EFFECTIVE_USER);
  expect(backendsIndex.HEADER_PRIVATE_KEY).toBe(headers.HEADER_PRIVATE_KEY);
  expect(backendsIndex.HEADER_PSK).toBe(headers.HEADER_PSK);
});
