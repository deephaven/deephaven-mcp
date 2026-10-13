import { expect, test } from "bun:test";
import { assertTrusted } from "../../src/auth/jsapi.ts";

test("assertTrusted: https anywhere, http only to this machine, nothing else", () => {
  expect(() => assertTrusted(new URL("https://dh.example.com"))).not.toThrow();
  expect(() => assertTrusted(new URL("http://localhost:10000"))).not.toThrow();
  expect(() => assertTrusted(new URL("http://dh.example.com"))).toThrow();
  expect(() => assertTrusted(new URL("ftp://localhost/dh-core.js"))).toThrow();
});
