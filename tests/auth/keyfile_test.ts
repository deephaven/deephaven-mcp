import { assertEquals, assertThrows } from "@std/assert";
import { decodeBase64, encodeBase64 } from "@std/encoding/base64";
import { generateBase64KeyPair } from "@deephaven-enterprise/auth-nodejs";
import { DhError } from "../../src/auth/errors.ts";
import { parseKeyFile } from "../../src/auth/keyfile.ts";

const withEc = (key: string) =>
  encodeBase64(
    new Uint8Array([...new TextEncoder().encode("EC:"), ...decodeBase64(key)]),
  );

Deno.test("parseKeyFile: legacy EC key file", () => {
  const kp = generateBase64KeyPair();
  const parsed = parseKeyFile(
    `user user-a\noperateas user-b\npublic ${withEc(kp.publicKey)}\nprivate ${
      withEc(kp.privateKey)
    }\n`,
  );
  assertEquals(parsed.user, "user-a");
  assertEquals(parsed.operateAs, "user-b");
  assertEquals(parsed.keyPair, {
    type: "ec",
    publicKey: kp.publicKey,
    privateKey: kp.privateKey,
  });
});

Deno.test("parseKeyFile: DSA keys (no EC: prefix) need a new login", () => {
  const e = assertThrows(
    () => parseKeyFile("user u\npublic AAAA\nprivate AAAA\n"),
    DhError,
  );
  assertEquals(e.code, "credential_unavailable");
});

Deno.test("parseKeyFile: missing lines", () => {
  assertThrows(() => parseKeyFile("user u\n"), DhError);
});
