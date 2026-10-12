import { expect, test } from "bun:test";
import { generateBase64KeyPair } from "@deephaven-enterprise/auth-nodejs";
import { parseKeyFile } from "../../src/auth/keyfile.ts";
import { DhError } from "../../src/errors.ts";

const withEc = (key: string) =>
  Buffer.concat([Buffer.from("EC:"), Buffer.from(key, "base64")]).toString(
    "base64",
  );

test("parseKeyFile: legacy EC key file", () => {
  const kp = generateBase64KeyPair();
  const parsed = parseKeyFile(
    `user user-a\noperateas user-b\npublic ${withEc(kp.publicKey)}\nprivate ${withEc(
      kp.privateKey,
    )}\n`,
  );
  expect(parsed.user).toBe("user-a");
  expect(parsed.operateAs).toBe("user-b");
  expect(parsed.keyPair).toEqual({
    type: "ec",
    publicKey: kp.publicKey,
    privateKey: kp.privateKey,
  });
});

test("parseKeyFile: DSA keys (no EC: prefix) need a new login", () => {
  expect(() => parseKeyFile("user u\npublic AAAA\nprivate AAAA\n")).toThrow(
    expect.objectContaining({ code: "credential_unavailable" }),
  );
});

test("parseKeyFile: missing lines", () => {
  expect(() => parseKeyFile("user u\n")).toThrow(DhError);
});
