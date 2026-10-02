// Key generation, signing and the SAML nonce, using auth-nodejs under Deno.
// Usage: deno task crypto
import { createPublicKey, verify } from "node:crypto";
import { Buffer } from "node:buffer";
import {
  generateBase64KeyPair,
  keyWithSentinel,
  signWithPrivateKey,
} from "@deephaven-enterprise/auth-nodejs";
import { check, done, runtime } from "./report.ts";

console.log(`crypto (${runtime})`);

const keyPair = await check(
  "generateBase64KeyPair",
  generateBase64KeyPair,
  (kp) => `type=${kp.type}`,
);

if (keyPair) {
  await check("key type is EC P-256", () => {
    const key = createPublicKey({
      key: Buffer.from(keyPair.publicKey, "base64"),
      format: "der",
      type: "spki",
    });
    const { namedCurve } = key.asymmetricKeyDetails ?? {};
    if (key.asymmetricKeyType !== "ec" || namedCurve !== "prime256v1") {
      throw new Error(`${key.asymmetricKeyType} ${namedCurve}`);
    }
    return `${key.asymmetricKeyType} ${namedCurve}`;
  });

  await check("sign nonce + verify (SHA256withECDSA, DER)", () => {
    const nonce = Buffer.from(crypto.getRandomValues(new Uint8Array(32)))
      .toString("base64");
    // deno-lint-ignore no-explicit-any
    const sig = signWithPrivateKey(nonce as any, keyPair.privateKey);
    const ok = verify(
      "sha256",
      Buffer.from(nonce, "base64"),
      {
        key: Buffer.from(keyPair.publicKey, "base64"),
        format: "der",
        type: "spki",
      },
      Buffer.from(sig, "base64"),
    );
    if (!ok) throw new Error("signature did not verify");
    // Java's SHA256withECDSA expects ASN.1 DER: SEQUENCE (0x30) of two INTEGERs.
    const first = Buffer.from(sig, "base64")[0];
    if (first !== 0x30) throw new Error(`not DER (first byte ${first})`);
    return "verified, DER-encoded";
  });

  await check("keyWithSentinel prefixes EC:", () => {
    const decoded = Buffer.from(
      keyWithSentinel(keyPair.type, keyPair.publicKey),
      "base64",
    );
    const prefix = decoded.subarray(0, 3).toString();
    if (prefix !== "EC:") throw new Error(prefix);
    return prefix;
  });
}

await check("SAML nonce from CSPRNG (96 bytes, URL-safe once encoded)", () => {
  const key = Buffer.from(crypto.getRandomValues(new Uint8Array(96)))
    .toString("base64");
  const url = new URL("https://example.invalid/saml/dologin");
  url.searchParams.set("key", key);
  const roundTrip = new URL(url.href).searchParams.get("key");
  if (roundTrip !== key) throw new Error("key did not round-trip");
  return `${key.length} chars, round-trips through URLSearchParams`;
});

done();
