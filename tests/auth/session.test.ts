import { expect, test } from "bun:test";
import { signInRequestFor } from "../../src/auth/session.ts";
import type { Profile } from "../../src/auth/store.ts";

test("signInFor: a saved key logs in with its key type, not the credential's tag", async () => {
  const profile: Profile = {
    name: "dhe.example.com:alice",
    server: "https://dhe.example.com",
    user: "alice",
    method: "saml",
    createdAt: "",
  };
  const spec = await signInRequestFor(profile, "enterprise", {
    type: "keyPair",
    keyType: "ec",
    publicKey: "PUB",
    privateKey: "PRIV",
    generated: true,
  });
  expect(spec).toMatchObject({
    method: "private-key",
    username: "alice",
    keyPair: { type: "ec", publicKey: "PUB", privateKey: "PRIV" },
  });
});
