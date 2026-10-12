import { expect, test } from "bun:test";
import { specForProfile } from "../../src/auth/session.ts";
import type { Profile } from "../../src/auth/store.ts";

test("specForProfile: a saved key logs in with its key type, not the credential's tag", async () => {
  const profile: Profile = {
    name: "dhe.example.com:alice",
    server: "https://dhe.example.com",
    user: "alice",
    method: "saml",
    createdAt: "",
  };
  const spec = await specForProfile(profile, "enterprise", {
    type: "keyPair",
    keyType: "ec",
    publicKey: "PUB",
    privateKey: "PRIV",
    generated: true,
  });
  expect(spec).toMatchObject({
    kind: "enterprise",
    username: "alice",
    keyPair: { type: "ec", publicKey: "PUB", privateKey: "PRIV" },
  });
});
