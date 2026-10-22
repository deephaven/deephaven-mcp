import { expect, test } from "bun:test";
import {
  signInRequestFor,
  trustAndResolveServer,
} from "../../src/auth/session.ts";
import type { Profile } from "../../src/auth/store.ts";

test("trustAndResolveServer: a saved origin is matched exactly, not in discovery order", async () => {
  const servers = {
    "http://localhost": { kind: "community" as const },
    "http://localhost:10000": { kind: "community" as const },
  };
  expect(
    (await trustAndResolveServer("http://localhost", servers, undefined, []))
      .origin,
  ).toBe("http://localhost");
  expect(
    (await trustAndResolveServer("localhost", servers, undefined, [])).origin,
  ).toBe("http://localhost:10000");
});

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
