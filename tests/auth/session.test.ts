import { expect, mock, spyOn, test } from "bun:test";
import * as enterprise from "../../src/actions/auth/enterprise.ts";
import {
  signInRequestFor,
  trustAndResolveServer,
} from "../../src/actions/auth/session.ts";
import { connectServer } from "../../src/actions/auth/signin.ts";
import type { Profile } from "../../src/actions/auth/store.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";

test("connectServer: acquired connections belong to lifecycle cleanup", async () => {
  const disconnect = mock(() => {});
  // Only the members connectServer uses are mocked.
  const conn = {
    origin: "https://example.test",
    dhe: {},
    client: { disconnect },
  } as unknown as enterprise.UnauthenticatedEnterpriseConnection;
  const connect = spyOn(enterprise, "connectEnterprise").mockResolvedValue(
    conn,
  );
  try {
    const lifecycle = new CommandLifecycle();
    await connectServer(conn.origin, "enterprise", "", lifecycle);
    expect(disconnect).not.toHaveBeenCalled();
    await lifecycle.cleanup();
    expect(disconnect).toHaveBeenCalledTimes(1);
    await connectServer(conn.origin, "enterprise", "");
    expect(disconnect).toHaveBeenCalledTimes(1);
  } finally {
    connect.mockRestore();
  }
});

test("connectServer: late connections are closed on abort or cleanup registration failure", async () => {
  for (const abort of [true, false]) {
    const lifecycle = new CommandLifecycle();
    const disconnect = mock(() => {});
    const pending =
      Promise.withResolvers<enterprise.UnauthenticatedEnterpriseConnection>();
    const started = Promise.withResolvers<void>();
    const connect = spyOn(enterprise, "connectEnterprise").mockImplementation(
      () => {
        started.resolve();
        return pending.promise;
      },
    );
    try {
      const attempt = connectServer(
        "https://example.test",
        "enterprise",
        "",
        lifecycle,
      );
      await started.promise;
      if (abort) lifecycle.receiveSignal("SIGINT");
      await lifecycle.cleanup();
      pending.resolve({
        origin: "https://example.test",
        dhe: {},
        client: { disconnect },
      } as unknown as enterprise.UnauthenticatedEnterpriseConnection);
      if (abort) {
        await expect(attempt).rejects.toMatchObject({ code: "cancelled" });
      } else {
        await expect(attempt).rejects.toThrow(
          "Cannot register cleanup after cleanup has started",
        );
      }
      expect(disconnect).toHaveBeenCalledTimes(1);
    } finally {
      connect.mockRestore();
    }
  }
});

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
