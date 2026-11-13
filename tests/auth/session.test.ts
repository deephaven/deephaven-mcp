import { expect, mock, spyOn, test } from "bun:test";
import * as enterprise from "../../src/actions/auth/enterprise.ts";
import {
  signInRequestFor,
  trustAndResolveServer,
} from "../../src/actions/auth/session.ts";
import { connectServer, signIn } from "../../src/actions/auth/signin.ts";
import type { Profile } from "../../src/actions/auth/store.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { runWithLifecycle } from "../../src/cli/run.ts";
import * as output from "../../src/output.ts";

const samlRequest = {
  method: "saml" as const,
  saml: { loginUrl: "https://example.test/saml", providerName: "SSO" },
  browser: false,
};

function samlConnection(
  login: () => Promise<void>,
  getUserInfo: () => Promise<{ username: string }> = async () => ({
    username: "alice",
  }),
) {
  const disconnect = mock(() => {});
  const conn = {
    origin: "https://example.test",
    dhe: {},
    client: { login, disconnect, getUserInfo },
  } as unknown as enterprise.UnauthenticatedEnterpriseConnection;
  return { conn, disconnect };
}

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

test("signIn: cancellation closes the active SSO replacement and stops retries", async () => {
  const notice = spyOn(output, "note").mockImplementation(() => {});
  let now = 0;
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  const waiting = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  const first = samlConnection(async () => {
    now += 60_000;
    throw new Error("Wait expired");
  });
  const replacement = samlConnection(() => {
    started.resolve();
    return waiting.promise;
  });
  const connect = spyOn(enterprise, "connectEnterprise")
    .mockResolvedValueOnce(first.conn)
    .mockResolvedValueOnce(replacement.conn);
  const lifecycle = new CommandLifecycle();
  try {
    const server = await connectServer(
      first.conn.origin,
      "enterprise",
      "",
      lifecycle,
    );
    const attempt = runWithLifecycle(lifecycle, () =>
      signIn(server, samlRequest, "auth_failed", lifecycle),
    );
    await started.promise;
    expect(connect).toHaveBeenCalledTimes(2);
    lifecycle.receiveSignal("SIGINT");
    await expect(attempt).rejects.toMatchObject({ code: "cancelled" });
    expect(replacement.disconnect).toHaveBeenCalledTimes(1);
    now += 60_000;
    waiting.reject(new Error("Connection closed"));
    await Bun.sleep(0);
    expect(connect).toHaveBeenCalledTimes(2);
  } finally {
    notice.mockRestore();
    clock.mockRestore();
    connect.mockRestore();
  }
});

test("signIn: cancellation closes the SSO replacement while reading the user", async () => {
  const notice = spyOn(output, "note").mockImplementation(() => {});
  let now = 0;
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  const pending = Promise.withResolvers<{ username: string }>();
  const started = Promise.withResolvers<void>();
  const first = samlConnection(async () => {
    now += 60_000;
    throw new Error("Wait expired");
  });
  const replacement = samlConnection(
    async () => {},
    () => {
      started.resolve();
      return pending.promise;
    },
  );
  const connect = spyOn(enterprise, "connectEnterprise")
    .mockResolvedValueOnce(first.conn)
    .mockResolvedValueOnce(replacement.conn);
  const lifecycle = new CommandLifecycle();
  try {
    const server = await connectServer(
      first.conn.origin,
      "enterprise",
      "",
      lifecycle,
    );
    const attempt = runWithLifecycle(lifecycle, () =>
      signIn(server, samlRequest, "auth_failed", lifecycle),
    );
    await started.promise;
    lifecycle.receiveSignal("SIGINT");
    await expect(attempt).rejects.toMatchObject({ code: "cancelled" });
    expect(replacement.disconnect).toHaveBeenCalledTimes(1);
    pending.resolve({ username: "alice" });
  } finally {
    notice.mockRestore();
    clock.mockRestore();
    connect.mockRestore();
  }
});

test("signIn: late SSO replacements close when cancellation or cleanup prevents registration", async () => {
  const notice = spyOn(output, "note").mockImplementation(() => {});
  try {
    for (const abort of [true, false]) {
      let now = 0;
      const clock = spyOn(Date, "now").mockImplementation(() => now);
      const first = samlConnection(async () => {
        now += 60_000;
        throw new Error("Wait expired");
      });
      const replacement = samlConnection(async () => {});
      const pending =
        Promise.withResolvers<enterprise.UnauthenticatedEnterpriseConnection>();
      const connecting = Promise.withResolvers<void>();
      const connect = spyOn(enterprise, "connectEnterprise")
        .mockResolvedValueOnce(first.conn)
        .mockImplementationOnce(() => {
          connecting.resolve();
          return pending.promise;
        });
      const lifecycle = new CommandLifecycle();
      try {
        const server = await connectServer(
          first.conn.origin,
          "enterprise",
          "",
          lifecycle,
        );
        const attempt = signIn(server, samlRequest, "auth_failed", lifecycle);
        await connecting.promise;
        if (abort) lifecycle.receiveSignal("SIGINT");
        await lifecycle.cleanup();
        pending.resolve(replacement.conn);
        if (abort) {
          await expect(attempt).rejects.toMatchObject({ code: "cancelled" });
        } else {
          await expect(attempt).rejects.toThrow(
            "Cannot register cleanup after cleanup has started",
          );
        }
        expect(replacement.disconnect).toHaveBeenCalledTimes(1);
      } finally {
        clock.mockRestore();
        connect.mockRestore();
      }
    }
  } finally {
    notice.mockRestore();
  }
});

test("signIn: successful SSO replacements remain usable with and without a lifecycle", async () => {
  const notice = spyOn(output, "note").mockImplementation(() => {});
  try {
    for (const useLifecycle of [true, false]) {
      let now = 0;
      const clock = spyOn(Date, "now").mockImplementation(() => now);
      const first = samlConnection(async () => {
        now += 60_000;
        throw new Error("Wait expired");
      });
      const intermediate = samlConnection(async () => {
        now += 60_000;
        throw new Error("Wait expired again");
      });
      const replacement = samlConnection(async () => {});
      const connect = spyOn(enterprise, "connectEnterprise")
        .mockResolvedValueOnce(first.conn)
        .mockResolvedValueOnce(intermediate.conn)
        .mockResolvedValueOnce(replacement.conn);
      const lifecycle = useLifecycle ? new CommandLifecycle() : undefined;
      try {
        const server = await connectServer(
          first.conn.origin,
          "enterprise",
          "",
          lifecycle,
        );
        const session = await signIn(
          server,
          samlRequest,
          "auth_failed",
          lifecycle,
        );
        expect(session.user).toBe("alice");
        expect(
          Object.is(session.enterprise?.client, replacement.conn.client),
        ).toBe(true);
        expect(intermediate.disconnect).toHaveBeenCalledTimes(1);
        expect(replacement.disconnect).not.toHaveBeenCalled();
        await lifecycle?.cleanup();
        expect(intermediate.disconnect).toHaveBeenCalledTimes(
          useLifecycle ? 2 : 1,
        );
        expect(replacement.disconnect).toHaveBeenCalledTimes(
          useLifecycle ? 1 : 0,
        );
      } finally {
        clock.mockRestore();
        connect.mockRestore();
      }
    }
  } finally {
    notice.mockRestore();
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

test("trustAndResolveServer: interrupted discovery reports only the safe address", async () => {
  const started = Promise.withResolvers<void>();
  const requestAborted = Promise.withResolvers<void>();
  let server: ReturnType<typeof Bun.serve> | undefined;
  const lifecycle = new CommandLifecycle();
  // Disable discovery's probe timeout so only the lifecycle signal can abort
  // the request.
  const timeout = spyOn(AbortSignal, "timeout").mockImplementation(
    () => new AbortController().signal,
  );
  try {
    server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (req) => {
        req.signal.addEventListener("abort", () => requestAborted.resolve(), {
          once: true,
        });
        started.resolve();
        return new Promise<Response>(() => {});
      },
    });
    const { port } = server;
    const attempt = runWithLifecycle(lifecycle, () =>
      trustAndResolveServer(
        `http://alice:password@127.0.0.1:${port}/path?token=secret#fragment`,
        {},
        undefined,
        [],
        lifecycle,
      ),
    );
    await started.promise;
    lifecycle.receiveSignal("SIGINT");
    await expect(attempt).rejects.toMatchObject({ code: "cancelled" });
    // The rejection alone comes from runWithLifecycle. With the probe timeout
    // disabled, only the lifecycle signal can abort the request; if it is not
    // wired through, this never resolves and the test times out.
    await requestAborted.promise;
    expect(lifecycle.failureReport).toMatchObject({
      target: `http://127.0.0.1:${port}`,
      human: {
        summary: `Cancelled while finding the server at http://127.0.0.1:${port}.`,
      },
    });
    expect(JSON.stringify(lifecycle.failureReport)).not.toMatch(
      /alice|password|token|secret|fragment|path/,
    );
  } finally {
    timeout.mockRestore();
    await server?.stop(true);
  }
});

test("signInFor: a saved key logs in with its key type, not the credential's tag", async () => {
  const profile: Profile & { user: string } = {
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
