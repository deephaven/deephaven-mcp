import { expect, mock, test } from "bun:test";
import {
  type CommunityConnection,
  loginCommunity,
} from "../../src/actions/auth/community.ts";
import { DhError } from "../../src/errors.ts";

function connection(login: (fail: (detail: unknown) => void) => Promise<void>) {
  let listener: ((event: { detail: unknown }) => void) | undefined;
  const remove = mock(() => {
    listener = undefined;
  });
  const ideConnection = {
    addEventListener: mock((type: string, callback: typeof listener) => {
      expect(type).toBe("requestfailed");
      listener = callback;
      return remove;
    }),
  };
  // Only the members loginCommunity uses are mocked.
  const conn = {
    origin: "http://localhost:10000",
    dh: { CoreClient: { LOGIN_TYPE_ANONYMOUS: "anonymous" } },
    handlers: [],
    client: {
      getAsIdeConnection: async () => ideConnection,
      login: () => login((detail) => listener?.({ detail })),
      disconnect: mock(() => listener?.({ detail: "Cleanup disconnect" })),
    },
  } as unknown as CommunityConnection;
  return { conn, remove };
}

test("Community login captures requestfailed after an empty disconnect rejection", async () => {
  const { conn, remove } = connection(
    (fail) =>
      new Promise((_, reject) => {
        reject({ type: "disconnect", detail: null });
        fail("Failed to connect: Error: Authentication details invalid");
        fail("Failed to connect: Error: UNAUTHENTICATED");
      }),
  );
  await expect(
    loginCommunity(
      conn,
      { method: "psk", secret: "test" },
      100,
      "auth_expired",
    ),
  ).rejects.toMatchObject({
    code: "auth_expired",
    detail: "Failed to connect: Error: Authentication details invalid",
  });
  expect(conn.client.disconnect).toHaveBeenCalledTimes(1);
  expect(remove).toHaveBeenCalledTimes(1);
});

test("Community login explains an empty disconnect without stringifying the event", async () => {
  const { conn, remove } = connection(async () => {
    throw { type: "disconnect", detail: null, toString: () => "Event@4" };
  });
  await expect(
    loginCommunity(conn, { method: "anonymous" }, 100, "auth_failed"),
  ).rejects.toMatchObject({
    code: "auth_failed",
    detail:
      "Connection closed during sign-in; the client supplied no failure reason.",
  });
  expect(remove).toHaveBeenCalledTimes(1);
});

test("Community login preserves direct reasons and typed failures", async () => {
  for (const failure of [
    new Error("Invalid token"),
    new DhError("server_unreachable", "Timed out"),
  ]) {
    const { conn, remove } = connection(async () => {
      throw failure;
    });
    const attempt = loginCommunity(
      conn,
      { method: "anonymous" },
      100,
      "auth_failed",
    );
    if (failure instanceof DhError) await expect(attempt).rejects.toBe(failure);
    else
      await expect(attempt).rejects.toMatchObject({ detail: "Invalid token" });
    expect(conn.client.disconnect).toHaveBeenCalledTimes(1);
    expect(remove).toHaveBeenCalledTimes(1);
  }
});

test("Community login removes its diagnostic listener on success and timeout", async () => {
  for (const timeout of [false, true]) {
    const { conn, remove } = connection(async () => {
      if (timeout) await new Promise(() => {});
    });
    const attempt = loginCommunity(
      conn,
      { method: "anonymous" },
      5,
      "auth_failed",
    );
    if (timeout) {
      await expect(attempt).rejects.toMatchObject({
        code: "server_unreachable",
      });
      expect(conn.client.disconnect).toHaveBeenCalledTimes(1);
    } else {
      await attempt;
      expect(conn.client.disconnect).not.toHaveBeenCalled();
    }
    expect(remove).toHaveBeenCalledTimes(1);
  }
});
