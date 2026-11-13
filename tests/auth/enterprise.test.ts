import { expect, jest, mock, spyOn, test } from "bun:test";
import type {
  AuthenticatedEnterpriseClient,
  UnauthenticatedEnterpriseClient,
} from "@deephaven-enterprise/jsapi-nodejs";
import {
  loginSaml,
  type UnauthenticatedEnterpriseConnection,
} from "../../src/actions/auth/enterprise.ts";
import type { EnterpriseApi } from "../../src/actions/auth/jsapi.ts";
import { DhError } from "../../src/errors.ts";
import * as output from "../../src/output.ts";

type SamlCredentials = { type: "saml"; token: string };

/** `client` once signed in; the same object, branded as loginSaml returns it. */
function signedIn(
  client: UnauthenticatedEnterpriseClient,
): AuthenticatedEnterpriseClient {
  return client as unknown as AuthenticatedEnterpriseClient;
}

function connection(login: (credentials: SamlCredentials) => Promise<void>) {
  const call = mock(login);
  const disconnect = mock(() => {});
  const conn: UnauthenticatedEnterpriseConnection = {
    origin: "https://example.test",
    dhe: {} as EnterpriseApi,
    client: {
      login: call,
      disconnect,
    } as unknown as UnauthenticatedEnterpriseClient,
  };
  return { conn, call, disconnect };
}

test("loginSaml: a pending sign-in has no overall timeout", async () => {
  jest.useFakeTimers({ now: 0 });
  const pending = Promise.withResolvers<void>();
  try {
    const first = connection(() => pending.promise);
    const reconnect = mock(
      async (): Promise<UnauthenticatedEnterpriseConnection> => {
        throw new Error("Unexpected reconnect");
      },
    );
    let settled = false;
    const attempt = loginSaml(first.conn, reconnect, "nonce");
    const observed = attempt.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    expect(jest.getTimerCount()).toBe(0);
    jest.advanceTimersByTime(600_000);
    await Promise.resolve();
    expect(settled).toBe(false);

    pending.resolve();
    expect((await attempt).client).toBe(signedIn(first.conn.client));
    await observed;
    expect(first.call.mock.calls).toEqual([[{ type: "saml", token: "nonce" }]]);
    expect(first.disconnect).not.toHaveBeenCalled();
    expect(reconnect).not.toHaveBeenCalled();
  } finally {
    pending.resolve();
    jest.useRealTimers();
  }
});

test("loginSaml: retries server wait-limit rejections beyond five minutes", async () => {
  jest.useFakeTimers({ now: 0 });
  const notice = spyOn(output, "note").mockImplementation(() => {});
  try {
    const failures = Array.from({ length: 6 }, () =>
      connection(async () => {
        jest.advanceTimersByTime(60_000);
        throw new Error("Server wait ended");
      }),
    );
    const success = connection(async () => {});
    const [first, ...others] = failures;
    if (!first) throw new Error("Missing first connection");
    const queue = [...others, success];
    const reconnect = mock(async () => {
      const next = queue.shift();
      if (!next) throw new Error("Unexpected reconnect");
      return next.conn;
    });

    expect((await loginSaml(first.conn, reconnect, "same-nonce")).client).toBe(
      signedIn(success.conn.client),
    );
    expect(Date.now()).toBe(360_000);
    expect(reconnect).toHaveBeenCalledTimes(6);
    expect(notice).toHaveBeenCalledTimes(1);
    expect(notice).toHaveBeenCalledWith("Browser error? Interrupt and retry.");
    for (const attempt of [...failures, success]) {
      expect(attempt.call.mock.calls).toEqual([
        [{ type: "saml", token: "same-nonce" }],
      ]);
    }
    for (const attempt of failures) {
      expect(attempt.disconnect).toHaveBeenCalledTimes(1);
    }
    expect(success.disconnect).not.toHaveBeenCalled();
  } finally {
    notice.mockRestore();
    jest.useRealTimers();
  }
});

for (const [elapsed, retry] of [
  [4_999, false],
  [5_000, true],
] as const) {
  test(`loginSaml: a rejection after ${elapsed}ms ${retry ? "retries" : "fails"}`, async () => {
    jest.useFakeTimers({ now: 0 });
    const notice = spyOn(output, "note").mockImplementation(() => {});
    try {
      const first = connection(async () => {
        jest.advanceTimersByTime(elapsed);
        throw new Error("Denied");
      });
      const next = connection(async () => {});
      const reconnect = mock(async () => next.conn);
      const attempt = loginSaml(first.conn, reconnect, "nonce");

      if (retry) {
        expect((await attempt).client).toBe(signedIn(next.conn.client));
        expect(notice).toHaveBeenCalledTimes(1);
        expect(reconnect).toHaveBeenCalledTimes(1);
        expect(next.call.mock.calls).toEqual([
          [{ type: "saml", token: "nonce" }],
        ]);
      } else {
        await expect(attempt).rejects.toMatchObject({
          code: "auth_failed",
          message: "Sign-in failed: Denied",
        });
        expect(notice).not.toHaveBeenCalled();
        expect(reconnect).not.toHaveBeenCalled();
      }
      expect(first.disconnect).toHaveBeenCalledTimes(1);
    } finally {
      notice.mockRestore();
      jest.useRealTimers();
    }
  });
}

test("loginSaml: a DhError remains final even after a long hold", async () => {
  jest.useFakeTimers({ now: 0 });
  try {
    const failure = new DhError("server_unreachable", "Connection lost");
    const first = connection(async () => {
      jest.advanceTimersByTime(60_000);
      throw failure;
    });
    const reconnect = mock(
      async (): Promise<UnauthenticatedEnterpriseConnection> => {
        throw new Error("Unexpected reconnect");
      },
    );

    await expect(loginSaml(first.conn, reconnect, "nonce")).rejects.toBe(
      failure,
    );
    expect(first.disconnect).toHaveBeenCalledTimes(1);
    expect(reconnect).not.toHaveBeenCalled();
  } finally {
    jest.useRealTimers();
  }
});

test("loginSaml: a failed reconnect is reported", async () => {
  jest.useFakeTimers({ now: 0 });
  const notice = spyOn(output, "note").mockImplementation(() => {});
  try {
    const first = connection(async () => {
      jest.advanceTimersByTime(60_000);
      throw new Error("Server wait ended");
    });
    const failure = new DhError("server_unreachable", "Connection lost");
    const reconnect = mock(
      async (): Promise<UnauthenticatedEnterpriseConnection> => {
        throw failure;
      },
    );

    await expect(loginSaml(first.conn, reconnect, "nonce")).rejects.toBe(
      failure,
    );
    expect(notice).toHaveBeenCalledTimes(1);
    expect(first.disconnect).toHaveBeenCalledTimes(1);
    expect(reconnect).toHaveBeenCalledTimes(1);
  } finally {
    notice.mockRestore();
    jest.useRealTimers();
  }
});
