import { afterEach, beforeEach, expect, mock, spyOn, test } from "bun:test";
import type { dh } from "@deephaven/jsapi-types";
import Log, { LoggerLevel } from "@deephaven/log";
import type { QueryInfo } from "@deephaven-enterprise/jsapi-types";
import * as jsapi from "../../src/actions/auth/jsapi.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { connectCorePlusWorker } from "../../src/actions/console/core.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";

// These isolated tests mock the loader that normally configures library logging.
const loggers = [
  Log.module("EnterpriseCorePlusManager"),
  Log.module("EnterpriseCorePlusManagerCache"),
  Log.module("createAuthenticatedCoreClient"),
];
let levels: number[];
beforeEach(() => {
  const silent = LoggerLevel.SILENT;
  if (silent === undefined) throw new Error("Missing vendor SILENT log level");
  levels = loggers.map((logger) => logger.level);
  for (const logger of loggers) logger.setLogLevel(silent);
});
afterEach(() => {
  loggers.forEach((logger, index) => {
    const level = levels[index];
    if (level === undefined) throw new Error("Missing original log level");
    logger.setLogLevel(level);
  });
});

test("Core+ worker uses vendor token and proxy-aware connection, then disposes it", async () => {
  const login = mock(async () => {});
  const disconnect = mock(() => {});
  const close = mock(() => {});
  const connection = { addEventListener: mock(() => {}), close };
  const getAsIdeConnection = mock(async () => connection);
  const coreClients: { url: string; options: dh.ConnectOptions | undefined }[] =
    [];
  class CoreClient {
    constructor(url: string, options?: dh.ConnectOptions) {
      coreClients.push({ url, options });
    }
    login = login;
    disconnect = disconnect;
    getAsIdeConnection = getAsIdeConnection;
  }
  const api = {
    CoreClient,
    IdeConnection: { EVENT_DISCONNECT: "disconnect" },
  } as unknown as typeof dh;
  const load = spyOn(jsapi, "loadCorePlusApi").mockResolvedValue(api);
  const query = {
    name: "IC - dh CLI - abc123",
    serial: "id",
    workerKind: "python",
    designated: {
      status: "Running",
      grpcUrl: "https://worker.example.com",
      envoyPrefix: "/worker",
      jsApiUrl: "https://worker.example.com/proxy/jsapi/dh-core.js",
    },
  } as unknown as QueryInfo;
  const addEventListener = mock(() => {});
  const removeEventListener = mock(() => {});
  const createAuthToken = mock(async () => "worker-token");
  const session = {
    cacheRoot: "/test-cache",
    enterprise: {
      client: {
        getKnownConfigs: () => [query],
        getServerConfigValues: async () => ({
          workerKinds: [{ name: "python", protocols: ["Community"] }],
        }),
        createAuthToken,
        addEventListener,
        removeEventListener,
      },
      dhe: {
        Client: {
          EVENT_CONFIG_ADDED: "added",
          EVENT_CONFIG_UPDATED: "updated",
          EVENT_CONFIG_REMOVED: "removed",
          EVENT_REQUEST_FAILED: "failed",
          EVENT_RECONNECT_AUTH_FAILED: "auth-failed",
        },
      },
    },
  } as unknown as Session;
  const lifecycle = new CommandLifecycle();
  try {
    const connected = await connectCorePlusWorker(
      session,
      query,
      lifecycle,
      lifecycle.signal,
    );
    expect(connected.close).toBe(close);
    expect(load).toHaveBeenCalledWith(
      "https://worker.example.com/proxy/jsapi/dh-core.js",
      "/test-cache",
      expect.any(Number),
    );
    expect(createAuthToken).toHaveBeenCalledWith("RemoteQueryProcessor");
    expect(login).toHaveBeenCalledWith({
      type: "io.deephaven.proto.auth.Token",
      token: "worker-token",
    });

    expect(coreClients[0]).toMatchObject({
      url: "https://worker.example.com",
      options: { headers: { "envoy-prefix": "/worker" } },
    });

    expect((await lifecycle.cleanup()).failures).toEqual([]);
    expect(disconnect).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledTimes(1);
    expect(removeEventListener).toHaveBeenCalledTimes(3);
  } finally {
    load.mockRestore();
  }
});

test("Core+ worker does not create a client when its token arrives after cancellation", async () => {
  const token = Promise.withResolvers<string>();
  const requested = Promise.withResolvers<void>();
  const construct = mock(() => {});
  class CoreClient {
    constructor() {
      construct();
    }
  }
  const api = { CoreClient } as unknown as typeof dh;
  const load = spyOn(jsapi, "loadCorePlusApi").mockResolvedValue(api);
  const query = {
    name: "IC - dh CLI - abc123",
    serial: "id",
    workerKind: "python",
    designated: {
      status: "Running",
      grpcUrl: "https://worker.example.com",
      jsApiUrl: "https://worker.example.com/jsapi/dh-core.js",
    },
  } as unknown as QueryInfo;
  const session = {
    cacheRoot: "/test-cache",
    enterprise: {
      client: {
        getKnownConfigs: () => [query],
        getServerConfigValues: async () => ({
          workerKinds: [{ name: "python", protocols: ["Community"] }],
        }),
        createAuthToken: () => {
          requested.resolve();
          return token.promise;
        },
        addEventListener: () => {},
        removeEventListener: () => {},
      },
      dhe: {
        Client: {
          EVENT_CONFIG_ADDED: "added",
          EVENT_CONFIG_UPDATED: "updated",
          EVENT_CONFIG_REMOVED: "removed",
        },
      },
    },
  } as unknown as Session;
  const lifecycle = new CommandLifecycle();
  try {
    const opening = connectCorePlusWorker(
      session,
      query,
      lifecycle,
      lifecycle.signal,
    );
    await requested.promise;
    lifecycle.abort(new Error("cancelled"));
    const cleanup = lifecycle.cleanup();
    token.resolve("worker-token");
    await expect(opening).rejects.toThrow("cancelled");
    expect((await cleanup).failures).toEqual([]);
    expect(construct).not.toHaveBeenCalled();
  } finally {
    token.resolve("worker-token");
    load.mockRestore();
  }
});
