import type { dh } from "@deephaven/jsapi-types";
import {
  createJsApiFactories,
  EnterpriseCorePlusManager,
  getCoreConnection,
} from "@deephaven-enterprise/jsapi-nodejs";
import type {
  EnterpriseDhType,
  QueryInfo,
} from "@deephaven-enterprise/jsapi-types";
import { loadEnterpriseApi } from "../../src/actions/auth/jsapi.ts";

const server = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  fetch: () => new Response("globalThis.iris = {};"),
});
try {
  const cache = process.argv[2];
  if (!cache) throw new Error("Cache path required");
  await loadEnterpriseApi(
    new URL(`http://127.0.0.1:${server.port}`),
    cache,
    5000,
  );
  const connection = { addEventListener() {}, close() {} };
  class CoreClient {
    async login() {}
    async getAsIdeConnection() {
      return connection;
    }
    disconnect() {}
  }
  const api = {
    CoreClient,
    IdeConnection: { EVENT_DISCONNECT: "disconnect" },
  } as unknown as typeof dh;
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
  const client = {
    getKnownConfigs: () => [query],
    createAuthToken: async () => "worker-token",
    addEventListener() {},
    removeEventListener() {},
  } as unknown as ConstructorParameters<typeof EnterpriseCorePlusManager>[1];
  const dhe = {
    Client: {
      EVENT_CONFIG_ADDED: "added",
      EVENT_CONFIG_UPDATED: "updated",
      EVENT_CONFIG_REMOVED: "removed",
    },
  } as unknown as EnterpriseDhType;
  const manager = new EnterpriseCorePlusManager(
    dhe,
    client,
    [
      {
        name: "python",
        title: "Python",
        description: "",
        protocols: ["Community"],
        worker_control: [],
        ephemeral_venv: false,
        ephemeral_venv_enabled: false,
      },
    ],
    async () => api,
    createJsApiFactories().createCoreClient,
  );
  try {
    await getCoreConnection({ query, corePlusManager: manager });
  } finally {
    await manager.dispose();
  }
  process.stdout.write(JSON.stringify({ ok: true }));
} finally {
  await server.stop(true);
}
