import { expect, test } from "bun:test";
import type { Session } from "../../src/actions/auth/signin.ts";
import {
  createEnterpriseConsole,
  findEnterpriseConsole,
  listEnterpriseConsoles,
  serverDefaults,
  waitForConsole,
  waitForConsoleDeletion,
} from "../../src/actions/console/enterprise.ts";
import {
  type EffectiveConsoleSettings,
  mergeConsoleSettings,
} from "../../src/actions/console/settings.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";

function sessionWith(client: Record<string, unknown>): Session {
  return {
    origin: "https://dhe.example.com",
    kind: "enterprise",
    cacheRoot: "/tmp/dh-test",
    user: "alice",
    // Only the client members each test needs are mocked.
    enterprise: {
      origin: "https://dhe.example.com",
      dhe: {},
      client,
    } as unknown as Session["enterprise"],
  };
}

function query(
  serial: string,
  name: string,
  state: string,
  type = "InteractiveConsole",
) {
  return {
    serial,
    name,
    type,
    designated: { status: state },
    scriptLanguage: "Python",
    heapSize: 4,
    dbServerName: "Query 1",
    workerKind: "python",
    jvmArgs: "-Dtest=true",
    envVarEntries: ["TEST", "value"],
    timeout: 600_000,
  };
}

test("console defaults: reads server-supported defaults", async () => {
  const session = sessionWith({
    getServerConfigValues: async () => ({
      defaultWorkerKind: "python",
      workerKinds: [
        { name: "enterprise", protocols: ["EnterpriseComm"] },
        { name: "python", protocols: ["Community"] },
      ],
    }),
    getDbServers: async () => [{ name: "Query 1" }, { name: "Query 2" }],
    getQueryConstants: async () => ({ consoleDefaultHeap: 4 }),
  });
  expect((await serverDefaults(session)).settings).toEqual({
    language: "python",
    heapSizeGb: 4,
    server: "Query 1",
    engine: "python",
    jvmArgs: [],
    environment: {},
    autoDeleteTimeoutSeconds: 600,
  });
});

test("console discovery filters CLI InteractiveConsole queries and uses stable serial IDs", () => {
  const session = sessionWith({
    getKnownConfigs: () => [
      query("full-id", "IC - dh CLI - abc123", "Running"),
      query("other-id", "IC - dh CLI - invalid!", "Running"),
      query("pq-id", "IC - dh CLI - def456", "Running", "Python"),
    ],
  });
  const consoles = listEnterpriseConsoles(session);
  expect(consoles).toHaveLength(1);
  expect(consoles[0]).toMatchObject({
    id: "full-id",
    shortcode: "abc123",
    state: "Running",
    language: "Python",
    heapSizeGb: 4,
    server: "Query 1",
    engine: "python",
    environment: { TEST: "value" },
    autoDeleteTimeoutSeconds: 600,
  });
  expect(findEnterpriseConsole(session, "abc123")?.id).toBe("full-id");
  expect(findEnterpriseConsole(session, "full-id")?.shortcode).toBe("abc123");
});

test("console defaults: null resets to server values and command settings override defaults", () => {
  const base: EffectiveConsoleSettings = {
    language: "python",
    heapSizeGb: 4,
    server: "Query 1",
    engine: "python",
    jvmArgs: [],
    environment: {},
    autoDeleteTimeoutSeconds: 600,
  };
  expect(
    mergeConsoleSettings(
      base,
      { heapSizeGb: 8, language: "groovy", environment: { DEFAULT: "yes" } },
      { heapSizeGb: null, environment: { COMMAND: "yes" } },
    ),
  ).toEqual({
    ...base,
    language: "groovy",
    environment: { COMMAND: "yes" },
  });
});

test("console creation: sends an InteractiveConsole query with seconds converted to milliseconds", async () => {
  let created: Record<string, unknown> | undefined;
  const settings: EffectiveConsoleSettings = {
    language: "groovy",
    heapSizeGb: 8,
    server: "Query 1",
    engine: "python",
    jvmArgs: ["-Danswer=42"],
    environment: { MODE: "test" },
    autoDeleteTimeoutSeconds: 120,
  };
  const session = sessionWith({
    getServerConfigValues: async () => ({
      defaultWorkerKind: "python",
      workerKinds: [{ name: "python", protocols: ["Community"] }],
    }),
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => ({ consoleDefaultHeap: 4 }),
    getQueryConfigurationTypes: async () =>
      new Map([["InteractiveConsole", { supportsCommunity: true }]]),
    getUserInfo: async () => ({ operateAs: "alice" }),
    createQuery: async (config: Record<string, unknown>) => {
      created = config;
      return "server-full-id";
    },
  });
  const id = await createEnterpriseConsole(
    session,
    "abc123",
    settings,
    new CommandLifecycle(),
  );
  expect(id).toBe("server-full-id");
  expect(created).toMatchObject({
    name: "IC - dh CLI - abc123",
    type: "InteractiveConsole",
    scriptLanguage: "Groovy",
    dbServerName: "Query 1",
    workerKind: "python",
    heapSize: 8,
    jvmArguments: ["-Danswer=42"],
    envVarEntries: ["MODE", "test"],
    envVars: "MODE=test",
    timeout: 120_000,
    scheduling: [
      "AutoDelete=true",
      "QueueName=InteractiveConsoleTemporaryQueue",
    ],
  });
});

test("console startup wait: observes a pending console until it is running", async () => {
  let state = "Initializing";
  const session = sessionWith({
    getKnownConfigs: () => [query("full-id", "IC - dh CLI - abc123", state)],
  });
  const lifecycle = new CommandLifecycle();
  const ready = waitForConsole(session, "full-id", lifecycle);
  setTimeout(() => {
    state = "Running";
  }, 20);
  expect((await ready).id).toBe("full-id");
  expect(lifecycle.failureReport).toMatchObject({
    outcome: "succeeded",
    continuation: "stopped",
    progress: "The console is ready.",
  });
});

test("console creation: reuse the settings snapshot without fetching metadata twice", async () => {
  let metadataReads = 0;
  const session = sessionWith({
    getServerConfigValues: async () => {
      metadataReads++;
      return {
        defaultWorkerKind: "python",
        workerKinds: [{ name: "python", protocols: ["Community"] }],
        scriptSessionProviders: ["Python"],
      };
    },
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => ({ consoleDefaultHeap: 4 }),
    getQueryConfigurationTypes: async () =>
      new Map([["InteractiveConsole", { supportsCommunity: true }]]),
    getUserInfo: async () => ({ operateAs: "alice" }),
    createQuery: async () => "full-id",
  });
  const snapshot = await serverDefaults(session);
  const lifecycle = new CommandLifecycle();
  expect(
    await createEnterpriseConsole(
      session,
      "abc123",
      snapshot.settings,
      lifecycle,
      snapshot,
    ),
  ).toBe("full-id");
  expect(metadataReads).toBe(1);
  expect(lifecycle.failureReport).toMatchObject({
    target: "abc123",
    phase: "starting",
    outcome: "unknown",
    continuation: "continues",
  });
  await expect(
    createEnterpriseConsole(
      session,
      "abc123",
      { ...snapshot.settings, server: "unavailable" },
      lifecycle,
      snapshot,
    ),
  ).rejects.toMatchObject({ code: "console_unsupported" });
});

test("console defaults: choose a supported engine and language when preferred values are unavailable", async () => {
  const session = sessionWith({
    getServerConfigValues: async () => ({
      defaultWorkerKind: "enterprise",
      workerKinds: [
        { name: "enterprise", protocols: ["EnterpriseComm"] },
        { name: "groovy", protocols: ["Community"] },
      ],
      scriptSessionProviders: ["Groovy"],
    }),
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => ({ consoleDefaultHeap: 4 }),
  });
  expect((await serverDefaults(session)).settings).toMatchObject({
    language: "groovy",
    engine: "groovy",
  });
});

test("console startup wait: terminal failures retain the observed state and return immediately", async () => {
  const session = sessionWith({
    getKnownConfigs: () => [query("full-id", "IC - dh CLI - abc123", "Failed")],
  });
  const lifecycle = new CommandLifecycle();
  await expect(
    waitForConsole(session, "full-id", lifecycle),
  ).rejects.toMatchObject({
    code: "console_failed",
    detail: "Last state: Failed",
  });
  expect(lifecycle.failureReport).toMatchObject({
    phase: "starting",
    outcome: "failed",
    continuation: "stopped",
    observation: { state: "Failed", ageSeconds: 0 },
  });
});

test("console deletion wait: disappearance confirms completion", async () => {
  const session = sessionWith({ getKnownConfigs: () => [] });
  const lifecycle = new CommandLifecycle();
  await waitForConsoleDeletion(
    session,
    {
      id: "full-id",
      shortcode: "abc123",
      name: "IC - dh CLI - abc123",
      state: "Stopped",
      language: null,
      heapSizeGb: null,
      server: null,
      engine: null,
      jvmArgs: null,
      environment: {},
      autoDeleteTimeoutSeconds: null,
    },
    lifecycle,
  );
  expect(lifecycle.failureReport).toMatchObject({
    phase: "deleting",
    outcome: "succeeded",
    continuation: "stopped",
    progress: "The console was deleted.",
  });
});
