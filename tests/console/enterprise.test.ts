import { expect, spyOn, test } from "bun:test";
import type { Session } from "../../src/actions/auth/signin.ts";
import {
  consoleInfo,
  createEnterpriseConsole,
  findEnterpriseConsole,
  listEnterpriseConsoles,
  serverDefaults,
  waitForConsole,
  waitForConsoleDeletion,
} from "../../src/actions/console/enterprise.ts";
import {
  consoleCreateRequestReport,
  consoleDeleteAcceptedReport,
  consoleDeleteRequestReport,
  consoleObservationReport,
  consoleStartupAcceptedReport,
  consoleStartupFailedReport,
  consoleStartupReport,
  consoleStateDetails,
} from "../../src/actions/console/reporting.ts";
import {
  type EffectiveConsoleSettings,
  mergeConsoleSettings,
} from "../../src/actions/console/settings.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";

const schedulingDefaults = [
  "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerDaily",
  "StartTime=07:55:00",
  "StopTime=23:55:00",
  "TimeZone=America/New_York",
];
const queryConstants = {
  consoleDefaultHeap: 4,
  minDataRatio: 0.1,
  maxDataRatio: 0.6,
  defaultSchedulingDetails: schedulingDefaults,
};

test("console recovery: saved profiles are explicit in create and delete commands", () => {
  for (const report of [
    consoleCreateRequestReport("abc123", "prod"),
    consoleDeleteRequestReport("abc123", "prod"),
  ]) {
    expect(report.recoveryCommand).toEqual([
      "dh",
      "console",
      "detail",
      "abc123",
      "--profile",
      "prod",
    ]);
  }
  expect(
    consoleDeleteRequestReport("abc123", undefined).recoveryCommand,
  ).toEqual(["dh", "console", "detail", "abc123"]);
});

test("console startup wait: initial discovery has a bounded unconfirmed outcome", async () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.setFailureReport(consoleCreateRequestReport("abc123", "prod"));
  const clock = spyOn(performance, "now").mockReturnValue(0);
  const session = sessionWith({
    getKnownConfigs: () => {
      clock.mockReturnValue(Number.MAX_SAFE_INTEGER);
      return [];
    },
  });
  try {
    await expect(
      waitForConsole(session, "abc123", lifecycle),
    ).rejects.toMatchObject({ code: "console_failed" });
    expect(lifecycle.failureReport.outcome).toBe("unknown");
    expect(lifecycle.failureReport.recoveryCommand).toContain("prod");
  } finally {
    clock.mockRestore();
  }
});

test("console unknown state: explain uncertainty without changing the state", () => {
  const explanation =
    "No worker state is available. Startup may be pending; the reason is not confirmed.";
  expect(consoleStateDetails("Unknown")).toEqual([explanation]);
  expect(consoleStateDetails("Running")).toEqual([]);
  expect(consoleStateDetails("Failed")).toEqual([]);
  const lifecycle = new CommandLifecycle();
  lifecycle.setFailureReport(consoleObservationReport("starting", "Unknown"));
  expect(lifecycle.failureReport).toMatchObject({
    progress: explanation,
    observation: { state: "Unknown" },
  });
  lifecycle.setFailureReport(
    consoleObservationReport("starting", "Initializing"),
  );
  expect(lifecycle.failureReport.progress).toBeUndefined();
  expect(lifecycle.failureReport.observation?.state).toBe("Initializing");
});

test("console reports: retain recovery context without stale continuation details", () => {
  const lifecycle = new CommandLifecycle();
  lifecycle.setFailureReport({
    ...consoleStartupAcceptedReport("abc123", "prod"),
    recoveryCommand: ["dh", "console", "detail", "abc123"],
  });
  lifecycle.setFailureReport(
    consoleObservationReport(
      "starting",
      "Unknown",
      lifecycle.failureReport.human,
    ),
  );
  expect(lifecycle.failureReport.human?.summary).toContain(
    "abc123 (profile prod)",
  );
  expect(lifecycle.failureReport.human?.details).toContain(
    "Last observed: Unknown, 0s ago.",
  );
  expect(lifecycle.failureReport.human?.details).toContain(
    consoleStateDetails("Unknown")[0],
  );
  lifecycle.setFailureReport(consoleStartupFailedReport("Failed"));
  expect(lifecycle.failureReport.human?.details?.join(" ")).not.toContain(
    "may still be starting",
  );
  expect(lifecycle.failureReport.recoveryCommand).toEqual([
    "dh",
    "console",
    "detail",
    "abc123",
  ]);
  lifecycle.setFailureReport(
    consoleDeleteAcceptedReport(lifecycle.failureReport.human),
  );
  expect(lifecycle.failureReport.human?.details?.join(" ")).toContain(
    "deletion may still complete after dh exits",
  );
});

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
    typeSpecificFields: {
      TerminationDelay: { type: "long", value: "600000" },
    },
  };
}

test("console defaults: reads server-supported defaults", async () => {
  const session = sessionWith({
    getServerConfigValues: async () => ({
      jvmProfileDefault: "default",
      defaultWorkerKind: "python",
      workerKinds: [
        { name: "enterprise", protocols: ["EnterpriseComm"] },
        { name: "python", protocols: ["Community"] },
      ],
    }),
    getDbServers: async () => [{ name: "Query 1" }, { name: "Query 2" }],
    getQueryConstants: async () => queryConstants,
    getTemporaryQueueNames: async () => ["DefaultTemporaryQueue"],
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

test("console defaults: reject missing or invalid required server metadata before creation", async () => {
  const validClient = {
    getServerConfigValues: async () => ({
      jvmProfileDefault: "Default",
      workerKinds: [{ name: "community", protocols: ["Community"] }],
    }),
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => queryConstants,
    getTemporaryQueueNames: async () => ["DefaultTemporaryQueue"],
  };
  for (const override of [
    { getTemporaryQueueNames: async () => [] },
    {
      getQueryConstants: async () => ({ ...queryConstants, minDataRatio: -1 }),
    },
    {
      getQueryConstants: async () => ({ ...queryConstants, minDataRatio: 0.8 }),
    },
    {
      getQueryConstants: async () => ({
        ...queryConstants,
        maxDataRatio: Number.NaN,
      }),
    },
    {
      getQueryConstants: async () => ({
        ...queryConstants,
        defaultSchedulingDetails: undefined,
      }),
    },
    {
      getServerConfigValues: async () => ({
        workerKinds: [{ name: "community", protocols: ["Community"] }],
      }),
    },
  ]) {
    await expect(
      serverDefaults(sessionWith({ ...validClient, ...override })),
    ).rejects.toMatchObject({ code: "console_unsupported" });
  }
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

test("console detail: idle timeout is independent of initialization timeout", () => {
  const config = query("full-id", "IC - dh CLI - abc123", "Running");
  config.timeout = 20_000;
  expect(consoleInfo(config).autoDeleteTimeoutSeconds).toBe(600);
  expect(
    consoleInfo({ ...config, typeSpecificFields: null })
      .autoDeleteTimeoutSeconds,
  ).toBeNull();
  for (const value of ["invalid", "", "-1000", "Infinity"]) {
    expect(() =>
      consoleInfo({
        ...config,
        typeSpecificFields: { TerminationDelay: { type: "long", value } },
      }),
    ).toThrow("invalid console idle timeout");
  }
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
      jvmProfileDefault: "default",
      defaultWorkerKind: "python",
      workerKinds: [{ name: "python", protocols: ["Community"] }],
    }),
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => queryConstants,
    getTemporaryQueueNames: async () => ["DefaultTemporaryQueue"],
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
    dataMemoryRatio: 0.1,
    jvmProfile: "default",
    restartUsers: 1,
    jvmArguments: ["-Danswer=42"],
    envVarEntries: ["MODE", "test"],
    envVars: "MODE=test",
    timeout: 600_000,
    typeSpecificFields: {
      TerminationDelay: { type: "long", value: "120000" },
    },
  });
  const scheduling = created?.scheduling;
  expect(Array.isArray(scheduling)).toBe(true);
  if (!Array.isArray(scheduling)) throw new Error("Missing scheduling payload");
  expect(scheduling).toEqual(
    expect.arrayContaining([
      "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerTemporary",
      "StartTime=07:55:00",
      "StopTime=23:55:00",
      "TimeZone=America/New_York",
      "TemporaryAutoDelete=true",
      "TemporaryQueueName=DefaultTemporaryQueue",
      "TemporaryExpirationTimeMillis=172800000",
    ]),
  );
  expect(
    scheduling.filter((entry: string) => entry.startsWith("SchedulerType=")),
  ).toHaveLength(1);
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

test("console startup wait: tolerate initial discovery delay but fail after disappearance", async () => {
  let reads = 0;
  const session = sessionWith({
    getKnownConfigs: () => {
      reads++;
      return reads === 2
        ? [query("full-id", "IC - dh CLI - abc123", "Initializing")]
        : [];
    },
  });
  const lifecycle = new CommandLifecycle();
  await expect(
    waitForConsole(session, "full-id", lifecycle),
  ).rejects.toMatchObject({ code: "console_not_found" });
  expect(reads).toBe(3);
  expect(lifecycle.failureReport).toMatchObject({
    outcome: "failed",
    continuation: "stopped",
  });
  expect(lifecycle.failureReport.human?.details?.join(" ")).not.toContain(
    "may still be starting",
  );
});

test("console creation: reuse the settings snapshot without fetching metadata twice", async () => {
  let metadataReads = 0;
  const session = sessionWith({
    getServerConfigValues: async () => {
      metadataReads++;
      return {
        jvmProfileDefault: "default",
        defaultWorkerKind: "python",
        workerKinds: [{ name: "python", protocols: ["Community"] }],
        scriptSessionProviders: ["Python"],
      };
    },
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => queryConstants,
    getTemporaryQueueNames: async () => ["DefaultTemporaryQueue"],
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

  const rejectedSession = sessionWith({
    getQueryConfigurationTypes: async () =>
      new Map([["InteractiveConsole", { supportsCommunity: true }]]),
    getUserInfo: async () => ({ operateAs: "alice" }),
    createQuery: async () => {
      throw new Error(
        "ValidationFailure: Invalid dataBufferPoolToHeapSizeRatio value: Must be in range [0.1,0.6]",
      );
    },
  });
  const rejectedLifecycle = new CommandLifecycle();
  await expect(
    rejectedLifecycle.runLifecycleAction(consoleStartupReport(), async () => {
      rejectedLifecycle.setFailureReport(
        consoleCreateRequestReport("def456", "test-profile"),
      );
      return await createEnterpriseConsole(
        rejectedSession,
        "def456",
        snapshot.settings,
        rejectedLifecycle,
        snapshot,
      );
    }),
  ).rejects.toMatchObject({ code: "console_failed" });
  expect(rejectedLifecycle.failureReport).toMatchObject({
    target: "def456",
    phase: "creating",
    continuation: "unknown",
    human: {
      details: [
        "The create result is not confirmed. Check this console before retrying.",
      ],
    },
  });
  expect(rejectedLifecycle.failureReport.progress).toBeUndefined();
});

test("console defaults: choose a supported engine and language when preferred values are unavailable", async () => {
  const session = sessionWith({
    getServerConfigValues: async () => ({
      jvmProfileDefault: "default",
      defaultWorkerKind: "enterprise",
      workerKinds: [
        { name: "enterprise", protocols: ["EnterpriseComm"] },
        { name: "groovy", protocols: ["Community"] },
      ],
      scriptSessionProviders: ["Groovy"],
    }),
    getDbServers: async () => [{ name: "Query 1" }],
    getQueryConstants: async () => queryConstants,
    getTemporaryQueueNames: async () => ["DefaultTemporaryQueue"],
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
