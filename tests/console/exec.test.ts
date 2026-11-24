import { expect, mock, spyOn, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { dh } from "@deephaven/jsapi-types";
import { Command } from "commander";
import { RUNTIME_FLAGS } from "../../scripts/build.ts";
import * as auth from "../../src/actions/auth/session.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { Store } from "../../src/actions/auth/store.ts";
import * as consoleActions from "../../src/actions/console/actions.ts";
import * as core from "../../src/actions/console/core.ts";
import type { EnterpriseConsoleInfo } from "../../src/actions/console/enterprise.ts";
import {
  execInConsole,
  resolveExecTarget,
} from "../../src/actions/console/exec.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { exec } from "../../src/commands/console/exec.ts";
import { askExecSource } from "../../src/commands/console/questions.ts";
import * as output from "../../src/output.ts";
import { lazyPrompter } from "../../src/ui/prompt.ts";
import { run, tempDir } from "../helpers.ts";

test("exec source: file and inline sources are validated before execution", async () => {
  const dir = await tempDir("dh-exec-source-");
  const file = join(dir, "analysis.py");
  await writeFile(file, "a = 1\n");
  expect(await askExecSource(file, undefined)).toEqual({ code: "a = 1\n" });
  expect(await askExecSource(undefined, "a = 1")).toEqual({ code: "a = 1" });
  await expect(askExecSource(file, "a = 1")).rejects.toMatchObject({
    code: "usage",
  });
  await expect(askExecSource(undefined, undefined)).rejects.toMatchObject({
    code: "usage",
  });
});

test("exec Enterprise: creates a console when none is running", async () => {
  const store = new Store(join(await tempDir("dh-exec-create-"), "cli"));
  const created = {
    id: "new-id",
    shortcode: "abc123",
    name: "IC - dh CLI - abc123",
    state: "Running",
    language: "Python",
    heapSizeGb: 4,
    server: "Query 1",
    engine: "DeephavenCommunity",
    jvmArgs: "",
    environment: {},
    autoDeleteTimeoutSeconds: 600,
  } satisfies EnterpriseConsoleInfo;
  const session = {
    origin: "https://enterprise.example.com",
    kind: "enterprise",
    cacheRoot: "",
    enterprise: {
      client: { getKnownConfigs: () => [] },
    },
    user: "alice",
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  const create = spyOn(consoleActions, "createConsole").mockResolvedValue(
    created,
  );
  try {
    await expect(
      resolveExecTarget(store, undefined, undefined, new CommandLifecycle()),
    ).resolves.toEqual({
      session,
      console: created,
      created: true,
    });
    expect(create).toHaveBeenCalledWith(
      store,
      undefined,
      {},
      false,
      expect.any(CommandLifecycle),
    );
  } finally {
    create.mockRestore();
    login.mockRestore();
  }
});

test("exec community: runs verbatim and reports API changes", async () => {
  const store = new Store(join(await tempDir("dh-exec-community-"), "cli"));
  const ideSession = {
    close: mock(() => {}),
    runCode: mock(async (code: string) => {
      expect(code).toBe("a = 1\n");
      return {
        error: "",
        changes: {
          created: [{ name: "a", type: "Table" }],
          updated: [],
          removed: [],
        },
      };
    }),
  };
  const ide = {
    running: mock(async () => ide),
    getConsoleTypes: mock(async () => ["python"]),
    startSession: mock(async () => ideSession),
  };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    cacheRoot: "",
    community: {
      origin: "https://community.example.com",
      dh: {},
      client: {
        getAsIdeConnection: mock(async () => ide),
      },
    },
    user: "anonymous",
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    await expect(
      execInConsole(
        store,
        { code: "a = 1\n" },
        undefined,
        undefined,
        new CommandLifecycle(),
      ),
    ).resolves.toEqual({
      console: {
        shortcode: "shared",
        id: "shared",
        created: false,
      },
      changes: {
        created: [{ name: "a", type: "Table" }],
        updated: [],
        removed: [],
      },
    });
    expect(ide.startSession).toHaveBeenCalledWith("python");
  } finally {
    login.mockRestore();
  }
});

test("exec community: script errors keep partial changes and return exec_failed", async () => {
  const store = new Store(join(await tempDir("dh-exec-failure-"), "cli"));
  const ideSession = {
    close: mock(() => {}),
    runCode: mock(async () => ({
      error: "Traceback: bad name",
      changes: {
        created: [{ name: "partial", type: "Table" }],
        updated: [],
        removed: [],
      },
    })),
  };
  const ide = {
    running: mock(async () => ide),
    getConsoleTypes: mock(async () => ["python"]),
    startSession: mock(async () => ideSession),
  };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    cacheRoot: "",
    community: {
      client: { getAsIdeConnection: mock(async () => ide) },
    },
    user: "anonymous",
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    const lifecycle = new CommandLifecycle();
    await expect(
      execInConsole(
        store,
        { code: "partial = empty_table(1)\nmissing" },
        undefined,
        undefined,
        lifecycle,
      ),
    ).rejects.toMatchObject({
      code: "exec_failed",
      detail: "Traceback: bad name",
    });
    expect(lifecycle.failureReport).toMatchObject({
      outcome: "failed",
      changes: { created: [{ name: "partial", type: "Table" }] },
    });
    expect(lifecycle.failureReport.human?.details ?? []).not.toContain(
      "The console and script outcome are unknown; dh exiting does not stop the remote script.",
    );
  } finally {
    login.mockRestore();
  }
});

test("exec community: discovers Groovy and reports unsupported languages", async () => {
  const store = new Store(join(await tempDir("dh-exec-language-"), "cli"));
  const ideSession = {
    close: mock(() => {}),
    runCode: mock(async () => ({
      error: "",
      changes: { created: [], updated: [], removed: [] },
    })),
  };
  const ide = {
    running: mock(async () => ide),
    getConsoleTypes: mock(async () => ["groovy"]),
    startSession: mock(async () => ideSession),
  };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    community: { client: { getAsIdeConnection: async () => ide } },
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    const lifecycle = new CommandLifecycle();
    await execInConsole(
      store,
      { code: "x = 1" },
      undefined,
      undefined,
      lifecycle,
    );
    expect(ide.startSession).toHaveBeenCalledWith("groovy");
    await lifecycle.cleanup();
    ide.getConsoleTypes.mockResolvedValueOnce([]);
    await expect(
      execInConsole(
        store,
        { code: "x = 1" },
        undefined,
        undefined,
        new CommandLifecycle(),
      ),
    ).rejects.toMatchObject({ code: "console_unsupported" });
    expect(ide.startSession).toHaveBeenCalledTimes(1);
  } finally {
    login.mockRestore();
  }
});

test.each(["abort", "cleanup"])(
  "exec: late session acquisition is closed after %s",
  async (ending) => {
    const store = new Store(join(await tempDir("dh-exec-abort-"), "cli"));
    const ideSession = {
      close: mock(() => {}),
      runCode: mock(async () => ({})),
    };
    let release!: (value: typeof ideSession) => void;
    let started!: () => void;
    const opening = new Promise<typeof ideSession>((resolve) => {
      release = resolve;
    });
    const startedPromise = new Promise<void>((resolve) => {
      started = resolve;
    });
    const ide = {
      running: async () => ide,
      getConsoleTypes: async () => ["python"],
      startSession: () => {
        started();
        return opening;
      },
    };
    const session = {
      origin: "https://community.example.com",
      kind: "community",
      community: { client: { getAsIdeConnection: async () => ide } },
    } as unknown as Session;
    const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
    const lifecycle = new CommandLifecycle();
    try {
      const execution = execInConsole(
        store,
        { code: "x=1" },
        undefined,
        undefined,
        lifecycle,
      );
      await startedPromise;
      if (ending === "abort") lifecycle.abort(new Error("cancelled"));
      await lifecycle.cleanup();
      release(ideSession);
      await expect(execution).rejects.toThrow(
        ending === "abort" ? "cancelled" : "Cannot register cleanup",
      );
      expect(ideSession.close).toHaveBeenCalledTimes(1);
      expect(ideSession.runCode).not.toHaveBeenCalled();
    } finally {
      login.mockRestore();
    }
  },
);

test.each(["connection", "running", "languages", "start"])(
  "exec: opening times out during %s without running the script",
  async (stage) => {
    const store = new Store(join(await tempDir("dh-exec-timeout-"), "cli"));
    const previousTimeout = process.env.DH_TIMEOUT;
    process.env.DH_TIMEOUT = "0.05";
    const gate = Promise.withResolvers<void>();
    const reached = Promise.withResolvers<void>();
    const closed = Promise.withResolvers<void>();
    const pause = async (name: string) => {
      if (stage !== name) return;
      reached.resolve();
      await gate.promise;
    };
    const ideSession = {
      close: mock(() => closed.resolve()),
      runCode: mock(async () => ({})),
    };
    const ide = {
      running: mock(async () => {
        await pause("running");
        return ide;
      }),
      getConsoleTypes: mock(async () => {
        await pause("languages");
        return ["python"];
      }),
      startSession: mock(async () => {
        await pause("start");
        return ideSession;
      }),
    };
    const getAsIdeConnection = mock(async () => {
      await pause("connection");
      return ide;
    });
    const session = {
      origin: "https://community.example.com",
      kind: "community",
      community: { client: { getAsIdeConnection } },
    } as unknown as Session;
    const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
    const lifecycle = new CommandLifecycle();
    try {
      const startedAt = performance.now();
      const execution = execInConsole(
        store,
        { code: "x=1" },
        undefined,
        undefined,
        lifecycle,
      );
      await reached.promise;
      await expect(execution).rejects.toMatchObject({
        code: "server_unreachable",
        headline: "Timed out while opening the console session",
      });
      expect(performance.now() - startedAt).toBeGreaterThanOrEqual(40);
      expect(performance.now() - startedAt).toBeLessThan(2_000);
      expect(lifecycle.failureReport.phase).toBe("opening");
      expect(ideSession.runCode).not.toHaveBeenCalled();
      await lifecycle.cleanup();
      gate.resolve();
      if (stage === "start") await closed.promise;
      else await Bun.sleep(10);
      expect(ideSession.close).toHaveBeenCalledTimes(stage === "start" ? 1 : 0);
      if (stage !== "start") expect(ide.startSession).not.toHaveBeenCalled();
      expect(ideSession.runCode).not.toHaveBeenCalled();
    } finally {
      gate.resolve();
      login.mockRestore();
      if (previousTimeout === undefined) delete process.env.DH_TIMEOUT;
      else process.env.DH_TIMEOUT = previousTimeout;
    }
  },
);

test("exec: a late session cleanup failure is reported after timeout", async () => {
  const store = new Store(join(await tempDir("dh-exec-timeout-close-"), "cli"));
  const previousTimeout = process.env.DH_TIMEOUT;
  process.env.DH_TIMEOUT = "0.05";
  const gate = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  const warned = Promise.withResolvers<void>();
  const ideSession = {
    close: mock(() => {
      throw new Error("private vendor detail");
    }),
    runCode: mock(async () => ({})),
  };
  const ide = {
    running: async () => ide,
    getConsoleTypes: async () => ["python"],
    startSession: async () => {
      started.resolve();
      await gate.promise;
      return ideSession;
    },
  };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    community: { client: { getAsIdeConnection: async () => ide } },
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  const note = spyOn(output, "note").mockImplementation(() => warned.resolve());
  try {
    const execution = execInConsole(
      store,
      { code: "x=1" },
      undefined,
      undefined,
      new CommandLifecycle(),
    );
    await started.promise;
    await expect(execution).rejects.toMatchObject({
      code: "server_unreachable",
    });
    gate.resolve();
    await warned.promise;
    expect(note).toHaveBeenCalledWith(
      "! Could not close a console session opened after timeout.",
      "warning",
    );
    expect(ideSession.runCode).not.toHaveBeenCalled();
  } finally {
    gate.resolve();
    note.mockRestore();
    login.mockRestore();
    if (previousTimeout === undefined) delete process.env.DH_TIMEOUT;
    else process.env.DH_TIMEOUT = previousTimeout;
  }
});

test.each(["worker", "start"])(
  "exec Enterprise: timeout during %s stops opening and closes late sessions",
  async (stage) => {
    const store = new Store(
      join(await tempDir("dh-exec-enterprise-timeout-"), "cli"),
    );
    const previousTimeout = process.env.DH_TIMEOUT;
    process.env.DH_TIMEOUT = "0.05";
    const gate = Promise.withResolvers<void>();
    const reached = Promise.withResolvers<void>();
    const closed = Promise.withResolvers<void>();
    const ideSession = {
      close: mock(() => closed.resolve()),
      runCode: mock(async () => ({})),
    };
    const worker = {
      running: async () => worker,
      startSession: mock(async () => {
        if (stage === "start") {
          reached.resolve();
          await gate.promise;
        }
        return ideSession;
      }),
    };
    const connect = spyOn(core, "connectCorePlusWorker").mockImplementation(
      async () => {
        if (stage === "worker") {
          reached.resolve();
          await gate.promise;
        }
        return worker as unknown as dh.IdeConnection;
      },
    );
    const query = {
      type: "InteractiveConsole",
      name: "IC - dh CLI - abc123",
      serial: "server-id",
      designated: { status: "Running" },
      replicas: [],
      scriptLanguage: "Python",
      heapSize: 4,
      dbServerName: "Query 1",
      workerKind: "python",
      jvmArguments: [],
      envVarEntries: [],
    };
    const session = {
      origin: "https://enterprise.example.com",
      kind: "enterprise",
      user: "alice",
      enterprise: {
        client: { getKnownConfigs: () => [query] },
        dhe: {},
      },
    } as unknown as Session;
    const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
    const lifecycle = new CommandLifecycle();
    try {
      const execution = execInConsole(
        store,
        { code: "x=1" },
        undefined,
        undefined,
        lifecycle,
      );
      await reached.promise;
      await expect(execution).rejects.toMatchObject({
        code: "server_unreachable",
      });
      await lifecycle.cleanup();
      gate.resolve();
      if (stage === "start") await closed.promise;
      else await Bun.sleep(10);
      expect(ideSession.close).toHaveBeenCalledTimes(stage === "start" ? 1 : 0);
      if (stage === "worker")
        expect(worker.startSession).not.toHaveBeenCalled();
      expect(ideSession.runCode).not.toHaveBeenCalled();
    } finally {
      gate.resolve();
      connect.mockRestore();
      login.mockRestore();
      if (previousTimeout === undefined) delete process.env.DH_TIMEOUT;
      else process.env.DH_TIMEOUT = previousTimeout;
    }
  },
);

test.each([
  ["Groovy", "groovy"],
  ["Python", "python"],
  ["unsupported", undefined],
  [null, undefined],
])(
  "exec Enterprise: respects console language %s",
  async (language, expected) => {
    const store = new Store(
      join(await tempDir("dh-exec-enterprise-language-"), "cli"),
    );
    const ideSession = {
      close: mock(() => {}),
      runCode: mock(async () => ({
        error: "",
        changes: { created: [], updated: [], removed: [] },
      })),
    };
    const worker = {
      startSession: mock(async () => ideSession),
    } as unknown as dh.IdeConnection;
    const query = {
      type: "InteractiveConsole",
      name: "IC - dh CLI - abc123",
      serial: "server-id",
      designated: { status: "Running" },
      replicas: [],
      scriptLanguage: language,
      heapSize: 4,
      dbServerName: "Query 1",
      workerKind: "python",
      jvmArguments: [],
      envVarEntries: [],
    };
    const session = {
      origin: "https://enterprise.example.com",
      kind: "enterprise",
      user: "alice",
      enterprise: { client: { getKnownConfigs: () => [query] }, dhe: {} },
    } as unknown as Session;
    const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
    const connect = spyOn(core, "connectCorePlusWorker").mockResolvedValue(
      worker,
    );
    const lifecycle = new CommandLifecycle();
    try {
      const execution = execInConsole(
        store,
        { code: "x=1" },
        undefined,
        undefined,
        lifecycle,
      );
      if (expected) {
        await expect(execution).resolves.toMatchObject({
          console: { id: "server-id" },
        });
        expect(worker.startSession).toHaveBeenCalledWith(expected);
      } else {
        await expect(execution).rejects.toMatchObject({
          code: "console_unsupported",
        });
        expect(connect).not.toHaveBeenCalled();
      }
      expect((await lifecycle.cleanup()).failures).toEqual([]);
    } finally {
      connect.mockRestore();
      login.mockRestore();
    }
  },
);

test("exec: DH_TIMEOUT does not limit script runtime", async () => {
  const store = new Store(join(await tempDir("dh-exec-runtime-"), "cli"));
  const previousTimeout = process.env.DH_TIMEOUT;
  process.env.DH_TIMEOUT = "0.02";
  const ideSession = {
    close: mock(() => {}),
    runCode: mock(async () => {
      await Bun.sleep(80);
      return { error: "", changes: { created: [], updated: [], removed: [] } };
    }),
  };
  const ide = {
    running: async () => ide,
    getConsoleTypes: async () => ["python"],
    startSession: async () => ideSession,
  };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    community: { client: { getAsIdeConnection: async () => ide } },
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  const lifecycle = new CommandLifecycle();
  try {
    await expect(
      execInConsole(store, { code: "x=1" }, undefined, undefined, lifecycle),
    ).resolves.toMatchObject({
      changes: { created: [], updated: [], removed: [] },
    });
    expect(ideSession.runCode).toHaveBeenCalledTimes(1);
    await lifecycle.cleanup();
    expect(ideSession.close).toHaveBeenCalledTimes(1);
  } finally {
    login.mockRestore();
    if (previousTimeout === undefined) delete process.env.DH_TIMEOUT;
    else process.env.DH_TIMEOUT = previousTimeout;
  }
});

test("exec: a reused console with no changes still emits its JSON result", async () => {
  const store = new Store(join(await tempDir("dh-exec-empty-"), "cli"));
  const ideSession = {
    close: mock(() => {}),
    runCode: mock(async () => ({
      error: "",
      changes: { created: [], updated: [], removed: [] },
    })),
  };
  const ide = {
    running: async () => ide,
    getConsoleTypes: async () => ["python"],
    startSession: async () => ideSession,
  };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    community: { client: { getAsIdeConnection: async () => ide } },
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  const lifecycle = new CommandLifecycle();
  try {
    const output = await exec.run(
      {
        store,
        lifecycle,
        signal: lifecycle.signal,
        ui: lazyPrompter(false),
        json: true,
        noPrompt: true,
        cli: new Command(),
      },
      [],
      { code: "x = 1" },
    );
    const script = `
      import { result, setFormat } from ${JSON.stringify(new URL("../../src/output.ts", import.meta.url).href)};
      const output = JSON.parse(await Bun.stdin.text());
      setFormat("json");
      result(() => output.human, output.json, output.empty);
    `;
    const rendered = await run(
      [process.execPath, ...RUNTIME_FLAGS, "-e", script],
      {},
      JSON.stringify(output),
    );
    expect(rendered.code).toBe(0);
    expect(JSON.parse(rendered.stdout)).toEqual({
      console: { shortcode: "shared", id: "shared", created: false },
      changes: { created: [], updated: [], removed: [] },
    });
    expect(rendered.stderr).toBe("");
  } finally {
    await lifecycle.cleanup();
    login.mockRestore();
  }
});

test("exec: session errors are classified without exposing vendor details", async () => {
  const store = new Store(join(await tempDir("dh-exec-open-error-"), "cli"));
  const failures = [
    { message: "not authorized SECRET", code: "console_forbidden" },
    { message: "ECONNREFUSED SECRET", code: "server_unreachable" },
    {
      message: "unsupported console language SECRET",
      code: "console_unsupported",
    },
    { message: "unexpected SECRET", code: "console_failed" },
  ];
  let failure = { message: "", code: "" };
  const session = {
    origin: "https://community.example.com",
    kind: "community",
    community: {
      client: {
        getAsIdeConnection: async () => {
          throw new Error(failure.message);
        },
      },
    },
  } as unknown as Session;
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    for (const item of failures) {
      failure = item;
      const lifecycle = new CommandLifecycle();
      try {
        await execInConsole(
          store,
          { code: "x=1" },
          undefined,
          undefined,
          lifecycle,
        );
        throw new Error("expected failure");
      } catch (error) {
        expect(error).toMatchObject({ code: item.code });
        expect(JSON.stringify(error)).not.toContain("SECRET");
        expect(String(error)).not.toContain("SECRET");
        expect(lifecycle.failureReport.phase).toBe("opening");
        expect(lifecycle.failureReport.human?.details).toBeUndefined();
      }
    }
  } finally {
    login.mockRestore();
  }
});
