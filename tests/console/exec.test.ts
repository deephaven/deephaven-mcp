import { expect, mock, spyOn, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Command } from "commander";
import { RUNTIME_FLAGS } from "../../scripts/build.ts";
import * as auth from "../../src/actions/auth/session.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { Store } from "../../src/actions/auth/store.ts";
import * as consoleActions from "../../src/actions/console/actions.ts";
import type { EnterpriseConsoleInfo } from "../../src/actions/console/enterprise.ts";
import {
  execInConsole,
  resolveExecTarget,
} from "../../src/actions/console/exec.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { exec } from "../../src/commands/console/exec.ts";
import { askExecSource } from "../../src/commands/console/questions.ts";
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
