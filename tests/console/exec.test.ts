import { expect, mock, spyOn, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
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
import { askExecSource } from "../../src/commands/console/questions.ts";
import { tempDir } from "../helpers.ts";

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
    await expect(
      execInConsole(
        store,
        { code: "partial = empty_table(1)\nmissing" },
        undefined,
        undefined,
        new CommandLifecycle(),
      ),
    ).rejects.toMatchObject({
      code: "exec_failed",
      detail: "Traceback: bad name",
    });
  } finally {
    login.mockRestore();
  }
});
