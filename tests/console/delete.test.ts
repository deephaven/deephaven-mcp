import { expect, spyOn, test } from "bun:test";
import { join } from "node:path";
import { Command } from "commander";
import * as auth from "../../src/actions/auth/session.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { Store } from "../../src/actions/auth/store.ts";
import {
  createConsole,
  detailConsole,
  requireEnterpriseConsole,
  resolveConsoleDeletion,
  selectConsole,
} from "../../src/actions/console/actions.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { remove } from "../../src/commands/console/delete.ts";
import { lazyPrompter } from "../../src/ui/prompt.ts";
import { tempDir } from "../helpers.ts";

test("Community console errors name the operation without startup retry advice", async () => {
  const store = new Store(join(await tempDir("dh-community-errors-"), "cli"));
  const lifecycle = new CommandLifecycle();
  const login = spyOn(auth, "requireAuth").mockResolvedValue({
    origin: "http://localhost:10000",
    kind: "community",
    cacheRoot: "/tmp/dh-test",
    user: "alice",
    // These calls fail before touching the connection.
    community: {
      origin: "http://localhost:10000",
      dh: {},
      client: {},
      handlers: [],
    } as unknown as Session["community"],
  });
  try {
    await expect(
      detailConsole(store, undefined, undefined, lifecycle),
    ).rejects.toMatchObject({
      message: "Console details are not available on Community servers",
    });
    await expect(
      resolveConsoleDeletion(store, undefined, undefined, lifecycle),
    ).rejects.toMatchObject({
      message: "Deleting consoles is not supported on Community servers",
    });
    await expect(
      requireEnterpriseConsole(store, undefined, lifecycle),
    ).rejects.toMatchObject({
      message:
        "Console creation defaults are not available on Community servers",
    });
    await expect(
      selectConsole(store, "", undefined, lifecycle),
    ).rejects.toMatchObject({
      code: "usage",
      hint: expect.stringContaining("console use shared"),
    });
    await expect(
      createConsole(store, undefined, {}, false, lifecycle),
    ).rejects.toMatchObject({
      message: "Creating consoles is not supported on Community servers",
    });
    expect(lifecycle.failureReport.human?.details).toBeUndefined();
  } finally {
    login.mockRestore();
  }
});

test("console delete: no selected or running console fails before confirmation", async () => {
  const store = new Store(join(await tempDir("dh-console-delete-"), "cli"));
  const login = spyOn(auth, "requireAuth").mockResolvedValue({
    origin: "https://dhe.example.com",
    kind: "enterprise",
    cacheRoot: "/tmp/dh-test",
    user: "alice",
    // Only the client member this test needs is mocked.
    enterprise: {
      origin: "https://dhe.example.com",
      dhe: {},
      client: { getKnownConfigs: () => [] },
    } as unknown as Session["enterprise"],
  });
  const ui = { ...lazyPrompter(false) };
  const confirm = spyOn(ui, "confirm");
  const lifecycle = new CommandLifecycle();
  try {
    for (const force of [false, true]) {
      await expect(
        remove.run(
          {
            store,
            lifecycle,
            signal: lifecycle.signal,
            ui,
            json: false,
            noPrompt: true,
            cli: new Command(),
          },
          [],
          { force },
        ),
      ).rejects.toMatchObject({
        code: "console_required",
        message: "No console is selected",
      });
    }
    expect(confirm).not.toHaveBeenCalled();
  } finally {
    login.mockRestore();
    confirm.mockRestore();
  }
});
