import { expect, spyOn, test } from "bun:test";
import { join } from "node:path";
import { Command } from "commander";
import * as auth from "../../src/actions/auth/session.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { Store } from "../../src/actions/auth/store.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { remove } from "../../src/commands/console/delete.ts";
import { lazyPrompter } from "../../src/ui/prompt.ts";
import { tempDir } from "../helpers.ts";

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
