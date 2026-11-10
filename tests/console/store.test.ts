import { expect, test } from "bun:test";
import { chmod, stat } from "node:fs/promises";
import { join } from "node:path";
import { Store } from "../../src/actions/auth/store.ts";
import type { ConsoleIdentity } from "../../src/actions/console/store.ts";
import {
  type ConsoleRegistry,
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
  rememberConsole,
  reserveShortcode,
} from "../../src/actions/console/store.ts";
import { tempDir, WINDOWS } from "../helpers.ts";

async function tempStore(): Promise<Store> {
  return new Store(join(await tempDir("dh-console-store-"), "cli"));
}

const identity = (user: string, operateAs = user): ConsoleIdentity => ({
  server: "https://dhe.example.com",
  user,
  operateAs,
});

test("console store: reservations are globally unique and survive deletion", async () => {
  const store = await tempStore();
  const sequence = ["aaaaaa", "aaaaaa", "bbbbbb"];
  const makeCode = () => sequence.shift() ?? "cccccc";

  const first = await reserveShortcode(store, identity("alice"), makeCode);
  const second = await reserveShortcode(store, identity("bob"), makeCode);
  expect([first, second]).toEqual(["aaaaaa", "bbbbbb"]);

  const registry = await store.readConsoleData<ConsoleRegistry>(
    EMPTY_CONSOLE_REGISTRY,
  );
  delete registry.selected[identityKey(identity("alice"))];
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (data) => {
    data.selected = registry.selected;
  });
  expect(
    (await store.readConsoleData<ConsoleRegistry>(EMPTY_CONSOLE_REGISTRY))
      .reservations.aaaaaa,
  ).toEqual(identity("alice"));
});

test("console store: concurrent profiles cannot reserve the same shortcode", async () => {
  const store = await tempStore();
  const codeGenerator = (collision: string, next: string) => {
    let first = true;
    return () => {
      if (first) {
        first = false;
        return collision;
      }
      return next;
    };
  };
  const [alice, bob] = await Promise.all([
    reserveShortcode(
      store,
      identity("alice"),
      codeGenerator("shared", "aaaaaa"),
    ),
    reserveShortcode(store, identity("bob"), codeGenerator("shared", "bbbbbb")),
  ]);
  expect(alice).not.toBe(bob);
  expect([alice, bob]).toContain("shared");
  expect([alice, bob].find((code) => code !== "shared")).toMatch(
    /^(aaaaaa|bbbbbb)$/,
  );
});

test("console store: identity includes server, login user and operate-as user", async () => {
  expect(identityKey(identity("alice", "trader"))).not.toBe(
    identityKey(identity("alice", "researcher")),
  );
  expect(
    identityKey({ ...identity("alice"), server: "https://other.example.com" }),
  ).not.toBe(identityKey(identity("alice")));
});

test("console store: full IDs preserve multiple profile mappings", async () => {
  const store = await tempStore();
  await rememberConsole(store, "aaaaaa", "full-id", identity("alice"));
  await rememberConsole(store, "bbbbbb", "full-id", identity("bob"));
  await rememberConsole(store, "aaaaaa", "full-id", identity("alice"));
  const registry = await store.readConsoleData<ConsoleRegistry>(
    EMPTY_CONSOLE_REGISTRY,
  );
  expect(registry.fullIds["full-id"]).toEqual([
    identity("alice"),
    identity("bob"),
  ]);
});

test.skipIf(WINDOWS)(
  "console store: file is owner-only and rejects broad permissions",
  async () => {
    const store = await tempStore();
    await reserveShortcode(store, identity("alice"), () => "abcdef");
    expect((await stat(store.consoleDataPath)).mode & 0o777).toBe(0o600);
    await chmod(store.consoleDataPath, 0o644);
    await expect(
      store.readConsoleData<ConsoleRegistry>(EMPTY_CONSOLE_REGISTRY),
    ).rejects.toMatchObject({ code: "credential_unavailable" });
  },
);
