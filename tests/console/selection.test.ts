import { expect, test } from "bun:test";
import { join } from "node:path";
import { Store } from "../../src/actions/auth/store.ts";
import { rememberOnlyRunningConsole } from "../../src/actions/console/actions.ts";
import type { ConsoleInfo } from "../../src/actions/console/enterprise.ts";
import {
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
} from "../../src/actions/console/store.ts";
import { tempDir } from "../helpers.ts";

const identity = {
  server: "https://dhe.example.com",
  user: "alice",
  operateAs: "alice",
};

function consoleFixture(id: string, state: string): ConsoleInfo {
  return {
    id,
    shortcode: "abc123",
    name: "IC - dh CLI - abc123",
    state,
    language: "Python",
    heapSizeGb: 1,
    server: "Query 1",
    engine: "DeephavenCommunity",
    jvmArgs: "",
    environment: {},
    autoDeleteTimeoutSeconds: 600,
  };
}

test("console selection: auto-select only one running console and persist its full ID", async () => {
  const store = new Store(join(await tempDir("dh-console-selection-"), "cli"));
  const consoles = [
    consoleFixture("running-id", "Connected"),
    consoleFixture("stopped-id", "Stopped"),
  ];
  expect(await rememberOnlyRunningConsole(store, identity, consoles)).toBe(
    "running-id",
  );
  expect(
    (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).selected,
  ).toEqual({ [identityKey(identity)]: "running-id" });
});

test("console selection: no automatic choice for zero or multiple running consoles", async () => {
  const store = new Store(join(await tempDir("dh-console-selection-"), "cli"));
  for (const consoles of [
    [],
    [consoleFixture("failed-id", "Failed")],
    [
      consoleFixture("first-id", "Running"),
      consoleFixture("second-id", "Running"),
    ],
  ]) {
    expect(
      await rememberOnlyRunningConsole(store, identity, consoles),
    ).toBeUndefined();
  }
  expect(
    (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).selected,
  ).toEqual({});
});
