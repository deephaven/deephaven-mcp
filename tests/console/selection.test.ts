import { expect, test } from "bun:test";
import { join } from "node:path";
import type { QueryInfo } from "@deephaven-enterprise/jsapi-nodejs";
import { Store } from "../../src/actions/auth/store.ts";
import { rememberOnlyRunningConsole } from "../../src/actions/console/actions.ts";
import { consoleInfo } from "../../src/actions/console/enterprise.ts";
import {
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
} from "../../src/actions/console/store.ts";
import { tempDir } from "../helpers.ts";

/** A console the server reports with `serial` in `status`. */
function consoleWith(serial: string, status: string) {
  return consoleInfo({ serial, status } as unknown as QueryInfo);
}

const identity = {
  server: "https://dhe.example.com",
  user: "alice",
  operateAs: "alice",
};

test("console selection: auto-select only one running console and persist its full ID", async () => {
  const store = new Store(join(await tempDir("dh-console-selection-"), "cli"));
  const consoles = [
    consoleWith("running-id", "Connected"),
    consoleWith("stopped-id", "Stopped"),
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
    [consoleWith("failed-id", "Failed")],
    [consoleWith("first-id", "Running"), consoleWith("second-id", "Running")],
  ]) {
    expect(
      await rememberOnlyRunningConsole(store, identity, consoles),
    ).toBeUndefined();
  }
  expect(
    (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).selected,
  ).toEqual({});
});
