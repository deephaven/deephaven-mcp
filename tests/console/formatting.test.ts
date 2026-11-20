import { expect, test } from "bun:test";
import { stripVTControlCharacters } from "node:util";
import type { ConsoleInfo } from "../../src/actions/console/enterprise.ts";
import {
  consoleHeading,
  consoleListLines,
  consoleStateLines,
} from "../../src/commands/console/formatting.ts";

test("console formatting: preserve heading, selection, and explanation indentation", () => {
  const console: ConsoleInfo = {
    name: "IC - dh CLI - abc123",
    id: "123",
    shortcode: "abc123",
    state: "Unknown",
    language: "Python",
    heapSizeGb: 1,
    server: "Query 1",
    engine: "DeephavenCommunity",
    jvmArgs: "",
    environment: {},
    autoDeleteTimeoutSeconds: 600,
  };
  const explanation =
    "    No worker state is available. Startup may be pending; the reason is not confirmed.";
  expect(stripVTControlCharacters(consoleHeading(console))).toBe(
    "IC - dh CLI - abc123 (123)",
  );
  expect(consoleStateLines("Unknown").map(stripVTControlCharacters)).toEqual([
    "  State: Unknown",
    explanation,
  ]);
  expect(consoleStateLines("Running").map(stripVTControlCharacters)).toEqual([
    "  State: Running",
  ]);
  expect(
    consoleListLines({ ...console, inUse: true }).map(stripVTControlCharacters),
  ).toEqual(["● abc123  Unknown  IC - dh CLI - abc123", explanation]);
  expect(
    consoleListLines({ ...console, state: "Running", inUse: false }).map(
      stripVTControlCharacters,
    ),
  ).toEqual(["  abc123  Running  IC - dh CLI - abc123"]);
});
