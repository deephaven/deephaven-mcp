import { expect, test } from "bun:test";
import { stripVTControlCharacters } from "node:util";
import { consoleInfo } from "../../src/actions/console/enterprise.ts";
import {
  consoleHeading,
  consoleListLines,
  consoleStateLines,
} from "../../src/commands/console/formatting.ts";

test("console formatting: preserve heading, selection, and explanation indentation", () => {
  const console = consoleInfo({
    name: "IC - dh CLI - abc123",
    serial: "123",
  });
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
