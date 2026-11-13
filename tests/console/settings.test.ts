import { expect, test } from "bun:test";
import {
  type ConsoleSettings,
  type EffectiveConsoleSettings,
  mergeConsoleSettings,
  parseConsoleSettings,
} from "../../src/actions/console/settings.ts";

const base: EffectiveConsoleSettings = {
  language: "python",
  heapSizeGb: 4,
  server: "Query 1",
  engine: "python",
  jvmArgs: [],
  environment: {},
  autoDeleteTimeoutSeconds: 600,
};

test("console settings: every nullable field resets to its server default", () => {
  const defaults: ConsoleSettings = {
    language: "groovy",
    heapSizeGb: 8,
    server: "Query 2",
    engine: "groovy",
    jvmArgs: ["-Dmode=test"],
    environment: { MODE: "test" },
    autoDeleteTimeoutSeconds: 900,
  };
  const resets = parseConsoleSettings(
    JSON.stringify(
      Object.fromEntries(Object.keys(defaults).map((key) => [key, null])),
    ),
  );
  expect(mergeConsoleSettings(base, defaults, resets)).toEqual(base);
  expect(mergeConsoleSettings(base, resets, {})).toEqual(base);
  expect(base.environment).toEqual({});
  expect(defaults.heapSizeGb).toBe(8);
});

test("console settings: validation retains input-specific errors", () => {
  for (const [field, value, message] of [
    [
      "language",
      "javascript",
      "Console language must be python, groovy, or null",
    ],
    ["heapSizeGb", 0, "Console heapSizeGb must be a positive number or null"],
    ["server", 1, "Console server must be a string or null"],
    ["engine", false, "Console engine must be a string or null"],
    ["jvmArgs", [1], "Console jvmArgs must be an array of strings or null"],
    [
      "environment",
      { MODE: 1 },
      "Console environment must be an object of string values or null",
    ],
    [
      "autoDeleteTimeoutSeconds",
      0.5,
      "Console autoDeleteTimeoutSeconds must be a positive integer or null",
    ],
  ] as const) {
    expect(() =>
      parseConsoleSettings(JSON.stringify({ [field]: value })),
    ).toThrow(message);
  }
});

test("console settings: effective creation settings reject empty names and invalid numeric overrides", () => {
  for (const patch of [
    { server: "" },
    { engine: "" },
    { heapSizeGb: Number.NaN },
    { heapSizeGb: -1 },
    { autoDeleteTimeoutSeconds: 0.5 },
  ]) {
    expect(() => mergeConsoleSettings(base, {}, patch)).toThrow();
  }
  expect(parseConsoleSettings('{"server":""}')).toEqual({ server: "" });
  expect(mergeConsoleSettings(base, {}, { heapSizeGb: 0.5 }).heapSizeGb).toBe(
    0.5,
  );
});
