import { expect, test } from "bun:test";
import {
  askConsoleDefaults,
  askConsoleSettings,
} from "../../src/commands/console/questions.ts";

test("console settings: flags override JSON and optional settings are preserved", () => {
  expect(
    askConsoleSettings(undefined, {
      json: JSON.stringify({
        language: "groovy",
        heapSizeGb: 8,
        jvmArgs: ["-Dtest=true"],
      }),
      language: "python",
      heapSizeGb: "12",
    }),
  ).toEqual({
    language: "python",
    heapSizeGb: 12,
    jvmArgs: ["-Dtest=true"],
  });
});

test("console settings: JSON null marks a field for reset", () => {
  expect(
    askConsoleSettings(undefined, {
      json: '{"heapSizeGb":null,"environment":null}',
    }),
  ).toEqual({ heapSizeGb: null, environment: null });
});

test("console settings: invalid and unknown input is rejected", () => {
  for (const json of [
    "not-json",
    "[]",
    '{"unknown":true}',
    '{"heapSizeGb":0}',
  ]) {
    expect(() => askConsoleSettings(undefined, { json })).toThrow();
  }
});

test("console defaults: distinguish show, reset, and explicit empty updates", () => {
  expect(askConsoleDefaults(undefined, {})).toEqual({ action: "show" });
  expect(askConsoleDefaults(undefined, { reset: true })).toEqual({
    action: "reset",
  });
  expect(askConsoleDefaults("{}", {})).toEqual({
    action: "update",
    settings: {},
  });
  expect(askConsoleDefaults(undefined, { json: "{}" })).toEqual({
    action: "update",
    settings: {},
  });
  expect(askConsoleDefaults(undefined, { heapSizeGb: "8" })).toEqual({
    action: "update",
    settings: { heapSizeGb: 8 },
  });
});

test("console defaults: reset conflicts with every settings source", () => {
  for (const options of [
    { json: "{}" },
    { language: "python" },
    { heapSizeGb: "8" },
    { server: "Query 1" },
    { engine: "python" },
  ]) {
    expect(() =>
      askConsoleDefaults(undefined, { ...options, reset: true }),
    ).toThrow("--reset cannot be combined with console settings");
  }
  expect(() => askConsoleDefaults("{}", { reset: true })).toThrow(
    "--reset cannot be combined with console settings",
  );
});

test("console settings: --json takes precedence over positional JSON", () => {
  expect(
    askConsoleSettings('{"heapSizeGb":4}', { json: '{"heapSizeGb":8}' }),
  ).toEqual({ heapSizeGb: 8 });
});
