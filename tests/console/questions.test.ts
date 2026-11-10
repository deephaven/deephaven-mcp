import { expect, test } from "bun:test";
import { askConsoleSettings } from "../../src/commands/console/questions.ts";

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
