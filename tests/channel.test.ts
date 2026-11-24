import { describe, expect, test } from "bun:test";
import { resolve } from "node:path";
import {
  channelError,
  nextVersion,
  notesStartTag,
  parseTag,
} from "../src/channel.ts";

describe("channelError", () => {
  test.each(["release", "rc", "alpha-bar", "a1", "a".repeat(40)])(
    "accepts %s",
    (name) => expect(channelError(name)).toBeUndefined(),
  );
  test.each(["", "Rc", "-x", "1a", "a_b", "a.b", "a".repeat(41)])(
    "rejects %p",
    (name) => expect(channelError(name)).toContain("Invalid channel"),
  );
  test("reserves dev", () => {
    expect(channelError("dev")).toContain("reserved");
  });
});

describe("[channels-pub-1] parseTag", () => {
  test("release tag", () => {
    expect(parseTag("cli-v3.1.0")).toEqual({
      tag: "cli-v3.1.0",
      version: "3.1.0",
      base: "3.1.0",
      channel: "release",
    });
  });
  test("prerelease tag splits on the last dot", () => {
    expect(parseTag("cli-v3.1.1-alpha-1.0")).toEqual({
      tag: "cli-v3.1.1-alpha-1.0",
      version: "3.1.1-alpha-1.0",
      base: "3.1.1",
      channel: "alpha-1",
      build: 0,
    });
  });
  test.each([
    "v2.1.0",
    "cli-vfoo",
    "cli-v3.1",
    "cli-v03.1.0",
    "cli-v03.1.1-alpha-1.0",
    "cli-v3.01.1-alpha-1.0",
    "cli-v3.1.0-rc",
    "cli-v3.1.0-rc.01",
    "cli-v3.1.0-Rc.0",
    "cli-v3.1.0-dev.0",
    "cli-v3.1.0-release.0",
    "cli-channel-rc",
  ])("ignores %s", (tag) => expect(parseTag(tag)).toBeUndefined());
});

describe("[channels-pub-1] nextVersion", () => {
  test.each([
    [[], "3.0.0", "release", "3.0.0"],
    [[], "3.0.0", "rc", "3.0.0-rc.0"],
    [["cli-v3.1.0"], "3.1.0", "rc", "3.1.1-rc.0"],
    [["cli-v3.1.0", "cli-v3.1.1-rc.0"], "3.1.0", "rc", "3.1.1-rc.1"],
    [
      ["cli-v3.1.0", "cli-v3.1.1-rc.1"],
      "3.1.0",
      "alpha-bar",
      "3.1.1-alpha-bar.0",
    ],
    [["cli-v3.1.0", "cli-v3.1.1-rc.1"], "3.1.0", "release", "3.1.1"],
    [["cli-v3.1.1"], "3.1.0", "rc", "3.1.2-rc.0"],
    [["cli-v3.1.1"], "3.2.0", "rc", "3.2.0-rc.0"],
    [["cli-v3.2.0"], "3.2.0", "rc", "3.2.1-rc.0"],
    [["cli-v3.2.0"], "3.1.0", "alpha-bar", "3.2.1-alpha-bar.0"],
    [
      ["v2.1.0", "cli-vfoo", "cli-v03.1.1-rc.0", "cli-v3.1.0"],
      "3.1.0",
      "rc",
      "3.1.1-rc.0",
    ],
    [
      ["cli-v3.1.1-rc.0", "cli-v3.1.1-rc.4", "cli-v3.1.1-rc.2"],
      "3.1.1",
      "rc",
      "3.1.1-rc.5",
    ],
    [["cli-v3.1.1-rc.3"], "3.2.0", "rc", "3.2.0-rc.0"],
  ] as [string[], string, string, string][])(
    "%p, package.json %s, %s → %s",
    (tags, pkg, channel, expected) => {
      expect(nextVersion(tags, pkg, channel)).toBe(expected);
    },
  );

  test("rejects a prerelease package.json version", () => {
    expect(() => nextVersion([], "3.2.0-beta", "rc")).toThrow("plain X.Y.Z");
  });
  test.each(["Rc", "dev", "-x"])("rejects channel %s", (channel) => {
    expect(() => nextVersion([], "3.0.0", channel)).toThrow();
  });
});

describe("[channels-pub-2] notesStartTag", () => {
  test.each([
    [["cli-v3.1.0"], "cli-v3.1.1", "cli-v3.1.0"],
    [["cli-v3.1.0", "cli-v3.1.1-rc.0"], "cli-v3.1.1-rc.1", "cli-v3.1.1-rc.0"],
    [["cli-v3.1.0"], "cli-v3.1.1-rc.0", "cli-v3.1.0"],
    [
      ["cli-v3.0.0", "cli-v3.1.0", "cli-v3.1.1-rc.0"],
      "cli-v3.1.1",
      "cli-v3.1.0",
    ],
    [["cli-v3.1.1-rc.0"], "cli-v3.1.1-alpha-bar.0", undefined],
    [[], "cli-v3.0.0", undefined],
  ] as [string[], string, string | undefined][])(
    "%p, %s → %s",
    (tags, tag, expected) => {
      expect(notesStartTag([...tags, tag], tag)).toBe(expected);
    },
  );
});

describe("[channels-pub-1] next-version script", () => {
  const script = resolve(import.meta.dir, "..", "scripts", "next-version.ts");
  test("prints a bare version", () => {
    const proc = Bun.spawnSync([process.execPath, script, "rc"]);
    expect(proc.exitCode).toBe(0);
    expect(proc.stdout.toString()).toMatch(/^\d+\.\d+\.\d+-rc\.\d+\n$/);
  });
  test("fails on a reserved channel", () => {
    const proc = Bun.spawnSync([process.execPath, script, "dev"]);
    expect(proc.exitCode).toBe(1);
    expect(proc.stderr.toString()).toContain("reserved");
  });
});
