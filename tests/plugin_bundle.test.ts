import { describe, expect, test } from "bun:test";
import {
  digestOf,
  insidePart,
  readManifest,
  revisionOf,
} from "../src/actions/plugins/bundle.ts";
import { readTar, TarError } from "../src/actions/plugins/tar.ts";

/** One ustar entry: a 512-byte header, then the data padded to 512. */
function entry(name: string, data: string, type = "0"): Uint8Array {
  const header = new Uint8Array(512);
  const put = (at: number, text: string) =>
    header.set(new TextEncoder().encode(text), at);
  const bytes = new TextEncoder().encode(data);
  put(0, name);
  put(100, "0000644\0");
  put(124, `${bytes.length.toString(8).padStart(11, "0")}\0`);
  put(156, type);
  put(257, "ustar\0");
  put(263, "00");
  header.fill(0x20, 148, 156);
  const sum = header.reduce((a, b) => a + b, 0);
  put(148, `${sum.toString(8).padStart(6, "0")}\0 `);
  const padded = new Uint8Array(Math.ceil(bytes.length / 512) * 512);
  padded.set(bytes);
  return concat(header, padded);
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

const end = new Uint8Array(1024);
const text = (b: Uint8Array | undefined) => new TextDecoder().decode(b);

describe("readTar", () => {
  test("reads regular files and skips directories, normalizing ./", () => {
    const files = readTar(
      concat(
        entry("./", "", "5"),
        entry("./revision", "sha256:x\n"),
        entry("./sub/", "", "5"),
        entry("./sub/a.json", "{}"),
        end,
      ),
    );
    expect([...files.keys()]).toEqual(["revision", "sub/a.json"]);
    expect(text(files.get("sub/a.json"))).toBe("{}");
  });

  test("a link, an absolute path or a .. rejects the whole stream", () => {
    for (const bad of [
      entry("./link", "", "2"),
      entry("./hard", "", "1"),
      entry("/etc/passwd", "x"),
      entry("./../x", "x"),
      entry("a/../../x", "x"),
    ]) {
      expect(() => readTar(concat(entry("ok", "1"), bad, end))).toThrow(
        TarError,
      );
    }
  });

  test("a bad checksum or a truncated file rejects it", () => {
    const e = entry("a", "data");
    e[0] = "b".charCodeAt(0);
    expect(() => readTar(concat(e, end))).toThrow("checksum");
    expect(() => readTar(entry("a", "data").subarray(0, 514))).toThrow(
      "truncated",
    );
  });
});

describe("the manifest", () => {
  const plugin = (extra: Record<string, unknown> = {}) => ({
    name: "ctl",
    kind: "shell",
    description: "D.",
    nouns: ["ctl"],
    spec: "ctl.json",
    digest: digestOf(["x"]),
    dh: ">=3.0.0",
    ...extra,
  });
  const manifest = (plugins: unknown[], extra = {}) => ({
    schema: 1,
    channel: "shell",
    revision: revisionOf([]),
    cluster: { kind: "enterprise", version: "v" },
    plugins,
    ...extra,
  });

  test("a bad entry is left out with its reason; the rest are kept", () => {
    const read = readManifest(
      manifest([
        plugin(),
        plugin({ name: "up", spec: "../up.json" }),
        plugin({ name: "abs", spec: "/etc/x.json" }),
        plugin({ name: "js", kind: "js" }),
        plugin({ name: "range", dh: "not a range" }),
      ]),
      "shell",
    );
    if (!read.ok) throw new Error(read.reason);
    expect(read.manifest.plugins.map((p) => p.name)).toEqual(["ctl"]);
    expect(read.problems.map((p) => p.name)).toEqual([
      "up",
      "abs",
      "js",
      "range",
    ]);
  });

  test("another channel is invalid; another schema is unsupported", () => {
    expect(readManifest(manifest([]), "js")).toMatchObject({
      ok: false,
      status: "invalid",
    });
    expect(readManifest(manifest([], { schema: 2 }), "shell")).toMatchObject({
      ok: false,
      status: "unsupported",
    });
  });

  test("paths must stay inside the part once normalized", () => {
    expect(insidePart("a/./b.json")).toBe("a/b.json");
    expect(insidePart("a/../b.json")).toBe("b.json");
    for (const bad of ["", ".", "..", "../a", "a/../../b", "/a", "a\\b", 3]) {
      expect(insidePart(bad)).toBeUndefined();
    }
  });

  test("the revision covers the plugins' digests and, for js, the shell list", () => {
    const plugins = [
      { name: "b", digest: "sha256:2" },
      { name: "a", digest: "sha256:1" },
    ];
    expect(revisionOf(plugins)).toBe(digestOf(["a:sha256:1\nb:sha256:2\n"]));
    expect(revisionOf([], [{ name: "ctl", nouns: ["ctl", "x"] }])).toBe(
      digestOf(["shell:ctl:ctl,x\n"]),
    );
  });
});
