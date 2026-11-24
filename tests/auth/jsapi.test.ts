import { expect, test } from "bun:test";
import { resolve } from "node:path";
import { RUNTIME_FLAGS } from "../../scripts/build.ts";
import {
  assertTrusted,
  loadCorePlusApi,
} from "../../src/actions/auth/jsapi.ts";
import { run, tempDir } from "../helpers.ts";

test("assertTrusted: https anywhere, http only to this machine, nothing else", () => {
  expect(() => assertTrusted(new URL("https://dh.example.com"))).not.toThrow();
  expect(() => assertTrusted(new URL("http://localhost:10000"))).not.toThrow();
  expect(() => assertTrusted(new URL("http://dh.example.com"))).toThrow();
  expect(() => assertTrusted(new URL("ftp://localhost/dh-core.js"))).toThrow();
});

test("Core+ API loader rejects untrusted or invalid URLs", async () => {
  await expect(
    loadCorePlusApi(
      "http://remote.example.com/jsapi/dh-core.js",
      "/unused",
      100,
    ),
  ).rejects.toMatchObject({ code: "server_unsupported" });
  await expect(
    loadCorePlusApi("https://remote.example.com/not-jsapi.js", "/unused", 100),
  ).rejects.toMatchObject({ code: "server_unsupported" });
});

test("Enterprise Core+ manager respects CLI logging after API initialization", async () => {
  const cache = await tempDir("dh-core-log-");
  const fixture = resolve(
    import.meta.dir,
    "../console/core_logging_fixture.ts",
  );
  for (const value of [undefined, "", "0", "1", "other"]) {
    const result = await run(
      [process.execPath, ...RUNTIME_FLAGS, fixture, cache],
      { DH_DEBUG: value },
    );
    expect(result.code, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({ ok: true });
    if (value === "1" || value === "other") {
      expect(result.stderr).toContain("[EnterpriseCorePlusManager]");
    } else {
      expect(result.stderr).toBe("");
    }
    expect(result.stderr).not.toContain("worker-token");
  }
});

test("Core+ API loader retains proxy prefix and isolates server paths", async () => {
  const paths: string[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const path = new URL(request.url).pathname;
      paths.push(path);
      if (path.endsWith("/dh-core.js")) {
        return new Response(
          `export default { marker: ${JSON.stringify(path)} };`,
        );
      }
      return new Response("", { status: 404 });
    },
  });
  try {
    const cache = await tempDir("dh-core-loader-");
    const base = `http://127.0.0.1:${server.port}`;
    const script = `
      import Log from "@deephaven/log";
      import { loadCorePlusApi } from ${JSON.stringify(new URL("../../src/actions/auth/jsapi.ts", import.meta.url).href)};
      const logger = Log.module("before-load");
      const one = await loadCorePlusApi(${JSON.stringify(`${base}/one/jsapi/dh-core.js`)}, ${JSON.stringify(cache)}, 5000);
      const two = await loadCorePlusApi(${JSON.stringify(`${base}/two/jsapi/dh-core.js`)}, ${JSON.stringify(cache)}, 5000);
      logger.info("must not corrupt JSON output");
      process.stdout.write(JSON.stringify([one.marker, two.marker]));
    `;
    const result = await run(
      [process.execPath, ...RUNTIME_FLAGS, "-e", script],
      { DH_DEBUG: undefined },
    );
    expect(result.code).toBe(0);
    expect(result.stderr).toBe("");
    expect(JSON.parse(result.stdout)).toEqual([
      "/one/jsapi/dh-core.js",
      "/two/jsapi/dh-core.js",
    ]);
    expect(paths).toEqual([
      "/one/jsapi/dh-core.js",
      "/one/jsapi/dh-internal.js",
      "/two/jsapi/dh-core.js",
      "/two/jsapi/dh-internal.js",
    ]);

    const debugScript = `
      import Log, { LoggerLevel } from "@deephaven/log";
      import { loadCorePlusApi } from ${JSON.stringify(new URL("../../src/actions/auth/jsapi.ts", import.meta.url).href)};
      Log.setLogLevel(LoggerLevel.DEBUG);
      const logger = Log.module("before-load");
      await loadCorePlusApi(${JSON.stringify(`${base}/one/jsapi/dh-core.js`)}, ${JSON.stringify(cache)}, 5000);
      logger.info("visible diagnostic");
      logger.debug("secret-token");
      process.stdout.write("ok");
    `;
    const debug = await run(
      [process.execPath, ...RUNTIME_FLAGS, "-e", debugScript],
      { DH_DEBUG: "1" },
    );
    expect(debug.code).toBe(0);
    expect(debug.stdout).toBe("ok");
    expect(debug.stderr).toContain("visible diagnostic");
    expect(debug.stderr).not.toContain("secret-token");
  } finally {
    await server.stop(true);
  }
});
