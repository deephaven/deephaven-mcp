import { expect, test } from "bun:test";
import {
  candidates,
  displayServerAddress,
  resolveServer,
  withPort,
} from "../../src/actions/auth/resolve.ts";

const origins = (input: string) => candidates(input).map((u) => u.origin);

test("displayServerAddress: only a supported address and port are retained", () => {
  for (const [input, expected] of [
    [
      "https://alice:password@example.com:8123/path?token=secret#fragment",
      "https://example.com:8123",
    ],
    [
      "alice:password@example.com:8123/path?token=secret#fragment",
      "example.com:8123",
    ],
    ["localhost:10000/path", "localhost:10000"],
    ["localhost:443/path", "localhost:443"],
    ["http://[::1]:10000/path", "http://[::1]:10000"],
    ["[::1]:8123", "[::1]:8123"],
    ["ftp://alice:password@example.com/path", undefined],
    ["https://alice:password@[broken/?token=secret", undefined],
    ["", undefined],
  ] as const) {
    expect(displayServerAddress(input)).toBe(expected);
  }
});

test("candidates: malformed addresses do not appear in errors", () => {
  for (const address of [
    "https://alice:password@[broken/?token=secret#fragment",
    "https://alice:password@example.com\\path?token=secret",
  ]) {
    try {
      candidates(address);
      throw new Error("expected invalid address to fail");
    } catch (error) {
      expect(error).toMatchObject({
        code: "usage",
        message: "Not a server address",
      });
    }
  }
});

test("candidates: bare host tries https ports", () => {
  expect(origins("dhe.example.com")).toEqual([
    "https://dhe.example.com",
    "https://dhe.example.com:8000",
    "https://dhe.example.com:8123",
  ]);
});

test("candidates: a pasted browser URL keeps its port and drops the path", () => {
  expect(origins("https://dhe.example.com:8123/iriside/?a=b#c")).toEqual([
    "https://dhe.example.com:8123",
  ]);
  expect(origins("dhe.example.com/iriside/")).toEqual(
    origins("dhe.example.com"),
  );
});

test("candidates: localhost defaults to http and the Community port first", () => {
  expect(origins("localhost")).toEqual([
    "http://localhost:10000",
    "http://localhost:8000",
    "http://localhost:8123",
    "http://localhost",
  ]);
  expect(origins("localhost:10123")).toEqual(["http://localhost:10123"]);
});

test("candidates: an explicit default port is tried alone", () => {
  expect(origins("https://dhe.example.com:443")).toEqual([
    "https://dhe.example.com",
  ]);
  expect(origins("dhe.example.com:443")).toEqual(["https://dhe.example.com"]);
  expect(origins("http://dhc.example.com:80")).toEqual([
    "http://dhc.example.com",
  ]);
  expect(origins("localhost:80")).toEqual(["http://localhost"]);
  expect(origins("localhost:443")).toEqual(["http://localhost:443"]);
  expect(origins(withPort("http://localhost"))).toEqual(["http://localhost"]);
  expect(origins(withPort("https://[::1]:8123"))).toEqual([
    "https://[::1]:8123",
  ]);
});

test("candidates: explicit http stays http; https is never downgraded", () => {
  expect(origins("http://dhc.example.com")[0]).toBe(
    "http://dhc.example.com:10000",
  );
  for (const o of origins("https://dhe.example.com")) {
    expect(new URL(o).protocol).toBe("https:");
  }
});

test("candidates: rejects other schemes", () => {
  expect(() => candidates("ftp://dhe.example.com")).toThrow(
    expect.objectContaining({ code: "usage" }),
  );
});

function fakeServer(paths: string[]) {
  return Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: (req) =>
      paths.includes(new URL(req.url).pathname)
        ? new Response("ok")
        : new Response("missing", { status: 404 }),
  });
}

test("resolveServer: detects Enterprise and Community", async () => {
  const dhe = fakeServer(["/iris/connection.json"]);
  const dhc = fakeServer(["/jsapi/dh-core.js"]);
  try {
    expect(await resolveServer(`127.0.0.1:${dhe.port}`)).toEqual({
      origin: `http://127.0.0.1:${dhe.port}`,
      kind: "enterprise",
    });
    expect(
      (await resolveServer(`http://127.0.0.1:${dhc.port}/ide/`)).kind,
    ).toBe("community");
  } finally {
    await dhe.stop(true);
    await dhc.stop(true);
  }
});

test("resolveServer: a non-Deephaven server is server_unreachable", async () => {
  const other = fakeServer([]);
  try {
    await expect(
      resolveServer(`127.0.0.1:${other.port}`),
    ).rejects.toMatchObject({ code: "server_unreachable" });
  } finally {
    await other.stop(true);
  }
});

test("resolveServer: failure reports exclude URL credentials and data", async () => {
  const other = fakeServer([]);
  try {
    await expect(
      resolveServer(
        `http://alice:password@127.0.0.1:${other.port}/path?token=secret#fragment`,
      ),
    ).rejects.toMatchObject({
      code: "server_unreachable",
      headline: `No Deephaven server found at http://127.0.0.1:${other.port}`,
    });
  } finally {
    await other.stop(true);
  }
});

test("resolveServer: a stalled Enterprise probe doesn't hide Community", async () => {
  const dhc = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: (req) =>
      new URL(req.url).pathname === "/jsapi/dh-core.js"
        ? new Response("ok")
        : new Promise<Response>(() => {}),
  });
  try {
    expect((await resolveServer(`127.0.0.1:${dhc.port}`, 300)).kind).toBe(
      "community",
    );
  } finally {
    await dhc.stop(true);
  }
});
