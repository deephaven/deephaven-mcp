import { expect, test } from "bun:test";
import { candidates, resolveServer } from "../../src/auth/resolve.ts";

const origins = (input: string) => candidates(input).map((u) => u.origin);

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
