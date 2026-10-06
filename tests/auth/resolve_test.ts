import { assertEquals, assertRejects } from "@std/assert";
import { DhError } from "../../src/auth/errors.ts";
import { candidates, resolveServer } from "../../src/auth/resolve.ts";

const origins = (input: string) => candidates(input).map((u) => u.origin);

Deno.test("candidates: bare host tries https ports", () => {
  assertEquals(origins("dhe.example.com"), [
    "https://dhe.example.com",
    "https://dhe.example.com:8000",
    "https://dhe.example.com:8123",
  ]);
});

Deno.test("candidates: a pasted browser URL keeps its port and drops the path", () => {
  assertEquals(origins("https://dhe.example.com:8123/iriside/?a=b#c"), [
    "https://dhe.example.com:8123",
  ]);
  assertEquals(origins("dhe.example.com/iriside/"), origins("dhe.example.com"));
});

Deno.test("candidates: localhost defaults to http and the Community port first", () => {
  assertEquals(origins("localhost"), [
    "http://localhost:10000",
    "http://localhost:8000",
    "http://localhost:8123",
    "http://localhost",
  ]);
  assertEquals(origins("localhost:10123"), ["http://localhost:10123"]);
});

Deno.test("candidates: an explicit default port is tried alone", () => {
  assertEquals(origins("https://dhe.example.com:443"), [
    "https://dhe.example.com",
  ]);
  assertEquals(origins("dhe.example.com:443"), ["https://dhe.example.com"]);
  assertEquals(origins("http://dhc.example.com:80"), [
    "http://dhc.example.com",
  ]);
  assertEquals(origins("localhost:80"), ["http://localhost"]);
});

Deno.test("candidates: explicit http stays http; https is never downgraded", () => {
  assertEquals(
    origins("http://dhc.example.com")[0],
    "http://dhc.example.com:10000",
  );
  for (const o of origins("https://dhe.example.com")) {
    assertEquals(new URL(o).protocol, "https:");
  }
});

Deno.test("candidates: rejects other schemes", () => {
  try {
    candidates("ftp://dhe.example.com");
    throw new Error("accepted ftp");
  } catch (e) {
    assertEquals((e as DhError).code, "usage");
  }
});

function fakeServer(paths: string[]) {
  return Deno.serve(
    { hostname: "127.0.0.1", port: 0, onListen() {} },
    (req) =>
      paths.includes(new URL(req.url).pathname)
        ? new Response("ok")
        : new Response("missing", { status: 404 }),
  );
}

Deno.test("resolveServer: detects Enterprise and Community", async () => {
  const dhe = fakeServer(["/iris/connection.json"]);
  const dhc = fakeServer(["/jsapi/dh-core.js"]);
  try {
    assertEquals(await resolveServer(`127.0.0.1:${dhe.addr.port}`), {
      origin: `http://127.0.0.1:${dhe.addr.port}`,
      kind: "enterprise",
    });
    assertEquals(
      (await resolveServer(`http://127.0.0.1:${dhc.addr.port}/ide/`)).kind,
      "community",
    );
  } finally {
    await dhe.shutdown();
    await dhc.shutdown();
  }
});

Deno.test("resolveServer: a non-Deephaven server is server_unreachable", async () => {
  const other = fakeServer([]);
  try {
    const e = await assertRejects(
      () => resolveServer(`127.0.0.1:${other.addr.port}`),
      DhError,
    );
    assertEquals(e.code, "server_unreachable");
  } finally {
    await other.shutdown();
  }
});
