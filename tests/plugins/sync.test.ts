import { expect, test } from "bun:test";
import { jsBase } from "../../src/actions/plugins/sync.ts";

test("jsBase keeps only the server's origin", () => {
  expect(
    jsBase("https://alice:password@example.com:8123/path?token=secret#fragment")
      .href,
  ).toBe("https://example.com:8123/iriside/cli-plugins/");
});
