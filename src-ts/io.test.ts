/**
 * Tests for io module.
 */
import { describe, it, expect } from "vitest";
import { loadBytes } from "./io.js";
import { writeFile, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

describe("loadBytes", () => {
  it("reads_file", async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), "dh-test-"));
    try {
      const filePath = join(tmpDir, "cert.pem");
      const content = Buffer.from("test-bytes");
      await writeFile(filePath, content);
      const result = await loadBytes(filePath);
      expect(result).toEqual(new Uint8Array(content));
    } finally {
      await rm(tmpDir, { recursive: true });
    }
  });

  it("none_returns_undefined", async () => {
    const result = await loadBytes(undefined);
    expect(result).toBeUndefined();
  });

  it("missing_file_throws", async () => {
    await expect(loadBytes("/tmp/does_not_exist_dh_test.pem")).rejects.toThrow(Error);
  });
});
