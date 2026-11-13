/**
 * Reads a tar stream (ustar, as `tar -cf - .` writes it) without a `tar`
 * command, for the `shell/` copy over SSH. Regular files and directories
 * only.
 */
import { insidePart } from "./bundle.ts";

/** The largest stream accepted. */
export const MAX_TAR_BYTES = 64 * 1024 * 1024;

const BLOCK = 512;

/** Why a tar stream was rejected. */
export class TarError extends Error {
  override readonly name = "TarError";
}

/**
 * The regular files in `bytes`, by normalized relative path. A link, a
 * device, an absolute path, a `..` component, or more than
 * {@link MAX_TAR_BYTES} rejects the whole stream.
 */
export function readTar(bytes: Uint8Array): Map<string, Uint8Array> {
  if (bytes.length > MAX_TAR_BYTES) {
    throw new TarError(`more than ${MAX_TAR_BYTES / 1024 / 1024} MiB`);
  }
  const files = new Map<string, Uint8Array>();
  let at = 0;
  while (at + BLOCK <= bytes.length) {
    const header = bytes.subarray(at, at + BLOCK);
    if (header.every((b) => b === 0)) break;
    checkChecksum(header);
    const name = joinName(text(header, 345, 155), text(header, 0, 100));
    const size = octal(header, 124, 12);
    const type = String.fromCharCode(header[156] ?? 0);
    at += BLOCK;
    const isFile = type === "0" || type === "\0";
    if (!isFile && type !== "5") {
      throw new TarError(`${name}: not a regular file or directory`);
    }
    const path = name === "." || name === "./" ? "." : insidePart(name);
    if (path === undefined) throw new TarError(`${name}: outside the bundle`);
    if (isFile) {
      if (at + size > bytes.length) throw new TarError(`${name}: truncated`);
      files.set(path, bytes.slice(at, at + size));
    }
    at += Math.ceil(size / BLOCK) * BLOCK;
  }
  return files;
}

/** The header's checksum field matches its bytes (the field itself read as spaces). */
function checkChecksum(header: Uint8Array): void {
  let sum = 0;
  for (let i = 0; i < BLOCK; i++) {
    sum += i >= 148 && i < 156 ? 0x20 : (header[i] ?? 0);
  }
  if (sum !== octal(header, 148, 8)) throw new TarError("bad header checksum");
}

function joinName(prefix: string, name: string): string {
  return prefix ? `${prefix}/${name}` : name;
}

/** The NUL-terminated text field at `[start, start + length)`. */
function text(header: Uint8Array, start: number, length: number): string {
  const field = header.subarray(start, start + length);
  const end = field.indexOf(0);
  return new TextDecoder().decode(end < 0 ? field : field.subarray(0, end));
}

/** The octal number field at `[start, start + length)`. */
function octal(header: Uint8Array, start: number, length: number): number {
  const value = text(header, start, length).trim();
  if (!/^[0-7]*$/.test(value)) throw new TarError("bad header number");
  return value === "" ? 0 : Number.parseInt(value, 8);
}
