import { dlopen, FFIType } from "bun:ffi";
import { closeSync, openSync } from "node:fs";
import { DhError } from "../errors.ts";

const WAIT_STEP_MS = 25;

/** Takes the lock without blocking; returns its release, or undefined if another process holds it. */
type TryLock = (path: string) => (() => void) | undefined;

/** The kernel's lock on an open file: released when the holder closes it or dies, so it can't go stale. */
function posixTryLock(): TryLock {
  const libc = dlopen(
    process.platform === "darwin" ? "libSystem.B.dylib" : "libc.so.6",
    { flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 } },
  );
  const LOCK_EX = 2;
  const LOCK_NB = 4;
  return (path) => {
    const fd = openSync(path, "a");
    if (libc.symbols.flock(fd, LOCK_EX | LOCK_NB) === 0) {
      return () => closeSync(fd);
    }
    closeSync(fd);
    return undefined;
  };
}

/** Windows: open with no sharing, which fails for everyone else until the handle closes. */
function windowsTryLock(): TryLock {
  const kernel32 = dlopen("kernel32.dll", {
    CreateFileW: {
      args: [
        FFIType.ptr,
        FFIType.u32,
        FFIType.u32,
        FFIType.ptr,
        FFIType.u32,
        FFIType.u32,
        FFIType.ptr,
      ],
      returns: FFIType.i64,
    },
    CloseHandle: { args: [FFIType.i64], returns: FFIType.i32 },
    GetLastError: { args: [], returns: FFIType.u32 },
  });
  const GENERIC_READ_WRITE = 0xc0000000;
  const OPEN_ALWAYS = 4;
  const FILE_ATTRIBUTE_NORMAL = 0x80;
  const INVALID_HANDLE_VALUE = -1n;
  const ERROR_SHARING_VIOLATION = 32;
  return (path) => {
    const wide = Buffer.from(`${path}\0`, "utf16le");
    // Share mode 0: until this handle closes, every other open of the file fails.
    const handle = BigInt(
      kernel32.symbols.CreateFileW(
        wide,
        GENERIC_READ_WRITE,
        0,
        null,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        null,
      ),
    );
    if (handle === INVALID_HANDLE_VALUE) {
      const error = kernel32.symbols.GetLastError();
      // Another process has the file open, so it holds the lock.
      if (error === ERROR_SHARING_VIOLATION) return undefined;
      throw new DhError(
        "internal",
        `Could not open ${path} (Windows error ${error})`,
      );
    }
    return () => kernel32.symbols.CloseHandle(handle);
  };
}

let tryLock: TryLock | undefined;

/** Runs `fn` holding an exclusive lock on `path` across processes, waiting up to `waitMs`. */
export async function withFileLock<T>(
  path: string,
  waitMs: number,
  fn: () => Promise<T>,
): Promise<T> {
  tryLock ??= process.platform === "win32" ? windowsTryLock() : posixTryLock();
  const deadline = Date.now() + waitMs;
  let release = tryLock(path);
  while (!release) {
    if (Date.now() > deadline) {
      throw new DhError(
        "internal",
        `Timed out waiting for ${path}`,
        "Another dh process is still updating the config; try again.",
      );
    }
    await new Promise((r) => setTimeout(r, WAIT_STEP_MS + Math.random() * 25));
    release = tryLock(path);
  }
  try {
    return await fn();
  } finally {
    release();
  }
}
