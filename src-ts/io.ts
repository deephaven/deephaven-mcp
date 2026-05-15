/**
 * Async file I/O utilities for deephaven-mcp.
 *
 * This module provides asynchronous I/O helpers for the Deephaven MCP project, including
 * coroutine-safe file reading for sensitive binary files such as TLS certificates and private keys.
 *
 * Features:
 * - Asynchronous, non-blocking binary file loading.
 * - Designed for use in session configuration and secure credential loading.
 * - Centralizes I/O logic for easier testing and patching in unit tests.
 *
 * @example
 * ```typescript
 * import { loadBytes } from "./io.js";
 * const certBytes = await loadBytes("/path/to/cert.pem");
 * ```
 */

import { readFile } from "node:fs/promises";
import pino from "pino";

const _logger = pino({ name: "deephaven-mcp:io" });

/**
 * Asynchronously load the contents of a binary file.
 *
 * This helper is used to read certificate and private key files for secure Deephaven session creation.
 * It is designed to be non-blocking using Node.js native async file I/O.
 *
 * @param path - Path to the file to load. If `undefined`, returns `undefined`.
 * @returns The contents of the file as a `Uint8Array`, or `undefined` if the path is `undefined`.
 * @throws Any exceptions encountered during file I/O (e.g., file not found, permission denied).
 *
 * @example
 * ```typescript
 * const certBytes = await loadBytes("/path/to/cert.pem");
 * if (certBytes !== undefined) {
 *   // Use certBytes for TLS configuration
 * }
 * ```
 */
export async function loadBytes(path: string | undefined): Promise<Uint8Array | undefined> {
  _logger.info(`Loading binary file: ${path}`);
  if (path === undefined) {
    return undefined;
  }
  try {
    const buf = await readFile(path);
    return new Uint8Array(buf);
  } catch (e) {
    _logger.error(`Error loading binary file ${path}: ${e}`);
    throw e;
  }
}
