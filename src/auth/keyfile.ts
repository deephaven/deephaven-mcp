import { readFile } from "node:fs/promises";
import { DhError, isErrno } from "../errors.ts";

export interface KeyPair {
  type: string;
  publicKey: string;
  privateKey: string;
}

export interface KeyFile {
  user?: string;
  operateAs?: string;
  keyPair: KeyPair;
}

const EC_PREFIX = "EC:";

/** Removes the `EC:` prefix Deephaven key files put inside the base64; `where` names the file in errors. */
function stripEc(value: string, where: string): string {
  const bytes = Buffer.from(value, "base64");
  const prefix = bytes.subarray(0, EC_PREFIX.length).toString("latin1");
  if (prefix !== EC_PREFIX) {
    throw new DhError(
      "credential_unavailable",
      `${where}: unsupported key type (only EC keys are supported)`,
      "Run `dh auth login` to create a new key.",
    );
  }
  return bytes.subarray(EC_PREFIX.length).toString("base64");
}

/** Parses a Deephaven Enterprise key file (`user`/`operateas`/`public`/`private` lines). */
export function parseKeyFile(text: string, where = "key file"): KeyFile {
  const fields = new Map<string, string>();
  for (const line of text.split(/\r?\n/)) {
    const [, key, value] = line.trim().match(/^(\S+)\s+(.+)$/) ?? [];
    if (key && value) fields.set(key.toLowerCase(), value.trim());
  }
  const pub = fields.get("public");
  const priv = fields.get("private");
  if (!pub || !priv) {
    throw new DhError(
      "credential_unavailable",
      `${where}: missing "public" or "private" line`,
    );
  }
  return {
    user: fields.get("user"),
    operateAs: fields.get("operateas"),
    keyPair: {
      type: "ec",
      publicKey: stripEc(pub, where),
      privateKey: stripEc(priv, where),
    },
  };
}

/** {@link parseKeyFile} on the file at `path`; a missing file is `credential_unavailable`. */
export async function readKeyFile(path: string): Promise<KeyFile> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (e) {
    if (isErrno(e, "ENOENT")) {
      throw new DhError(
        "credential_unavailable",
        `Key file not found: ${path}`,
      );
    }
    throw e;
  }
  return parseKeyFile(text, path);
}
