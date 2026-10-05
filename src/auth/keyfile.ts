import { decodeBase64, encodeBase64 } from "@std/encoding/base64";
import { DhError } from "./errors.ts";

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

function stripEc(value: string, where: string): string {
  const bytes = decodeBase64(value);
  const prefix = new TextDecoder().decode(bytes.subarray(0, EC_PREFIX.length));
  if (prefix !== EC_PREFIX) {
    throw new DhError(
      "credential_unavailable",
      `${where}: unsupported key type (only EC keys are supported)`,
      "Run `dh auth login` to create a new key.",
    );
  }
  return encodeBase64(bytes.subarray(EC_PREFIX.length));
}

/** Parses a Deephaven Enterprise key file (`user`/`operateas`/`public`/`private` lines). */
export function parseKeyFile(text: string, where = "key file"): KeyFile {
  const fields = new Map<string, string>();
  for (const line of text.split(/\r?\n/)) {
    const match = line.trim().match(/^(\S+)\s+(.+)$/);
    if (match) fields.set(match[1].toLowerCase(), match[2].trim());
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

export async function readKeyFile(path: string): Promise<KeyFile> {
  let text: string;
  try {
    text = await Deno.readTextFile(path);
  } catch (e) {
    if (e instanceof Deno.errors.NotFound) {
      throw new DhError(
        "credential_unavailable",
        `Key file not found: ${path}`,
      );
    }
    throw e;
  }
  return parseKeyFile(text, path);
}
