import { DhError } from "../errors.ts";

let stdinUsed = false;

/** All of stdin, minus one trailing newline. Only one `--*-stdin` flag per run can use it. */
async function readStdin(flag: string): Promise<string> {
  if (stdinUsed) {
    throw new DhError("usage", `Only one --*-stdin flag can be used (${flag})`);
  }
  stdinUsed = true;
  const text = await Bun.stdin.text();
  const value = text.replace(/\r?\n$/, "");
  if (!value) throw new DhError("usage", `${flag}: stdin was empty`);
  return value;
}

/** A secret read from a flag or prompt. */
export interface Secret {
  value: string;
  /** Env var name, if the secret came from `--*-env`. */
  env?: string;
}

/** `--<name>-stdin`, then `--<name>-env <VAR>`, then `ask` (a masked prompt). */
export async function readSecret(
  name: string,
  stdin: boolean | undefined,
  env: string | undefined,
  ask: () => Promise<string>,
): Promise<Secret> {
  if (stdin) return { value: await readStdin(`--${name}-stdin`) };
  if (env) {
    const value = process.env[env];
    if (!value) throw new DhError("usage", `--${name}-env: ${env} is not set`);
    return { value, env };
  }
  return { value: await ask() };
}
