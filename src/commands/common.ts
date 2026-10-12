import { DhError } from "../auth/errors.ts";

export interface GlobalOptions {
  profile?: string;
  output?: "human" | "json";
  /** False with `--no-prompt`. */
  prompt?: boolean;
  /** False with `--no-input`, the hidden older name of `--no-prompt`. */
  input?: boolean;
}

export function promptsAllowed(g: GlobalOptions): boolean {
  return g.prompt !== false && g.input !== false;
}

/** Commands receive `optsWithGlobals()`, which includes the root's options. */
export function globals(options: unknown): GlobalOptions {
  return options as GlobalOptions;
}

/** `--profile`, then `DH_PROFILE`. */
export function profileName(g: GlobalOptions): string | undefined {
  return g.profile ?? (process.env.DH_PROFILE || undefined);
}

let stdinUsed = false;

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

export interface SecretSource {
  /** Value, if supplied by flag or prompt. */
  value: string;
  /** Env var name, if the secret came from `--*-env`. */
  env?: string;
}

/** `--<name>-stdin`, then `--<name>-env <VAR>`, then a masked prompt. */
export async function secretFrom(
  name: string,
  stdin: boolean | undefined,
  env: string | undefined,
  prompt: () => Promise<string>,
): Promise<SecretSource> {
  if (stdin) return { value: await readStdin(`--${name}-stdin`) };
  if (env) {
    const value = process.env[env];
    if (!value) throw new DhError("usage", `--${name}-env: ${env} is not set`);
    return { value, env };
  }
  return { value: await prompt() };
}
