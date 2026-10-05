import { DhError } from "../auth/errors.ts";

export interface GlobalOptions {
  profile?: string;
  output?: "human" | "json";
  input?: boolean;
}

/** Cliffy types a subcommand's options without the root's global options. */
export function globals(options: unknown): GlobalOptions {
  return options as GlobalOptions;
}

let stdinUsed = false;

async function readStdin(flag: string): Promise<string> {
  if (stdinUsed) {
    throw new DhError("usage", `Only one --*-stdin flag can be used (${flag})`);
  }
  stdinUsed = true;
  const text = await new Response(Deno.stdin.readable).text();
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
    const value = Deno.env.get(env);
    if (!value) throw new DhError("usage", `--${name}-env: ${env} is not set`);
    return { value, env };
  }
  return { value: await prompt() };
}
