import { DhError } from "../auth/errors.ts";

export interface Choice<T> {
  label: string;
  value: T;
  hint?: string;
}

/** Every prompt names the flag that answers it without a TTY. */
export interface Prompter {
  readonly interactive: boolean;
  select<T>(message: string, choices: Choice<T>[], flag: string): Promise<T>;
  multiSelect<T>(
    message: string,
    choices: Choice<T>[],
    flag: string,
  ): Promise<T[]>;
  confirm(message: string, initial: boolean, flag: string): Promise<boolean>;
  text(message: string, flag: string): Promise<string>;
  secret(message: string, flag: string): Promise<string>;
}

function missing(flag: string): Promise<never> {
  return Promise.reject(
    new DhError(
      "usage",
      `Input needed: pass ${flag}`,
      "Prompts are off without a TTY or with --no-input.",
    ),
  );
}

const noInput: Prompter = {
  interactive: false,
  select: (_m, _c, flag) => missing(flag),
  multiSelect: (_m, _c, flag) => missing(flag),
  confirm: (_m, _i, flag) => missing(flag),
  text: (_m, flag) => missing(flag),
  secret: (_m, flag) => missing(flag),
};

/** Prompts render on stderr, so stdout stays clean for `-o json`. */
export function canPrompt(inputEnabled: boolean): boolean {
  return inputEnabled && Deno.stdin.isTerminal() && Deno.stderr.isTerminal();
}

export async function getPrompter(inputEnabled: boolean): Promise<Prompter> {
  if (!canPrompt(inputEnabled)) return noInput;
  const { inkPrompter } = await import("./ink.tsx");
  return inkPrompter;
}
