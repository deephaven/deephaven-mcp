import { DhError } from "../errors.ts";

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

/** The `usage` error a question raises when it can't prompt: names `flag` to pass instead. */
function missing(flag: string): Promise<never> {
  return Promise.reject(
    new DhError(
      "usage",
      `Input needed: pass ${flag}`,
      "Prompts are off without a TTY or with --no-prompt.",
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
  return inputEnabled && Boolean(process.stdin.isTTY && process.stderr.isTTY);
}

/** Ink prompts when possible, loading Ink only when the first question is asked. */
export function lazyPrompter(inputEnabled: boolean): Prompter {
  if (!canPrompt(inputEnabled)) return noInput;
  const ink = () => import("./ink.tsx").then((m) => m.inkPrompter);
  return {
    interactive: true,
    select: async (m, c, f) => (await ink()).select(m, c, f),
    multiSelect: async (m, c, f) => (await ink()).multiSelect(m, c, f),
    confirm: async (m, i, f) => (await ink()).confirm(m, i, f),
    text: async (m, f) => (await ink()).text(m, f),
    secret: async (m, f) => (await ink()).secret(m, f),
  };
}
