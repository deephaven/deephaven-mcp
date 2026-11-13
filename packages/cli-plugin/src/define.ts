/**
 * The plugin API (v1), shared by both entries. Each entry fixes what
 * `ctx.enterprise()` returns: `unknown` from the main entry, the Enterprise
 * JS API's types from `@deephaven/cli-plugin/enterprise`.
 */
import type { dh } from "@deephaven/jsapi-types";

/** The plugin API major version this package defines; `definePlugin` records it. */
export const API_VERSION = 1;

/** The Community JS API, as `dh-core.js` exports it. */
export type DhApi = typeof dh;
/** A connection to a Community server. */
export type CoreClient = dh.CoreClient;

/** Which kind of profile a plugin's commands need; another kind is a `usage` error. */
export type Requires = "enterprise" | "community" | "any";

/** The `dh` error codes `ctx.fail` can give. */
export type FailCode =
  | "usage"
  | "not_found"
  | "conflict"
  | "permission_denied"
  | "plugin_failed";

/** What a leaf's `run` gets. `E` is what `enterprise()` returns. */
export interface PluginContextOf<E> {
  /** Positional arguments by name, already parsed and validated. */
  args: Record<string, string | undefined>;
  /** Options by key, already parsed and validated. */
  options: Record<string, string | number | boolean | string[] | undefined>;
  /** The active profile. */
  profile: {
    name: string;
    server: string;
    kind: "enterprise" | "community";
    user: string;
  };
  /** Signed in with the profile's saved credentials on first call; the same connection after. */
  enterprise(): Promise<E>;
  /** Signed in with the profile's saved credentials on first call; the same connection after. */
  community(): Promise<{ api: DhApi; client: CoreClient }>;
  /** Questions; without a terminal or with --no-prompt they fail naming `flag`. */
  ui: {
    confirm(
      question: string,
      opts: { flag: string; default?: boolean },
    ): Promise<boolean>;
    select<T extends string>(
      question: string,
      choices: T[],
      opts: { flag: string },
    ): Promise<T>;
  };
  /** Progress to stderr; silent with -o json. */
  progress(message: string): void;
  /** Aborted on Ctrl-C. */
  signal: AbortSignal;
  /** True with -o json. */
  json: boolean;
  /** Fails the command with one of dh's codes. */
  fail(code: FailCode, message: string, hint?: string): never;
}

/** A positional argument. */
export interface ArgDef {
  description: string;
  required?: boolean;
}

/** An option; its key is camelCase in code, `--kebab-case` on the command line. */
export interface OptionDef {
  type: "string" | "number" | "boolean";
  description: string;
  multiple?: boolean;
  choices?: string[];
  required?: boolean;
  default?: string | number | boolean | string[];
}

/** One field of a command's `-o json` result, documented in help and `dh agents`. */
export interface FieldDef {
  type: "string" | "number" | "boolean" | "object" | "array";
  description: string;
}

/**
 * What `run` returns. `rows`: an array of objects (a table in human mode);
 * `object`: key/value lines; `text`: a string, printed as is (`-o json` gives
 * `{"text": …}`); `none`: nothing.
 */
export interface OutputDef {
  kind: "rows" | "object" | "text" | "none";
  fields?: Record<string, FieldDef>;
}

/** A command that runs: `run` gets parsed values and returns data. */
export interface LeafDef<C> {
  description: string;
  args?: Record<string, ArgDef>;
  options?: Record<string, OptionDef>;
  output: OutputDef;
  run(ctx: C): unknown;
  /** Overrides the default human rendering; never changes `-o json`. */
  // biome-ignore lint/suspicious/noExplicitAny: whatever this leaf's `run` returns.
  human?(result: any): string | string[];
}

/** A command with subcommands. */
export interface GroupDef<C> {
  description: string;
  commands: Record<string, CommandDef<C>>;
}

/** A group (with `commands`) or a leaf (with `run`). Names: `^[a-z][a-z0-9-]*$`. */
export type CommandDef<C> = GroupDef<C> | LeafDef<C>;

interface PluginBase {
  /** The plugin's identity, unique in its bundle. */
  name: string;
  description: string;
  requires: Requires;
  /** Registered only for users in at least one of these groups. Unset: everyone. */
  groups?: string[];
}

/**
 * A plugin: one noun named `name` (`commands`), or several top-level nouns
 * (`nouns`), each shaped like a command.
 */
export type PluginDefinitionOf<C> = PluginBase &
  (
    | { commands: Record<string, CommandDef<C>>; nouns?: never }
    | { nouns: Record<string, CommandDef<C>>; commands?: never }
  );

/** A plugin as `definePlugin` returns it: the definition, with the API version it was built against. */
export type DefinedPlugin<C> = PluginDefinitionOf<C> & {
  api: typeof API_VERSION;
};

/** Records the API version; the definition is otherwise returned as is. */
export function defineWith<C>(plugin: PluginDefinitionOf<C>): DefinedPlugin<C> {
  return { ...plugin, api: API_VERSION };
}
