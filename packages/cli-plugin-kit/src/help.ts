/** What a tool's `--help` output says about one command. */
export interface ParsedHelp {
  /** Subcommand names, e.g. `intraday`, `metadata`; none for a leaf. `help` is left out. */
  subcommands: string[];
  /**
   * A one-item list on the usage line, e.g. dhctl's `logger [generate]`, in
   * help that lists no options of its own. It may be a subcommand or a
   * placeholder; {@link isUsageFor} on its own help tells.
   */
  candidate?: string;
  /** Subcommands' descriptions, from a `Commands:`-style listing, by name. */
  descriptions: Record<string, string>;
  /** The tool lists a `help` command here, so `… help <sub>` may show a subcommand's help without running it. */
  hasHelpCommand: boolean;
  /** The first sentence of prose after the usage line, if any. */
  description?: string;
  /** The command's own options, as far as they could be read; `-h/--help` is left out. */
  options: ParsedOption[];
}

/** One option from a help's option lines. */
export interface ParsedOption {
  /** Its spellings, then its value if it takes one: `-t, --table-name <arg>`, `-pf, --pwfile <arg>`. */
  flags: string;
  description: string;
  /** Shown outside brackets on the usage line (Commons CLI's `-t <arg>`). */
  required?: boolean;
}

/** One-item lists that are placeholders, not subcommands. */
const PLACEHOLDERS = new Set([
  "arguments",
  "args",
  "options",
  "opts",
  "command",
  "cmd",
  "flags",
  "file",
  "files",
  "path",
  "paths",
  "name",
  "value",
  "input",
  "output",
]);
/** An option line: `-x`, `--long`, or Commons CLI's `-x,--long`, indented. */
const OPTION_LINE = /^\s+-{1,2}[a-z0-9]/im;
/** How many lines a description may run over. */
const MAX_PROSE_LINES = 3;
/** A subcommand list: `[a|b|c]` (dhctl) or `{a,b,c}` (argparse). */
const LIST =
  /^[[{]([a-z0-9][a-z0-9_-]*(?:[|,][a-z0-9][a-z0-9_-]*)*)[\]}](?:\.\.\.)?$/i;
/** Headings of sections that list subcommands: `Commands:`, `Available Commands:`, `SUBCOMMANDS`, … */
const COMMANDS_HEADING =
  /^(?:available |supported |core |general |additional )?(?:sub)?commands:?$/i;
/** Headings of sections whose entries may describe subcommands, but whose names may be other arguments. */
const POSITIONALS_HEADING = /^positional arguments:?$/i;

/**
 * Reads `--help` output for the command at `path` (the tool's name, then
 * subcommand names). Subcommands come from, in order:
 *
 * - a `[a|b|c]` or `{a,b,c}` list after `path` on the usage line (dhctl,
 *   argparse), which may follow options such as `[-h]`;
 * - a `Supported subcommands [a|b|c]` line (dhconfig);
 * - a `Commands:`-style section of `name  description` lines (Cobra, Click,
 *   clap, commander, picocli).
 *
 * The usage line may follow other text (dhctl's `Action failed: Usage: …`),
 * or sit on the line after a bare `Usage:` (Cobra).
 */
export function parseHelp(text: string, path: string[]): ParsedHelp {
  const lines = text.split(/\r?\n/);
  const sections = sectionEntries(lines);
  const at = usageIndex(lines);
  const tokens = at < 0 ? [] : usageTokens(lines, at);
  const usage = startsWithPath(tokens, path)
    ? listIn(tokens.slice(path.length))
    : {};
  const listed =
    usage.list ??
    subcommandsLine(lines) ??
    (sections.commands.length > 0 ? sections.commands : undefined) ??
    [];
  const base = {
    descriptions: sections.descriptions,
    hasHelpCommand: listed.includes("help"),
    description: at < 0 ? undefined : proseAfter(lines.slice(at + 1)),
    options: optionsIn(lines, at),
  };
  // Nothing else lists subcommands; a one-item list may still be one, but only
  // in help that reads like a group's: a command listing its own options is a
  // leaf, and trying `<leaf> <item> --help` would run the leaf.
  if (listed.length === 0 && usage.candidate && !OPTION_LINE.test(text)) {
    return { ...base, subcommands: [], candidate: usage.candidate };
  }
  return { ...base, subcommands: listed.filter((n) => n !== "help") };
}

/** Whether `text`'s usage line is for the command at `path`, e.g. `dhctl logger generate`. */
export function isUsageFor(text: string, path: string[]): boolean {
  const lines = text.split(/\r?\n/);
  const at = usageIndex(lines);
  return at >= 0 && startsWithPath(usageTokens(lines, at), path);
}

/** Whether `text` has a usage line at all; a tool without `--help` may print an error instead. */
export function hasUsage(text: string): boolean {
  return usageIndex(text.split(/\r?\n/)) >= 0;
}

/** The line with `usage:` (after any prefix), or a bare `Usage`/`USAGE` heading. */
function usageIndex(lines: string[]): number {
  return lines.findIndex(
    (l) => /\busage:/i.test(l) || /^\s*usage\s*$/i.test(l),
  );
}

/** The usage line's words: after `usage:` (and anything before it), or on the next line after a bare `Usage:`. */
function usageTokens(lines: string[], at: number): string[] {
  const rest = (lines[at] ?? "").replace(/^.*?\busage:?\s*/i, "").trim();
  const usage = rest || (lines.slice(at + 1).find((l) => l.trim()) ?? "");
  return usage.trim().split(/\s+/);
}

/** Whether `tokens` begin with `path`; the tool may be named by its full path. */
function startsWithPath(tokens: string[], path: string[]): boolean {
  if ((tokens[0] ?? "").split("/").at(-1) !== path[0]) return false;
  return path.every((name, i) => i === 0 || tokens[i] === name);
}

/**
 * In `tokens` (what follows the command on the usage line): the first list of
 * two or more names, skipping options like `[-h]`; else a one-item list right
 * after the command that isn't a placeholder like `[options]`.
 */
function listIn(tokens: string[]): { list?: string[]; candidate?: string } {
  let candidate: string | undefined;
  for (const [i, token] of tokens.entries()) {
    const names = (LIST.exec(token)?.[1] ?? "").split(/[|,]/).filter(Boolean);
    if (names.length > 1) return { list: names };
    const [only] = names;
    if (i === 0 && only && !PLACEHOLDERS.has(only.toLowerCase())) {
      candidate = only;
    }
  }
  return { candidate };
}

/** The names on a line like `Supported subcommands [add|delete|list]`, or undefined. */
function subcommandsLine(lines: string[]): string[] | undefined {
  for (const line of lines) {
    const list =
      /^\s*(?:supported |available )?(?:sub)?commands:?\s*[[{]([a-z0-9][a-z0-9_-]*(?:[|,][a-z0-9][a-z0-9_-]*)+)[\]}]\s*$/i.exec(
        line,
      );
    if (list) return (list[1] ?? "").split(/[|,]/);
  }
  return undefined;
}

/**
 * Entries of `Commands:`-style sections (names and descriptions) and of
 * argparse's `positional arguments:` (descriptions only, since other
 * arguments are listed there too).
 */
function sectionEntries(lines: string[]): {
  commands: string[];
  descriptions: Record<string, string>;
} {
  const commands: string[] = [];
  const descriptions: Record<string, string> = {};
  for (let i = 0; i < lines.length; i++) {
    const heading = (lines[i] ?? "").trim();
    const isCommands = COMMANDS_HEADING.test(heading);
    if (!/^\S/.test(lines[i] ?? "")) continue;
    if (!isCommands && !POSITIONALS_HEADING.test(heading)) continue;
    for (const entry of entriesAfter(lines, i + 1)) {
      if (isCommands) commands.push(entry.name);
      if (entry.description) descriptions[entry.name] = entry.description;
    }
  }
  return { commands, descriptions };
}

/**
 * The `  name [args]  description` lines of the section starting at `from`,
 * up to the next unindented line. Entries share the first entry's indent;
 * a line starting at or past the previous description's column continues it
 * (picocli indents those two further than the description).
 */
function entriesAfter(
  lines: string[],
  from: number,
): { name: string; description?: string }[] {
  const entries: { name: string; description?: string }[] = [];
  let indent: number | undefined;
  /** Where the last entry's description starts; wrapped lines start there too. */
  let column: number | undefined;
  for (const line of lines.slice(from)) {
    if (/^\S/.test(line)) break;
    if (!line.trim()) continue;
    const depth = line.length - line.trimStart().length;
    const entry = /^\s+([a-z0-9][a-z0-9_-]*):?(?:,\s*[a-z0-9_-]+)*(.*)$/i.exec(
      line,
    );
    if (entry && (indent === undefined || depth === indent)) {
      indent = depth;
      const description = lastColumn(entry[2] ?? "");
      column = description ? line.lastIndexOf(description) : undefined;
      entries.push({ name: entry[1] ?? "", description });
      continue;
    }
    const last = entries.at(-1);
    if (last?.description && column !== undefined && depth >= column) {
      last.description = `${last.description} ${line.trim()}`;
    }
  }
  return entries;
}

/**
 * The description in what follows an entry's name: the last column, after
 * arguments (`[options] [server]`) or examples (bun's `./my-script.ts`), which
 * are separated by two or more spaces. None if that's only arguments.
 */
function lastColumn(rest: string): string | undefined {
  const columns = rest
    .trim()
    .split(/\s{2,}/)
    .filter(Boolean);
  const last = columns.at(-1);
  if (!last) return undefined;
  if (columns.length === 1 && /^[[<(]/.test(last)) return undefined;
  return last;
}

/**
 * The option lines anywhere in the help: indented, starting with `-`, the
 * spellings and value up to two or more spaces, then the description, which
 * may wrap onto lines starting at its column. Covers Commons CLI
 * (`-t,--table-name <arg>`), argparse (`-t T, --table T`), Cobra
 * (`-n, --name string`), Click (`--name TEXT`) and commander/clap
 * (`-o, --output <format>`). Values are written `<value>`, lowercase. The
 * usage block at `at` (-1 if none) is skipped.
 */
function optionsIn(lines: string[], at: number): ParsedOption[] {
  const usage = at < 0 ? [] : usageBlock(lines, at);
  const required = requiredFlags(usage);
  const body =
    at < 0 ? lines : [...lines.slice(0, at), ...lines.slice(at + usage.length)];
  const options: ParsedOption[] = [];
  const seen = new Set<string>();
  let column: number | undefined;
  let last: ParsedOption | undefined;
  for (const line of body) {
    if (!line.trim()) continue;
    // A wrapped description, even one starting with `-` (dhctl wraps
    // `-pf`'s onto a line starting `--user`).
    const depth = line.length - line.trimStart().length;
    if (depth === column) {
      if (last) last.description = `${last.description} ${line.trim()}`;
      continue;
    }
    const option = /^(\s+)(-{1,2}[a-z0-9].*?)(?:\s{2,}(\S.*))?$/i.exec(line);
    const parsed = option ? optionSpellings(option[2] ?? "") : undefined;
    if (!option || !parsed) {
      column = undefined;
      last = undefined;
      continue;
    }
    const description = (option[3] ?? "").trim();
    column = description ? line.lastIndexOf(description) : undefined;
    last = undefined;
    const key = parsed.names.at(-1) ?? "";
    if (
      seen.has(key) ||
      parsed.names.some((n) => n === "-h" || n === "--help")
    ) {
      continue;
    }
    seen.add(key);
    last = { flags: parsed.flags, description };
    if (parsed.names.some((n) => required.has(n))) last.required = true;
    options.push(last);
  }
  return options;
}

/** `-t,--table-name <arg>` → names `-t`, `--table-name` and flags `-t, --table-name <arg>`; undefined if not an option. */
function optionSpellings(
  column: string,
): { names: string[]; flags: string } | undefined {
  const names: string[] = [];
  let value: string | undefined;
  for (const token of column.split(/,\s*|\s+|=/).filter(Boolean)) {
    if (/^-{1,2}[a-z0-9][\w-]*$/i.test(token)) names.push(token);
    else if (names.length > 0) value ??= token;
    else return undefined;
  }
  if (names.length === 0) return undefined;
  const placeholder = value?.replace(/^[[<]|[\]>]$/g, "").toLowerCase();
  return {
    names,
    flags: `${names.join(", ")}${placeholder ? ` <${placeholder}>` : ""}`,
  };
}

/** The usage line at `at` and the indented lines it wraps onto (Commons CLI wraps `[-s\n       <arg>]`). */
function usageBlock(lines: string[], at: number): string[] {
  const first = lines[at] ?? "";
  // Continuation lines start where the usage did, after `usage: `.
  const start = /^.*?\busage:\s*/i.exec(first)?.[0].length;
  const block = [first];
  for (const line of lines.slice(at + 1)) {
    const depth = line.length - line.trimStart().length;
    if (!start || !line.trim() || depth !== start) break;
    block.push(line);
  }
  return block;
}

/**
 * Flags shown outside brackets in the usage block: Commons CLI's way of
 * marking an option required (`… [-pf <arg>] -t <arg>  [-v]`).
 */
function requiredFlags(usage: string[]): Set<string> {
  const required = new Set<string>();
  let depth = 0;
  for (const token of usage.join(" ").split(/\s+/)) {
    if (depth === 0 && /^-{1,2}[a-z0-9]/i.test(token)) required.add(token);
    depth += (token.match(/[[{]/g) ?? []).length;
    depth -= (token.match(/[\]}]/g) ?? []).length;
  }
  return required;
}

/**
 * The first sentence of prose: not an option, error or instruction. It may
 * wrap onto the next lines, up to {@link MAX_PROSE_LINES}.
 */
function proseAfter(lines: string[]): string | undefined {
  const start = lines.findIndex(isProse);
  if (start < 0) return undefined;
  const parts: string[] = [];
  for (const raw of lines.slice(start, start + MAX_PROSE_LINES)) {
    if (!isProse(raw)) break;
    parts.push(raw.trim());
    if (raw.trim().endsWith(".")) break;
  }
  return parts.join(" ");
}

function isProse(raw: string): boolean {
  const line = raw.trim();
  if (!line || raw.startsWith(" ") || line.startsWith("-")) return false;
  if (/^(error|action failed|for .* help|options|version)\b/i.test(line)) {
    return false;
  }
  return !line.endsWith(":");
}
