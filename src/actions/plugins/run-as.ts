/**
 * Running a tool as one of the cluster's service accounts (`tool.runAs`;
 * plans/cluster-plugins.md, "Running as a service account").
 */
import { readFile } from "node:fs/promises";
import { userInfo } from "node:os";
import { DhError } from "../../errors.ts";
import type { RunAs } from "./spec.ts";

/** Where the installer records the service accounts, on every node. */
export const CLUSTER_CNF = "/etc/sysconfig/deephaven/cluster.cnf";

/** Each role's variable in `cluster.cnf`, and the account when it's not there. */
export const ACCOUNTS: Record<
  RunAs["account"],
  { variable: string; fallback: string }
> = {
  admin: { variable: "DH_ADMIN_USER", fallback: "irisadmin" },
  merge: { variable: "DH_MERGE_USER", fallback: "dbmerge" },
  query: { variable: "DH_QUERY_USER", fallback: "dbquery" },
};

/**
 * Whether the user's `args` (as the tool gets them) bring their own
 * identity: one of `unlessFlags`, or `<flag>=…`, or a one-letter short flag
 * with its value attached (`-kmy.key`). Scanning stops at the first `--`.
 */
export function bringsOwnIdentity(
  args: string[],
  unlessFlags: string[] = [],
): boolean {
  for (const a of args) {
    if (a === "--") return false;
    for (const flag of unlessFlags) {
      if (a === flag || a.startsWith(`${flag}=`)) return true;
      if (/^-[^-]$/.test(flag) && a.startsWith(flag) && a.length > 2) {
        return true;
      }
    }
  }
  return false;
}

/**
 * The account's name in `cluster.cnf`'s text: the first line starting
 * `<VARIABLE>=`, its value with one pair of surrounding quotes removed;
 * else the role's fallback. Parsed, never sourced.
 */
export function accountIn(
  text: string | undefined,
  role: RunAs["account"],
): string {
  const { variable, fallback } = ACCOUNTS[role];
  const line = (text ?? "")
    .split("\n")
    .find((l) => l.startsWith(`${variable}=`));
  const value = unquote(line?.slice(variable.length + 1).trim() ?? "");
  return value || fallback;
}

function unquote(value: string): string {
  const q = value[0];
  return (q === '"' || q === "'") && value.length >= 2 && value.endsWith(q)
    ? value.slice(1, -1)
    : value;
}

/** The account for `role` on this machine. */
export async function localAccount(role: RunAs["account"]): Promise<string> {
  const text = await readFile(CLUSTER_CNF, "utf8").catch(() => undefined);
  return accountIn(text, role);
}

/** Whether this process already runs as `account`. */
export function isCurrentUser(account: string): boolean {
  try {
    return userInfo().username === account;
  } catch {
    return false;
  }
}

/**
 * The same lookup as {@link accountIn}, as `sh` for the remote side: sets
 * `a` to the account's name.
 */
export function accountScript(role: RunAs["account"]): string {
  const { variable, fallback } = ACCOUNTS[role];
  return [
    `a=$(sed -n 's/^${variable}=//p' ${CLUSTER_CNF} 2>/dev/null | head -n 1)`,
    `case $a in \\"*\\") a=\${a#\\"}; a=\${a%\\"};; \\'*\\') a=\${a#\\'}; a=\${a%\\'};; esac`,
    `[ -n "$a" ] || a=${fallback}`,
  ].join("; ");
}

/**
 * `tool_failed` when `stderr` shows sudo refused to run `tool` as
 * `account`; undefined when it doesn't (the tool itself failed). `dh` never
 * falls back to running as the user.
 */
export function sudoRefused(
  stderr: string,
  tool: string,
  account: string | undefined,
  runAs: RunAs,
  fields: Record<string, unknown>,
): DhError | undefined {
  const as = account ?? ACCOUNTS[runAs.account].fallback;
  if (/sudo: (a password is required|a terminal is required)/.test(stderr)) {
    return new DhError(
      "tool_failed",
      `sudo needs a password to run ${tool} as ${as}`,
      "Run it in a terminal to enter sudo's password.",
      undefined,
      { toolExit: 1, ...fields },
    );
  }
  if (
    /is not allowed to (execute|run)|is not in the sudoers|may not run sudo/.test(
      stderr,
    )
  ) {
    const flags = runAs.unlessFlags ?? [];
    return new DhError(
      "tool_failed",
      `sudo refused to run ${tool} as ${as}`,
      flags.length > 0
        ? `You can't run ${tool} as ${as}; pass your own credentials: ${flags.join(" ")}`
        : `You can't run ${tool} as ${as}.`,
      undefined,
      { toolExit: 1, ...fields },
    );
  }
  return undefined;
}

/** `Runs as the cluster's admin account (irisadmin), or as you with --key or --user.` */
export function runAsLine(runAs: RunAs, account: string | undefined): string {
  const as = account ?? ACCOUNTS[runAs.account].fallback;
  const flags = runAs.unlessFlags ?? [];
  const long = flags.filter((f) => f.startsWith("--"));
  const shown = long.length > 0 ? long : flags;
  const own = shown.length > 0 ? `, or as you with ${shown.join(" or ")}` : "";
  return `Runs as the cluster's ${runAs.account} account (${as})${own}.`;
}

/** `sudo`'s argv to run `argv` as `account`: `-n` without a terminal, so it never waits on a prompt. */
export function sudoArgv(
  account: string,
  argv: string[],
  interactive: boolean,
): string[] {
  return ["sudo", ...(interactive ? [] : ["-n"]), "-u", account, ...argv];
}
