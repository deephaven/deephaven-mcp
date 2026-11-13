/**
 * The periodic plugin check after other commands: at most once per
 * `DH_PLUGIN_REFRESH` hours (default 24; 0 = every run), with a short
 * timeout and `BatchMode`, failing silently, like the self-updater's check.
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { Command } from "commander";
import { commandPath, pluginExecOf } from "../../cli/command.ts";
import { BIN } from "../../version.ts";
import { Store } from "../auth/store.ts";
import { cacheDir, readCachedPart } from "./cache.ts";
import { activeProfile, readConfigQuietly, serverSsh } from "./settings.ts";
import { syncJs, syncShell } from "./sync.ts";

const DEFAULT_HOURS = 24;
const TIMEOUT_MS = 5_000;

/**
 * Brings the active profile's server's `shell/` cache up to date if it's
 * due. `cmd` is the command that ran: plugin commands, `dh plugin …` and
 * login check for themselves.
 */
export async function refreshPlugins(
  cmd: Command,
  profileFlag: string | undefined,
): Promise<void> {
  const path = commandPath(cmd);
  if (
    pluginExecOf(cmd) !== undefined ||
    path.startsWith(`${BIN} plugin`) ||
    path === `${BIN} auth login`
  ) {
    return;
  }
  // The bundle on disk replaces both of the server's parts.
  if (process.env.DH_PLUGIN_BUNDLE) return;
  const config = await readConfigQuietly();
  const active = activeProfile(config, profileFlag);
  if (!active) return;
  const origin = active.profile.server;
  const store = new Store();
  // Stamped before fetching, so a failing or slow server is throttled too.
  const due = async (channel: "js" | "shell") => {
    const stamp = join(cacheDir(origin, channel), "checked");
    const cached = await readCachedPart(origin, channel);
    const last = await readFile(stamp, "utf8").catch(() => "");
    if (cached && !isDue(cached.state.fetchedAt)) return false;
    if (last && !isDue(last)) return false;
    await mkdir(dirname(stamp), { recursive: true });
    await writeFile(stamp, new Date().toISOString());
    return true;
  };
  const checks: Promise<unknown>[] = [];
  if (await due("js")) {
    checks.push(
      syncJs(store, origin, {
        profileFlag,
        groups: true,
        timeoutMs: TIMEOUT_MS,
      }),
    );
  }
  if (serverSsh(config, origin).enabled && (await due("shell"))) {
    checks.push(
      syncShell(store, config, origin, {
        prompt: false,
        timeoutMs: TIMEOUT_MS,
      }),
    );
  }
  // Each fails silently, without stopping the other.
  const results = await Promise.allSettled(checks);
  const failed = results.find((r) => r.status === "rejected");
  if (failed) throw (failed as PromiseRejectedResult).reason;
}

/** Whether a check last done at `fetchedAt` is due again. */
function isDue(fetchedAt: string): boolean {
  const hours = Number(process.env.DH_PLUGIN_REFRESH ?? DEFAULT_HOURS);
  const interval = (Number.isFinite(hours) ? hours : DEFAULT_HOURS) * 3600_000;
  const last = Date.parse(fetchedAt);
  return Number.isNaN(last) || Date.now() - last >= interval;
}
