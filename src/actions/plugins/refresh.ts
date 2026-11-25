/**
 * The periodic plugin check after other commands: at most once per
 * `DH_PLUGIN_REFRESH` hours (default 24; 0 = every run; `off` = never), in a
 * background `dh plugin sync`, so it never delays the command.
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { Command } from "commander";
import { commandPath, pluginExecOf } from "../../cli/command.ts";
import { BIN } from "../../version.ts";
import { selfCommand } from "../auth/session.ts";
import { cacheDir, readCachedPart } from "./cache.ts";
import { activeProfile, readConfigQuietly, serverSsh } from "./settings.ts";

const DEFAULT_HOURS = 24;

/**
 * If the active profile's server's plugins are due a check, starts
 * `dh plugin sync` for it in the background and returns without waiting.
 * `cmd` is the command that ran: plugin commands, `dh plugin …` and login
 * check for themselves.
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
  if (process.env.DH_PLUGIN_REFRESH === "off") return;
  // The bundle on disk replaces both of the server's parts.
  if (process.env.DH_PLUGIN_BUNDLE) return;
  const config = await readConfigQuietly();
  const active = activeProfile(config, profileFlag);
  if (!active) return;
  const origin = active.profile.server;
  // Stamped before the sync starts, so a failing or slow server is throttled too.
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
  const js = await due("js");
  const shell = serverSsh(config, origin).enabled && (await due("shell"));
  if (!js && !shell) return;
  const debug = Boolean(process.env.DH_DEBUG);
  const proc = Bun.spawn(
    selfCommand([
      "plugin",
      "sync",
      "--no-prompt",
      "--profile",
      active.profile.name,
    ]),
    {
      // The parent checks for updates itself.
      env: { ...process.env, DH_AUTO_UPDATE: "off" },
      // Failures are silent, but for DH_DEBUG.
      stdio: ["ignore", "ignore", debug ? "inherit" : "ignore"],
      // Outlives this process, and the terminal's signals to it.
      detached: process.platform !== "win32",
    },
  );
  proc.unref();
}

/** Whether a check last done at `fetchedAt` is due again. */
function isDue(fetchedAt: string): boolean {
  const hours = Number(process.env.DH_PLUGIN_REFRESH ?? DEFAULT_HOURS);
  const interval = (Number.isFinite(hours) ? hours : DEFAULT_HOURS) * 3600_000;
  const last = Date.parse(fetchedAt);
  return Number.isNaN(last) || Date.now() - last >= interval;
}
