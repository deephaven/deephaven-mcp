/**
 * Questions the plugin commands ask. Each takes flags and plain data, asks only
 * what the flags didn't answer, and returns plain data.
 */
import { isAbsolute } from "node:path";
import {
  hostsDefaultFirst,
  type SshSettings,
} from "../../actions/plugins/settings.ts";
import { DhError } from "../../errors.ts";
import type { Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";

/** `--path`, checked: absolute, since plugins never look tools up on `PATH`. */
export function askSettingPath(path: string): string {
  if (!isAbsolute(path)) {
    throw new DhError("usage", `--path must be an absolute path: ${path}`);
  }
  return path;
}

/** One `--ssh-host`, checked: an SSH destination, non-empty and without whitespace. */
function askHost(host: string): string {
  if (host === "" || /\s/.test(host)) {
    throw new DhError(
      "usage",
      `--ssh-host must be a host name without spaces: "${host}"`,
      "e.g. infra-1, irisadmin@infra-1.example.com, or an alias from ~/.ssh/config",
    );
  }
  return host;
}

/**
 * Every `--ssh-host` given, checked, in order without duplicates. With
 * `clear`, `--ssh-host ""` alone gives none (back to the server's hostname).
 */
export function askSshHosts(values: string[], clear: boolean): string[] {
  if (clear && values.includes("")) {
    if (values.every((v) => v === "")) return [];
    throw new DhError(
      "usage",
      `--ssh-host "" can't be combined with other hosts`,
    );
  }
  return [...new Set(values.map(askHost))];
}

/**
 * The host to use: `flag` (the last `--ssh-host`) if it's one of `ssh`'s
 * hosts, else `usage`; without it, the default host.
 */
export function askSshHost(
  ssh: SshSettings,
  plugin: string,
  flag: string | undefined,
): string {
  if (flag === undefined) return ssh.defaultHost;
  if (ssh.hosts.includes(flag)) return flag;
  const add = [...ssh.hosts, flag].map((h) => `--ssh-host ${h}`).join(" ");
  const command = ssh.override ? `plugin set ${plugin}` : "plugin ssh enable";
  throw new DhError(
    "usage",
    `${flag} isn't one of ${plugin}'s SSH hosts on ${ssh.server}: ${ssh.hosts.join(", ")}`,
    `Add it: \`${BIN} ${command} ${add}\`.`,
  );
}

/**
 * The plugin's new default host: `flag` if it's one of the hosts, the only
 * host if there's one, else picked from the hosts (`--ssh-host`).
 */
export async function askDefaultSshHost(
  ui: Prompter,
  ssh: SshSettings,
  plugin: string,
  flag: string | undefined,
): Promise<string> {
  if (flag !== undefined) return askSshHost(ssh, plugin, flag);
  if (ssh.hosts.length === 1) return ssh.defaultHost;
  return await ui.select(
    `Which host should ${plugin} run on by default (on ${ssh.server})?`,
    hostsDefaultFirst(ssh).map((h) => ({
      label: h,
      value: h,
      ...(h === ssh.defaultHost ? { hint: "(default)" } : {}),
    })),
    "--ssh-host",
  );
}
