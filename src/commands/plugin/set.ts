import { Option } from "commander";
import type { Config } from "../../actions/auth/store.ts";
import type { Discovery } from "../../actions/plugins/discover.ts";
import {
  type ActiveProfile,
  activeProfile,
  hostsDefaultFirst,
  readConfig,
  requireProfile,
  sshSettings,
  upgradeServerPluginSettings,
} from "../../actions/plugins/settings.ts";
import { collect, defineCommand } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { askPlugin } from "./index.ts";
import { askSettingPath, askSshHosts } from "./questions.ts";
import { checkSshHostsToSave } from "./ssh-checks.ts";

interface SetOptions {
  path?: string;
  sshHost?: string[];
  /** `--no-check` → false. */
  check?: boolean;
}

/** `dh plugin set <name>`: a plugin's SSH hosts on the profile's server, or its tool's path here. */
export function setCommand(found: Discovery) {
  return defineCommand<SetOptions>({
    name: "set",
    description:
      "Set a plugin's own SSH hosts on the profile's server, or its tool's path on this machine.",
    args: [{ name: "<name>", description: "The plugin, e.g. crcat." }],
    options: [
      new Option(
        "--ssh-host <host>",
        `SSH destination for this plugin instead of the server's hosts; repeat for several, which replace the list ("" goes back to the server's).`,
      ).argParser(collect),
      new Option(
        "--path <path>",
        "The tool's absolute path on this machine, for a plugin from this node's bundle.",
      ),
      new Option("--no-check", "Don't check the SSH hosts before saving them."),
    ],

    async run(ctx, [nameArg], opts) {
      const plugin = askPlugin(found, nameArg ?? "");
      if (opts.path === undefined && opts.sshHost === undefined) {
        throw new DhError(
          "usage",
          "Nothing to set",
          "Pass --ssh-host or --path.",
        );
      }
      const path =
        opts.path === undefined ? undefined : askSettingPath(opts.path);
      const sshHosts =
        opts.sshHost === undefined
          ? undefined
          : askSshHosts(opts.sshHost, true);
      const before = await readConfig(ctx.store);
      const active =
        sshHosts === undefined
          ? activeProfile(before, ctx.profileFlag)
          : requireProfile(before, ctx.profileFlag);
      const toCheck = opts.check === false ? [] : (sshHosts ?? []);
      if (toCheck.length > 0) {
        await checkSshHostsToSave(
          { plugin: plugin.name, toolPath: plugin.spec?.tool.path },
          toCheck,
        );
      }
      const config = await ctx.store.update((state) => {
        applySettings(state.config, plugin.name, {
          path,
          server: active?.profile.server,
          sshHosts,
        });
        return state.config;
      });
      const now = effectiveSettings(config, plugin.name, active);
      return {
        human: [`✔ Updated ${plugin.name}`, ...settingsLines(now)],
        json: now,
      };
    },
  });
}

/** A change to a plugin's settings; undefined leaves a setting as it is. */
interface SettingsChange {
  path?: string;
  /** The origin whose settings `sshHosts` go to. */
  server?: string;
  /** Replaces the list; empty removes it. */
  sshHosts?: string[];
}

function applySettings(
  config: Config,
  name: string,
  change: SettingsChange,
): void {
  if (change.path !== undefined) {
    config.plugins = {
      ...config.plugins,
      [name]: { ...config.plugins?.[name], path: change.path },
    };
  }
  if (change.sshHosts === undefined || change.server === undefined) return;
  const server = config.servers[change.server];
  if (!server) {
    throw new DhError(
      "internal",
      `The profile's server ${change.server} is missing from config.json`,
      `Run \`${BIN} auth login\` again.`,
    );
  }
  const settings = upgradeServerPluginSettings({ ...server.plugins?.[name] });
  if (change.sshHosts.length > 0) settings.sshHosts = change.sshHosts;
  else delete settings.sshHosts;
  const kept = settings.defaultSshHost;
  if (kept !== undefined && !settings.sshHosts?.includes(kept)) {
    delete settings.defaultSshHost;
  }
  const plugins = { ...server.plugins, [name]: settings };
  if (Object.keys(settings).length === 0) delete plugins[name];
  if (Object.keys(plugins).length > 0) server.plugins = plugins;
  else delete server.plugins;
}

/** A plugin's settings in effect; `null` where unset or not applicable. */
export interface EffectiveSettings {
  plugin: string;
  /** This machine's `path`. */
  path: string | null;
  /** With a profile: its server, the plugin's SSH hosts (default first), and the default. */
  server: string | null;
  sshHosts: string[] | null;
  defaultSshHost: string | null;
  /** Whether `sshHosts` are the plugin's own rather than the server's. */
  ownSshHosts: boolean;
}

export function effectiveSettings(
  config: Config,
  name: string,
  active: ActiveProfile | undefined,
): EffectiveSettings {
  const ssh = active ? sshSettings(config, active.profile, name) : undefined;
  return {
    plugin: name,
    path: config.plugins?.[name]?.path ?? null,
    server: ssh?.server ?? null,
    sshHosts: ssh ? hostsDefaultFirst(ssh) : null,
    defaultSshHost: ssh?.defaultHost ?? null,
    ownSshHosts: ssh?.override ?? false,
  };
}

/** The SSH hosts and `path`, when set. */
function settingsLines(s: EffectiveSettings): string[] {
  return [
    ...(s.sshHosts ? [`  ssh hosts: ${sshHostsText(s)}`] : []),
    ...(s.path ? [`  path: ${s.path}`] : []),
  ];
}

/** `dis-2 (default), dis-1 (<server>)`; `(default)` only with two or more. */
function sshHostsText(s: EffectiveSettings): string {
  if (!s.sshHosts) return "(no profile)";
  const many = s.sshHosts.length > 1;
  const hosts = s.sshHosts.map((h) =>
    many && h === s.defaultSshHost ? `${h} (default)` : h,
  );
  const whose = s.ownSshHosts ? "" : ", the server's";
  return `${hosts.join(", ")} (${s.server}${whose})`;
}
