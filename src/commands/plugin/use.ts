import { Option } from "commander";
import type { Config } from "../../actions/auth/store.ts";
import type { Discovery } from "../../actions/plugins/discover.ts";
import {
  hostsDefaultFirst,
  readConfig,
  requireProfile,
  type SshSettings,
  sshSettings,
  upgradeServerPluginSettings,
} from "../../actions/plugins/settings.ts";
import { collect, defineCommand, type Output } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { askPlugin } from "./index.ts";
import { askDefaultSshHost } from "./questions.ts";

interface UseOptions {
  sshHost?: string[];
}

/**
 * `dh plugin use <name>`: which of an `ssh` plugin's SSH hosts runs it by
 * default on the profile's server.
 */
export function pluginUseCommand(found: Discovery) {
  return defineCommand<UseOptions>({
    name: "use",
    description: "Pick which of a plugin's SSH hosts it runs on by default.",
    args: [{ name: "<name>", description: "The plugin, e.g. crcat." }],
    options: [
      new Option(
        "--ssh-host <host>",
        "The new default host; one of the plugin's SSH hosts.",
      ).argParser(collect),
    ],

    async run(ctx, [nameArg], opts) {
      const spec = askPlugin(found, nameArg ?? "");
      const hostFlag = opts.sshHost?.at(-1);
      const config = await readConfig(ctx.store);
      if (spec.exec === "local") {
        throw new DhError(
          "usage",
          `${spec.name} runs on this machine, so it has no SSH hosts`,
          `It's from this machine's bundle; \`${BIN} plugin list\` shows where each plugin comes from.`,
        );
      }
      const ssh = sshSettings(
        config,
        requireProfile(config, ctx.profileFlag).profile,
        spec.name,
      );
      const host = await askDefaultSshHost(ctx.ui, ssh, spec.name, hostFlag);
      if (ssh.hosts.length > 1) {
        await ctx.store.update(({ config }) =>
          writeDefaultSshHost(config, spec.name, ssh.server, host),
        );
      }
      return pluginUseOutput(spec.name, ssh, host);
    },
  });
}

/** The default host's line, and the JSON. */
function pluginUseOutput(name: string, ssh: SshSettings, host: string): Output {
  return {
    human: [`✔ ${name} runs on ${host} by default on ${ssh.server}`],
    json: {
      plugin: name,
      server: ssh.server,
      sshHosts: hostsDefaultFirst({ ...ssh, defaultHost: host }),
      defaultSshHost: host,
    },
  };
}

/** Sets `host` as the plugin's `defaultSshHost` on `server`, in `config`. */
function writeDefaultSshHost(
  config: Config,
  name: string,
  server: string,
  host: string,
): void {
  const entry = config.servers[server];
  if (!entry) {
    throw new DhError(
      "internal",
      `The profile's server ${server} is missing from config.json`,
      `Run \`${BIN} auth login\` again.`,
    );
  }
  const settings = upgradeServerPluginSettings({
    ...entry.plugins?.[name],
  });
  settings.defaultSshHost = host;
  entry.plugins = { ...entry.plugins, [name]: settings };
}
