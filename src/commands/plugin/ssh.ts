import { Option } from "commander";
import {
  readConfig,
  requireProfile,
  serverSsh,
  setServerSsh,
} from "../../actions/plugins/settings.ts";
import { syncShell } from "../../actions/plugins/sync.ts";
import { collect, defineCommand } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { askSshHosts } from "./questions.ts";
import { checkSshHostsToSave } from "./ssh-checks.ts";
import { syncOutput } from "./sync.ts";

interface EnableOptions {
  sshHost?: string[];
}

/** `dh plugin ssh enable|disable`: whether the profile's server's `shell` plugins are used, over SSH. */
export function sshCommand() {
  const enable = defineCommand<EnableOptions>({
    name: "enable",
    description:
      "Use the profile's server's plugins over SSH: check the connection, then copy them.",
    options: [
      new Option(
        "--ssh-host <host>",
        "SSH destination for the server's nodes; repeat for several, which replace the list (default: the server's hostname).",
      ).argParser(collect),
    ],
    async run(ctx, _args, opts) {
      const config = await readConfig(ctx.store);
      const { profile } = requireProfile(config, ctx.profileFlag);
      const origin = profile.server;
      const hosts =
        opts.sshHost === undefined
          ? undefined
          : askSshHosts(opts.sshHost, false);
      const toCheck = hosts ?? serverSsh(config, origin).hosts;
      await checkSshHostsToSave({}, toCheck);
      const planned = setServerSsh(config, origin, { enabled: true, hosts });
      const sync = await syncShell(ctx.store, planned, origin, {
        prompt: ctx.ui.interactive,
      });
      await ctx.store.update(({ config }) => {
        Object.assign(
          config,
          setServerSsh(config, origin, { enabled: true, hosts }),
        );
      });
      const out = syncOutput(origin, [sync]);
      return {
        human: [`✔ SSH plugins enabled for ${origin}`, ...[out.human].flat()],
        json: { server: origin, enabled: true, sshHosts: toCheck, sync },
      };
    },
  });
  const disable = defineCommand({
    name: "disable",
    description: "Stop using the profile's server's plugins over SSH.",
    async run(ctx) {
      const config = await readConfig(ctx.store);
      const { profile } = requireProfile(config, ctx.profileFlag);
      const origin = profile.server;
      await ctx.store.update(({ config }) => {
        Object.assign(config, setServerSsh(config, origin, { enabled: false }));
      });
      return {
        human: [`✔ SSH plugins disabled for ${origin}`],
        json: { server: origin, enabled: false },
      };
    },
  });
  return defineCommand({
    name: "ssh",
    description:
      "Enable or disable the profile's server's plugins that run over SSH.",
    commands: [enable, disable],
    async run() {
      throw new DhError(
        "usage",
        "Missing enable or disable",
        `Run \`${BIN} plugin ssh --help\` for usage.`,
      );
    },
  });
}
