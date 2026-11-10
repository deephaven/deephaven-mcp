import { Option } from "commander";
import { createConsole } from "../../actions/console/actions.ts";
import type { ConsoleSettings } from "../../actions/console/store.ts";
import { defineCommand } from "../../cli/command.ts";
import { askConsoleSettings, type ConsoleSettingOptions } from "./questions.ts";

interface CreateOptions extends ConsoleSettingOptions {
  use?: boolean;
}

export const create = defineCommand<CreateOptions>({
  name: "create",
  description: "Create an interactive console.",
  args: [
    { name: "[settings]", description: "Console settings as a JSON object." },
  ],
  options: [
    new Option("--language <language>", "Console language.").choices([
      "python",
      "groovy",
    ]),
    new Option("--heap-size-gb <n>", "Console heap size in GB."),
    new Option("--server <name>", "Console server name."),
    new Option("--engine <name>", "Console engine name."),
    new Option("--json <settings>", "Console settings as a JSON object."),
    new Option("--no-use", "Create without selecting the console."),
  ],

  async run(ctx, [settingsJson], opts) {
    const settings = askConsoleSettings(
      settingsJson,
      opts,
    ) satisfies ConsoleSettings;
    const console = await createConsole(
      ctx.store,
      ctx.profileFlag,
      settings,
      opts.use === false,
      ctx.lifecycle,
    );
    return {
      human: `✔ Created ${console.shortcode} (${console.id})`,
      json: {
        id: console.id,
        shortcode: console.shortcode,
        name: console.name,
        state: console.state,
        selected: opts.use !== false,
      },
    };
  },
});
