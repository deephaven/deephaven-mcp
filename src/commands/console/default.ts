import { Option } from "commander";
import {
  getConsoleDefaults,
  requireEnterpriseConsole,
  setConsoleDefaults,
} from "../../actions/console/actions.ts";
import { defineCommand } from "../../cli/command.ts";
import { askConsoleDefaults, type ConsoleSettingOptions } from "./questions.ts";

interface DefaultOptions extends ConsoleSettingOptions {
  reset?: boolean;
}

export const defaults = defineCommand<DefaultOptions>({
  name: "default",
  description: "Show or set console creation defaults.",
  args: [
    { name: "[settings]", description: "Console settings as a JSON object." },
  ],
  options: [
    new Option("--language <language>", "Default console language.").choices([
      "python",
      "groovy",
    ]),
    new Option("--heap-size-gb <n>", "Default console heap size in GB."),
    new Option("--server <name>", "Default console server name."),
    new Option("--engine <name>", "Default console engine name."),
    new Option("--json <settings>", "Console settings as a JSON object."),
    new Option("--reset", "Reset every default to the server default."),
  ],

  async run(ctx, [settingsJson], opts) {
    const request = askConsoleDefaults(settingsJson, opts);
    await requireEnterpriseConsole(ctx.store, ctx.profileFlag, ctx.lifecycle);
    if (request.action !== "show") {
      await setConsoleDefaults(
        ctx.store,
        request.action === "update" ? request.settings : {},
        request.action === "reset",
      );
      return { human: "", json: {}, empty: true };
    }
    const configured = await getConsoleDefaults(ctx.store);
    const fields = Object.keys(configured);
    return {
      human:
        fields.length === 0
          ? "Using server defaults."
          : Object.entries(configured).map(
              ([key, value]) => `  ${key}: ${JSON.stringify(value)}`,
            ),
      json: configured,
    };
  },
});
