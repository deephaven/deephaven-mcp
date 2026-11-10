import { Option } from "commander";
import {
  getConsoleDefaults,
  requireEnterpriseConsole,
  setConsoleDefaults,
} from "../../actions/console/actions.ts";
import type { ConsoleSettings } from "../../actions/console/store.ts";
import { defineCommand } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { askConsoleSettings, type ConsoleSettingOptions } from "./questions.ts";

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
    const settings = askConsoleSettings(
      settingsJson,
      opts,
    ) satisfies ConsoleSettings;
    const hasSettings =
      settingsJson !== undefined ||
      opts.json !== undefined ||
      opts.language !== undefined ||
      opts.heapSizeGb !== undefined ||
      opts.server !== undefined ||
      opts.engine !== undefined;
    if (opts.reset && hasSettings) {
      throw new DhError(
        "usage",
        "--reset cannot be combined with console settings",
      );
    }
    await requireEnterpriseConsole(ctx.store, ctx.profileFlag, ctx.lifecycle);
    if (opts.reset || hasSettings) {
      await setConsoleDefaults(ctx.store, settings, Boolean(opts.reset));
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
