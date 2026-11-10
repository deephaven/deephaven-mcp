import { selectConsole } from "../../actions/console/actions.ts";
import { defineCommand } from "../../cli/command.ts";

export const use = defineCommand({
  name: "use",
  description: "Set the console in use for its profile.",
  args: [
    { name: "[console]", description: "Console shortcode or full server ID." },
  ],

  async run(ctx, [target = ""]) {
    await selectConsole(ctx.store, target, ctx.profileFlag, ctx.lifecycle);
    return {
      human: "",
      json: {},
      empty: true,
    };
  },
});
