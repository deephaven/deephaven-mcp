import { Option } from "commander";
import { deleteConsole } from "../../actions/console/actions.ts";
import { defineCommand } from "../../cli/command.ts";
import { confirmDeleteConsole } from "./questions.ts";

interface DeleteOptions {
  force?: boolean;
}

export const remove = defineCommand<DeleteOptions>({
  name: "delete",
  description: "Delete a console.",
  args: [
    { name: "[console]", description: "Console shortcode or full server ID." },
  ],
  options: [new Option("-f, --force", "Delete without confirmation.")],

  async run(ctx, [target], opts) {
    if (!(await confirmDeleteConsole(ctx.ui, target, Boolean(opts.force)))) {
      return { human: "", json: {}, empty: true };
    }
    await deleteConsole(ctx.store, target, ctx.profileFlag, ctx.lifecycle);
    return { human: "", json: {}, empty: true };
  },
});
