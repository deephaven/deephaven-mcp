import { Option } from "commander";
import {
  deleteConsole,
  resolveConsoleDeletion,
} from "../../actions/console/actions.ts";
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
    const resolved = await resolveConsoleDeletion(
      ctx.store,
      target,
      ctx.profileFlag,
      ctx.lifecycle,
    );
    if (
      !(await confirmDeleteConsole(
        ctx.ui,
        resolved.console.shortcode,
        Boolean(opts.force),
      ))
    ) {
      return { human: "", json: {}, empty: true };
    }
    await deleteConsole(ctx.store, resolved, ctx.lifecycle);
    return { human: "", json: {}, empty: true };
  },
});
