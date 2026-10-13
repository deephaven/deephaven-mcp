import { renameProfile } from "../../auth/flows.ts";
import { defineCommand } from "../../cli/command.ts";

export const rename = defineCommand({
  name: "rename",
  description: "Rename a profile.",
  args: [{ name: "<profile>" }, { name: "<name>" }],

  async run(ctx, [from = "", to = ""]) {
    await renameProfile(ctx.store, from, to);
    return { human: `✔ Renamed ${from} to ${to}`, json: { from, to } };
  },
});
