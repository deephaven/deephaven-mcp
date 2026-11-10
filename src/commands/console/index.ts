import { listConsoles } from "../../actions/console/actions.ts";
import { defineCommand } from "../../cli/command.ts";
import { create } from "./create.ts";
import { defaults } from "./default.ts";
import { remove } from "./delete.ts";
import { detail } from "./detail.ts";
import { use } from "./use.ts";

export const consoleCommand = defineCommand({
  name: "console",
  description: "Manage interactive consoles.",
  commands: [detail, use, create, defaults, remove],

  async run(ctx) {
    const consoles = await listConsoles(
      ctx.store,
      ctx.profileFlag,
      ctx.lifecycle,
    );
    return {
      human:
        consoles.length === 0
          ? "No CLI-created consoles."
          : consoles.map(
              (console) =>
                `${console.inUse ? "●" : " "} ${console.shortcode}  ${console.state}  ${console.name}`,
            ),
      json: consoles,
    };
  },
});
