import { detailConsole } from "../../actions/console/actions.ts";
import { defineCommand } from "../../cli/command.ts";

export const detail = defineCommand({
  name: "detail",
  description: "Show every setting and status of a console.",
  args: [
    { name: "[console]", description: "Console shortcode or full server ID." },
  ],

  async run(ctx, [target]) {
    const console = await detailConsole(
      ctx.store,
      target,
      ctx.profileFlag,
      ctx.lifecycle,
    );
    return {
      human: [
        `${console.name} (${console.id})`,
        `  State: ${console.state}`,
        `  Language: ${console.language ?? "unknown"}`,
        `  Heap size (GB): ${console.heapSizeGb ?? "unknown"}`,
        `  Server: ${console.server ?? "unknown"}`,
        `  Engine: ${console.engine ?? "unknown"}`,
        `  JVM arguments: ${console.jvmArgs ?? ""}`,
        `  Environment: ${JSON.stringify(console.environment)}`,
        `  Auto-delete timeout (seconds): ${console.autoDeleteTimeoutSeconds ?? "unknown"}`,
      ],
      json: console,
    };
  },
});
