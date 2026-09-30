import { Command } from "@cliffy/command";
import { autoUpdate } from "./updater.ts";
import { VERSION } from "./version.ts";

await new Command()
  .name("dh")
  .version(VERSION)
  .description("Deephaven command line interface.")
  .action(function () {
    this.showHelp();
  })
  .parse(Deno.args);

try {
  await autoUpdate();
} catch (e) {
  if (Deno.env.get("DH_DEBUG")) console.error(`dh: auto-update failed: ${e}`);
}
