import { DEV_CHANNEL } from "../../channel.ts";
import { defineCommand } from "../../cli/command.ts";
import { followedChannel, readMeta } from "../../install-meta.ts";
import { CHANNEL } from "../../version.ts";
import { use } from "./use.ts";

/** `channel` alone shows which channel this install follows. */
export const channel = defineCommand({
  name: "channel",
  description: "Show or change the release channel this install follows.",
  commands: [use],

  async run() {
    const { channel, source } = followedChannel(await readMeta());
    const human = [`${channel} (${source})`];
    if (CHANNEL === DEV_CHANNEL) {
      human.push("! Running a dev build, which never updates.");
    } else if (CHANNEL !== channel) {
      human.push(
        `! Running the ${CHANNEL} build; the ${channel} build installs at the next update.`,
      );
    }
    return { human, json: { channel, source, build: CHANNEL } };
  },
});
