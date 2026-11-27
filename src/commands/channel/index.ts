import { styleText } from "node:util";
import { DEV_CHANNEL, RELEASE_CHANNEL } from "../../channel.ts";
import { defineCommand } from "../../cli/command.ts";
import { followedChannel, readMeta } from "../../install-meta.ts";
import { channelManifestUrl, fetchManifest, HttpError } from "../../updater.ts";
import { BIN, CHANNEL, VERSION } from "../../version.ts";
import { use } from "./use.ts";

/**
 * The channels `dh channel` always lists. Others (alpha channels) appear only
 * when followed: they are never advertised to people or agents.
 */
const LISTED = [RELEASE_CHANNEL, "rc"];
const NOT_PUBLISHED = "not published";
const UNKNOWN = "?";

/** A channel's current version, `not published` on a 404, `?` on any other failure. */
async function currentVersion(
  channel: string,
  signal: AbortSignal,
): Promise<string> {
  try {
    return (await fetchManifest(channelManifestUrl(channel), signal)).version;
  } catch (e) {
    signal.throwIfAborted();
    return e instanceof HttpError && e.status === 404 ? NOT_PUBLISHED : UNKNOWN;
  }
}

/** `channel` alone lists release, rc and the followed channel, like `auth` lists profiles. */
export const channel = defineCommand({
  name: "channel",
  description: "List release channels, marking the one this install follows.",
  commands: [use],

  async run(ctx) {
    const build = { build: CHANNEL, version: VERSION };
    // Dev builds never update, so no channel applies; from source, dh.meta
    // would even be read from beside the bun runtime.
    if (CHANNEL === DEV_CHANNEL) {
      return {
        human: `${DEV_CHANNEL} (never updates)`,
        json: { channels: [], ...build },
      };
    }
    const followed = followedChannel(await readMeta());
    const names = LISTED.includes(followed) ? LISTED : [...LISTED, followed];
    const versions = await Promise.all(
      names.map((name) => currentVersion(name, ctx.signal)),
    );
    const width = Math.max(...names.map((name) => name.length));
    const human = names.map(
      (name, i) =>
        `${name === followed ? styleText("green", "●") : " "} ${name.padEnd(width)}   ${versions[i]}`,
    );
    const unknown = names.filter((_, i) => versions[i] === UNKNOWN);
    if (unknown.length) human.push(`! Couldn't check ${unknown.join(", ")}.`);
    if (CHANNEL !== followed) {
      const off =
        process.env.DH_AUTO_UPDATE === "off" ? "; auto-update is off" : "";
      human.push(
        `! Running ${VERSION} from the ${CHANNEL} channel, not ${followed}${off}.`,
        `  Run \`${BIN} channel use ${followed}\` to install ${followed}'s build now.`,
      );
    }
    const channels = names.map((name, i) => ({
      channel: name,
      version:
        versions[i] === NOT_PUBLISHED || versions[i] === UNKNOWN
          ? null
          : versions[i],
      following: name === followed,
    }));
    return { human, json: { channels, ...build } };
  },
});
