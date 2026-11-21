import { channelError } from "../../channel.ts";
import { defineCommand } from "../../cli/command.ts";
import { DhError, reasonOf } from "../../errors.ts";
import { updateMeta } from "../../install-meta.ts";
import {
  fetchManifest,
  HttpError,
  install,
  manifestUrl,
} from "../../updater.ts";
import { BIN, STANDALONE, VERSION } from "../../version.ts";

export const use = defineCommand({
  name: "use",
  description: "Follow a release channel and update to its build now.",
  args: [
    { name: "<channel>", description: "Channel name, e.g. release or rc." },
  ],

  async run(ctx, [name]) {
    const channel = name ?? "";
    const invalid = channelError(channel);
    if (invalid) {
      throw new DhError(
        "usage",
        invalid,
        `Run \`${BIN} channel use release\` to follow the default channel.`,
      );
    }
    if (!STANDALONE) {
      throw new DhError(
        "usage",
        `${BIN} channel use needs an installed ${BIN}`,
        "Install it with the install script, then run this again.",
      );
    }

    // Before saving anything, so a typo leaves the install as it was.
    const url = manifestUrl(channel);
    let manifest: Awaited<ReturnType<typeof fetchManifest>>;
    try {
      manifest = await fetchManifest(url, ctx.signal);
    } catch (e) {
      ctx.signal.throwIfAborted();
      if (e instanceof HttpError && e.status === 404) {
        throw new DhError(
          "channel_not_found",
          `No published build on channel ${channel}`,
          `Run \`${BIN} channel use release\` to follow the default channel.`,
        );
      }
      throw new DhError(
        "server_unreachable",
        `Could not check channel ${channel}`,
        undefined,
        reasonOf(e),
      );
    }

    await updateMeta({ channel });
    try {
      await install(manifest, url, ctx.signal);
    } catch (e) {
      ctx.signal.throwIfAborted();
      throw new DhError(
        "server_unreachable",
        `Following ${channel}, but installing ${manifest.version} failed`,
        "It installs at the next update check.",
        reasonOf(e),
      );
    }
    const to = manifest.version;
    return {
      human:
        to === VERSION
          ? `✔ Following ${channel}; already on ${to}`
          : `✔ Following ${channel}; updated ${VERSION} → ${to}`,
      json: { channel, from: VERSION, to },
    };
  },
});
