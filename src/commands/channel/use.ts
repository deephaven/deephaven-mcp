import { channelError, DEV_CHANNEL } from "../../channel.ts";
import { defineCommand } from "../../cli/command.ts";
import { DhError, reasonOf } from "../../errors.ts";
import { withMetaLock, writeMeta } from "../../install-meta.ts";
import {
  download,
  fetchManifest,
  HttpError,
  manifestUrl,
  replaceExecutable,
} from "../../updater.ts";
import { BIN, CHANNEL, VERSION } from "../../version.ts";

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
    // Dev builds (from source or a local build) are never replaced, like auto-update.
    if (CHANNEL === DEV_CHANNEL) {
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

    // Downloaded before saving anything, so a failure leaves the install as it was.
    let binary: Uint8Array | undefined;
    try {
      binary = await download(manifest, url, ctx.signal);
    } catch (e) {
      ctx.signal.throwIfAborted();
      throw new DhError(
        "server_unreachable",
        `Could not download ${manifest.version} from channel ${channel}; nothing changed`,
        `Run \`${BIN} channel use ${channel}\` again.`,
        reasonOf(e),
      );
    }
    // Together under the lock, so the build always matches the saved channel.
    // Swapped first: if that fails, the channel isn't saved either.
    await withMetaLock(
      async () => {
        if (binary) await replaceExecutable(binary);
        await writeMeta({ channel });
      },
      { wait: true },
    );
    const to = manifest.version;
    return {
      human: [
        `✔ Following ${channel}`,
        to === VERSION ? `  Already on ${to}` : `  Updated ${VERSION} → ${to}`,
      ],
      json: { channel, from: VERSION, to },
    };
  },
});
