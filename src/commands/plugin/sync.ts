import {
  readConfig,
  requireProfile,
  serverSsh,
} from "../../actions/plugins/settings.ts";
import {
  type PartSync,
  syncJs,
  syncShell,
} from "../../actions/plugins/sync.ts";
import { defineCommand, type Output } from "../../cli/command.ts";
import { DhError, reasonOf } from "../../errors.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";

/** `dh plugin sync`: bring the active profile's server's plugins up to date now. */
export function syncCommand() {
  return defineCommand({
    name: "sync",
    description:
      "Fetch the profile's server's plugins now: over the web, and over SSH if enabled.",
    async run(ctx) {
      const config = await readConfig(ctx.store);
      const { profile } = requireProfile(config, ctx.profileFlag);
      const results: ChannelResult[] = [
        await attempt("js", () =>
          syncJs(ctx.store, profile.server, {
            profileFlag: ctx.profileFlag,
            groups: true,
            lifecycle: ctx.lifecycle,
          }),
        ),
        serverSsh(config, profile.server).enabled
          ? await attempt("shell", () =>
              syncShell(ctx.store, config, profile.server, {
                prompt: ctx.ui.interactive,
              }),
            )
          : { channel: "shell", status: "disabled" },
      ];
      const failed = results.find((r) => r.status === "failed");
      if (failed && "error" in failed) {
        // Every channel's line first: one failing doesn't hide the other.
        for (const r of results) note(`${r.channel}: ${syncText(r)}`);
        throw failed.error;
      }
      return syncOutput(profile.server, results);
    },
  });
}

/** What syncing one channel gave: its result, SSH not enabled, or its error. */
export type ChannelResult =
  | PartSync
  | { channel: "shell"; status: "disabled" }
  | { channel: "js" | "shell"; status: "failed"; error: DhError };

/** `fn`'s result, or its error as a `failed` result. */
async function attempt(
  channel: "js" | "shell",
  fn: () => Promise<PartSync>,
): Promise<ChannelResult> {
  try {
    const { manifest: _, ...result } = (await fn()) as PartSync & {
      manifest?: unknown;
    };
    return result;
  } catch (e) {
    const error =
      e instanceof DhError ? e : new DhError("internal", reasonOf(e) ?? "?");
    return { channel, status: "failed", error };
  }
}

/** One line per channel: what changed, or why nothing could. */
export function syncOutput(server: string, results: ChannelResult[]): Output {
  return {
    human: results.map((r) => `${r.channel}: ${syncText(r)}`),
    json: { server, channels: results },
  };
}

function syncText(r: ChannelResult): string {
  if (r.status === "failed") return `✖ ${r.error.message}`;
  if (r.status === "disabled") {
    return `SSH not enabled (\`${BIN} plugin ssh enable\`)`;
  }
  if (r.status === "unchanged") return "unchanged";
  if (r.status === "none") {
    return r.path
      ? `no dh-cli bundle at ${r.path} on ${r.from}`
      : "this server provides no dh plugins";
  }
  const parts = [
    r.added.length > 0 && `added ${r.added.join(", ")}`,
    r.changed.length > 0 && `changed ${r.changed.join(", ")}`,
    r.removed.length > 0 && `removed ${r.removed.join(", ")}`,
  ].filter(Boolean);
  return `updated from ${r.from}${parts.length > 0 ? `: ${parts.join("; ")}` : ""}`;
}
