import type {
  Discovery,
  Plugin,
  PluginSource,
} from "../../actions/plugins/discover.ts";
import {
  activeProfile,
  hostsDefaultFirst,
  localPath,
  readConfig,
  sshSettings,
} from "../../actions/plugins/settings.ts";
import type { Context } from "../../cli/command.ts";
import { type CommandSpec, defineCommand } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { checkCommand } from "./check.ts";
import { setCommand } from "./set.ts";
import { sshCommand } from "./ssh.ts";
import { syncCommand } from "./sync.ts";
import { pluginUseCommand } from "./use.ts";

/** `dh plugin`, built from what startup discovered, so it reports exactly what's registered. */
export function pluginGroup(found: Discovery): CommandSpec<never> {
  const list = defineCommand({
    name: "list",
    description:
      "List the plugins: where they come from, how each runs, and any that can't.",
    run: (ctx) => listPlugins(ctx, found),
  });
  return defineCommand({
    name: "plugin",
    description: "Commands the cluster provides, and how they run.",
    commands: [
      list,
      syncCommand(),
      sshCommand(),
      setCommand(found),
      pluginUseCommand(found),
      checkCommand(found),
    ],
    run: (ctx) => listPlugins(ctx, found),
  });
}

/** The plugin named `name`, else `usage`. */
export function askPlugin(found: Discovery, name: string): Plugin {
  const plugin = found.plugins.find((p) => p.name === name && p.entry);
  if (plugin) return plugin;
  throw new DhError(
    "usage",
    `No plugin named "${name}"`,
    `Run \`${BIN} plugin list\`.`,
  );
}

/** One channel's source, as `dh plugin list` shows it. */
export interface ChannelInfo {
  channel: "js" | "shell";
  /** `env` (`DH_PLUGIN_BUNDLE`), `local` (this node's install), `ssh`, or null when there's none. */
  source: string | null;
  /** The directory read in place, for `env` and `local`. */
  dir: string | null;
  server: string | null;
  sshHost: string | null;
  revision: string | null;
  clusterVersion: string | null;
  fetchedAt: string | null;
  /** Why there are no plugins from it, if so. */
  reason: string | null;
}

/** Each channel's source, or why it has none; `js` first. */
export function channelsOf(
  found: Discovery,
  server: string | null,
): ChannelInfo[] {
  const channels: ChannelInfo[] = [
    ...found.sources.map(channelInfo),
    ...found.noSource.map((n) => ({
      channel: n.channel,
      source: null,
      dir: null,
      server,
      sshHost: null,
      revision: null,
      clusterVersion: null,
      fetchedAt: null,
      reason: n.reason,
    })),
  ];
  return channels.sort((a, b) => a.channel.localeCompare(b.channel));
}

function channelInfo(s: PluginSource): ChannelInfo {
  const m = s.manifest;
  return {
    channel: s.channel,
    source: s.kind,
    dir: s.kind === "ssh" ? null : s.dir,
    server: s.origin ?? null,
    sshHost: s.state?.host ?? null,
    revision: m?.revision ?? null,
    clusterVersion: m?.cluster.version ?? s.state?.clusterVersion ?? null,
    fetchedAt: s.state?.fetchedAt ?? null,
    reason: s.missing ?? null,
  };
}

/** Each channel's source, then each plugin: how it runs and its state. */
async function listPlugins(ctx: Context, found: Discovery) {
  const config = await readConfig(ctx.store);
  const active = activeProfile(config, ctx.profileFlag);
  const channels = channelsOf(found, active?.profile.server ?? null);
  const plugins = found.plugins.map((p) => {
    const ssh =
      p.exec === "ssh" && active
        ? sshSettings(config, active.profile, p.name)
        : undefined;
    return {
      name: p.name,
      kind: p.channel,
      nouns: p.entry?.nouns ?? [],
      exec: p.exec,
      tool: p.spec ? localOrRemote(p, config) : null,
      sshHosts: ssh ? hostsDefaultFirst(ssh) : null,
      defaultSshHost: ssh?.defaultHost ?? null,
      runAs: p.spec?.tool.runAs
        ? {
            account: p.spec.tool.runAs.account,
            unlessFlags: p.spec.tool.runAs.unlessFlags ?? [],
          }
        : null,
      state: p.state,
      reason: p.reason ?? null,
    };
  });
  const width = Math.max(0, ...plugins.map((p) => p.name.length));
  const human: string[] = [];
  for (const c of channels) {
    human.push(channelLine(c));
    for (const p of plugins.filter((p) => p.kind === c.channel)) {
      const where = p.sshHosts ? `${hostsText(p)} ` : "";
      const as = p.runAs ? `, runs as ${p.runAs.account}` : "";
      let how: string = p.exec;
      if (p.exec === "js") how = "js: runs in dh";
      else if (p.tool) how = `${p.exec}: ${where}${p.tool}${as}`;
      const state = p.state === "ready" ? "" : `  (${p.reason ?? p.state})`;
      const nouns =
        p.nouns.length === 0 || p.nouns.join() === p.name
          ? ""
          : `  [${p.nouns.join(", ")}]`;
      human.push(`  ${p.name.padEnd(width)}   ${how}${nouns}${state}`);
    }
  }
  // The `shell` plugins the `js` manifest names, when SSH isn't enabled to get them.
  const overSsh = found.noSource.some(
    (n) => n.channel === "shell" && n.reason === "SSH not enabled",
  )
    ? (found.sources.find((s) => s.channel === "js")?.manifest?.shell ?? [])
    : [];
  const signIn = found.signIn;
  if (signIn && signIn.plugins.length > 0) {
    human.push(
      `Sign in to use this server's other plugins (${signIn.plugins.join(", ")}): \`${BIN} auth login --profile ${signIn.profile}\``,
    );
  }
  if (overSsh.length > 0) {
    human.push(
      `Over SSH, this server also has: ${overSsh.map((p) => p.name).join(", ")}. \`${BIN} plugin ssh enable\` to use them.`,
    );
  }
  return {
    human,
    json: {
      channels,
      plugins,
      overSsh: overSsh.map((p) => p.name),
      signIn: signIn?.plugins ?? [],
    },
  };
}

/** The tool's path where it runs: this machine's setting for a local one. */
function localOrRemote(
  p: Plugin,
  config: Awaited<ReturnType<typeof readConfig>>,
): string | null {
  if (!p.spec) return null;
  return p.exec === "local" ? localPath(config, p.spec) : p.spec.tool.path;
}

/** `shell: over SSH from infra-1 (https://…), cluster 2026.01…, revision sha256:1a2b3c4d, fetched …`. */
function channelLine(c: ChannelInfo): string {
  const from = {
    env: `DH_PLUGIN_BUNDLE (${c.dir})`,
    local: `this node (${c.dir})`,
    ssh: `over SSH${c.sshHost ? ` from ${c.sshHost}` : ""} (${c.server})`,
    web: `from ${c.server}`,
  }[c.source ?? ""];
  if (c.reason || !from) {
    return `${c.channel}: ${from ? `${from}: ` : ""}${c.reason ?? "none"}`;
  }
  const details = [
    c.clusterVersion && `cluster ${c.clusterVersion}`,
    c.revision && `revision ${c.revision.slice(0, "sha256:".length + 8)}`,
    c.fetchedAt && `fetched ${c.fetchedAt}`,
  ].filter(Boolean);
  return `${c.channel}: ${from}${details.length > 0 ? `, ${details.join(", ")}` : ""}`;
}

/** `dis-2*, dis-1`: the hosts, default first and starred when there are several. */
function hostsText(p: {
  sshHosts: string[] | null;
  defaultSshHost: string | null;
}): string {
  if (!p.sshHosts) return "";
  const many = p.sshHosts.length > 1;
  return p.sshHosts
    .map((h) => (many && h === p.defaultSshHost ? `${h}*` : h))
    .join(", ");
}
