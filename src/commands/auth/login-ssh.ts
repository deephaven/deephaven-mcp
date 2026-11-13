/**
 * `dh auth login <server> --method ssh`: a profile with SSH access to the
 * cluster's nodes and no sign-in (plans/ssh-profiles.md).
 */
import { defaultName, newProfileId } from "../../actions/auth/profile.ts";
import type { Config, Store } from "../../actions/auth/store.ts";
import { DEFAULT_BUNDLE } from "../../actions/plugins/bundle.ts";
import { readClusterOverSsh } from "../../actions/plugins/ssh.ts";
import { storeShellCopy } from "../../actions/plugins/sync.ts";
import type { Context, Output } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";
import { askSshHosts } from "../plugin/questions.ts";
import { type Check, checkSshLogin, stepOnHost } from "../plugin/ssh-checks.ts";
import type { LoginOptions } from "./login.ts";
import { askSshLoginHosts } from "./questions.ts";

/**
 * Questions (the SSH hosts), then actions: check the connection, read the
 * cluster's address and copy `shell/` in one run, then save the server
 * entry, the cache and the profile. A node with no bundle saves nothing.
 */
export async function sshLogin(
  ctx: Context,
  typed: string,
  opts: LoginOptions,
): Promise<Output> {
  const hosts =
    opts.sshHost !== undefined
      ? askSshHosts(opts.sshHost, false)
      : await askSshLoginHosts(ctx.ui, typed);
  const host = hosts[0] ?? "";
  // A person may answer ssh's own password or 2FA prompt; the connection is reused after.
  const mode = ctx.noPrompt ? "batch" : "prompt";
  const checks: Check[] = [];
  await checkSshLogin({}, host, false, stepOnHost(checks, [host], host), mode);
  const bundle = opts.sshBundle ?? DEFAULT_BUNDLE;
  const { frontend, copy } = await readClusterOverSsh(
    host,
    `${bundle}/shell`,
    mode,
  );
  const origin = clusterOrigin(frontend, typed);
  if (copy.kind !== "copied") {
    throw new DhError(
      "server_unsupported",
      `No dh plugins at ${bundle} on ${host}`,
      "This DHE release may predate them; or set the path with --ssh-bundle <path>.",
    );
  }
  const manifest = await storeShellCopy(ctx.store, origin, host, copy);
  note(
    `✔ Cluster ${origin} (Deephaven Enterprise ${manifest.cluster.version})`,
  );
  const names = manifest.plugins.map((p) => p.name);
  if (names.length > 0) note(`✔ Plugins: ${names.join(", ")}`);
  const saved = await saveSshProfile(ctx.store, {
    origin,
    hosts: opts.sshHost !== undefined ? hosts : undefined,
    bundle: opts.sshBundle,
    name: opts.name,
    makeDefault: opts.default,
  });
  return {
    human: [
      `✔ Saved profile "${saved.name}"${saved.isDefault ? " (default)" : ""}`,
    ],
    json: {
      profile: saved.name,
      server: origin,
      user: null,
      method: "ssh",
      sshHosts: hosts,
      default: saved.isDefault,
      plugins: { shell: names },
    },
  };
}

/**
 * The cluster's origin: `https://<FRONTEND_HOST>:<FRONTEND_PORT>` from the
 * node, else the typed address if it has a port; never a guessed port.
 */
function clusterOrigin(
  frontend: { host: string; port: string },
  typed: string,
): string {
  if (frontend.host && /^\d+$/.test(frontend.port)) {
    return new URL(`https://${frontend.host}:${frontend.port}`).origin;
  }
  const url = new URL(typed.includes("://") ? typed : `https://${typed}`);
  if (url.port) return url.origin;
  throw new DhError(
    "usage",
    `Can't tell which port ${url.hostname}'s web server uses; give the full URL, e.g. https://${url.hostname}:8000`,
  );
}

/**
 * Saves the SSH-only profile: the server entry (existing settings kept;
 * `ssh.hosts` replaced only if given), then the profile, reusing one
 * already on that server. The default follows login's rules.
 */
async function saveSshProfile(
  store: Store,
  p: {
    origin: string;
    hosts?: string[];
    bundle?: string;
    name?: string;
    makeDefault?: boolean;
  },
): Promise<{ name: string; isDefault: boolean }> {
  return await store.update(({ config, credentials }) => {
    const entry = config.servers[p.origin];
    config.servers[p.origin] = {
      ...entry,
      kind: entry?.kind ?? "enterprise",
      ssh: {
        ...entry?.ssh,
        enabled: true,
        ...(p.hosts ? { hosts: p.hosts } : {}),
        ...(p.bundle ? { bundle: p.bundle } : {}),
      },
    };
    const existing = Object.entries(config.profiles).find(
      ([, x]) => x.server === p.origin && x.method === "ssh",
    );
    const id =
      existing?.[0] ??
      newProfileId(
        (id) => id in config.profiles || id in credentials.credentials,
      );
    const name =
      p.name ?? existing?.[1].name ?? defaultName(config, p.origin, "ssh");
    assertNameFree(config, name, id);
    config.profiles[id] = {
      name,
      server: p.origin,
      method: "ssh",
      createdAt: existing?.[1].createdAt ?? new Date().toISOString(),
    };
    const hasDefault =
      config.defaultProfile !== undefined &&
      config.defaultProfile in config.profiles &&
      config.defaultProfile !== id;
    if (
      p.makeDefault === true ||
      (p.makeDefault === undefined && !hasDefault)
    ) {
      config.defaultProfile = id;
    }
    return { name, isDefault: config.defaultProfile === id };
  });
}

function assertNameFree(config: Config, name: string, id: string): void {
  const other = Object.entries(config.profiles).find(
    ([otherId, x]) => x.name === name && otherId !== id,
  );
  if (other) {
    throw new DhError(
      "usage",
      `A profile named "${name}" exists`,
      `Pass another --name, or \`${BIN} auth rename\` it.`,
    );
  }
}
