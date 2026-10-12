import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BIN, STANDALONE } from "../version.ts";
import {
  type CommunityConnection,
  type CommunityLogin,
  communityUser,
  connectCommunity,
  loginCommunity,
} from "./community.ts";
import {
  connectEnterprise,
  type EnterpriseConnection,
  loginKey,
  loginPassword,
} from "./enterprise.ts";
import { DhError, timeoutMs } from "./errors.ts";
import { keyPairOf } from "./flows.ts";
import { type KeyPair, readKeyFile } from "./keyfile.ts";
import { findByName } from "./profile.ts";
import { resolveServer } from "./resolve.ts";
import {
  type Credential,
  type Kind,
  type Profile,
  type State,
  Store,
} from "./store.ts";

export type LoginSpec =
  | { kind: "community"; login: CommunityLogin }
  | {
      kind: "enterprise";
      username: string;
      operateAs?: string;
      password?: string;
      keyPair?: KeyPair;
    };

export interface Session {
  origin: string;
  kind: Kind;
  user: string;
  operateAs?: string;
  profileName?: string;
  community?: CommunityConnection;
  enterprise?: EnterpriseConnection;
}

/** Connects to `origin` and logs in with `spec`. */
export async function connectAndLogin(
  origin: string,
  spec: LoginSpec,
  cacheRoot: string,
  rejected: "auth_failed" | "auth_expired",
): Promise<Session> {
  const timeout = timeoutMs();
  if (spec.kind === "community") {
    const conn = await connectCommunity(origin, cacheRoot, timeout);
    await loginCommunity(conn, spec.login, timeout, rejected);
    return {
      origin,
      kind: "community",
      user: communityUser(spec.login),
      community: conn,
    };
  }
  const conn = await connectEnterprise(origin, cacheRoot, timeout);
  if (spec.keyPair) {
    await loginKey(
      conn,
      spec.username,
      spec.keyPair,
      spec.operateAs,
      timeout,
      rejected,
    );
  } else {
    await loginPassword(
      conn,
      spec.username,
      spec.password ?? "",
      spec.operateAs,
      timeout,
    );
  }
  return {
    origin,
    kind: "enterprise",
    user: spec.username,
    operateAs: spec.operateAs,
    enterprise: conn,
  };
}

/** Set in a re-run process to the `--ca-cert` file it trusts. */
const LOADED_CA = "DH_LOADED_CA_CERT";

export function loadedCa(): string | undefined {
  return process.env[LOADED_CA];
}

/**
 * TLS trust is fixed at process start, so a custom CA means re-running this
 * process with `NODE_EXTRA_CA_CERTS` set. `extraArgs` carry choices already made.
 */
export async function ensureCa(
  caCert: string | undefined,
  extraArgs: string[] = [],
): Promise<void> {
  if (!caCert || loadedCa() === caCert) return;
  let pem: string;
  try {
    pem = await readFile(caCert, "utf8");
  } catch {
    throw new DhError("usage", `CA certificate not found: ${caCert}`);
  }
  // Keep CAs the user already trusts (e.g. a corporate proxy's), for updates too.
  const existing = process.env.NODE_EXTRA_CA_CERTS;
  const dir = existing ? await mkdtemp(join(tmpdir(), `${BIN}-ca-`)) : "";
  const bundle = existing ? join(dir, "ca.pem") : caCert;
  if (existing) {
    const theirs = await readFile(existing, "utf8").catch(() => "");
    await writeFile(bundle, `${theirs}\n${pem}`);
  }
  // Compiled: argv[1] is a virtual path to skip, and runtime flags are built in.
  const args = STANDALONE
    ? process.argv.slice(2)
    : [...process.execArgv, ...process.argv.slice(1)];
  const proc = Bun.spawn([process.execPath, ...args, ...extraArgs], {
    env: { ...process.env, NODE_EXTRA_CA_CERTS: bundle, [LOADED_CA]: caCert },
    stdio: ["inherit", "inherit", "inherit"],
  });
  const code = await proc.exited;
  if (dir) await rm(dir, { recursive: true, force: true });
  process.exit(code);
}

function readSecret(cred: Credential, profile: Profile): string | undefined {
  if (cred.type === "secret") return cred.value;
  if (cred.type !== "envRef") return undefined;
  const value = process.env[cred.env];
  if (value === undefined) {
    throw new DhError(
      "credential_unavailable",
      `Profile "${profile.name}" needs ${cred.env}, which is not set`,
      `Set ${cred.env}, or run \`${BIN} auth login --profile ${profile.name}\`.`,
    );
  }
  if (!cred.basicToken) return value;
  const at = value.indexOf(":");
  if (at < 0 || value.slice(0, at) !== profile.user) {
    throw new DhError(
      "credential_unavailable",
      `Profile "${profile.name}" needs ${cred.env} to be "${profile.user}:<password>"`,
      `Fix ${cred.env}, or run \`${BIN} auth login --profile ${profile.name}\`.`,
    );
  }
  return value.slice(at + 1);
}

export async function specForProfile(
  profile: Profile,
  kind: Kind,
  cred: Credential,
): Promise<LoginSpec> {
  if (kind === "community") {
    return {
      kind,
      login: {
        method: profile.method,
        handler: profile.handler,
        username: profile.method === "basic" ? profile.user : undefined,
        secret: readSecret(cred, profile),
      },
    };
  }
  const keyPair =
    cred.type === "keyPair"
      ? keyPairOf(cred)
      : cred.type === "keyFile"
        ? (await readKeyFile(cred.path)).keyPair
        : undefined;
  return {
    kind,
    username: profile.user,
    operateAs: profile.operateAs,
    keyPair,
    password: keyPair ? undefined : readSecret(cred, profile),
  };
}

/** `DH_SERVER` + `DH_*` credentials; never touches the config directory. */
async function envSession(): Promise<Session> {
  const env = (name: string) => process.env[name] || undefined;
  const secrets = [
    "DH_PASSWORD",
    "DH_PSK",
    "DH_PRIVATE_KEY_FILE",
    "DH_AUTH_TOKEN",
  ].filter(env);
  if (secrets.length > 1) {
    throw new DhError("usage", `Set only one of ${secrets.join(", ")}`);
  }
  await ensureCa(env("DH_CA_CERT"));
  const { origin, kind } = await resolveServer(env("DH_SERVER")!);
  const username = env("DH_USERNAME");
  const operateAs = env("DH_OPERATE_AS");
  let spec: LoginSpec;
  if (kind === "enterprise") {
    const keyFile = env("DH_PRIVATE_KEY_FILE");
    const key = keyFile ? await readKeyFile(keyFile) : undefined;
    const user = username ?? key?.user;
    if (!user || (!key && !env("DH_PASSWORD"))) {
      throw new DhError(
        "usage",
        "Enterprise needs DH_USERNAME with DH_PASSWORD, or DH_PRIVATE_KEY_FILE",
      );
    }
    spec = {
      kind,
      username: user,
      operateAs: operateAs ?? key?.operateAs,
      keyPair: key?.keyPair,
      password: env("DH_PASSWORD"),
    };
  } else {
    if (env("DH_PRIVATE_KEY_FILE")) {
      throw new DhError(
        "usage",
        "DH_PRIVATE_KEY_FILE is only for Enterprise servers",
      );
    }
    const login: CommunityLogin = env("DH_PSK")
      ? { method: "psk", secret: env("DH_PSK") }
      : env("DH_PASSWORD")
        ? { method: "basic", username, secret: env("DH_PASSWORD") }
        : env("DH_AUTH_TOKEN")
          ? {
              method: "custom",
              handler: env("DH_AUTH_HANDLER"),
              secret: env("DH_AUTH_TOKEN"),
            }
          : { method: "anonymous" };
    if (login.method === "basic" && !username) {
      throw new DhError("usage", "DH_PASSWORD needs DH_USERNAME");
    }
    if (login.method === "custom" && !login.handler) {
      throw new DhError("usage", "DH_AUTH_TOKEN needs DH_AUTH_HANDLER");
    }
    spec = { kind, login };
  }
  // The jsapi loader makes this run's own owner-only folder inside it.
  return await connectAndLogin(origin, spec, tmpdir(), "auth_failed");
}

async function profileSession(
  store: Store,
  state: State,
  id: string,
): Promise<Session> {
  const profile = state.config.profiles[id];
  if (!profile) throw new DhError("internal", `No profile ${id}`);
  const server = state.config.servers[profile.server];
  const cred = state.credentials.credentials[id] ?? { type: "none" };
  await ensureCa(server?.caCert);
  const spec = await specForProfile(profile, server?.kind ?? "community", cred);
  try {
    const session = await connectAndLogin(
      profile.server,
      spec,
      store.cacheDir,
      "auth_expired",
    );
    return { ...session, profileName: profile.name };
  } catch (e) {
    if (e instanceof DhError && e.code === "auth_expired") {
      throw new DhError(
        "auth_expired",
        `${profile.server} rejected the saved credentials for "${profile.name}"`,
        `Run \`${BIN} auth login --profile ${profile.name}\`.`,
      );
    }
    throw e;
  }
}

/** `--profile` → `DH_PROFILE` → `DH_SERVER` → default. */
export async function requireAuth(
  profileFlag?: string,
  store = new Store(),
): Promise<Session> {
  const name = profileFlag ?? process.env.DH_PROFILE;
  if (!name && process.env.DH_SERVER) return await envSession();
  const state = await store.read();
  const id = name
    ? findByName(state.config, name)?.[0]
    : state.config.defaultProfile;
  if (name && !id) {
    throw new DhError(
      "usage",
      `No profile named "${name}"`,
      `Run \`${BIN} auth\` to list profiles.`,
    );
  }
  if (!id || !state.config.profiles[id]) {
    throw new DhError(
      "auth_required",
      "You're not logged in to a Deephaven server.",
      `Run \`${BIN} auth login\` to get started.`,
    );
  }
  return await profileSession(store, state, id);
}
