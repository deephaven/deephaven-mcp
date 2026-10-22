import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DhError, isErrno, reasonOf } from "../errors.ts";
import { note } from "../output.ts";
import { BIN, STANDALONE } from "../version.ts";
import type { CommunityMethod } from "./community.ts";
import { keyPairOf } from "./flows.ts";
import { type KeyPair, readKeyFile } from "./keyfile.ts";
import { findByName } from "./profile.ts";
import {
  candidates,
  type ResolvedServer,
  resolveServer,
  withPort,
} from "./resolve.ts";
import {
  connectServer,
  disconnectServer,
  type Session,
  type SignInRequest,
  signIn,
} from "./signin.ts";
import {
  type Config,
  type Credential,
  type Kind,
  type Profile,
  type State,
  Store,
} from "./store.ts";

/** Connects to `origin` and signs in; closes the connection if sign-in fails. A rejection throws `rejected`. */
async function connectAndSignIn(
  origin: string,
  kind: Kind,
  request: SignInRequest,
  cacheRoot: string,
  rejected: "auth_failed" | "auth_expired",
): Promise<Session> {
  const server = await connectServer(origin, kind, cacheRoot);
  try {
    return await signIn(server, request, rejected);
  } catch (e) {
    disconnectServer(server);
    throw e;
  }
}

/** Set in a re-run process to the `--ca-cert` file it trusts. */
const LOADED_CA = "DH_LOADED_CA_CERT";

/** The DH_LOADED_CA_CERT this process was re-run with, if any. */
export function loadedCa(): string | undefined {
  return process.env[LOADED_CA];
}

/**
 * TLS trust is fixed at process start, so a custom CA means re-running this
 * process with `NODE_EXTRA_CA_CERTS` set. `extraArgs` carry choices already
 * made; `command` replaces this run's arguments.
 */
export async function ensureCa(
  caCert: string | undefined,
  extraArgs: string[] = [],
  command?: string[],
): Promise<void> {
  if (!caCert || loadedCa() === caCert) return;
  let pem: string;
  try {
    pem = await readFile(caCert, "utf8");
  } catch (e) {
    throw isErrno(e, "ENOENT")
      ? new DhError("usage", `CA certificate not found: ${caCert}`)
      : new DhError(
          "usage",
          `Can't read CA certificate ${caCert}`,
          undefined,
          reasonOf(e),
        );
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
  const runtime = STANDALONE
    ? []
    : [...process.execArgv, process.argv[1] ?? ""];
  const args = command ?? process.argv.slice(2);
  const proc = Bun.spawn(
    [process.execPath, ...runtime, ...args, ...extraArgs],
    {
      env: { ...process.env, NODE_EXTRA_CA_CERTS: bundle, [LOADED_CA]: caCert },
      stdio: ["inherit", "inherit", "inherit"],
    },
  );
  const code = await proc.exited;
  if (dir) await rm(dir, { recursive: true, force: true });
  process.exit(code);
}

const KIND_LABEL: Record<Kind, string> = {
  enterprise: "Deephaven Enterprise",
  community: "Deephaven Community",
};

/**
 * Trusts `caCert` (re-running with `rerun` appended if it must), then finds
 * the server `input` names: a saved one, else by probing it.
 */
export async function trustAndResolveServer(
  input: string,
  servers: Config["servers"],
  caCert: string | undefined,
  rerun: string[],
): Promise<ResolvedServer> {
  // A saved origin is exact; anything else expands in discovery order.
  const exact = input in servers;
  const knownOrigin = exact
    ? input
    : candidates(input)
        .map((u) => u.origin)
        .find((o) => o in servers);
  const known = knownOrigin ? servers[knownOrigin] : undefined;
  await ensureCa(caCert ?? known?.caCert, rerun);
  // A new CA is checked by probing.
  if (knownOrigin && known && !caCert) {
    return { origin: knownOrigin, kind: known.kind };
  }
  const found = await resolveServer(exact ? withPort(input) : input);
  note(`✔ ${found.origin} (${KIND_LABEL[found.kind]})`);
  return found;
}

/**
 * The secret a saved `cred` holds, reading its env var now if it names one.
 * Undefined for credentials without a secret (keys, none).
 */
function savedSecret(cred: Credential, profile: Profile): string | undefined {
  if (cred.type === "secret") return cred.value;
  if (cred.type !== "envRef") return undefined;
  const value = process.env[cred.env];
  if (value === undefined) {
    throw new DhError(
      "credential_unavailable",
      `Profile "${profile.name}" needs ${cred.env} set`,
      `Set ${cred.env}, or run \`${BIN} auth login --profile ${profile.name}\`.`,
    );
  }
  if (!cred.basicToken) return value;
  const at = value.indexOf(":");
  if (at < 0 || value.slice(0, at) !== profile.user) {
    throw new DhError(
      "credential_unavailable",
      `${cred.env} must be "${profile.user}:<password>"`,
      `Fix ${cred.env}, or run \`${BIN} auth login --profile ${profile.name}\`.`,
    );
  }
  return value.slice(at + 1);
}

/** The key a saved credential signs in with: stored in it, or read from its key file. */
async function savedKeyPair(cred: Credential): Promise<KeyPair | undefined> {
  if (cred.type === "keyPair") return keyPairOf(cred);
  if (cred.type === "keyFile") return (await readKeyFile(cred.path)).keyPair;
  return undefined;
}

/** How a saved profile signs in. */
export async function signInRequestFor(
  profile: Profile,
  kind: Kind,
  cred: Credential,
): Promise<SignInRequest> {
  if (kind === "community") {
    return {
      method: profile.method as CommunityMethod,
      handler: profile.handler,
      username: profile.method === "basic" ? profile.user : undefined,
      secret: savedSecret(cred, profile),
    };
  }
  const keyPair = await savedKeyPair(cred);
  const who = { username: profile.user, operateAs: profile.operateAs };
  return keyPair
    ? { method: "private-key", ...who, keyPair }
    : {
        method: "password",
        ...who,
        password: savedSecret(cred, profile) ?? "",
      };
}

/** `DH_SERVER` + `DH_*` credentials; never touches the config directory. */
async function envSession(server: string): Promise<Session> {
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
  const { origin, kind } = await resolveServer(server);
  const request =
    kind === "enterprise"
      ? await enterpriseRequestFromEnv()
      : communityRequestFromEnv();
  // The jsapi loader makes this run's own owner-only folder inside it.
  return await connectAndSignIn(origin, kind, request, tmpdir(), "auth_failed");
}

/** An env var's value; empty counts as unset. */
function env(name: string): string | undefined {
  return process.env[name] || undefined;
}

/** A key from `DH_PRIVATE_KEY_FILE`, else `DH_USERNAME` + `DH_PASSWORD`. */
async function enterpriseRequestFromEnv(): Promise<SignInRequest> {
  const keyFile = env("DH_PRIVATE_KEY_FILE");
  const key = keyFile ? await readKeyFile(keyFile) : undefined;
  const user = env("DH_USERNAME") ?? key?.user;
  if (!user || (!key && !env("DH_PASSWORD"))) {
    throw new DhError(
      "usage",
      "Enterprise needs DH_USERNAME with DH_PASSWORD, or DH_PRIVATE_KEY_FILE",
    );
  }
  const who = {
    username: user,
    operateAs: env("DH_OPERATE_AS") ?? key?.operateAs,
  };
  return key
    ? { method: "private-key", ...who, keyPair: key.keyPair }
    : { method: "password", ...who, password: env("DH_PASSWORD") ?? "" };
}

/** `DH_PSK`, `DH_USERNAME` + `DH_PASSWORD`, `DH_AUTH_HANDLER` + `DH_AUTH_TOKEN`, else anonymous. */
function communityRequestFromEnv(): SignInRequest {
  if (env("DH_PRIVATE_KEY_FILE")) {
    throw new DhError(
      "usage",
      "DH_PRIVATE_KEY_FILE is only for Enterprise servers",
    );
  }
  const username = env("DH_USERNAME");
  const handler = env("DH_AUTH_HANDLER");
  if (env("DH_PSK")) return { method: "psk", secret: env("DH_PSK") };
  if (env("DH_PASSWORD")) {
    if (!username) throw new DhError("usage", "DH_PASSWORD needs DH_USERNAME");
    return { method: "basic", username, secret: env("DH_PASSWORD") };
  }
  if (env("DH_AUTH_TOKEN")) {
    if (!handler) {
      throw new DhError("usage", "DH_AUTH_TOKEN needs DH_AUTH_HANDLER");
    }
    return { method: "custom", handler, secret: env("DH_AUTH_TOKEN") };
  }
  return { method: "anonymous" };
}

/** Signs in as the saved profile `id`; a rejection becomes `auth_expired` with a re-login hint. */
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
  const kind = server?.kind ?? "community";
  const request = await signInRequestFor(profile, kind, cred);
  try {
    const session = await connectAndSignIn(
      profile.server,
      kind,
      request,
      store.cacheDir,
      "auth_expired",
    );
    return { ...session, profileName: profile.name };
  } catch (e) {
    if (e instanceof DhError && e.code === "auth_expired") {
      throw new DhError(
        "auth_expired",
        `The server rejected the saved credentials for "${profile.name}"`,
        `Run \`${BIN} auth login --profile ${profile.name}\`.`,
        e.detail,
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
  const envServer = process.env.DH_SERVER;
  if (!name && envServer) return await envSession(envServer);
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
