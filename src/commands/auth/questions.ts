/**
 * Questions the auth commands ask. Each takes flags and plain data, asks only
 * what the flags didn't answer, and returns plain data. None touches the
 * network or the config directory.
 */
import { resolve } from "node:path";
import { DhError } from "../../auth/errors.ts";
import { keyPairOf, noProfile, type ToSave } from "../../auth/flows.ts";
import { type KeyFile, type KeyPair, readKeyFile } from "../../auth/keyfile.ts";
import { describePlan, type LegacyItem } from "../../auth/legacy.ts";
import {
  findByIdentity,
  findByName,
  sortedProfiles,
} from "../../auth/profile.ts";
import type { Offer, Session, SignIn } from "../../auth/signin.ts";
import type {
  Config,
  Credential,
  Kind,
  Method,
  State,
} from "../../auth/store.ts";
import { note } from "../../output.ts";
import type { Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import type { LoginOptions } from "./login.ts";

// ── login ───────────────────────────────────────────────────────────────

/** Where to sign in. `rerun`: the flags giving the same answers, for a re-run. */
export type Where =
  | { server: string; profile?: string; rerun: string[] }
  | "remove";

/** What's known about the identity from a profile being signed in again. */
export interface Known {
  method?: Method;
  username?: string;
  operateAs?: string;
  handler?: string;
  keyFile?: string;
  /** Key saved with `--copy-key`. */
  copiedKey?: KeyPair;
}

const OTHER = "__other__";

/** `profileName`: sign in again as that profile. Otherwise the server, or the menu. */
export async function askWhere(
  ui: Prompter,
  state: State,
  serverArg: string | undefined,
  profileName: string | undefined,
  opts: LoginOptions,
): Promise<Where> {
  const { config } = state;
  if (profileName) {
    const found = findByName(config, profileName);
    if (!found) throw noProfile(profileName);
    return { server: found[1].server, profile: found[0], rerun: [] };
  }
  if (serverArg) return { server: serverArg, rerun: [] };
  const menu =
    Object.keys(config.profiles).length > 0 &&
    ui.interactive &&
    !opts.method &&
    !opts.privateKeyFile;
  const action = menu ? await askMenuAction(ui, config) : "server";
  if (action === "remove") return "remove";
  if (action === "reauth") {
    const id = await ui.select(
      "Which profile?",
      sortedProfiles(config).map(([id, p]) => ({ label: p.name, value: id })),
      "--profile",
    );
    const p = config.profiles[id];
    if (!p) throw noProfile(id);
    return { server: p.server, profile: id, rerun: [`--profile=${p.name}`] };
  }
  const server =
    action === "user"
      ? await ui.select(
          "Which server?",
          Object.keys(config.servers).map((o) => ({ label: o, value: o })),
          "<server>",
        )
      : await askServer(ui, config);
  return { server, rerun: ["--", server] };
}

async function askMenuAction(
  ui: Prompter,
  config: Config,
): Promise<"server" | "user" | "reauth" | "remove"> {
  note("Authenticated profiles:");
  for (const [id, p] of sortedProfiles(config)) {
    const isDefault = id === config.defaultProfile;
    note(
      `  ${isDefault ? "●" : " "} ${p.name}${isDefault ? "   (default)" : ""}`,
    );
  }
  return await ui.select(
    "What would you like to do?",
    [
      { label: "Log in to another server", value: "server" as const },
      { label: "Log in as a different user", value: "user" as const },
      { label: "Re-authenticate a profile", value: "reauth" as const },
      { label: "Remove a profile", value: "remove" as const },
    ],
    "<server>",
  );
}

async function askServer(ui: Prompter, config: Config): Promise<string> {
  const known = Object.entries(config.servers);
  const picked =
    known.length === 0
      ? OTHER
      : await ui.select(
          "What server do you want to log in to?",
          [
            ...known.map(([origin, s]) => ({
              label: origin,
              value: origin,
              hint: `(${s.kind === "enterprise" ? "Enterprise" : "Community"}${
                s.importedFrom ? ` · ${s.importedFrom.split("#").pop()}` : ""
              })`,
            })),
            { label: "Other…", value: OTHER },
          ],
          "<server>",
        );
  return picked === OTHER
    ? await ui.text("What server do you want to log in to?", "<server>")
    : picked;
}

export function knownIdentity(state: State, id: string | undefined): Known {
  const p = id ? state.config.profiles[id] : undefined;
  if (!id || !p) return {};
  const cred = state.credentials.credentials[id];
  return {
    method: p.method,
    username: ["basic", "password", "private-key"].includes(p.method)
      ? p.user
      : undefined,
    operateAs: p.operateAs,
    handler: p.handler,
    keyFile: cred?.type === "keyFile" ? cred.path : undefined,
    copiedKey:
      cred?.type === "keyPair" && !cred.generated ? keyPairOf(cred) : undefined,
  };
}

const KEY_FILE: Offer = { method: "private-key", label: "Key file" };

/** `--method`, then the profile's method, then the only one, else asks. */
export async function askMethod(
  ui: Prompter,
  kind: Kind,
  offered: Offer[],
  known: Known,
  opts: LoginOptions,
): Promise<Offer> {
  // Enterprise key login always works, though servers don't advertise it.
  const keyLogin = kind === "enterprise";
  if (!keyLogin && opts.privateKeyFile) {
    throw new DhError(
      "usage",
      "--private-key-file is only for Enterprise servers",
    );
  }
  const wanted =
    opts.method ?? (opts.privateKeyFile ? "private-key" : undefined);
  if (keyLogin && wanted === "private-key") return KEY_FILE;
  if (keyLogin && !wanted && known.method === "private-key") return KEY_FILE;
  const handler = opts.handler ?? known.handler;
  const pick = (method: Method) => {
    const matches = offered.filter((o) => o.method === method);
    return matches.find((o) => o.handler === handler) ?? matches[0];
  };
  if (wanted) {
    const found = pick(wanted);
    if (!found) {
      throw new DhError(
        "usage",
        `This server doesn't offer --method ${wanted}`,
        `Available: ${offered.map((o) => o.method).join(", ")}`,
      );
    }
    return found.method === "custom" && opts.handler
      ? { ...found, handler: opts.handler }
      : found;
  }
  if (offered.length === 0) {
    throw new DhError(
      "server_unsupported",
      `This server offers no sign-in method ${BIN} supports`,
      keyLogin ? "Sign in with an existing key: --private-key-file" : undefined,
    );
  }
  const previous = known.method && pick(known.method);
  if (previous) return previous;
  const [only] = offered;
  if (only && offered.length === 1) return only;
  return await ui.select(
    "How do you want to sign in?",
    offered.map((o) => ({ label: o.label, value: o })),
    "--method",
  );
}

/** The secrets and names `offer` needs: how to sign in, and what to save. */
export async function askCredentials(
  ui: Prompter,
  offer: Offer,
  known: Known,
  opts: LoginOptions,
): Promise<{ how: SignIn; save: ToSave }> {
  const operateAs = opts.operateAs ?? known.operateAs;
  const username = async () =>
    opts.username ??
    known.username ??
    (await ui.text("Username", "--username"));
  const password = () =>
    secretFrom("password", opts.passwordStdin, opts.passwordEnv, () =>
      ui.secret("Password", "--password-stdin"),
    );

  switch (offer.method) {
    case "saml":
      return {
        how: {
          method: "saml",
          saml: offer.saml,
          waitMs: (opts.timeout ?? 300) * 1000,
          browser: opts.browser !== false,
        },
        save: { method: "saml", credential: "new-key" },
      };

    case "password":
      return {
        how: {
          method: "password",
          username: await username(),
          password: (await password()).value,
          operateAs,
        },
        save: { method: "password", credential: "new-key" },
      };

    case "private-key": {
      let path: string | undefined;
      let key: KeyFile;
      if (!opts.privateKeyFile && known.copiedKey) {
        key = { keyPair: known.copiedKey };
      } else {
        path = resolve(
          opts.privateKeyFile ??
            known.keyFile ??
            (await ui.text("Key file", "--private-key-file")),
        );
        key = await readKeyFile(path);
      }
      const user =
        opts.username ??
        key.user ??
        known.username ??
        (await ui.text("Username", "--username"));
      const credential: Credential =
        path && !opts.copyKey
          ? { type: "keyFile", path }
          : {
              type: "keyPair",
              keyType: key.keyPair.type,
              publicKey: key.keyPair.publicKey,
              privateKey: key.keyPair.privateKey,
              generated: false,
            };
      return {
        how: {
          method: "private-key",
          username: user,
          keyPair: key.keyPair,
          operateAs: operateAs ?? key.operateAs,
        },
        save: { method: "private-key", credential },
      };
    }

    case "anonymous":
      return {
        how: { method: "anonymous" },
        save: { method: "anonymous", credential: { type: "none" } },
      };

    case "psk": {
      const psk = await secretFrom("psk", opts.pskStdin, opts.pskEnv, () =>
        ui.secret("Pre-shared key", "--psk-stdin"),
      );
      return {
        how: { method: "psk", secret: psk.value },
        save: { method: "psk", credential: saved(psk) },
      };
    }

    case "basic": {
      const user = await username();
      const pw = await password();
      if (!pw.env) {
        note(
          "! The password is saved in credentials.json, readable only by your account. Use --password-env to save a variable name instead.",
        );
      }
      return {
        how: { method: "basic", username: user, secret: pw.value },
        save: { method: "basic", credential: saved(pw) },
      };
    }

    case "custom": {
      const handler = offer.handler;
      if (!handler) throw new DhError("usage", "Input needed: pass --handler");
      const token = await secretFrom(
        "token",
        opts.tokenStdin,
        opts.tokenEnv,
        () => ui.secret("Token", "--token-stdin"),
      );
      return {
        how: { method: "custom", handler, secret: token.value },
        save: { method: "custom", handler, credential: saved(token) },
      };
    }
  }
}

function saved(source: SecretSource): Credential {
  return source.env
    ? { type: "envRef", env: source.env }
    : { type: "secret", value: source.value };
}

/** SSO picks the account in the browser; check it's the intended one. */
export async function confirmAccount(
  ui: Prompter,
  session: Session,
  offer: Offer,
  opts: LoginOptions,
): Promise<void> {
  if (offer.method !== "saml") return;
  const saml = offer.saml;
  if (opts.expectUser) {
    if (opts.expectUser === session.user) return;
    throw new DhError(
      "auth_failed",
      `Signed in as ${session.user}, expected ${opts.expectUser}`,
    );
  }
  // The server already asked the user to confirm the account.
  if (saml.confirmUrl) return;
  if (await ui.confirm(`Use ${session.user}?`, true, "--expect-user")) return;
  throw new DhError(
    "cancelled",
    "Not saved.",
    `To use another account, pick or add it in ${saml.providerName}'s account chooser, or sign out of ${saml.providerName} in your browser, then run \`${BIN} auth login\` again.`,
  );
}

/** `--[no-]default`; else asks when another profile is the default. Undefined: only if none is. */
export async function askDefault(
  ui: Prompter,
  config: Config,
  session: Session,
  flag: boolean | undefined,
): Promise<boolean | undefined> {
  if (flag !== undefined) return flag;
  const current = config.defaultProfile;
  if (current === undefined || !(current in config.profiles)) return undefined;
  const same = findByIdentity(
    config,
    session.origin,
    session.user,
    session.operateAs,
  );
  if (same?.[0] === current) return undefined;
  return ui.interactive
    ? await ui.confirm("Make this the default?", false, "--default")
    : false;
}

// ── logout, use ─────────────────────────────────────────────────────────

const ALL = "__all__";

/** `--all`, the named profile, the only profile, else asks. */
export async function askProfilesToRemove(
  ui: Prompter,
  config: Config,
  name: string | undefined,
  all: boolean | undefined,
): Promise<string[]> {
  const profiles = sortedProfiles(config);
  const everyId = profiles.map(([id]) => id);
  if (all) return everyId;
  if (name) {
    const found = findByName(config, name);
    if (!found) throw noProfile(name);
    return [found[0]];
  }
  const [only] = profiles;
  if (only && profiles.length === 1) return [only[0]];
  const picked = await ui.select(
    "Which profile do you want to remove?",
    [
      ...profiles.map(([id, p]) => ({ label: p.name, value: id })),
      { label: "All profiles", value: ALL },
    ],
    "<profile> or --all",
  );
  return picked === ALL ? everyId : [picked];
}

export async function askNewDefault(
  ui: Prompter,
  config: Config,
): Promise<string> {
  return await ui.select(
    "Which profile should be the default?",
    sortedProfiles(config).map(([id, p]) => ({
      label: p.name,
      value: id,
      hint: id === config.defaultProfile ? "(default)" : undefined,
    })),
    "<profile>",
  );
}

// ── migrate ─────────────────────────────────────────────────────────────

export async function askMigrateItems(
  ui: Prompter,
  pending: LegacyItem[],
  yes: boolean | undefined,
): Promise<LegacyItem[]> {
  if (yes) return pending;
  return await ui.multiSelect(
    "Which servers do you want to migrate?",
    pending.map((i) => ({
      label: `${i.name.padEnd(12)} ${new URL(i.origin).host}`,
      hint: describePlan(i),
      value: i,
    })),
    "--yes",
  );
}

// ── secrets ─────────────────────────────────────────────────────────────

let stdinUsed = false;

async function readStdin(flag: string): Promise<string> {
  if (stdinUsed) {
    throw new DhError("usage", `Only one --*-stdin flag can be used (${flag})`);
  }
  stdinUsed = true;
  const text = await Bun.stdin.text();
  const value = text.replace(/\r?\n$/, "");
  if (!value) throw new DhError("usage", `${flag}: stdin was empty`);
  return value;
}

interface SecretSource {
  value: string;
  /** Env var name, if the secret came from `--*-env`. */
  env?: string;
}

/** `--<name>-stdin`, then `--<name>-env <VAR>`, then a masked prompt. */
async function secretFrom(
  name: string,
  stdin: boolean | undefined,
  env: string | undefined,
  ask: () => Promise<string>,
): Promise<SecretSource> {
  if (stdin) return { value: await readStdin(`--${name}-stdin`) };
  if (env) {
    const value = process.env[env];
    if (!value) throw new DhError("usage", `--${name}-env: ${env} is not set`);
    return { value, env };
  }
  return { value: await ask() };
}
