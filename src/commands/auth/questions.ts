/**
 * Questions the auth commands ask. Each takes flags and plain data, asks only
 * what the flags didn't answer, and returns plain data. None touches the
 * network or the config directory.
 */
import { resolve } from "node:path";
import { keyPairOf, noProfile, type ProfileToSave } from "../../auth/flows.ts";
import { type KeyFile, type KeyPair, readKeyFile } from "../../auth/keyfile.ts";
import { describePlan, type LegacyItem } from "../../auth/legacy.ts";
import {
  findByIdentity,
  findByName,
  sortedProfiles,
} from "../../auth/profile.ts";
import type {
  OfferedMethod,
  Session,
  SignInRequest,
} from "../../auth/signin.ts";
import type {
  Config,
  Credential,
  Kind,
  Method,
  State,
} from "../../auth/store.ts";
import { readSecret, type Secret } from "../../cli/secret.ts";
import { DhError } from "../../errors.ts";
import { note } from "../../output.ts";
import type { Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import type { LoginOptions } from "./login.ts";

// ── login ───────────────────────────────────────────────────────────────

/** Where to sign in. `rerun`: the flags giving the same answers, for a re-run. */
export type LoginTarget =
  | { server: string; profile?: string; rerun: string[] }
  | "remove";

/** What a profile being signed in again already says about its identity. */
export interface PriorSignIn {
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
export async function askLoginTarget(
  ui: Prompter,
  state: State,
  serverArg: string | undefined,
  profileName: string | undefined,
  opts: LoginOptions,
): Promise<LoginTarget> {
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
  const action = menu ? await askLoginMenuChoice(ui, config) : "server";
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
      : await askServerAddress(ui, config);
  return { server, rerun: ["--", server] };
}

async function askLoginMenuChoice(
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

async function askServerAddress(ui: Prompter, config: Config): Promise<string> {
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

export function priorSignIn(state: State, id: string | undefined): PriorSignIn {
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

const KEY_FILE_METHOD: OfferedMethod = {
  method: "private-key",
  label: "Key file",
};

/** `--method`, then the profile's method, then the only one offered, else asks. */
export async function askSignInMethod(
  ui: Prompter,
  kind: Kind,
  offered: OfferedMethod[],
  prior: PriorSignIn,
  opts: LoginOptions,
): Promise<OfferedMethod> {
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
  if (keyLogin && wanted === "private-key") return KEY_FILE_METHOD;
  if (keyLogin && !wanted && prior.method === "private-key") {
    return KEY_FILE_METHOD;
  }
  const handler = opts.handler ?? prior.handler;
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
  const previous = prior.method && pick(prior.method);
  if (previous) return previous;
  const [only] = offered;
  if (only && offered.length === 1) return only;
  return await ui.select(
    "How do you want to sign in?",
    offered.map((o) => ({ label: o.label, value: o })),
    "--method",
  );
}

/** The names and secrets `chosen` needs: the sign-in request, and what to save. */
export async function askSignInCredentials(
  ui: Prompter,
  chosen: OfferedMethod,
  prior: PriorSignIn,
  opts: LoginOptions,
): Promise<{ request: SignInRequest; toSave: ProfileToSave }> {
  const operateAs = opts.operateAs ?? prior.operateAs;
  const username = async () =>
    opts.username ??
    prior.username ??
    (await ui.text("Username", "--username"));
  const password = () =>
    readSecret("password", opts.passwordStdin, opts.passwordEnv, () =>
      ui.secret("Password", "--password-stdin"),
    );

  switch (chosen.method) {
    case "saml":
      return {
        request: {
          method: "saml",
          saml: chosen.saml,
          waitMs: (opts.timeout ?? 300) * 1000,
          browser: opts.browser !== false,
        },
        toSave: { method: "saml", credential: "new-key" },
      };

    case "password":
      return {
        request: {
          method: "password",
          username: await username(),
          password: (await password()).value,
          operateAs,
        },
        toSave: { method: "password", credential: "new-key" },
      };

    case "private-key": {
      let path: string | undefined;
      let key: KeyFile;
      if (!opts.privateKeyFile && prior.copiedKey) {
        key = { keyPair: prior.copiedKey };
      } else {
        path = resolve(
          opts.privateKeyFile ??
            prior.keyFile ??
            (await ui.text("Key file", "--private-key-file")),
        );
        key = await readKeyFile(path);
      }
      const user =
        opts.username ??
        key.user ??
        prior.username ??
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
        request: {
          method: "private-key",
          username: user,
          keyPair: key.keyPair,
          operateAs: operateAs ?? key.operateAs,
        },
        toSave: { method: "private-key", credential },
      };
    }

    case "anonymous":
      return {
        request: { method: "anonymous" },
        toSave: { method: "anonymous", credential: { type: "none" } },
      };

    case "psk": {
      const psk = await readSecret("psk", opts.pskStdin, opts.pskEnv, () =>
        ui.secret("Pre-shared key", "--psk-stdin"),
      );
      return {
        request: { method: "psk", secret: psk.value },
        toSave: { method: "psk", credential: credentialFor(psk) },
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
        request: { method: "basic", username: user, secret: pw.value },
        toSave: { method: "basic", credential: credentialFor(pw) },
      };
    }

    case "custom": {
      const handler = chosen.handler;
      if (!handler) throw new DhError("usage", "Input needed: pass --handler");
      const token = await readSecret(
        "token",
        opts.tokenStdin,
        opts.tokenEnv,
        () => ui.secret("Token", "--token-stdin"),
      );
      return {
        request: { method: "custom", handler, secret: token.value },
        toSave: { method: "custom", handler, credential: credentialFor(token) },
      };
    }
  }
}

/** A secret from `--*-env` is saved as the variable's name, else as the value. */
function credentialFor(secret: Secret): Credential {
  return secret.env
    ? { type: "envRef", env: secret.env }
    : { type: "secret", value: secret.value };
}

/** SSO picks the account in the browser; check it's the intended one. */
export async function confirmSsoAccount(
  ui: Prompter,
  session: Session,
  chosen: OfferedMethod,
  opts: LoginOptions,
): Promise<void> {
  if (chosen.method !== "saml") return;
  const saml = chosen.saml;
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
export async function askMakeDefault(
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

export async function askDefaultProfile(
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

export async function askLegacyEntriesToMigrate(
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
