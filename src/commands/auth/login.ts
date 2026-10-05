import { resolve } from "@std/path";
import {
  type CommunityLogin,
  communityMethods,
  communityUser,
  connectCommunity,
  disconnect as disconnectCommunity,
  loginCommunity,
} from "../../auth/community.ts";
import {
  connectEnterprise,
  disconnect as disconnectEnterprise,
  enterpriseAuthConfig,
  type EnterpriseConnection,
  loginKey,
  loginPassword,
  loginSaml,
  userName,
} from "../../auth/enterprise.ts";
import { describe, DhError, timeoutMs } from "../../auth/errors.ts";
import {
  authorizeComputer,
  closeSession,
  type NewProfile,
  revokeWith,
  saveProfile,
} from "../../auth/flows.ts";
import { readKeyFile } from "../../auth/keyfile.ts";
import {
  findByIdentity,
  findByName,
  sortedProfiles,
} from "../../auth/profile.ts";
import { candidates, resolveServer } from "../../auth/resolve.ts";
import { openBrowser, samlNonce, signInUrl } from "../../auth/saml.ts";
import { ensureCa, type Session } from "../../auth/session.ts";
import {
  type Credential,
  type Kind,
  type Method,
  type State,
  Store,
} from "../../auth/store.ts";
import { note, result } from "../../output.ts";
import { type Choice, getPrompter, type Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import { globals, secretFrom } from "../common.ts";
import { runImport } from "./import.ts";
import { logout, type LogoutOptions } from "./logout.ts";

export const METHODS: Method[] = [
  "saml",
  "password",
  "private-key",
  "psk",
  "basic",
  "anonymous",
  "custom",
];

export interface LoginOptions {
  method?: Method;
  username?: string;
  passwordStdin?: boolean;
  passwordEnv?: string;
  pskStdin?: boolean;
  pskEnv?: string;
  handler?: string;
  tokenStdin?: boolean;
  tokenEnv?: string;
  privateKeyFile?: string;
  copyKey?: boolean;
  caCert?: string;
  timeout?: number;
  operateAs?: string;
  browser?: boolean;
  expectUser?: string;
  default?: boolean;
  yes?: boolean;
}

const KIND_LABEL: Record<Kind, string> = {
  enterprise: "Deephaven Enterprise",
  community: "Deephaven Community",
};
const OTHER = "__other__";

/** Where to log in, and anything already known about the identity. */
interface Target {
  origin: string;
  kind: Kind;
  method?: Method;
  username?: string;
  operateAs?: string;
  handler?: string;
}

interface Result {
  session: Session;
  method: Method;
  credential: Credential;
  handler?: string;
}

function methodFlagError(method: Method, available: Method[]): DhError {
  return new DhError(
    "usage",
    `This server doesn't offer --method ${method}`,
    `Available: ${available.join(", ")}`,
  );
}

async function chooseMethod(
  prompt: Prompter,
  options: LoginOptions,
  target: Target,
  choices: Choice<Method>[],
): Promise<Method> {
  const available = choices.map((c) => c.value);
  const wanted = options.method ??
    (options.privateKeyFile ? "private-key" : undefined);
  if (wanted) {
    if (wanted !== "private-key" && !available.includes(wanted)) {
      throw methodFlagError(wanted, available);
    }
    return wanted;
  }
  if (target.method && available.includes(target.method)) return target.method;
  if (available.length === 1) return available[0];
  return await prompt.select(
    "How do you want to sign in?",
    choices,
    "--method",
  );
}

async function enterpriseLogin(
  prompt: Prompter,
  options: LoginOptions,
  target: Target,
  cache: string,
): Promise<Result> {
  const timeout = timeoutMs();
  const origin = target.origin;
  const conn = await connectEnterprise(origin, cache, timeout);
  let auth;
  try {
    auth = await enterpriseAuthConfig(conn, timeout);
  } catch (e) {
    disconnectEnterprise(conn);
    throw e;
  }
  const choices: Choice<Method>[] = [
    ...(auth.saml
      ? [{ label: `${auth.saml.providerName} (SSO)`, value: "saml" as const }]
      : []),
    ...(auth.passwordsEnabled
      ? [{ label: "Username and password", value: "password" as const }]
      : []),
  ];
  const method = await chooseMethod(prompt, options, target, choices);
  const operateAs = options.operateAs ?? target.operateAs;

  if (method === "private-key") {
    const path = resolve(
      options.privateKeyFile ??
        await prompt.text("Key file", "--private-key-file"),
    );
    const key = await readKeyFile(path);
    const username = options.username ?? key.user ?? target.username ??
      await prompt.text("Username", "--username");
    const as = operateAs ?? key.operateAs;
    await loginKey(conn, username, key.keyPair, as, timeout, "auth_failed");
    note(`✔ Signed in as ${username}`);
    return {
      session: {
        origin,
        kind: "enterprise",
        user: username,
        operateAs: as,
        enterprise: conn,
      },
      method,
      credential: options.copyKey
        ? {
          type: "keyPair",
          keyType: key.keyPair.type,
          publicKey: key.keyPair.publicKey,
          privateKey: key.keyPair.privateKey,
          generated: false,
        }
        : { type: "keyFile", path },
    };
  }

  let signedIn: EnterpriseConnection;
  let user: string;
  if (method === "saml") {
    const saml = auth.saml!;
    const nonce = samlNonce();
    const url = signInUrl(saml.confirmUrl ?? saml.loginUrl, nonce);
    if (options.browser === false) {
      note(`Sign in at:\n  ${url}`);
    } else {
      note(
        `Opening your browser to sign in. If it doesn't open, go to:\n  ${url}`,
      );
      await openBrowser(url);
    }
    note("Waiting for sign-in… (Ctrl-C to cancel)");
    signedIn = await loginSaml(
      conn,
      () => connectEnterprise(origin, cache, timeout),
      nonce,
      (options.timeout ?? 300) * 1000,
    );
    user = await userName(signedIn);
    if (options.expectUser && options.expectUser !== user) {
      disconnectEnterprise(signedIn);
      throw new DhError(
        "auth_failed",
        `Signed in as ${user}, expected ${options.expectUser}`,
      );
    }
    if (!saml.confirmUrl && !options.expectUser) {
      const ok = await prompt.confirm(`Use ${user}?`, true, "--expect-user");
      if (!ok) {
        disconnectEnterprise(signedIn);
        throw new DhError(
          "cancelled",
          "Not saved.",
          `To use another account, pick or add it in ${saml.providerName}'s account chooser, or sign out of ${saml.providerName} in your browser, then run \`${BIN} auth login\` again.`,
        );
      }
    }
  } else {
    user = options.username ?? target.username ??
      await prompt.text("Username", "--username");
    const { value } = await secretFrom(
      "password",
      options.passwordStdin,
      options.passwordEnv,
      () => prompt.secret("Password", "--password-stdin"),
    );
    await loginPassword(conn, user, value, operateAs, timeout);
    signedIn = conn;
  }
  note(`✔ Signed in as ${user}`);
  const session: Session = {
    origin,
    kind: "enterprise",
    user,
    operateAs,
    enterprise: signedIn,
  };
  const { credential, comment } = await authorizeComputer(session, cache);
  note(`✔ Authorized this computer (key "${comment}")`);
  return { session, method, credential };
}

async function communityLogin(
  prompt: Prompter,
  options: LoginOptions,
  target: Target,
  cache: string,
): Promise<Result> {
  const timeout = timeoutMs();
  const conn = await connectCommunity(target.origin, cache, timeout);
  const offered = communityMethods(conn.handlers);
  const labels: Record<Method, string> = {
    anonymous: "Anonymous",
    psk: "Pre-shared key",
    basic: "Username and password",
    custom: "Custom",
    saml: "SSO",
    password: "Password",
    "private-key": "Key file",
  };
  const choices = offered.map((o) => ({
    label: o.method === "custom" ? o.handler : labels[o.method],
    value: o.method,
  }));
  let method: Method;
  try {
    method = await chooseMethod(prompt, options, target, choices);
  } catch (e) {
    disconnectCommunity(conn.client);
    throw e;
  }
  const handler = method === "custom"
    ? options.handler ?? target.handler ??
      offered.find((o) => o.method === "custom")?.handler
    : undefined;
  const login: CommunityLogin = { method, handler };
  let credential = { type: "none" } as Credential;
  const store = (source: { value: string; env?: string }) => {
    login.secret = source.value;
    credential = source.env
      ? { type: "envRef", env: source.env }
      : { type: "secret", value: source.value };
  };
  if (method === "psk") {
    store(
      await secretFrom(
        "psk",
        options.pskStdin,
        options.pskEnv,
        () => prompt.secret("Pre-shared key", "--psk-stdin"),
      ),
    );
  } else if (method === "basic") {
    login.username = options.username ?? target.username ??
      await prompt.text("Username", "--username");
    store(
      await secretFrom(
        "password",
        options.passwordStdin,
        options.passwordEnv,
        () => prompt.secret("Password", "--password-stdin"),
      ),
    );
  } else if (method === "custom") {
    if (!handler) {
      throw new DhError("usage", "Input needed: pass --handler");
    }
    store(
      await secretFrom(
        "token",
        options.tokenStdin,
        options.tokenEnv,
        () => prompt.secret("Token", "--token-stdin"),
      ),
    );
  }
  await loginCommunity(conn, login, timeout, "auth_failed");
  const user = communityUser(login);
  note(`✔ Signed in${method === "anonymous" ? " anonymously" : ` as ${user}`}`);
  if (method === "basic" && credential.type === "secret") {
    note(
      "! The password is saved in credentials.json, readable only by you. Use --password-env to save a variable name instead.",
    );
  }
  return {
    session: {
      origin: target.origin,
      kind: "community",
      user,
      community: conn,
    },
    method,
    credential,
    handler,
  };
}

async function pickTarget(
  prompt: Prompter,
  options: LoginOptions,
  state: State,
  serverArg: string | undefined,
  reauth: Target | undefined,
): Promise<Target> {
  if (reauth) {
    await ensureCa(state.config.servers[reauth.origin]?.caCert);
    return reauth;
  }
  let input = serverArg;
  const known = Object.entries(state.config.servers);
  if (!input) {
    const picked = known.length === 0 ? OTHER : await prompt.select(
      "What server do you want to log in to?",
      [
        ...known.map(([origin, s]) => ({
          label: origin,
          value: origin,
          hint: `(${s.kind === "enterprise" ? "Enterprise" : "Community"}${
            s.importedFrom ? ` · ${s.importedFrom}` : ""
          })`,
        })),
        { label: "Other…", value: OTHER },
      ],
      "<server>",
    );
    input = picked === OTHER
      ? await prompt.text("What server do you want to log in to?", "<server>")
      : picked;
  }
  const knownOrigin = candidates(input).map((u) => u.origin).find((o) =>
    o in state.config.servers
  );
  await ensureCa(
    options.caCert ?? (knownOrigin && state.config.servers[knownOrigin].caCert),
    serverArg ? [] : [input],
  );
  if (knownOrigin && !options.caCert) {
    return {
      origin: knownOrigin,
      kind: state.config.servers[knownOrigin].kind,
    };
  }
  const resolved = await resolveServer(input);
  note(`✔ ${resolved.origin} (${KIND_LABEL[resolved.kind]})`);
  return resolved;
}

/** The "already logged in" menu. Returns undefined when the user removed a profile. */
async function menu(
  prompt: Prompter,
  state: State,
): Promise<{ target?: Target; server?: string } | undefined> {
  const profiles = sortedProfiles(state.config);
  note("Authenticated profiles:");
  for (const [id, p] of profiles) {
    note(
      `  ${id === state.config.defaultProfile ? "●" : " "} ${p.name}${
        id === state.config.defaultProfile ? "   (default)" : ""
      }`,
    );
  }
  const action = await prompt.select("What would you like to do?", [
    { label: "Log in to another server", value: "server" },
    { label: "Log in as a different user", value: "user" },
    { label: "Re-authenticate a profile", value: "reauth" },
    { label: "Remove a profile", value: "remove" },
  ], "<server>");
  if (action === "server") return {};
  if (action === "remove") return undefined;
  if (action === "user") {
    const server = await prompt.select(
      "Which server?",
      Object.keys(state.config.servers).map((o) => ({ label: o, value: o })),
      "<server>",
    );
    return { server };
  }
  const id = await prompt.select(
    "Which profile?",
    profiles.map(([id, p]) => ({ label: p.name, value: id })),
    "--profile",
  );
  return { target: targetOf(state, id) };
}

function targetOf(state: State, id: string): Target {
  const p = state.config.profiles[id];
  return {
    origin: p.server,
    kind: state.config.servers[p.server]?.kind ?? "community",
    method: p.method,
    username: p.method === "basic" || p.method === "password" ||
        p.method === "private-key"
      ? p.user
      : undefined,
    operateAs: p.operateAs,
    handler: p.handler,
  };
}

export async function login(
  options: LoginOptions,
  serverArg?: string,
): Promise<void> {
  const g = globals(options);
  const store = new Store();
  await store.ensureDir();
  const prompt = await getPrompter(g.input !== false);
  let state = await store.read();

  const hasProfiles = Object.keys(state.config.profiles).length > 0;
  if (!hasProfiles && !state.config.legacyImport && !serverArg) {
    const count = await runImport(store, prompt, { yes: options.yes }, true);
    if (count) {
      result(
        () => `Imported ${count} profile${count === 1 ? "" : "s"}.`,
        { imported: count },
      );
      return;
    }
    state = await store.read();
  }

  let reauth: Target | undefined;
  if (g.profile) {
    const found = findByName(state.config, g.profile);
    if (!found) {
      throw new DhError(
        "usage",
        `No profile named "${g.profile}"`,
        `Run \`${BIN} auth\` to list profiles.`,
      );
    }
    reauth = targetOf(state, found[0]);
  } else if (
    !serverArg && hasProfiles && prompt.interactive && !options.method &&
    !options.privateKeyFile
  ) {
    const choice = await menu(prompt, state);
    if (!choice) return await logout(options as LogoutOptions);
    reauth = choice.target;
    serverArg = choice.server;
  }

  const target = await pickTarget(prompt, options, state, serverArg, reauth);
  const cache = store.cacheDir;
  const { session, method, credential, handler } = target.kind === "enterprise"
    ? await enterpriseLogin(prompt, options, target, cache)
    : await communityLogin(prompt, options, target, cache);

  try {
    state = await store.read();
    const same = findByIdentity(
      state.config,
      session.origin,
      session.user,
      session.operateAs,
    );
    const defaultId = state.config.defaultProfile;
    const hasDefault = defaultId !== undefined &&
      defaultId in state.config.profiles;
    let makeDefault = options.default;
    if (makeDefault === undefined && hasDefault && same?.[0] !== defaultId) {
      makeDefault = prompt.interactive
        ? await prompt.confirm("Make this the default?", false, "--default")
        : false;
    }
    const profile: NewProfile = {
      origin: session.origin,
      kind: target.kind,
      caCert: options.caCert ? resolve(options.caCert) : undefined,
      user: session.user,
      operateAs: session.operateAs,
      method,
      handler,
      credential,
    };
    const saved = await saveProfile(store, profile, makeDefault);
    try {
      await revokeWith(session, saved.previous);
    } catch (e) {
      note(`! Could not delete the previous key: ${describe(e)}`);
    }
    result(
      () => [
        saved.isDefault
          ? `Logged in. Default profile: ${saved.name}`
          : `Logged in. Profile: ${saved.name}`,
        `Run \`${BIN} --help\` to learn more.`,
      ],
      {
        profile: saved.name,
        server: session.origin,
        user: session.user,
        default: saved.isDefault,
      },
    );
  } finally {
    closeSession(session);
  }
}
