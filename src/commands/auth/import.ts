import {
  connectCommunity,
  disconnect,
  loginCommunity,
} from "../../auth/community.ts";
import {
  connectEnterprise,
  disconnect as disconnectEnterprise,
  loginKey,
} from "../../auth/enterprise.ts";
import { describe, DhError, timeoutMs } from "../../auth/errors.ts";
import {
  authorizeComputer,
  closeSession,
  revokeWith,
  saveProfile,
} from "../../auth/flows.ts";
import { parseKeyFile, readKeyFile } from "../../auth/keyfile.ts";
import {
  describePlan,
  findLegacy,
  type LegacyItem,
  resolveSecret,
  type SecretRef,
} from "../../auth/legacy.ts";
import { connectAndLogin } from "../../auth/session.ts";
import type { Credential, Method } from "../../auth/store.ts";
import { Store } from "../../auth/store.ts";
import { note, result } from "../../output.ts";
import { getPrompter, type Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import { globals } from "../common.ts";

export interface ImportOptions {
  from?: string;
  yes?: boolean;
}

interface Imported {
  item: string;
  profile?: string;
  detail: string;
  ok: boolean;
}

function secretCredential(ref: SecretRef, value: string): Credential {
  return "env" in ref
    ? { type: "envRef", env: ref.env }
    : { type: "secret", value };
}

function importedFrom(item: LegacyItem): string {
  return `dhcli:${item.source}`;
}

function reason(item: LegacyItem, e: unknown): string {
  if (e instanceof DhError && e.code === "server_unreachable") {
    return `can't reach ${new URL(item.origin).host} (${e.hint ?? e.message})`;
  }
  return e instanceof DhError && e.hint
    ? `${e.message} (${e.hint})`
    : describe(e);
}

/** `waiting` is told what the import is about to wait on, before each server call. */
async function importItem(
  store: Store,
  item: LegacyItem,
  waiting: (what: string) => void,
): Promise<{ profile: string; detail: string }> {
  if (item.caCert && Deno.env.get("DENO_CERT") !== item.caCert) {
    throw new DhError(
      "usage",
      `uses a custom CA; run \`${BIN} auth login ${item.origin} --ca-cert ${item.caCert}\``,
    );
  }
  const cache = store.cacheDir;
  const timeout = timeoutMs();
  const host = new URL(item.origin).host;
  const plan = item.plan;
  const save = async (
    user: string,
    method: Method,
    credential: Credential,
    extra: { operateAs?: string; handler?: string } = {},
  ) => (await saveProfile(store, {
    origin: item.origin,
    kind: item.kind,
    caCert: item.caCert,
    importedFrom: importedFrom(item),
    user,
    method,
    credential,
    ...extra,
  }));

  if (plan.method === "password") {
    const password = await resolveSecret(plan.password);
    if (!password) {
      throw new DhError(
        "credential_unavailable",
        "env" in plan.password
          ? `needs $${plan.password.env} set`
          : "no saved password",
      );
    }
    waiting(
      `signing in to ${host} as ${plan.username}, then authorizing this computer`,
    );
    const session = await connectAndLogin(
      item.origin,
      {
        kind: "enterprise",
        username: plan.username,
        password,
        operateAs: plan.operateAs,
      },
      cache,
      "auth_failed",
    );
    try {
      const { credential } = await authorizeComputer(session, cache);
      let saved;
      try {
        saved = await save(plan.username, "password", credential, {
          operateAs: plan.operateAs,
        });
      } catch (e) {
        await revokeWith(session, credential).catch(() => {});
        throw e;
      }
      await revokeWith(session, saved.previous).catch((e) =>
        note(
          `  ! ${item.name}: could not delete the previous key: ${describe(e)}`,
        )
      );
      return { profile: saved.name, detail: "authorized this computer" };
    } finally {
      closeSession(session);
    }
  }

  if (plan.method === "private-key") {
    const key = plan.keyPath
      ? await readKeyFile(plan.keyPath)
      : parseKeyFile(plan.keyText ?? "", item.source);
    if (!key.user) throw new DhError("usage", "key file has no user line");
    waiting(`signing in to ${host} as ${key.user} with the key`);
    const conn = await connectEnterprise(item.origin, cache, timeout);
    await loginKey(
      conn,
      key.user,
      key.keyPair,
      key.operateAs,
      timeout,
      "auth_failed",
    );
    disconnectEnterprise(conn);
    const credential: Credential = plan.keyPath
      ? { type: "keyFile", path: plan.keyPath }
      : {
        type: "keyPair",
        keyType: key.keyPair.type,
        publicKey: key.keyPair.publicKey,
        privateKey: key.keyPair.privateKey,
        generated: false,
      };
    const profile = (await save(key.user, "private-key", credential, {
      operateAs: key.operateAs,
    })).name;
    return {
      profile,
      detail: plan.keyPath ? `uses ${plan.keyPath}` : "key copied",
    };
  }

  // Community.
  let username: string | undefined;
  let secret: string | undefined;
  let credential: Credential = { type: "none" };
  let ref: SecretRef | undefined;
  if (plan.method !== "anonymous") {
    ref = plan.secret;
    secret = await resolveSecret(ref);
  }
  if (plan.method === "basic") {
    username = plan.username;
    if (!username) {
      // v1 Basic stores "user:password" in one token.
      const at = secret?.indexOf(":") ?? -1;
      if (!secret || at < 0) {
        throw new DhError(
          "credential_unavailable",
          "needs a user:password token",
        );
      }
      username = secret.slice(0, at);
      secret = secret.slice(at + 1);
      // An env var keeps holding user:password; the password is split off at use.
      if (!("env" in ref!)) ref = { literal: secret };
    }
  }
  if (ref) {
    if (!secret && !("env" in ref)) {
      throw new DhError("credential_unavailable", "no saved secret");
    }
    credential = secretCredential(ref, secret ?? "");
    if (
      credential.type === "envRef" && plan.method === "basic" && !plan.username
    ) {
      credential.basicToken = true;
    }
  }
  const handler = plan.method === "custom" ? plan.handler : undefined;
  const login = { method: plan.method, handler, username, secret };
  let detail = describePlan(item);
  if (secret !== undefined || plan.method === "anonymous") {
    waiting(`signing in to ${host} (${describePlan(item)})`);
    const conn = await connectCommunity(item.origin, cache, timeout);
    await loginCommunity(conn, login, timeout, "auth_failed");
    disconnect(conn.client);
  } else {
    detail += " (not verified; variable not set)";
  }
  const user = plan.method === "basic"
    ? username!
    : plan.method === "custom"
    ? handler!.split(".").pop()!
    : plan.method;
  const profile = (await save(user, plan.method, credential, { handler })).name;
  return { profile, detail };
}

/** Returns the number of profiles imported, or `undefined` if nothing was found. */
export async function runImport(
  store: Store,
  prompt: Prompter,
  options: ImportOptions,
  offered: boolean,
): Promise<number | undefined> {
  const legacy = await findLegacy(options.from);
  if (!legacy || legacy.items.length === 0) return undefined;
  if (offered && !options.yes && !prompt.interactive) return undefined;
  note(`Found a dhcli config at ${legacy.path}.`);
  const { config: current } = await store.read();
  const done = legacy.items.filter((i) =>
    Object.values(current.profiles).some((p) =>
      p.importedFrom === importedFrom(i)
    )
  );
  for (const i of done) note(`  – ${i.name}: already imported`);
  let items = legacy.items.filter((i) => !done.includes(i));
  if (items.length === 0) return 0;
  note(
    "Importing signs in to each server to check its credentials, so the servers must be reachable now (for example, on VPN).",
  );
  if (!options.yes) {
    items = await prompt.multiSelect(
      "Which servers do you want to import?",
      items.map((i) => ({
        label: `${i.name.padEnd(12)} ${new URL(i.origin).host}`,
        hint: describePlan(i),
        value: i,
      })),
      "--yes",
    );
  }
  for (const n of legacy.notImported) note(`  – ${n}: not used, not imported`);

  const outcomes: Imported[] = [];
  const seconds = timeoutMs() / 1000;
  for (const item of items) {
    try {
      const { profile, detail } = await importItem(
        store,
        item,
        (what) =>
          note(
            `  … ${item.name}: ${what} (gives up after ${seconds}s if unreachable)`,
          ),
      );
      outcomes.push({ item: item.name, profile, detail, ok: true });
      note(`  ✔ ${item.name} → ${profile}  ${detail}`);
    } catch (e) {
      const detail = reason(item, e);
      outcomes.push({ item: item.name, detail, ok: false });
      note(`  ! ${item.name}: ${detail}`);
    }
  }
  const failed = outcomes.filter((o) => !o.ok).length;
  if (failed) {
    note(
      `${failed} not imported. Fix the problems above (for example, connect to the servers' network), then run \`${BIN} auth import\` to retry. Imported servers are skipped.`,
    );
  }

  const anyOk = outcomes.some((o) => o.ok);
  await store.update(({ config }) => {
    // All failed → leave unset so the next login offers it again.
    if (items.length === 0) config.legacyImport = "declined";
    else if (anyOk) config.legacyImport = "done";
    const legacyDefault = outcomes.find((o) =>
      o.ok && o.item === legacy.defaultName
    );
    if (legacyDefault) {
      const id = Object.entries(config.profiles).find(([, p]) =>
        p.name === legacyDefault.profile
      )?.[0];
      if (id) config.defaultProfile = id;
    }
  });
  return outcomes.filter((o) => o.ok).length;
}

export async function importCommand(options: ImportOptions): Promise<void> {
  const g = globals(options);
  const store = new Store();
  await store.ensureDir();
  const prompt = await getPrompter(g.input !== false);
  const count = await runImport(store, prompt, options, false);
  if (count === undefined) {
    result(() => "No legacy config found.", { imported: 0 });
    return;
  }
  result(() => `Imported ${count} profile${count === 1 ? "" : "s"}.`, {
    imported: count,
  });
}
