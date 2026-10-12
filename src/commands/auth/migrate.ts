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
import { DhError, describe, reasonOf, timeoutMs } from "../../auth/errors.ts";
import {
  addOrphan,
  authorizeComputer,
  closeSession,
  orphansNote,
  revokeOrphans,
  revokeWith,
  type Saved,
  saveProfile,
} from "../../auth/flows.ts";
import { assertTrusted } from "../../auth/jsapi.ts";
import { parseKeyFile, readKeyFile } from "../../auth/keyfile.ts";
import {
  describePlan,
  findLegacy,
  type LegacyItem,
  resolveSecret,
  type SecretRef,
} from "../../auth/legacy.ts";
import { connectAndLogin, loadedCa, type Session } from "../../auth/session.ts";
import type { Credential, Method } from "../../auth/store.ts";
import { Store } from "../../auth/store.ts";
import { note, result } from "../../output.ts";
import { getPrompter, type Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import { globals, promptsAllowed } from "../common.ts";

export interface MigrateOptions {
  from?: string;
  yes?: boolean;
}

interface Migrated {
  item: LegacyItem;
  profile?: string;
  detail: string;
  ok: boolean;
}

function secretCredential(ref: SecretRef, value: string): Credential {
  return "env" in ref
    ? { type: "envRef", env: ref.env }
    : { type: "secret", value };
}

/** Which config file and entry a profile came from; `#` separates the two. */
function importedFrom(path: string, item: LegacyItem): string {
  return `dhcli:${path}#${item.source}`;
}

function reason(item: LegacyItem, e: unknown): string {
  if (e instanceof DhError && e.code === "server_unreachable") {
    return `can't reach ${new URL(item.origin).host} (${e.hint ?? e.message})`;
  }
  return e instanceof DhError && e.hint
    ? `${e.message} (${e.hint})`
    : describe(e);
}

/** Deletes earlier undeletable keys, then the key this profile replaced (remembered if that fails). */
async function cleanUp(
  store: Store,
  session: Session,
  previous: Credential | undefined,
  name: string,
): Promise<void> {
  const orphans = await revokeOrphans(store, session).catch((e) => ({
    deleted: 0,
    kept: 1,
    failed: reasonOf(e),
  }));
  const orphanLine = orphansNote(orphans);
  if (orphanLine) note(`  ${orphanLine}`);
  try {
    await revokeWith(session, previous);
  } catch (e) {
    await store.update(({ credentials }) =>
      addOrphan(credentials, session.origin, session.user, previous),
    );
    note(`  ! ${name}: could not delete the previous key: ${describe(e)}`);
  }
}

/** `waiting` is told what migrating is about to wait on, before each server call. */
async function migrateItem(
  store: Store,
  legacyPath: string,
  item: LegacyItem,
  waiting: (what: string) => void,
): Promise<{ profile: string; detail: string }> {
  if (item.caCert && loadedCa() !== item.caCert) {
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
  ) =>
    await saveProfile(store, {
      origin: item.origin,
      kind: item.kind,
      caCert: item.caCert,
      importedFrom: importedFrom(legacyPath, item),
      user,
      method,
      credential,
      ...extra,
    });

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
      const { credential } = await authorizeComputer(store, session, cache);
      let saved: Saved;
      try {
        saved = await save(plan.username, "password", credential, {
          operateAs: plan.operateAs,
        });
      } catch (e) {
        await revokeWith(session, credential).catch(() => {});
        throw e;
      }
      await cleanUp(store, session, saved.previous, item.name);
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
    const credential: Credential = plan.keyPath
      ? { type: "keyFile", path: plan.keyPath }
      : {
          type: "keyPair",
          keyType: key.keyPair.type,
          publicKey: key.keyPair.publicKey,
          privateKey: key.keyPair.privateKey,
          generated: false,
        };
    let saved: Saved | undefined;
    try {
      saved = await save(key.user, "private-key", credential, {
        operateAs: key.operateAs,
      });
      const session: Session = {
        origin: item.origin,
        kind: "enterprise",
        user: key.user,
        operateAs: key.operateAs,
        enterprise: conn,
      };
      await cleanUp(store, session, saved.previous, item.name);
    } finally {
      disconnectEnterprise(conn);
    }
    return {
      profile: saved.name,
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
      credential.type === "envRef" &&
      plan.method === "basic" &&
      !plan.username
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
    // Not signing in now, but later use would still refuse an untrusted origin.
    assertTrusted(new URL(item.origin));
    detail += " (not verified; variable not set)";
  }
  const user =
    plan.method === "basic"
      ? username!
      : plan.method === "custom"
        ? handler!.split(".").pop()!
        : plan.method;
  const profile = (await save(user, plan.method, credential, { handler })).name;
  return { profile, detail };
}

/** Returns the number of profiles migrated, or `undefined` if nothing was found. */
export async function runMigrate(
  store: Store,
  prompt: Prompter,
  options: MigrateOptions,
  offered: boolean,
): Promise<number | undefined> {
  const legacy = await findLegacy(options.from);
  if (!legacy || legacy.items.length === 0) return undefined;
  if (offered && !options.yes && !prompt.interactive) return undefined;
  note(`Found a dhcli config at ${legacy.path}.`);
  const { config: current } = await store.read();
  const done = legacy.items.filter((i) =>
    Object.values(current.profiles).some(
      (p) =>
        p.server === i.origin &&
        [p.importedFrom ?? []].flat().includes(importedFrom(legacy.path, i)),
    ),
  );
  for (const i of done) note(`  – ${i.name}: already migrated`);
  let items = legacy.items.filter((i) => !done.includes(i));
  if (items.length === 0) return 0;
  note(
    "Migrating signs in to each server to check its credentials, so the servers must be reachable now (for example, on VPN).",
  );
  if (!options.yes) {
    items = await prompt.multiSelect(
      "Which servers do you want to migrate?",
      items.map((i) => ({
        label: `${i.name.padEnd(12)} ${new URL(i.origin).host}`,
        hint: describePlan(i),
        value: i,
      })),
      "--yes",
    );
  }
  for (const n of legacy.notImported) note(`  – ${n}: not used, not migrated`);

  const outcomes: Migrated[] = [];
  const seconds = timeoutMs() / 1000;
  for (const item of items) {
    try {
      const { profile, detail } = await migrateItem(
        store,
        legacy.path,
        item,
        (what) =>
          note(
            `  … ${item.name}: ${what} (gives up after ${seconds}s if unreachable)`,
          ),
      );
      outcomes.push({ item, profile, detail, ok: true });
      note(`  ✔ ${item.name} → ${profile}  ${detail}`);
    } catch (e) {
      const detail = reason(item, e);
      outcomes.push({ item, detail, ok: false });
      note(`  ! ${item.name}: ${detail}`);
    }
  }
  const failed = outcomes.filter((o) => !o.ok).length;
  if (failed) {
    note(
      `${failed} not migrated. Fix the problems above (for example, connect to the servers' network), then run \`${BIN} auth migrate\` to retry. Migrated servers are skipped.`,
    );
  }

  const anyOk = outcomes.some((o) => o.ok);
  await store.update(({ config }) => {
    // All failed → leave unset so the next login offers it again.
    if (items.length === 0) config.legacyImport = "declined";
    else if (anyOk) config.legacyImport = "done";
    const legacyDefault = outcomes.find(
      (o) =>
        o.ok &&
        o.item.name === legacy.defaultItem?.name &&
        o.item.kind === legacy.defaultItem.kind,
    );
    if (legacyDefault) {
      const id = Object.entries(config.profiles).find(
        ([, p]) => p.name === legacyDefault.profile,
      )?.[0];
      if (id) config.defaultProfile = id;
    }
  });
  return outcomes.filter((o) => o.ok).length;
}

export async function migrateCommand(options: MigrateOptions): Promise<void> {
  const g = globals(options);
  const store = new Store();
  await store.ensureDir();
  const prompt = await getPrompter(promptsAllowed(g));
  const count = await runMigrate(store, prompt, options, false);
  if (count === undefined) {
    result(() => "No legacy config found.", { migrated: 0 });
    return;
  }
  result(() => `Migrated ${count} profile${count === 1 ? "" : "s"}.`, {
    migrated: count,
  });
}
