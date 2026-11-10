import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { runLifecycleAction } from "../../cli/lifecycle.ts";
import { DhError, type FailureReport } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { findByName } from "../auth/profile.ts";
import { requireAuth, requireEnvironmentAuth } from "../auth/session.ts";
import { disconnectServer, type Session } from "../auth/signin.ts";
import type { Profile, Store } from "../auth/store.ts";
import {
  type ConsoleInfo,
  createEnterpriseConsole,
  deleteEnterpriseConsole,
  type EffectiveConsoleSettings,
  findEnterpriseConsole,
  isRunning,
  listEnterpriseConsoles,
  serverDefaults,
  waitForConsole,
  waitForConsoleDeletion,
} from "./enterprise.ts";
import {
  type ConsoleIdentity,
  type ConsoleRegistry,
  type ConsoleSettings,
  consoleIdentity,
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
  rememberConsole,
  reserveShortcode,
} from "./store.ts";

const COMMUNITY_SHARED_ID = "shared";

export interface ConsoleListItem extends ConsoleInfo {
  inUse: boolean;
}

function report(
  action: string,
  target?: string,
  profile?: string,
): FailureReport {
  const where = target ? ` console ${target}` : " consoles";
  const identity = profile ? ` (profile ${profile})` : "";
  return {
    command: `console ${action}`,
    target,
    profile,
    operation: `console ${action}`,
    human: {
      summary: `Interrupted while ${action}${where}${identity}.`,
      context: `While ${action}${where}${identity}.`,
    },
  };
}

/** Describe cancellation while the CLI is still waiting for console startup. */
function createConsoleStartupReport(): FailureReport {
  return {
    command: "console create",
    operation: "console startup",
    phase: "starting",
    outcome: "unknown",
    human: {
      summary: "Interrupted while waiting for the console to start.",
      details: ["The console keeps starting on the server after dh exits."],
      recoveryLabel: "Check whether it started",
    },
  };
}

/** Describe a create request accepted by the server but not yet confirmed ready. */
function createConsoleAcceptedReport(
  shortcode: string,
  profileName: string | undefined,
): FailureReport {
  const profile = profileName ?? "environment";
  return {
    target: shortcode,
    profile: profileName,
    phase: "creating",
    outcome: "unknown",
    continuation: "unknown",
    recoveryCommand: [BIN, "console", "detail", shortcode],
    human: {
      summary: `Interrupted while waiting for console ${shortcode} (profile ${profile}) to start.`,
      details: [
        "The server accepted the create request; startup is not confirmed.",
        "The console keeps starting on the server after dh exits.",
      ],
      recoveryLabel: "Check whether it started",
    },
  };
}

function profileForIdentity(
  profiles: Record<string, Profile>,
  identity: ConsoleIdentity,
): [string, Profile] | undefined {
  return Object.entries(profiles).find(
    ([, profile]) =>
      profile.server === identity.server &&
      profile.user === identity.user &&
      (profile.operateAs ?? profile.user) === identity.operateAs,
  );
}

async function authForIdentity(
  store: Store,
  identity: ConsoleIdentity,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<Session> {
  const { config } = await store.read();
  const explicit = profileFlag ? findByName(config, profileFlag) : undefined;
  if (profileFlag && !explicit) {
    throw new DhError("usage", `No profile named "${profileFlag}"`);
  }
  if (explicit && !sameIdentity(consoleIdentity(explicit[1]), identity)) {
    throw new DhError(
      "usage",
      `Console belongs to a different profile than "${profileFlag}"`,
    );
  }
  const selected = explicit ?? profileForIdentity(config.profiles, identity);
  if (!selected) {
    const session = await requireEnvironmentAuth(lifecycle);
    if (sameIdentity(identityOf(session), identity)) return session;
    disconnectServer(session);
    throw new DhError(
      "usage",
      "The current DH_SERVER identity does not match this console",
      `Set DH_SERVER and matching credentials for ${identity.server}, or use a saved profile.`,
    );
  }
  return await requireAuth(selected[1].name, store, lifecycle);
}

function sameIdentity(a: ConsoleIdentity, b: ConsoleIdentity): boolean {
  return identityKey(a) === identityKey(b);
}

function identityOf(session: Session): ConsoleIdentity {
  return consoleIdentity({
    server: session.origin,
    user: session.user,
    operateAs: session.operateAs,
  });
}

function itemsForSession(
  session: Session,
  registry: ConsoleRegistry,
): ConsoleListItem[] {
  const identity = identityOf(session);
  if (session.community) {
    return [
      {
        id: COMMUNITY_SHARED_ID,
        shortcode: COMMUNITY_SHARED_ID,
        name: "Community shared console",
        state: "Running",
        language: null,
        heapSizeGb: null,
        server: session.origin,
        engine: null,
        jvmArgs: null,
        environment: {},
        autoDeleteTimeoutSeconds: null,
        inUse: true,
      },
    ];
  }
  const selected = registry.selected[identityKey(identity)];
  return listEnterpriseConsoles(session).map((console) => ({
    ...console,
    inUse: selected === console.id || selected === console.shortcode,
  }));
}

async function selectOnlyRunning(
  store: Store,
  registry: ConsoleRegistry,
  session: Session,
  items: ConsoleListItem[],
): Promise<ConsoleListItem[]> {
  if (session.community) return items;
  const identity = identityOf(session);
  const key = identityKey(identity);
  const remembered = registry.selected[key];
  if (remembered && items.some((item) => item.id === remembered)) return items;
  if (remembered) {
    await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (data) => {
      delete data.selected[key];
    });
  }
  const running = items.filter((item) => isRunning(item.state));
  if (running.length !== 1) return items;
  const chosen = running[0];
  if (!chosen) return items;
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (data) => {
    data.selected[key] = chosen.id;
  });
  return items.map((item) => ({ ...item, inUse: item.id === chosen.id }));
}

export async function listConsoles(
  store: Store,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ConsoleListItem[]> {
  const session = await requireAuth(profileFlag, store, lifecycle);
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  const items = itemsForSession(session, registry);
  if (session.enterprise) {
    const identity = identityOf(session);
    for (const item of items) {
      await rememberConsole(store, item.shortcode, item.id, identity);
    }
    const liveIds = new Set(items.map((item) => item.id));
    await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (data) => {
      for (const [id, identities] of Object.entries(data.fullIds)) {
        data.fullIds[id] = identities.filter(
          (known) =>
            identityKey(known) !== identityKey(identity) || liveIds.has(id),
        );
        if (data.fullIds[id]?.length === 0) delete data.fullIds[id];
      }
      for (const [shortcode, id] of Object.entries(data.shortcodeIds)) {
        if (!liveIds.has(id) && data.reservations[shortcode]) {
          const reserved = data.reservations[shortcode];
          if (identityKey(reserved) === identityKey(identity)) {
            delete data.shortcodeIds[shortcode];
          }
        }
      }
    });
  }
  return await selectOnlyRunning(store, registry, session, items);
}

export async function requireEnterpriseConsole(
  store: Store,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<Session> {
  const session = await requireAuth(profileFlag, store, lifecycle);
  if (session.community) {
    throw new DhError(
      "console_unsupported",
      "This console operation is not supported on Community servers",
    );
  }
  return session;
}

function identitiesForTarget(
  registry: ConsoleRegistry,
  target: string,
): ConsoleIdentity[] {
  const reserved = registry.reservations[target];
  if (reserved) return [reserved];
  return registry.fullIds[target] ?? [];
}

async function sessionForTarget(
  store: Store,
  target: string,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<Session> {
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  let candidates = identitiesForTarget(registry, target);
  if (profileFlag && candidates.length > 0) {
    const { config } = await store.read();
    const selected = findByName(config, profileFlag);
    if (!selected) {
      throw new DhError("usage", `No profile named "${profileFlag}"`);
    }
    candidates = candidates.filter((identity) =>
      sameIdentity(identity, consoleIdentity(selected[1])),
    );
    if (candidates.length === 0) {
      throw new DhError(
        "usage",
        `Console "${target}" does not belong to profile "${profileFlag}"`,
      );
    }
  }
  if (candidates.length > 1) {
    throw new DhError(
      "usage",
      `Console ID "${target}" is ambiguous`,
      `Use --profile with one of: ${await profileNamesFor(store, candidates)}`,
    );
  }
  const identity = candidates[0];
  if (identity) {
    return await authForIdentity(store, identity, profileFlag, lifecycle);
  }
  return await requireAuth(profileFlag, store, lifecycle);
}

async function profileNamesFor(
  store: Store,
  identities: ConsoleIdentity[],
): Promise<string> {
  const { config } = await store.read();
  const names = identities
    .map((identity) => profileForIdentity(config.profiles, identity)?.[1].name)
    .filter((name): name is string => name !== undefined);
  return names.join(", ");
}

async function consoleForTarget(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<{ session: Session; console: ConsoleInfo }> {
  const session = target
    ? await sessionForTarget(store, target, profileFlag, lifecycle)
    : await requireAuth(profileFlag, store, lifecycle);
  if (session.community) {
    throw new DhError(
      "console_unsupported",
      "Console details are not available on Community servers",
    );
  }
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  const identity = identityOf(session);
  let chosen = target;
  if (!chosen) {
    chosen = registry.selected[identityKey(identity)];
    if (!chosen) {
      const running = listEnterpriseConsoles(session).filter((item) =>
        isRunning(item.state),
      );
      if (running.length === 1) {
        chosen = running[0]?.id;
        if (chosen) {
          await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (data) => {
            data.selected[identityKey(identity)] = chosen ?? "";
          });
        }
      }
    }
  }
  if (!chosen) {
    throw new DhError(
      "console_required",
      "No console is selected",
      `Run \`${BIN} console use <console>\` or create a console first.`,
    );
  }
  const console = findEnterpriseConsole(session, chosen);
  if (!console) {
    await clearMissingReference(store, identity, chosen);
    throw new DhError(
      "console_not_found",
      `Console "${chosen}" was not found`,
      `Run \`${BIN} console\` to list consoles.`,
    );
  }
  return { session, console };
}

async function clearMissingReference(
  store: Store,
  identity: ConsoleIdentity,
  target: string,
): Promise<void> {
  const key = identityKey(identity);
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    const fullId = registry.shortcodeIds[target] ?? target;
    if (
      registry.selected[key] === target ||
      registry.selected[key] === fullId
    ) {
      delete registry.selected[key];
    }
    delete registry.shortcodeIds[target];
    const matches = registry.fullIds[fullId];
    if (matches) {
      registry.fullIds[fullId] = matches.filter(
        (known) => identityKey(known) !== key,
      );
      if (registry.fullIds[fullId]?.length === 0) {
        delete registry.fullIds[fullId];
      }
    }
    for (const [shortcode, id] of Object.entries(registry.shortcodeIds)) {
      if (id === fullId) delete registry.shortcodeIds[shortcode];
    }
  });
}

export async function detailConsole(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ConsoleInfo> {
  const { session, console } = await consoleForTarget(
    store,
    target,
    profileFlag,
    lifecycle,
  );
  await rememberConsole(
    store,
    console.shortcode,
    console.id,
    identityOf(session),
  );
  return console;
}

export async function selectConsole(
  store: Store,
  target: string,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<void> {
  const session = await sessionForTarget(store, target, profileFlag, lifecycle);
  const identity = identityOf(session);
  if (session.community) {
    if (target !== COMMUNITY_SHARED_ID && target !== session.origin) {
      throw new DhError(
        "console_not_found",
        `Console "${target}" was not found`,
        "Community servers provide one shared console: shared.",
      );
    }
    await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
      registry.selected[identityKey(identity)] = COMMUNITY_SHARED_ID;
    });
    return;
  }
  const console = findEnterpriseConsole(session, target);
  if (!console) {
    throw new DhError(
      "console_not_found",
      `Console "${target}" was not found`,
      `Run \`${BIN} console\` to list consoles.`,
    );
  }
  if (!isRunning(console.state)) {
    throw new DhError(
      "console_not_ready",
      `Console ${target} is ${console.state}`,
    );
  }
  await rememberConsole(store, console.shortcode, console.id, identity);
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    registry.selected[identityKey(identity)] = console.id;
  });
}

export async function getConsoleDefaults(
  store: Store,
): Promise<ConsoleSettings> {
  return (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).defaults;
}

export async function setConsoleDefaults(
  store: Store,
  patch: ConsoleSettings,
  reset: boolean,
): Promise<void> {
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    if (reset) {
      registry.defaults = {};
      return;
    }
    Object.assign(registry.defaults, patch);
  });
}

/** Create a console and scope cancellation reporting to its startup lifecycle. */
export async function createConsole(
  store: Store,
  profileFlag: string | undefined,
  settings: ConsoleSettings,
  noUse: boolean,
  lifecycle: CommandLifecycle,
): Promise<ConsoleInfo> {
  return await runLifecycleAction(
    lifecycle,
    createConsoleStartupReport(),
    createConsoleAction,
    store,
    profileFlag,
    settings,
    noUse,
    lifecycle,
  );
}

/** Resolve settings, reserve an ID, create the console, and optionally select it. */
async function createConsoleAction(
  store: Store,
  profileFlag: string | undefined,
  settings: ConsoleSettings,
  noUse: boolean,
  lifecycle: CommandLifecycle,
): Promise<ConsoleInfo> {
  const session = await requireAuth(profileFlag, store, lifecycle);
  if (session.community) {
    throw new DhError(
      "console_unsupported",
      "Creating consoles is not supported on Community servers",
    );
  }
  const base = await serverDefaults(session);
  const stored = await getConsoleDefaults(store);
  const resolved = mergeConsoleSettings(base.settings, stored, settings);
  const identity = identityOf(session);
  const shortcode = await reserveShortcode(store, identity);
  lifecycle.setFailureReport(
    createConsoleAcceptedReport(shortcode, session.profileName),
  );
  const id = await createEnterpriseConsole(
    session,
    shortcode,
    resolved,
    lifecycle,
  );
  await rememberConsole(store, shortcode, id, identity);
  const created = await waitForConsole(session, id, lifecycle);
  if (!noUse) {
    await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
      registry.selected[identityKey(identity)] = id;
    });
  }
  return created;
}

export function mergeConsoleSettings(
  base: EffectiveConsoleSettings,
  defaults: ConsoleSettings,
  patch: ConsoleSettings,
): EffectiveConsoleSettings {
  const merged = structuredClone(base);
  for (const source of [defaults, patch]) {
    for (const [key, value] of Object.entries(source)) {
      if (value === undefined) continue;
      if (value === null) {
        resetConsoleSetting(merged, base, key);
        continue;
      }
      switch (key) {
        case "language":
          if (value !== "python" && value !== "groovy") {
            throw new DhError(
              "usage",
              "Console language must be python or groovy",
            );
          }
          merged.language = value;
          break;
        case "heapSizeGb":
          if (
            typeof value !== "number" ||
            !Number.isFinite(value) ||
            value <= 0
          ) {
            throw new DhError(
              "usage",
              "Console heap size must be a positive number",
            );
          }
          merged.heapSizeGb = value;
          break;
        case "server":
          if (typeof value !== "string" || !value) {
            throw new DhError(
              "usage",
              "Console server must be a non-empty name",
            );
          }
          merged.server = value;
          break;
        case "engine":
          if (typeof value !== "string" || !value) {
            throw new DhError(
              "usage",
              "Console engine must be a non-empty name",
            );
          }
          merged.engine = value;
          break;
        case "jvmArgs":
          if (
            !Array.isArray(value) ||
            !value.every((argument) => typeof argument === "string")
          ) {
            throw new DhError("usage", "Console JVM arguments must be strings");
          }
          merged.jvmArgs = value;
          break;
        case "environment":
          if (!isStringRecord(value)) {
            throw new DhError(
              "usage",
              "Console environment values must be strings",
            );
          }
          merged.environment = value;
          break;
        case "autoDeleteTimeoutSeconds":
          if (
            typeof value !== "number" ||
            !Number.isInteger(value) ||
            value <= 0
          ) {
            throw new DhError(
              "usage",
              "Console auto-delete timeout must be a positive integer",
            );
          }
          merged.autoDeleteTimeoutSeconds = value;
          break;
      }
    }
  }
  return merged;
}

function resetConsoleSetting(
  merged: EffectiveConsoleSettings,
  base: EffectiveConsoleSettings,
  key: string,
): void {
  switch (key) {
    case "language":
      merged.language = base.language;
      break;
    case "heapSizeGb":
      merged.heapSizeGb = base.heapSizeGb;
      break;
    case "server":
      merged.server = base.server;
      break;
    case "engine":
      merged.engine = base.engine;
      break;
    case "jvmArgs":
      merged.jvmArgs = base.jvmArgs;
      break;
    case "environment":
      merged.environment = base.environment;
      break;
    case "autoDeleteTimeoutSeconds":
      merged.autoDeleteTimeoutSeconds = base.autoDeleteTimeoutSeconds;
      break;
  }
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.values(value).every((item) => typeof item === "string")
  );
}

export async function deleteConsole(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<void> {
  return await runLifecycleAction(
    lifecycle,
    report("delete", target),
    deleteConsoleAction,
    store,
    target,
    profileFlag,
    lifecycle,
  );
}

async function deleteConsoleAction(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<void> {
  const { session, console } = await consoleForTarget(
    store,
    target,
    profileFlag,
    lifecycle,
  );
  lifecycle.setFailureReport({
    target: console.shortcode,
    profile: session.profileName,
    phase: "deleting",
    outcome: "unknown",
    continuation: "unknown",
    recoveryCommand: [BIN, "console", "detail", console.shortcode],
  });
  await deleteEnterpriseConsole(session, console);
  lifecycle.setFailureReport({
    outcome: "unknown",
    continuation: "continues",
    progress:
      "The server accepted the delete request; deletion is not confirmed.",
  });
  await waitForConsoleDeletion(session, console, lifecycle);
  await clearMissingReference(store, identityOf(session), console.id);
}

export async function consoleSettingsForServer(
  session: Session,
  registry: ConsoleSettings,
): Promise<EffectiveConsoleSettings> {
  return mergeConsoleSettings(
    (await serverDefaults(session)).settings,
    registry,
    {},
  );
}
