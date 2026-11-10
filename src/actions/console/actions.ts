import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { runLifecycleAction } from "../../cli/lifecycle.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { findByName } from "../auth/profile.ts";
import { requireAuth, requireEnvironmentAuth } from "../auth/session.ts";
import { disconnectServer, type Session } from "../auth/signin.ts";
import type { Profile, Store } from "../auth/store.ts";
import {
  type ConsoleInfo,
  createEnterpriseConsole,
  deleteEnterpriseConsole,
  findEnterpriseConsole,
  isRunning,
  listEnterpriseConsoles,
  serverDefaults,
  waitForConsole,
  waitForConsoleDeletion,
} from "./enterprise.ts";
import {
  consoleActionReport,
  consoleCreateRequestReport,
  consoleDeleteAcceptedReport,
  consoleDeleteRequestReport,
  consoleStartupReport,
} from "./reporting.ts";
import { type ConsoleSettings, mergeConsoleSettings } from "./settings.ts";
import {
  type ConsoleIdentity,
  type ConsoleRegistry,
  clearMissingReference,
  consoleIdentity,
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
  reconcileDiscoveredConsoles,
  rememberConsole,
  rememberConsoleSelection,
  reserveShortcode,
} from "./store.ts";

const COMMUNITY_SHARED_ID = "shared";

/** A discovered console annotated with this identity's current selection. */
export interface ConsoleListItem extends ConsoleInfo {
  inUse: boolean;
}

/** Find a saved profile by stable identity rather than its mutable display name. */
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

/** Authenticate the mapped identity, enforcing any explicit profile constraint. */
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
    return await authEnvironmentForIdentity(identity, lifecycle);
  }
  return await requireAuth(selected[1].name, store, lifecycle);
}

/** Recover an unsaved console identity only when environment credentials match it. */
async function authEnvironmentForIdentity(
  identity: ConsoleIdentity,
  lifecycle: CommandLifecycle,
): Promise<Session> {
  const session = await requireEnvironmentAuth(lifecycle);
  if (sameIdentity(identityOf(session), identity)) return session;
  disconnectServer(session);
  throw new DhError(
    "usage",
    "The current DH_SERVER identity does not match this console",
    `Set DH_SERVER and matching credentials for ${identity.server}, or use a saved profile.`,
  );
}

/** Compare server, login user, and effective operate-as user together. */
function sameIdentity(a: ConsoleIdentity, b: ConsoleIdentity): boolean {
  return identityKey(a) === identityKey(b);
}

/** Derive the registry owner from an authenticated session. */
function identityOf(session: Session): ConsoleIdentity {
  return consoleIdentity({
    server: session.origin,
    user: session.user,
    operateAs: session.operateAs,
  });
}

/** Discover consoles and mark the current selection, including Community's singleton. */
function itemsForSession(
  session: Session,
  registry: ConsoleRegistry,
): ConsoleListItem[] {
  const identity = identityOf(session);
  if (session.community) {
    return [communitySharedConsole(session.origin)];
  }
  const selected = registry.selected[identityKey(identity)];
  return listEnterpriseConsoles(session).map((console) => ({
    ...console,
    inUse: selected === console.id || selected === console.shortcode,
  }));
}

/** Represent Community's always-selected singleton without inventing Enterprise settings. */
function communitySharedConsole(origin: string): ConsoleListItem {
  return {
    id: COMMUNITY_SHARED_ID,
    shortcode: COMMUNITY_SHARED_ID,
    name: "Community shared console",
    state: "Running",
    language: null,
    heapSizeGb: null,
    server: origin,
    engine: null,
    jvmArgs: null,
    environment: {},
    autoDeleteTimeoutSeconds: null,
    inUse: true,
  };
}

/** Retain a live selection, or clear it and auto-select the sole running console. */
async function reconcileConsoleSelection(
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
    await rememberConsoleSelection(store, identity, undefined);
  }
  const chosen = await rememberOnlyRunningConsole(store, identity, items);
  if (!chosen) return items;
  return items.map((item) => ({ ...item, inUse: item.id === chosen }));
}

/** Remember the sole running console; stopped and failed consoles are not candidates. */
export async function rememberOnlyRunningConsole(
  store: Store,
  identity: ConsoleIdentity,
  consoles: ConsoleInfo[],
): Promise<string | undefined> {
  const running = consoles.filter((console) => isRunning(console.state));
  if (running.length !== 1) return undefined;
  const chosen = running[0];
  if (!chosen) return undefined;
  await rememberConsoleSelection(store, identity, chosen.id);
  return chosen.id;
}

/** List consoles, refresh cached references, and resolve automatic selection. */
export async function listConsoles(
  store: Store,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ConsoleListItem[]> {
  const session = await requireAuth(profileFlag, store, lifecycle);
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  const items = itemsForSession(session, registry);
  if (session.enterprise) {
    await reconcileDiscoveredConsoles(store, identityOf(session), items);
  }
  return await reconcileConsoleSelection(store, registry, session, items);
}

/** Authenticate a console-management session, rejecting Community-only connections. */
export async function requireEnterpriseConsole(
  store: Store,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<Session> {
  const session = await requireAuth(profileFlag, store, lifecycle);
  if (session.community) {
    throw new DhError(
      "console_unsupported",
      "Console creation defaults are not available on Community servers",
    );
  }
  return session;
}

/** Prefer a permanent shortcode reservation over full-ID identity mappings. */
function identitiesForTarget(
  registry: ConsoleRegistry,
  target: string,
): ConsoleIdentity[] {
  const reserved = registry.reservations[target];
  if (reserved) return [reserved];
  return registry.fullIds[target] ?? [];
}

/** Resolve a mapped target before normal environment and default-profile precedence. */
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

/** Render available saved profiles for an ambiguous full-ID error. */
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

/** Resolve explicit or remembered targets, allowing stopped and failed consoles. */
async function consoleForTarget(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
  operation: "detail" | "delete" = "detail",
): Promise<{ session: Session; console: ConsoleInfo }> {
  const session = target
    ? await sessionForTarget(store, target, profileFlag, lifecycle)
    : await requireAuth(profileFlag, store, lifecycle);
  if (session.community) {
    throw new DhError(
      "console_unsupported",
      operation === "delete"
        ? "Deleting consoles is not supported on Community servers"
        : "Console details are not available on Community servers",
    );
  }
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  const identity = identityOf(session);
  let chosen = target;
  if (!chosen) {
    chosen = registry.selected[identityKey(identity)];
    if (!chosen) {
      chosen = await rememberOnlyRunningConsole(
        store,
        identity,
        listEnterpriseConsoles(session),
      );
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

/** Read details and remember mappings for future explicit-target resolution. */
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

/** Select a running Enterprise console or Community's shared console. */
export async function selectConsole(
  store: Store,
  target: string,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<void> {
  const session = await sessionForTarget(store, target, profileFlag, lifecycle);
  if (!target) {
    throw new DhError(
      "usage",
      "A console is required for console use",
      session.community
        ? `Community servers provide one shared console. Run \`${BIN} console use shared\`.`
        : `Run \`${BIN} console\` to list consoles, then \`${BIN} console use <console>\`.`,
    );
  }
  const identity = identityOf(session);
  if (session.community) {
    if (target !== COMMUNITY_SHARED_ID && target !== session.origin) {
      throw new DhError(
        "console_not_found",
        `Console "${target}" was not found`,
        "Community servers provide one shared console: shared.",
      );
    }
    await rememberConsoleSelection(store, identity, COMMUNITY_SHARED_ID);
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
  await rememberConsoleSelection(store, identity, console.id);
}

/** Read global creation overrides without resolving server defaults. */
export async function getConsoleDefaults(
  store: Store,
): Promise<ConsoleSettings> {
  return (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).defaults;
}

/** Patch global creation overrides, or remove all overrides when reset is requested. */
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
    consoleStartupReport(),
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
    consoleCreateRequestReport(shortcode, session.profileName),
  );
  const id = await createEnterpriseConsole(
    session,
    shortcode,
    resolved,
    lifecycle,
    base,
  );
  await rememberConsole(store, shortcode, id, identity);
  const created = await waitForConsole(session, id, lifecycle);
  if (!noUse) {
    await rememberConsoleSelection(store, identity, id);
  }
  return created;
}

/** Resolve the console before asking the user to confirm deletion. */
export async function resolveConsoleDeletion(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<{ session: Session; console: ConsoleInfo }> {
  return await consoleForTarget(
    store,
    target,
    profileFlag,
    lifecycle,
    "delete",
  );
}

/** Delete a resolved console with scoped interruption and recovery reporting. */
export async function deleteConsole(
  store: Store,
  resolved: { session: Session; console: ConsoleInfo },
  lifecycle: CommandLifecycle,
): Promise<void> {
  return await runLifecycleAction(
    lifecycle,
    consoleActionReport(
      "delete",
      resolved.console.shortcode,
      resolved.session.profileName,
    ),
    deleteConsoleAction,
    store,
    resolved,
    lifecycle,
  );
}

/** Submit deletion, wait for disappearance, and clear stale local references. */
async function deleteConsoleAction(
  store: Store,
  { session, console }: { session: Session; console: ConsoleInfo },
  lifecycle: CommandLifecycle,
): Promise<void> {
  lifecycle.setFailureReport(
    consoleDeleteRequestReport(console.shortcode, session.profileName),
  );
  await deleteEnterpriseConsole(session, console);
  lifecycle.setFailureReport(consoleDeleteAcceptedReport());
  await waitForConsoleDeletion(session, console, lifecycle);
  await clearMissingReference(store, identityOf(session), console.id);
}
