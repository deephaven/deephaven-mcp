import { DhError } from "../../errors.ts";
import type { Store } from "../auth/store.ts";
import type { ConsoleSettings } from "./settings.ts";

/** A stable selection owner, independent of saved profile names. */
export interface ConsoleIdentity {
  server: string;
  user: string;
  operateAs: string;
}

/** Persisted console references, permanent reservations, and creation defaults. */
export interface ConsoleRegistry {
  version: 1;
  reservations: Record<string, ConsoleIdentity>;
  fullIds: Record<string, ConsoleIdentity[]>;
  shortcodeIds: Record<string, string>;
  shortcodeOwners?: Record<string, ConsoleIdentity[]>;
  /** Per-owner IDs; absent in registries written before collision support. */
  shortcodeFullIds?: Record<string, Record<string, string>>;
  selected: Record<string, string>;
  defaults: ConsoleSettings;
}

export const EMPTY_CONSOLE_REGISTRY: ConsoleRegistry = {
  version: 1,
  reservations: {},
  fullIds: {},
  shortcodeIds: {},
  selected: {},
  defaults: {},
};

/** Resolve reserved and discovered shortcodes before interpreting a full ID. */
export function identitiesForTarget(
  registry: ConsoleRegistry,
  target: string,
): ConsoleIdentity[] {
  const owners = ownersForShortcode(registry, target);
  const reserved = registry.reservations[target];
  if (reserved) {
    return [
      reserved,
      ...owners.filter((owner) => identityKey(owner) !== identityKey(reserved)),
    ];
  }
  return target in registry.shortcodeIds
    ? owners
    : (registry.fullIds[target] ?? []);
}

/** Read discovered owners, including registries that predate explicit ownership. */
function ownersForShortcode(
  registry: ConsoleRegistry,
  shortcode: string,
): ConsoleIdentity[] {
  const id = registry.shortcodeIds[shortcode];
  if (!id) return [];
  return registry.shortcodeOwners?.[shortcode] ?? registry.fullIds[id] ?? [];
}

/** Read identity-specific IDs, upgrading the legacy single-ID shape on write. */
function fullIdsForShortcode(
  registry: ConsoleRegistry,
  shortcode: string,
): Record<string, string> {
  const stored = registry.shortcodeFullIds?.[shortcode];
  if (stored) return stored;
  const id = registry.shortcodeIds[shortcode];
  if (!id) return {};
  const owners = ownersForShortcode(registry, shortcode);
  return Object.fromEntries(owners.map((owner) => [identityKey(owner), id]));
}

const SHORTCODE_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";
const SHORTCODE_LENGTH = 6;
const MAX_RESERVATION_ATTEMPTS = 100;

/** Normalize identities so an omitted operate-as user means the login user. */
export function consoleIdentity(
  profile: Pick<ConsoleIdentity, "server" | "user"> & {
    operateAs?: string;
  },
): ConsoleIdentity {
  return {
    server: profile.server,
    user: profile.user,
    operateAs: profile.operateAs ?? profile.user,
  };
}

/** Encode all identity components without delimiter ambiguity. */
export function identityKey(identity: ConsoleIdentity): string {
  return JSON.stringify([identity.server, identity.user, identity.operateAs]);
}

/** Reserves a globally unique code and persists its stable profile identity before create. */
export async function reserveShortcode(
  store: Store,
  identity: ConsoleIdentity,
  makeCode: () => string = randomShortcode,
): Promise<string> {
  return await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    for (let attempt = 0; attempt < MAX_RESERVATION_ATTEMPTS; attempt++) {
      const shortcode = makeCode();
      if (!/^[a-z0-9]{6}$/.test(shortcode)) {
        throw new DhError(
          "internal",
          "Shortcode generator returned an invalid code",
        );
      }
      if (
        shortcode in registry.reservations ||
        shortcode in registry.shortcodeIds
      )
        continue;
      registry.reservations[shortcode] = identity;
      return shortcode;
    }
    throw new DhError(
      "internal",
      "Could not reserve a unique console shortcode",
    );
  });
}

/** Remember a discovered console without reserving or reassigning its shortcode. */
export async function rememberConsole(
  store: Store,
  shortcode: string,
  fullId: string,
  identity: ConsoleIdentity,
): Promise<void> {
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    rememberConsoleInRegistry(registry, shortcode, fullId, identity);
  });
}

/** Add mappings to an already locked registry without duplicating identities. */
function rememberConsoleInRegistry(
  registry: ConsoleRegistry,
  shortcode: string,
  fullId: string,
  identity: ConsoleIdentity,
): void {
  const owners = [...ownersForShortcode(registry, shortcode)];
  const ids = fullIdsForShortcode(registry, shortcode);
  ids[identityKey(identity)] = fullId;
  registry.shortcodeFullIds ??= {};
  registry.shortcodeFullIds[shortcode] = ids;
  const matches = registry.fullIds[fullId] ?? [];
  if (!matches.some((known) => identityKey(known) === identityKey(identity))) {
    matches.push(identity);
  }
  registry.fullIds[fullId] = matches;
  registry.shortcodeIds[shortcode] = fullId;
  registry.shortcodeOwners ??= {};
  if (!owners.some((owner) => identityKey(owner) === identityKey(identity))) {
    owners.push(identity);
  }
  registry.shortcodeOwners[shortcode] = owners;
}

/** Prune only references unchanged since discovery began, preserving concurrent writes. */
export async function reconcileDiscoveredConsoles(
  store: Store,
  identity: ConsoleIdentity,
  consoles: { id: string; shortcode: string }[],
  snapshot: ConsoleRegistry,
): Promise<void> {
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    const key = identityKey(identity);
    for (const console of consoles) {
      const currentId = fullIdsForShortcode(registry, console.shortcode)[key];
      const previousId = fullIdsForShortcode(snapshot, console.shortcode)[key];
      if (currentId !== previousId) continue;
      rememberConsoleInRegistry(
        registry,
        console.shortcode,
        console.id,
        identity,
      );
    }
    const liveIds = new Set(consoles.map((console) => console.id));
    const liveShortcodes = new Set(
      consoles.map((console) => console.shortcode),
    );
    for (const shortcode of Object.keys(snapshot.shortcodeIds)) {
      const previousId = fullIdsForShortcode(snapshot, shortcode)[key];
      if (
        previousId !== undefined &&
        fullIdsForShortcode(registry, shortcode)[key] === previousId &&
        !liveShortcodes.has(shortcode)
      )
        removeShortcodeOwner(registry, shortcode, identity);
    }
    for (const [id, identities] of Object.entries(registry.fullIds)) {
      const wasKnown = snapshot.fullIds[id]?.some(
        (known) => identityKey(known) === key,
      );
      if (!wasKnown || liveIds.has(id)) continue;
      registry.fullIds[id] = identities.filter(
        (known) => identityKey(known) !== key,
      );
      if (registry.fullIds[id].length === 0) delete registry.fullIds[id];
    }
  });
}

/** Set or clear one identity's selection without changing auth profile defaults. */
export async function rememberConsoleSelection(
  store: Store,
  identity: ConsoleIdentity,
  target: string | undefined,
): Promise<void> {
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    const key = identityKey(identity);
    if (target === undefined) delete registry.selected[key];
    else registry.selected[key] = target;
  });
}

/** Remove stale references for one identity, retaining permanent reservations. */
export async function clearMissingReference(
  store: Store,
  identity: ConsoleIdentity,
  target: string,
): Promise<void> {
  const key = identityKey(identity);
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    const fullId = fullIdsForShortcode(registry, target)[key] ?? target;
    if (
      registry.selected[key] === target ||
      registry.selected[key] === fullId
    ) {
      delete registry.selected[key];
    }
    for (const shortcode of Object.keys(registry.shortcodeIds)) {
      if (fullIdsForShortcode(registry, shortcode)[key] === fullId)
        removeShortcodeOwner(registry, shortcode, identity);
    }
    const matches = registry.fullIds[fullId];
    if (matches) {
      registry.fullIds[fullId] = matches.filter(
        (known) => identityKey(known) !== key,
      );
      if (registry.fullIds[fullId].length === 0)
        delete registry.fullIds[fullId];
    }
  });
}

/** Remove only one identity's ownership, retaining other servers' references. */
function removeShortcodeOwner(
  registry: ConsoleRegistry,
  shortcode: string,
  identity: ConsoleIdentity,
): void {
  const owners = ownersForShortcode(registry, shortcode);
  const remaining = owners.filter(
    (owner) => identityKey(owner) !== identityKey(identity),
  );
  if (remaining.length === owners.length) return;
  const ids = fullIdsForShortcode(registry, shortcode);
  delete ids[identityKey(identity)];
  registry.shortcodeOwners ??= {};
  if (remaining.length > 0) {
    registry.shortcodeOwners[shortcode] = remaining;
    registry.shortcodeFullIds ??= {};
    registry.shortcodeFullIds[shortcode] = ids;
    const remainingId = Object.values(ids)[0];
    if (remainingId) registry.shortcodeIds[shortcode] = remainingId;
  } else {
    delete registry.shortcodeOwners[shortcode];
    delete registry.shortcodeIds[shortcode];
    if (registry.shortcodeFullIds) delete registry.shortcodeFullIds[shortcode];
  }
}
/** Generate a six-character candidate; reservation enforces uniqueness. */
function randomShortcode(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(SHORTCODE_LENGTH));
  return Array.from(bytes, (byte) =>
    SHORTCODE_ALPHABET.charAt(byte % SHORTCODE_ALPHABET.length),
  ).join("");
}
