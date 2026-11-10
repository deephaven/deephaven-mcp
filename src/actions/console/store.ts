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
  const reserved = registry.reservations[target];
  if (reserved) return [reserved];
  const id = registry.shortcodeIds[target];
  if (id) {
    return registry.shortcodeOwners?.[target] ?? registry.fullIds[id] ?? [];
  }
  return registry.fullIds[target] ?? [];
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
  const matches = registry.fullIds[fullId] ?? [];
  if (!matches.some((known) => identityKey(known) === identityKey(identity))) {
    matches.push(identity);
  }
  registry.fullIds[fullId] = matches;
  registry.shortcodeIds[shortcode] = fullId;
  registry.shortcodeOwners ??= {};
  const owners = registry.shortcodeOwners[shortcode] ?? [];
  if (!owners.some((owner) => identityKey(owner) === identityKey(identity))) {
    owners.push(identity);
  }
  registry.shortcodeOwners[shortcode] = owners;
}

/** Reconcile one identity's discovered consoles under a single registry lock. */
export async function reconcileDiscoveredConsoles(
  store: Store,
  identity: ConsoleIdentity,
  consoles: { id: string; shortcode: string }[],
): Promise<void> {
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    for (const console of consoles) {
      rememberConsoleInRegistry(
        registry,
        console.shortcode,
        console.id,
        identity,
      );
    }
    const liveIds = new Set(consoles.map((console) => console.id));
    const key = identityKey(identity);
    for (const [shortcode, id] of Object.entries(registry.shortcodeIds)) {
      if (!liveIds.has(id)) removeShortcodeOwner(registry, shortcode, identity);
    }
    for (const [id, identities] of Object.entries(registry.fullIds)) {
      registry.fullIds[id] = identities.filter(
        (known) => identityKey(known) !== key || liveIds.has(id),
      );
      if (registry.fullIds[id]?.length === 0) delete registry.fullIds[id];
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
    const fullId = registry.shortcodeIds[target] ?? target;
    if (
      registry.selected[key] === target ||
      registry.selected[key] === fullId
    ) {
      delete registry.selected[key];
    }
    for (const [shortcode, id] of Object.entries(registry.shortcodeIds)) {
      if (id === fullId) removeShortcodeOwner(registry, shortcode, identity);
    }
    const matches = registry.fullIds[fullId];
    if (matches) {
      registry.fullIds[fullId] = matches.filter(
        (known) => identityKey(known) !== key,
      );
      if (registry.fullIds[fullId]?.length === 0)
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
  const owners =
    registry.shortcodeOwners?.[shortcode] ??
    identitiesForTarget(registry, shortcode);
  const remaining = owners.filter(
    (owner) => identityKey(owner) !== identityKey(identity),
  );
  if (remaining.length === owners.length) return;
  registry.shortcodeOwners ??= {};
  if (remaining.length > 0) {
    registry.shortcodeOwners[shortcode] = remaining;
  } else {
    delete registry.shortcodeOwners[shortcode];
    delete registry.shortcodeIds[shortcode];
  }
}
/** Generate a six-character candidate; reservation enforces uniqueness. */
function randomShortcode(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(SHORTCODE_LENGTH));
  return Array.from(
    bytes,
    (byte) => SHORTCODE_ALPHABET[byte % SHORTCODE_ALPHABET.length] ?? "a",
  ).join("");
}
