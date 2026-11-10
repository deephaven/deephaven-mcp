import { DhError } from "../../errors.ts";
import type { Store } from "../auth/store.ts";

export interface ConsoleIdentity {
  server: string;
  user: string;
  operateAs: string;
}

export interface ConsoleSettings {
  language?: "python" | "groovy" | null;
  heapSizeGb?: number | null;
  server?: string | null;
  engine?: string | null;
  jvmArgs?: string[] | null;
  environment?: Record<string, string> | null;
  autoDeleteTimeoutSeconds?: number | null;
}

export interface ConsoleRegistry {
  version: 1;
  reservations: Record<string, ConsoleIdentity>;
  fullIds: Record<string, ConsoleIdentity[]>;
  shortcodeIds: Record<string, string>;
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

const SHORTCODE_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";
const SHORTCODE_LENGTH = 6;
const MAX_RESERVATION_ATTEMPTS = 100;

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
      if (shortcode in registry.reservations) continue;
      registry.reservations[shortcode] = identity;
      return shortcode;
    }
    throw new DhError(
      "internal",
      "Could not reserve a unique console shortcode",
    );
  });
}

export async function rememberConsole(
  store: Store,
  shortcode: string,
  fullId: string,
  identity: ConsoleIdentity,
): Promise<void> {
  await store.updateConsoleData(EMPTY_CONSOLE_REGISTRY, (registry) => {
    let matches = registry.fullIds[fullId];
    if (!matches) {
      matches = [];
      registry.fullIds[fullId] = matches;
    }
    if (
      !matches.some((known) => identityKey(known) === identityKey(identity))
    ) {
      matches.push(identity);
    }
    registry.shortcodeIds[shortcode] = fullId;
  });
}

function randomShortcode(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(SHORTCODE_LENGTH));
  return Array.from(
    bytes,
    (byte) => SHORTCODE_ALPHABET[byte % SHORTCODE_ALPHABET.length] ?? "a",
  ).join("");
}
