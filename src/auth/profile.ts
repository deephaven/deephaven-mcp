import type { Config, Profile } from "./store.ts";

export function newProfileId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(4));
  return "p_" + Array.from(bytes, (b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/** `<host>:<user>[/<operateAs>]`, adding the port and then a suffix on collision. */
export function defaultName(
  config: Config,
  server: string,
  user: string,
  operateAs?: string,
): string {
  const url = new URL(server);
  const who = operateAs && operateAs !== user ? `${user}/${operateAs}` : user;
  const taken = new Set(Object.values(config.profiles).map((p) => p.name));
  const base = [`${url.hostname}:${who}`, `${url.host}:${who}`].find((n) =>
    !taken.has(n)
  );
  if (base) return base;
  for (let i = 2;; i++) {
    const name = `${url.host}:${who}-${i}`;
    if (!taken.has(name)) return name;
  }
}

export function findByName(
  config: Config,
  name: string,
): [string, Profile] | undefined {
  return Object.entries(config.profiles).find(([, p]) => p.name === name);
}

export function findByIdentity(
  config: Config,
  server: string,
  user: string,
  operateAs?: string,
): [string, Profile] | undefined {
  return Object.entries(config.profiles).find(([, p]) =>
    p.server === server && p.user === user &&
    (p.operateAs ?? "") === (operateAs ?? "")
  );
}

/** Removes `id`; if it was the default, picks another on the same server first. */
export function removeProfile(
  config: Config,
  id: string,
): string | undefined {
  const removed = config.profiles[id];
  delete config.profiles[id];
  if (config.defaultProfile !== id) return config.defaultProfile;
  const rest = Object.entries(config.profiles);
  const next = rest.find(([, p]) => p.server === removed?.server) ?? rest[0];
  config.defaultProfile = next?.[0];
  return config.defaultProfile;
}

export function sortedProfiles(config: Config): [string, Profile][] {
  return Object.entries(config.profiles).sort(([, a], [, b]) =>
    a.name.localeCompare(b.name)
  );
}
