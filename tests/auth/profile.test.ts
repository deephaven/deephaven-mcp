import { expect, test } from "bun:test";
import {
  defaultName,
  findByIdentity,
  newProfileId,
  removeProfile,
} from "../../src/auth/profile.ts";
import { type Config, emptyState, type Profile } from "../../src/auth/store.ts";

test("newProfileId: skips ids already taken", () => {
  let tries = 0;
  const id = newProfileId(() => ++tries < 4);
  expect(tries).toBe(4);
  expect(id).toMatch(/^p_[0-9a-f]{8}$/);
});

function profile(name: string, server: string, user: string): Profile {
  return { name, server, user, method: "password", createdAt: "" };
}

function config(profiles: Record<string, Profile>, def?: string): Config {
  return { ...emptyState().config, profiles, defaultProfile: def };
}

test("defaultName: host:user, then host:port:user on collision", () => {
  const c = config({
    a: profile(
      "dhe.example.com:user-a",
      "https://dhe.example.com:8123",
      "user-a",
    ),
  });
  expect(defaultName(c, "https://dhe.example.com:9000", "user-a")).toBe(
    "dhe.example.com:9000:user-a",
  );
  expect(defaultName(c, "https://dhe.example.com:8123", "user-b")).toBe(
    "dhe.example.com:user-b",
  );
});

test("defaultName: operate-as is part of the name", () => {
  expect(
    defaultName(config({}), "https://dhe.example.com", "user-a", "user-b"),
  ).toBe("dhe.example.com:user-a/user-b");
  expect(
    defaultName(config({}), "https://dhe.example.com", "user-a", "user-a"),
  ).toBe("dhe.example.com:user-a");
});

test("findByIdentity: server + user + operate-as", () => {
  const c = config({
    a: profile("x", "https://dhe.example.com", "user-a"),
    b: {
      ...profile("y", "https://dhe.example.com", "user-a"),
      operateAs: "user-b",
    },
  });
  expect(findByIdentity(c, "https://dhe.example.com", "user-a")?.[0]).toBe("a");
  expect(
    findByIdentity(c, "https://dhe.example.com", "user-a", "user-b")?.[0],
  ).toBe("b");
});

test("removeProfile: promotes a profile on the same server first", () => {
  const c = config(
    {
      a: profile("a", "https://one.example.com", "u1"),
      b: profile("b", "https://two.example.com", "u2"),
      c: profile("c", "https://one.example.com", "u3"),
    },
    "a",
  );
  expect(removeProfile(c, "a")).toBe("c");
  expect(removeProfile(c, "b")).toBe("c");
  expect(removeProfile(c, "c")).toBeUndefined();
});
