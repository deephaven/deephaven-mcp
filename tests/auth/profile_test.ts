import { assertEquals } from "@std/assert";
import {
  defaultName,
  findByIdentity,
  removeProfile,
} from "../../src/auth/profile.ts";
import { type Config, emptyState, type Profile } from "../../src/auth/store.ts";

function profile(name: string, server: string, user: string): Profile {
  return { name, server, user, method: "password", createdAt: "" };
}

function config(profiles: Record<string, Profile>, def?: string): Config {
  return { ...emptyState().config, profiles, defaultProfile: def };
}

Deno.test("defaultName: host:user, then host:port:user on collision", () => {
  const c = config({
    a: profile(
      "dhe.example.com:user-a",
      "https://dhe.example.com:8123",
      "user-a",
    ),
  });
  assertEquals(
    defaultName(c, "https://dhe.example.com:9000", "user-a"),
    "dhe.example.com:9000:user-a",
  );
  assertEquals(
    defaultName(c, "https://dhe.example.com:8123", "user-b"),
    "dhe.example.com:user-b",
  );
});

Deno.test("defaultName: operate-as is part of the name", () => {
  assertEquals(
    defaultName(config({}), "https://dhe.example.com", "user-a", "user-b"),
    "dhe.example.com:user-a/user-b",
  );
  assertEquals(
    defaultName(config({}), "https://dhe.example.com", "user-a", "user-a"),
    "dhe.example.com:user-a",
  );
});

Deno.test("findByIdentity: server + user + operate-as", () => {
  const c = config({
    a: profile("x", "https://dhe.example.com", "user-a"),
    b: {
      ...profile("y", "https://dhe.example.com", "user-a"),
      operateAs: "user-b",
    },
  });
  assertEquals(
    findByIdentity(c, "https://dhe.example.com", "user-a")?.[0],
    "a",
  );
  assertEquals(
    findByIdentity(c, "https://dhe.example.com", "user-a", "user-b")?.[0],
    "b",
  );
});

Deno.test("removeProfile: promotes a profile on the same server first", () => {
  const c = config({
    a: profile("a", "https://one.example.com", "u1"),
    b: profile("b", "https://two.example.com", "u2"),
    c: profile("c", "https://one.example.com", "u3"),
  }, "a");
  assertEquals(removeProfile(c, "a"), "c");
  assertEquals(removeProfile(c, "b"), "c");
  assertEquals(removeProfile(c, "c"), undefined);
});
