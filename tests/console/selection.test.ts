import { expect, mock, spyOn, test } from "bun:test";
import { join } from "node:path";
import type { ReplicaStatus } from "@deephaven-enterprise/jsapi-types";
import * as auth from "../../src/actions/auth/session.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { type Profile, Store } from "../../src/actions/auth/store.ts";
import {
  createConsole,
  deleteConsole,
  detailConsole,
  listConsoles,
  rememberOnlyRunningConsole,
  resolveConsoleDeletion,
  selectConsole,
  setConsoleDefaults,
} from "../../src/actions/console/actions.ts";
import type {
  ConsoleServerSettings,
  consoleInfo,
  EnterpriseConsoleInfo,
} from "../../src/actions/console/enterprise.ts";
import * as enterprise from "../../src/actions/console/enterprise.ts";
import {
  type ConsoleIdentity,
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
  rememberConsole,
  rememberConsoleSelection,
  reserveShortcode,
} from "../../src/actions/console/store.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { DhError } from "../../src/errors.ts";
import { tempDir } from "../helpers.ts";

const identity = {
  server: "https://dhe.example.com",
  user: "alice",
  operateAs: "alice",
};

function consoleFixture(id: string, state: string): EnterpriseConsoleInfo {
  return {
    id,
    shortcode: "abc123",
    name: "IC - dh CLI - abc123",
    state,
    language: "Python",
    heapSizeGb: 1,
    server: "Query 1",
    engine: "DeephavenCommunity",
    jvmArgs: "",
    environment: {},
    autoDeleteTimeoutSeconds: 600,
  };
}

test("console selection: auto-select only one running console and persist its full ID", async () => {
  const store = new Store(join(await tempDir("dh-console-selection-"), "cli"));
  const consoles = [
    consoleFixture("running-id", "Running"),
    consoleFixture("stopped-id", "Stopped"),
  ];
  expect(await rememberOnlyRunningConsole(store, identity, consoles)).toBe(
    "running-id",
  );
  expect(
    (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).selected,
  ).toEqual({ [identityKey(identity)]: "running-id" });
});

/** Mock only discovery and disconnect; authentication is intercepted at its boundary. */
function sessionFixture(
  owner: ConsoleIdentity,
  consoles = [consoleFixture("full-id", "Running")],
) {
  const disconnect = mock(() => {});
  const queries: Parameters<typeof consoleInfo>[0][] = consoles.map(
    (console) => ({
      serial: console.id,
      name: console.name,
      type: "InteractiveConsole",
      designated: { status: console.state } as ReplicaStatus,
      replicas: [],
      scriptLanguage: console.language,
      heapSize: 1,
      dbServerName: "Query 1",
      workerKind: "DeephavenCommunity",
      jvmArguments: [],
      envVarEntries: [],
      typeSpecificFields: null,
    }),
  );
  const session = {
    origin: owner.server,
    kind: "enterprise",
    cacheRoot: "/tmp/dh-test",
    user: owner.user,
    operateAs: owner.operateAs,
    enterprise: {
      origin: owner.server,
      dhe: {},
      client: { getKnownConfigs: () => queries, disconnect },
    },
  } as unknown as Session;
  return { session, disconnect };
}

async function saveProfile(store: Store, name: string, owner: ConsoleIdentity) {
  const profile: Profile = {
    ...owner,
    name,
    method: "password",
    createdAt: new Date().toISOString(),
  };
  await store.update((state) => {
    state.config.profiles[name] = profile;
    state.config.defaultProfile = name;
  });
}

test("console routing: cached targets override defaults after profile rename", async () => {
  const store = new Store(join(await tempDir("dh-console-routing-"), "cli"));
  const other = { ...identity, server: "https://other.example.com" };
  await saveProfile(store, "renamed", identity);
  await saveProfile(store, "default-other", other);
  await rememberConsole(store, "abc123", "full-id", identity);
  const { session } = sessionFixture(identity);
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    for (const target of ["abc123", "full-id"]) {
      await detailConsole(store, target, undefined, new CommandLifecycle());
      await selectConsole(store, target, undefined, new CommandLifecycle());
      await resolveConsoleDeletion(
        store,
        target,
        undefined,
        new CommandLifecycle(),
      );
    }
    expect(login).toHaveBeenCalledTimes(6);
    for (const call of login.mock.calls) expect(call[0]).toBe("renamed");
    login.mockClear();
    await expect(
      detailConsole(store, "abc123", "default-other", new CommandLifecycle()),
    ).rejects.toMatchObject({ code: "usage" });
    expect(login).not.toHaveBeenCalled();
  } finally {
    login.mockRestore();
  }
});

test("console routing: ambiguous full IDs and shortcodes require a matching explicit profile", async () => {
  const store = new Store(join(await tempDir("dh-console-ambiguous-"), "cli"));
  const other = { ...identity, server: "https://other.example.com" };
  await saveProfile(store, "first", identity);
  await saveProfile(store, "second", other);
  await rememberConsole(store, "abc123", "full-id", identity);
  await rememberConsole(store, "abc123", "full-id", other);
  const { session } = sessionFixture(other);
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    for (const target of ["full-id", "abc123"]) {
      await expect(
        detailConsole(store, target, undefined, new CommandLifecycle()),
      ).rejects.toMatchObject({
        code: "usage",
        message: expect.stringContaining("ambiguous"),
      });
      expect(login).not.toHaveBeenCalled();
      expect(
        (await detailConsole(store, target, "second", new CommandLifecycle()))
          .id,
      ).toBe("full-id");
      expect(login).toHaveBeenCalledWith(
        "second",
        store,
        expect.any(CommandLifecycle),
      );
      login.mockClear();
    }
  } finally {
    login.mockRestore();
  }
});

test("console routing: environment-only targets must match the authenticated identity", async () => {
  const store = new Store(join(await tempDir("dh-console-env-"), "cli"));
  await rememberConsole(store, "abc123", "full-id", identity);
  const matching = sessionFixture(identity);
  const environment = spyOn(auth, "requireEnvironmentAuth").mockResolvedValue(
    matching.session,
  );
  const login = spyOn(auth, "requireAuth");
  try {
    expect(
      (await detailConsole(store, "abc123", undefined, new CommandLifecycle()))
        .id,
    ).toBe("full-id");
    for (const owner of [
      { ...identity, server: "https://other.example.com" },
      { ...identity, user: "other-user" },
      { ...identity, operateAs: "other-user" },
    ]) {
      const mismatching = sessionFixture(owner);
      environment.mockResolvedValue(mismatching.session);
      await expect(
        detailConsole(store, "abc123", undefined, new CommandLifecycle()),
      ).rejects.toMatchObject({
        code: "usage",
        message: expect.stringContaining("does not match"),
      });
      expect(mismatching.disconnect).toHaveBeenCalledTimes(1);
    }
    expect(matching.disconnect).not.toHaveBeenCalled();
    expect(login).not.toHaveBeenCalled();
  } finally {
    environment.mockRestore();
    login.mockRestore();
  }
});

class InterleavedStore extends Store {
  afterSnapshot?: () => Promise<void>;

  override async readConsoleData<T extends object>(empty: T): Promise<T> {
    const data = await super.readConsoleData(empty);
    const afterSnapshot = this.afterSnapshot;
    this.afterSnapshot = undefined;
    if (afterSnapshot) await afterSnapshot();
    return data;
  }
}

test("console selection: listing preserves a choice made after its registry snapshot", async () => {
  const store = new InterleavedStore(
    join(await tempDir("dh-console-race-"), "cli"),
  );
  await rememberConsoleSelection(store, identity, "gone-id");
  store.afterSnapshot = () =>
    rememberConsoleSelection(store, identity, "second-id");
  const consoles = [
    {
      ...consoleFixture("first-id", "Running"),
      shortcode: "aaaaaa",
      name: "IC - dh CLI - aaaaaa",
    },
    {
      ...consoleFixture("second-id", "Running"),
      shortcode: "bbbbbb",
      name: "IC - dh CLI - bbbbbb",
    },
  ];
  const { session } = sessionFixture(identity, consoles);
  const login = spyOn(auth, "requireAuth").mockResolvedValue(session);
  try {
    const listed = await listConsoles(store, undefined, new CommandLifecycle());
    expect(
      listed.filter((console) => console.inUse).map((console) => console.id),
    ).toEqual(["second-id"]);
    expect(
      (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).selected[
        identityKey(identity)
      ],
    ).toBe("second-id");
  } finally {
    login.mockRestore();
  }
});

test("console routing: stale listing preserves a concurrent create's full-ID profile mapping", async () => {
  const store = new InterleavedStore(
    join(await tempDir("dh-console-create-race-"), "cli"),
  );
  await saveProfile(store, "owner", identity);
  await saveProfile(store, "unrelated-default", {
    ...identity,
    server: "https://other.example.com",
  });
  const listing = sessionFixture(identity, []);
  const created = sessionFixture(identity);
  store.afterSnapshot = async () => {
    await rememberConsole(store, "abc123", "full-id", identity);
    await rememberConsoleSelection(store, identity, "full-id");
  };
  const login = spyOn(auth, "requireAuth").mockResolvedValue(listing.session);
  try {
    expect(await listConsoles(store, "owner", new CommandLifecycle())).toEqual(
      [],
    );
    const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
    expect(registry.fullIds["full-id"]).toEqual([identity]);
    expect(registry.shortcodeFullIds?.abc123).toEqual({
      [identityKey(identity)]: "full-id",
    });
    expect(registry.selected[identityKey(identity)]).toBe("full-id");
    login.mockClear();
    login.mockResolvedValue(created.session);
    expect(
      (await detailConsole(store, "full-id", undefined, new CommandLifecycle()))
        .id,
    ).toBe("full-id");
    expect(login).toHaveBeenCalledWith(
      "owner",
      store,
      expect.any(CommandLifecycle),
    );
  } finally {
    login.mockRestore();
  }
});

test("console selection: no automatic choice for zero or multiple running consoles", async () => {
  const store = new Store(join(await tempDir("dh-console-selection-"), "cli"));
  for (const consoles of [
    [],
    [consoleFixture("failed-id", "Failed")],
    [
      consoleFixture("first-id", "Running"),
      consoleFixture("second-id", "Running"),
    ],
  ]) {
    expect(
      await rememberOnlyRunningConsole(store, identity, consoles),
    ).toBeUndefined();
  }
  expect(
    (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).selected,
  ).toEqual({});
});

test.each([
  ["selects the new console", false, "new-id"],
  ["keeps the previous selection with noUse", true, "old-id"],
])("console create: %s", async (_label, noUse, selected) => {
  const store = new Store(join(await tempDir("dh-console-create-"), "cli"));
  const { session } = sessionFixture(identity, []);
  const created = consoleFixture("new-id", "Initializing");
  await setConsoleDefaults(store, { heapSizeGb: 8 }, false);
  await rememberConsoleSelection(store, identity, "old-id");
  let shortcode = "";
  let settings: unknown;
  let mappedBeforeWait: string | undefined;
  const spies = [
    spyOn(auth, "requireAuth").mockResolvedValue(session),
    spyOn(enterprise, "serverDefaults").mockResolvedValue({
      settings: {
        language: "python",
        heapSizeGb: 4,
        server: "Query 1",
        engine: "python",
        jvmArgs: [],
        environment: {},
        autoDeleteTimeoutSeconds: 600,
      },
    } as unknown as ConsoleServerSettings),
    spyOn(enterprise, "createEnterpriseConsole").mockImplementation(
      async (_session, code, resolved) => {
        shortcode = code;
        settings = resolved;
        return "new-id";
      },
    ),
    spyOn(enterprise, "waitForConsole").mockImplementation(async () => {
      // Remembered before waiting, so an interrupted startup stays addressable.
      mappedBeforeWait = (await store.readConsoleData(EMPTY_CONSOLE_REGISTRY))
        .shortcodeIds[shortcode];
      return created;
    }),
  ];
  try {
    expect(
      await createConsole(
        store,
        undefined,
        { language: "groovy" },
        noUse,
        new CommandLifecycle(),
      ),
    ).toBe(created);
  } finally {
    for (const spy of spies) spy.mockRestore();
  }
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  expect(settings).toMatchObject({ language: "groovy", heapSizeGb: 8 });
  expect(registry.reservations[shortcode]).toEqual(identity);
  expect(mappedBeforeWait).toBe("new-id");
  expect(registry.selected[identityKey(identity)]).toBe(selected);
});

/** A remembered, selected console whose deletion request the server accepts. */
async function deletionFixture() {
  const store = new Store(join(await tempDir("dh-console-delete-"), "cli"));
  await reserveShortcode(store, identity, () => "abc123");
  await rememberConsole(store, "abc123", "full-id", identity);
  await rememberConsoleSelection(store, identity, "full-id");
  const before = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  const resolved = {
    session: sessionFixture(identity, []).session,
    console: consoleFixture("full-id", "Running"),
  };
  const request = spyOn(
    enterprise,
    "deleteEnterpriseConsole",
  ).mockResolvedValue();
  return { store, before, resolved, request };
}

test("console delete: clears local references after the console disappears, keeping the reservation", async () => {
  const { store, resolved, request } = await deletionFixture();
  const wait = spyOn(enterprise, "waitForConsoleDeletion").mockResolvedValue();
  try {
    await deleteConsole(store, resolved, new CommandLifecycle());
    expect(request).toHaveBeenCalledWith(resolved.session, resolved.console);
  } finally {
    request.mockRestore();
    wait.mockRestore();
  }
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  expect(registry.selected).toEqual({});
  expect(registry.shortcodeIds).toEqual({});
  expect(registry.fullIds).toEqual({});
  expect(registry.reservations).toEqual({ abc123: identity });
});

test("console delete: keeps local references when disappearance is not confirmed", async () => {
  const { store, before, resolved, request } = await deletionFixture();
  const failure = new DhError("console_failed", "Deletion not confirmed");
  const wait = spyOn(enterprise, "waitForConsoleDeletion").mockRejectedValue(
    failure,
  );
  try {
    await expect(
      deleteConsole(store, resolved, new CommandLifecycle()),
    ).rejects.toBe(failure);
    expect(request).toHaveBeenCalledTimes(1);
  } finally {
    request.mockRestore();
    wait.mockRestore();
  }
  expect(await store.readConsoleData(EMPTY_CONSOLE_REGISTRY)).toEqual(before);
});
