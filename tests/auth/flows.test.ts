import { expect, spyOn, test } from "bun:test";
import * as enterprise from "../../src/actions/auth/enterprise.ts";
import { commitSignIn } from "../../src/actions/auth/flows.ts";
import { oldKeyCleanupReport } from "../../src/actions/auth/reporting.ts";
import type { Session } from "../../src/actions/auth/signin.ts";
import { Store } from "../../src/actions/auth/store.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { tempDir } from "../helpers.ts";

test("profile authorization failure reports that saving has not started", async () => {
  const lifecycle = new CommandLifecycle();
  const session: Session = {
    origin: "https://example.com",
    kind: "enterprise",
    cacheRoot: "",
    user: "alice",
  };
  await expect(
    commitSignIn(
      new Store(),
      session,
      { method: "password", credential: "new-key" },
      undefined,
      lifecycle,
    ),
  ).rejects.toMatchObject({ code: "internal" });
  expect(lifecycle.failureReport).toMatchObject({
    operation: "authorizing this computer",
    target: session.origin,
    outcome: "unknown",
    progress: "Sign-in completed; profile not saved.",
  });
  expect(lifecycle.failureReport.human?.details).toEqual([
    "The profile has not been saved; key authorization may be incomplete.",
  ]);
});

test("old-key cleanup keeps the server target", () => {
  expect(oldKeyCleanupReport("alice", "https://example.com")).toMatchObject({
    target: "https://example.com",
    outcome: "succeeded",
    human: {
      summary:
        "Cancelled while cleaning up old keys for profile alice at https://example.com.",
    },
  });
});

test("rejected key upload leaves the profile save unstarted", async () => {
  const lifecycle = new CommandLifecycle();
  const store = new Store(await tempDir("dh-flow-test-"));
  const session: Session = {
    origin: "https://example.com",
    kind: "enterprise",
    cacheRoot: store.cacheDir,
    user: "alice",
    enterprise: {
      origin: "https://example.com",
      dhe: {},
      client: {},
    } as unknown as enterprise.AuthenticatedEnterpriseConnection,
  };
  const upload = spyOn(enterprise, "uploadKey").mockRejectedValue(
    new Error("upload rejected"),
  );
  const remove = spyOn(enterprise, "deleteKey").mockResolvedValue(undefined);
  try {
    await expect(
      commitSignIn(
        store,
        session,
        { method: "password", credential: "new-key" },
        undefined,
        lifecycle,
      ),
    ).rejects.toThrow("upload rejected");
    expect(lifecycle.failureReport).toMatchObject({
      operation: "authorizing this computer",
      target: session.origin,
      outcome: "unknown",
      progress: "Sign-in completed; profile not saved.",
    });
    expect(Object.keys((await store.read()).config.profiles)).toHaveLength(0);
  } finally {
    upload.mockRestore();
    remove.mockRestore();
  }
});

test("interrupted old-key cleanup keeps the saved profile and target", async () => {
  const lifecycle = new CommandLifecycle();
  const store = new Store(await tempDir("dh-flow-test-"));
  const session: Session = {
    origin: "https://example.com",
    kind: "enterprise",
    cacheRoot: store.cacheDir,
    user: "alice",
    enterprise: {
      origin: "https://example.com",
      dhe: {},
      client: {},
    } as unknown as enterprise.AuthenticatedEnterpriseConnection,
  };
  const started = Promise.withResolvers<void>();
  const pending = Promise.withResolvers<void>();
  const read = store.read.bind(store);
  let reads = 0;
  const readSpy = spyOn(store, "read").mockImplementation(async () => {
    if (++reads === 2) {
      started.resolve();
      await pending.promise;
    }
    return await read();
  });
  try {
    const attempt = commitSignIn(
      store,
      session,
      { method: "password", credential: { type: "none" } },
      undefined,
      lifecycle,
    );
    await started.promise;
    lifecycle.receiveSignal("SIGINT");
    expect(lifecycle.failureReport).toMatchObject({
      target: session.origin,
      profile: "example.com:alice",
      outcome: "succeeded",
      progress: "Profile saved.",
    });
    expect(lifecycle.failureReport.human?.summary).toContain(
      "at https://example.com",
    );
    pending.resolve();
    await expect(attempt).rejects.toMatchObject({ code: "cancelled" });
  } finally {
    pending.resolve();
    readSpy.mockRestore();
  }
});
