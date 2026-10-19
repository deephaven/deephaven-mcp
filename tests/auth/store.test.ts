import { expect, test } from "bun:test";
import { chmod, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { addOrphan, revokeOrphans, saveProfile } from "../../src/auth/flows.ts";
import { Store } from "../../src/auth/store.ts";
import { run, tempDir, WINDOWS } from "../helpers.ts";

async function tempStore(): Promise<Store> {
  return new Store(join(await tempDir("dh-store-"), "cli"));
}

/** Whether `path`'s ACL drops inheritance, then each rule, with "me" for the current user. */
async function windowsAcl(path: string, kind: "dir" | "file") {
  const info = kind === "dir" ? "DirectoryInfo" : "FileInfo";
  // .NET only: cmdlet modules can fail to load when launched from pwsh 7.
  const out = await run(
    [
      "powershell",
      "-NoProfile",
      "-Command",
      `
      $acl = [IO.${info}]::new($env:DH_TEST_ACL_PATH).GetAccessControl('Access')
      $me = [Security.Principal.WindowsIdentity]::GetCurrent().User
      "protected=$($acl.AreAccessRulesProtected)"
      foreach ($r in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
        $who = if ($r.IdentityReference -eq $me) { 'me' } else { $r.IdentityReference.Value }
        "$who $($r.AccessControlType) $($r.FileSystemRights)"
      }`,
    ],
    { DH_TEST_ACL_PATH: path },
  );
  expect(out.code, out.stderr).toBe(0);
  return out.stdout
    .trim()
    .split("\n")
    .map((line) => line.trim());
}

/** Me, plus SYSTEM and Administrators, which can read everything anyway. */
const OWNERS = ["me", "S-1-5-18", "S-1-5-32-544"];

function expectOwnerOnly(acl: string[], inherited: boolean) {
  expect(acl[0]).toBe(`protected=${inherited ? "False" : "True"}`);
  expect(acl).toContain("me Allow FullControl");
  for (const rule of acl.slice(1)) {
    expect(OWNERS).toContain(rule.split(" ")[0] ?? "");
  }
}

test("store: round-trips config and credentials", async () => {
  const store = await tempStore();
  await store.update(({ config, credentials }) => {
    config.servers["https://dhe.example.com"] = { kind: "enterprise" };
    credentials.credentials.p_1 = { type: "secret", value: "s3cret" };
  });
  const state = await store.read();
  expect(state.config.servers["https://dhe.example.com"]?.kind).toBe(
    "enterprise",
  );
  expect(state.credentials.credentials.p_1).toEqual({
    type: "secret",
    value: "s3cret",
  });
  const config = await readFile(store.configPath, "utf8");
  expect(config).not.toContain("s3cret");
});

test.skipIf(WINDOWS)(
  "store: owner-only files, and refuses a readable credentials file",
  async () => {
    const store = await tempStore();
    await store.update(() => {});
    expect((await stat(store.dir)).mode & 0o777).toBe(0o700);
    expect((await stat(store.credentialsPath)).mode & 0o777).toBe(0o600);
    await chmod(store.credentialsPath, 0o644);
    await expect(store.read()).rejects.toMatchObject({
      code: "credential_unavailable",
    });
  },
);

test.skipIf(!WINDOWS)(
  "store: owner-only ACLs on Windows, re-securing an existing folder",
  async () => {
    const store = await tempStore();
    await store.update(() => {});
    expectOwnerOnly(await windowsAcl(store.dir, "dir"), false);
    expectOwnerOnly(await windowsAcl(store.credentialsPath, "file"), false);
    expectOwnerOnly(await windowsAcl(store.configPath, "file"), true);

    const everyone = await run([
      "icacls",
      store.dir,
      "/grant",
      "*S-1-1-0:(OI)(CI)R",
      "/Q",
    ]);
    expect(everyone.code).toBe(0);
    await store.ensureDir();
    expectOwnerOnly(await windowsAcl(store.dir, "dir"), false);
  },
);

test("store: concurrent updates in one process don't lose writes", async () => {
  const store = await tempStore();
  await Promise.all(
    Array.from({ length: 20 }, (_, i) =>
      store.update(({ config }) => {
        config.servers[`https://h${i}.example.com`] = { kind: "community" };
      }),
    ),
  );
  expect(Object.keys((await store.read()).config.servers).length).toBe(20);
});

test("store: concurrent processes don't lose writes", async () => {
  const store = await tempStore();
  const module = pathToFileURL(
    resolve(import.meta.dir, "../../src/auth/store.ts"),
  ).href;
  const worker = (n: number) =>
    run([
      process.execPath,
      "-e",
      `import { Store } from "${module}";
       const s = new Store(${JSON.stringify(store.dir)});
       for (let i = 0; i < 10; i++) {
         await s.update(({ config }) => {
           config.servers["https://p${n}-" + i + ".example.com"] = { kind: "community" };
         });
       }`,
    ]);
  const results = await Promise.all([worker(1), worker(2), worker(3)]);
  for (const r of results) expect(r.code, r.stderr).toBe(0);
  expect(Object.keys((await store.read()).config.servers).length).toBe(30);
});

test("store: a leftover lock file doesn't block; only a live holder does", async () => {
  const store = await tempStore();
  await mkdir(store.dir, { recursive: true });
  const lock = join(store.dir, "config.lock");
  await writeFile(lock, `${hostname()} 1 left by an older dh`);
  const started = Date.now();
  await store.update(() => {});
  expect(Date.now() - started).toBeLessThan(5_000);

  const module = pathToFileURL(
    resolve(import.meta.dir, "../../src/auth/lock.ts"),
  ).href;
  const holder = Bun.spawn(
    [
      process.execPath,
      "-e",
      `import { withFileLock } from "${module}";
       await withFileLock(${JSON.stringify(lock)}, 5000, async () => {
         console.log("held");
         await Bun.sleep(1500);
       });`,
    ],
    { stdout: "pipe" },
  );
  const reader = holder.stdout.getReader();
  await reader.read();
  const waited = Date.now();
  await store.update(() => {});
  expect(Date.now() - waited).toBeGreaterThan(500);
  await holder.exited;
});

test("saveProfile: saving a new key drops its pending record; sign-ins skip pending ones", async () => {
  const store = await tempStore();
  const key = (publicKey: string) => ({
    type: "keyPair" as const,
    keyType: "ec",
    publicKey,
    privateKey: "PRIV",
    generated: true,
  });
  const later = new Date(Date.now() + 60_000).toISOString();
  await store.update(({ credentials }) => {
    addOrphan(credentials, "https://dhe", "alice", key("NEW"), later);
    addOrphan(credentials, "https://dhe", "alice", key("OTHER"), later);
  });
  // Another sign-in's key is still being authorized, so nothing is touched.
  const session = { origin: "https://dhe", user: "alice", enterprise: {} };
  expect(await revokeOrphans(store, session as never)).toEqual({
    deleted: 0,
    kept: 0,
    failed: undefined,
  });
  await saveProfile(store, {
    origin: "https://dhe",
    kind: "enterprise",
    user: "alice",
    method: "password",
    credential: key("NEW"),
  });
  const { credentials } = await store.read();
  expect(credentials.orphans?.map((o) => o.publicKey)).toEqual(["OTHER"]);
});

test("store: the config directory is found on first use, not on construction", () => {
  const saved = {
    HOME: process.env.HOME,
    DH_CONFIG_DIR: process.env.DH_CONFIG_DIR,
  };
  try {
    delete process.env.HOME;
    delete process.env.DH_CONFIG_DIR;
    const store = new Store();
    expect(() => store.dir).toThrow();
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});

test("saveProfile: keeps every legacy entry merged into one profile", async () => {
  const store = await tempStore();
  const save = (importedFrom: string) =>
    saveProfile(store, {
      origin: "http://localhost:10000",
      kind: "community",
      user: "psk",
      method: "psk",
      importedFrom,
      credential: { type: "none" },
    });
  const first = await save("dhcli:a#one");
  const second = await save("dhcli:a#two");
  expect(second.id).toBe(first.id);
  const { config } = await store.read();
  expect(config.profiles[first.id]?.importedFrom).toEqual([
    "dhcli:a#one",
    "dhcli:a#two",
  ]);
});
