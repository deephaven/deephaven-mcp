import { assert, assertEquals, assertRejects } from "@std/assert";
import { join, toFileUrl } from "@std/path";
import { DhError } from "../../src/auth/errors.ts";
import { Store } from "../../src/auth/store.ts";

const WINDOWS = Deno.build.os === "windows";

async function tempStore(): Promise<Store> {
  return new Store(
    join(await Deno.makeTempDir({ prefix: "dh-store-" }), "cli"),
  );
}

/** Whether `path`'s ACL drops inheritance, then each rule, with "me" for the current user. */
async function windowsAcl(path: string, kind: "dir" | "file") {
  const info = kind === "dir" ? "DirectoryInfo" : "FileInfo";
  // .NET only: cmdlet modules can fail to load when launched from pwsh 7.
  const out = await new Deno.Command("powershell", {
    args: [
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
    env: { DH_TEST_ACL_PATH: path },
  }).output();
  const text = new TextDecoder().decode(out.stdout);
  assert(out.success, new TextDecoder().decode(out.stderr));
  return text.trim().split("\n").map((line) => line.trim());
}

Deno.test("store: round-trips config and credentials", async () => {
  const store = await tempStore();
  await store.update(({ config, credentials }) => {
    config.servers["https://dhe.example.com"] = { kind: "enterprise" };
    credentials.credentials.p_1 = { type: "secret", value: "s3cret" };
  });
  const state = await store.read();
  assertEquals(
    state.config.servers["https://dhe.example.com"].kind,
    "enterprise",
  );
  assertEquals(state.credentials.credentials.p_1, {
    type: "secret",
    value: "s3cret",
  });
  const config = await Deno.readTextFile(store.configPath);
  assert(!config.includes("s3cret"), "secret leaked into config.json");
});

Deno.test({
  name: "store: owner-only files, and refuses a readable credentials file",
  ignore: WINDOWS,
  async fn() {
    const store = await tempStore();
    await store.update(() => {});
    assertEquals((await Deno.stat(store.dir)).mode! & 0o777, 0o700);
    assertEquals((await Deno.stat(store.credentialsPath)).mode! & 0o777, 0o600);
    await Deno.chmod(store.credentialsPath, 0o644);
    const e = await assertRejects(() => store.read(), DhError);
    assertEquals(e.code, "credential_unavailable");
  },
});

Deno.test({
  name: "store: owner-only ACLs on Windows, re-securing an existing folder",
  ignore: !WINDOWS,
  async fn() {
    const store = await tempStore();
    await store.update(() => {});
    const ownerOnly = ["protected=True", "me Allow FullControl"];
    assertEquals(await windowsAcl(store.dir, "dir"), ownerOnly);
    assertEquals(await windowsAcl(store.credentialsPath, "file"), ownerOnly);
    assertEquals(await windowsAcl(store.configPath, "file"), [
      "protected=False",
      "me Allow FullControl",
    ]);

    const everyone = await new Deno.Command("icacls", {
      args: [store.dir, "/grant", "*S-1-1-0:(OI)(CI)R", "/Q"],
    }).output();
    assert(everyone.success);
    await store.ensureDir();
    assertEquals(await windowsAcl(store.dir, "dir"), ownerOnly);
  },
});

Deno.test("store: concurrent updates in one process don't lose writes", async () => {
  const store = await tempStore();
  await Promise.all(
    Array.from({ length: 20 }, (_, i) =>
      store.update(({ config }) => {
        config.servers[`https://h${i}.example.com`] = { kind: "community" };
      })),
  );
  assertEquals(Object.keys((await store.read()).config.servers).length, 20);
});

Deno.test("store: concurrent processes don't lose writes", async () => {
  const store = await tempStore();
  const module = toFileUrl(join(Deno.cwd(), "src/auth/store.ts")).href;
  const worker = (n: number) =>
    new Deno.Command(Deno.execPath(), {
      args: [
        "eval",
        `import { Store } from "${module}";
         const s = new Store(${JSON.stringify(store.dir)});
         for (let i = 0; i < 10; i++) {
           await s.update(({ config }) => {
             config.servers["https://p${n}-" + i + ".example.com"] = { kind: "community" };
           });
         }`,
      ],
    }).output();
  const results = await Promise.all([worker(1), worker(2), worker(3)]);
  for (const r of results) {
    assert(r.success, new TextDecoder().decode(r.stderr));
  }
  assertEquals(Object.keys((await store.read()).config.servers).length, 30);
});
