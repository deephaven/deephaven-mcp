import { describe, expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { WINDOWS } from "./helpers.ts";
import { fakeGh, originAndClone, ROOT, sh } from "./release_helpers.ts";

const SCRIPT = join(ROOT, "scripts", "publish.ts");

/** A built release in `dir`, as `release.ts` leaves it. */
async function dist(dir: string) {
  await mkdir(dir, { recursive: true });
  for (const name of [
    "dh-linux-x64",
    "SHA256SUMS",
    "install.sh",
    "install.ps1",
    "manifest.json",
  ]) {
    await writeFile(join(dir, name), name);
  }
  return dir;
}

async function setup(channel: string) {
  const repo = await originAndClone();
  const gh = await fakeGh(repo.tmp, repo.origin);
  const out = await dist(join(repo.tmp, "dist"));
  const branch = `cli-channel/${channel}`;
  repo.git(repo.work, "checkout", "-q", "-b", branch);
  /** Commits on the channel branch and pushes it, like a merge would. */
  const land = async (name: string) => {
    const sha = await repo.commit(name, name);
    repo.git(repo.work, "push", "-q", "origin", branch);
    return sha;
  };
  /** Runs publish.ts for `tag` at `sha`, from a checkout with every tag. */
  const publish = (tag: string, sha: string) => {
    repo.git(repo.work, "fetch", "-q", "--tags", "origin");
    return sh([process.execPath, SCRIPT, tag, out], repo.work, {
      ...repo.env,
      ...gh.env,
      GITHUB_REPOSITORY: "example/repo",
      GITHUB_SHA: sha,
      GH_TOKEN: undefined,
    });
  };
  /** The commit `ref` names in the origin, or "" if none. */
  const originRef = (ref: string) =>
    sh(
      ["git", "rev-parse", "-q", "--verify", `${ref}^{commit}`],
      repo.origin,
    ).stdout.trim();
  return { ...repo, gh, out, land, publish, originRef };
}

describe.skipIf(WINDOWS)("[channels-pub-3] publish", () => {
  test("creates the versioned release first, then moves the channel release, manifest last", async () => {
    const { gh, out, land, publish, originRef } = await setup("rc");
    const first = await land("one");
    const r1 = publish("cli-v3.0.0-rc.0", first);
    expect(r1.code, r1.stderr).toBe(0);
    expect(originRef("refs/tags/cli-v3.0.0-rc.0")).toBe(first);
    expect(originRef("refs/tags/cli-channel-rc")).toBe(first);
    const calls1 = await gh.calls();
    expect(calls1[0]).toStartWith("release create cli-v3.0.0-rc.0 ");
    expect(calls1[0]).toContain(
      `--target ${first} --latest=false --generate-notes --prerelease`,
    );
    expect(calls1[0]).not.toContain("--notes-start-tag");
    expect(calls1.slice(1)).toEqual([
      "release view cli-channel-rc --json tagName",
      "release create cli-channel-rc --verify-tag --prerelease --latest=false --title rc channel (currently cli-v3.0.0-rc.0) --notes Moving release for the rc channel. Its tag and manifest.json are replaced on every publish. Do not delete.",
      `release upload cli-channel-rc ${join(out, "manifest.json")} --clobber`,
    ]);

    // The next build moves the existing channel release.
    const second = await land("two");
    const r2 = publish("cli-v3.0.0-rc.1", second);
    expect(r2.code, r2.stderr).toBe(0);
    expect(originRef("refs/tags/cli-channel-rc")).toBe(second);
    const calls2 = (await gh.calls()).slice(calls1.length);
    expect(calls2[0]).toContain("--notes-start-tag cli-v3.0.0-rc.0");
    expect(calls2.slice(1)).toEqual([
      "release view cli-channel-rc --json tagName",
      "release edit cli-channel-rc --title rc channel (currently cli-v3.0.0-rc.1)",
      `release upload cli-channel-rc ${join(out, "manifest.json")} --clobber`,
    ]);
  });

  test("stops after the versioned release when the branch has moved on", async () => {
    const { gh, land, publish, originRef } = await setup("rc");
    const older = await land("one");
    const newer = await land("two");
    const r = publish("cli-v3.0.0-rc.0", older);
    expect(r.code, r.stderr).toBe(0);
    expect(r.stdout).toContain(
      "cli-channel/rc moved on; channel release unchanged",
    );
    expect(originRef("refs/tags/cli-v3.0.0-rc.0")).toBe(older);
    expect(originRef("refs/tags/cli-channel-rc")).toBe("");
    const calls = await gh.calls();
    expect(calls).toHaveLength(1);
    expect(calls[0]).toStartWith("release create cli-v3.0.0-rc.0 ");
    expect(newer).not.toBe(older);
  });

  test("a failed release create leaves no versioned tag", async () => {
    const { gh, land, publish, originRef } = await setup("rc");
    const sha = await land("one");
    await gh.failCreate();
    const r = publish("cli-v3.0.0-rc.0", sha);
    expect(r.code).not.toBe(0);
    expect(originRef("refs/tags/cli-v3.0.0-rc.0")).toBe("");
    expect(originRef("refs/tags/cli-channel-rc")).toBe("");
  });

  test("fails, without creating the channel release, when viewing it fails for another reason", async () => {
    const { gh, land, publish } = await setup("rc");
    const sha = await land("one");
    await gh.failView();
    const r = publish("cli-v3.0.0-rc.0", sha);
    expect(r.code).not.toBe(0);
    expect(r.stderr).toContain("rate limit");
    const calls = await gh.calls();
    expect(calls.at(-1)).toBe("release view cli-channel-rc --json tagName");
    expect(
      calls.some((c) => c.startsWith("release create cli-channel-rc")),
    ).toBe(false);
  });

  test("on release: a regular release, install scripts before the manifest", async () => {
    const { gh, out, land, publish } = await setup("release");
    const sha = await land("one");
    const r = publish("cli-v3.0.0", sha);
    expect(r.code, r.stderr).toBe(0);
    const calls = await gh.calls();
    expect(calls[0]).toContain(
      `--target ${sha} --latest=false --generate-notes`,
    );
    expect(calls[0]).not.toContain("--prerelease");
    expect(calls.filter((c) => c.startsWith("release upload"))).toEqual(
      ["install.sh", "install.ps1", "manifest.json"].map(
        (name) =>
          `release upload cli-channel-release ${join(out, name)} --clobber`,
      ),
    );
  });
});
