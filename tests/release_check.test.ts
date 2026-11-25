import { describe, expect, test } from "bun:test";
import { join } from "node:path";
import { WINDOWS } from "./helpers.ts";
import { fakeGh, originAndClone, ROOT, sh } from "./release_helpers.ts";

const SCRIPT = join(ROOT, "scripts", "release-check.ts");
const check = (args: string[], cwd = ROOT, env: Record<string, string> = {}) =>
  sh([process.execPath, SCRIPT, ...args], cwd, { GITHUB_ACTIONS: "", ...env });

describe("[channels-pub-5] release-source", () => {
  const REPO = "example/repo";
  const source = (ref: string, headRepo = REPO) =>
    check(["source", ref, headRepo], ROOT, { GITHUB_REPOSITORY: REPO });
  test.each(["cli-channel/rc", "hotfix/crash"])("passes %s", (ref) => {
    expect(source(ref).code).toBe(0);
  });
  test.each([
    ["v3_rewrite", REPO],
    ["feature/x", REPO],
    ["cli-channel/alpha-bar", REPO],
    ["hotfix", REPO],
    ["cli-channel/rc", "fork/repo"],
    ["hotfix/crash", "fork/repo"],
  ])("fails %s from %s", (ref, headRepo) => {
    const r = source(ref, headRepo);
    expect(r.code).toBe(1);
    expect(r.stderr).toContain(
      "PRs into cli-channel/release must come from cli-channel/rc or a hotfix/* branch in this repo",
    );
  });
});

/**
 * v3_rewrite → rc → release with merge commits, then a hotfix into release.
 * Returns helpers to run `release-contains` from rc's point of view.
 */
async function releasedWithHotfix() {
  const repo = await originAndClone();
  const { work, git, commit } = repo;
  const merge = (branch: string) =>
    git(work, "merge", "-q", "--no-ff", "-m", `Merge ${branch}`, branch);
  git(work, "checkout", "-q", "-b", "cli-channel/release");
  git(work, "checkout", "-q", "v3_rewrite");
  await commit("feature.txt", "feature\n");
  git(work, "checkout", "-q", "-b", "cli-channel/rc");
  git(work, "checkout", "-q", "cli-channel/release");
  merge("cli-channel/rc");
  // The hotfix starts from the released v3_rewrite commit.
  const base = git(work, "merge-base", "cli-channel/release", "v3_rewrite");
  git(work, "checkout", "-q", "-b", "hotfix/crash", base);
  await commit("fix.txt", "fix\n");
  git(work, "checkout", "-q", "cli-channel/release");
  merge("hotfix/crash");
  git(
    work,
    "push",
    "-q",
    "origin",
    "v3_rewrite",
    "cli-channel/rc",
    "cli-channel/release",
  );
  /** Runs the check with rc checked out, as the PR's head. */
  const containsFromRc = () => {
    git(work, "checkout", "-q", "cli-channel/rc");
    git(work, "fetch", "-q", "origin");
    return check(["contains"], work);
  };
  return { ...repo, merge, containsFromRc };
}

describe.skipIf(WINDOWS)("[channels-pub-5] release-contains", () => {
  test("fails until the hotfix reaches rc through v3_rewrite, then passes", async () => {
    const { work, git, commit, merge, containsFromRc } =
      await releasedWithHotfix();

    const missing = containsFromRc();
    expect(missing.code).toBe(1);
    expect(missing.stderr).toContain("fix.txt");
    expect(missing.stderr).toContain(
      "Merge the hotfix into v3_rewrite, then v3_rewrite into cli-channel/rc.",
    );

    git(work, "checkout", "-q", "v3_rewrite");
    await commit("more.txt", "more\n");
    merge("hotfix/crash");
    git(work, "checkout", "-q", "cli-channel/rc");
    merge("v3_rewrite");
    expect(containsFromRc().code).toBe(0);
  });

  test("a second hotfix fails until the first reaches v3_rewrite", async () => {
    const { work, git, commit, merge } = await releasedWithHotfix();
    const containsFrom = (branch: string) => {
      git(work, "checkout", "-q", branch);
      git(work, "fetch", "-q", "origin");
      return check(["contains"], work);
    };
    const branchHotfix = async (name: string) => {
      const base = git(work, "merge-base", "cli-channel/release", "v3_rewrite");
      git(work, "checkout", "-q", "-b", name, base);
      await commit(`${name.replace("/", "-")}.txt`, "fix\n");
    };

    // Branched before hotfix/crash was merged forward: lacks it.
    await branchHotfix("hotfix/early");
    expect(containsFrom("hotfix/early").code).toBe(1);

    git(work, "checkout", "-q", "v3_rewrite");
    merge("hotfix/crash");
    git(work, "push", "-q", "origin", "v3_rewrite");
    await branchHotfix("hotfix/late");
    expect(containsFrom("hotfix/late").code).toBe(0);
  });

  test("a cherry-picked fix counts", async () => {
    const { work, git, containsFromRc } = await releasedWithHotfix();
    git(work, "checkout", "-q", "cli-channel/rc");
    git(work, "cherry-pick", "hotfix/crash");
    expect(containsFromRc().code).toBe(0);
  });

  // Why channel branches take merge commits only: a squash hides the fix.
  test("a squashed v3_rewrite → rc merge fails even with the fix present", async () => {
    const { work, git, commit, merge, containsFromRc } =
      await releasedWithHotfix();
    git(work, "checkout", "-q", "v3_rewrite");
    await commit("more.txt", "more\n");
    merge("hotfix/crash");
    git(work, "checkout", "-q", "cli-channel/rc");
    git(work, "merge", "-q", "--squash", "v3_rewrite");
    git(work, "commit", "-q", "-m", "Squash v3_rewrite");
    expect(sh(["git", "show", "HEAD:fix.txt"], work).stdout).toBe("fix\n");
    expect(containsFromRc().code).toBe(1);
  });
});

describe.skipIf(WINDOWS)("[channels-pub-4] release origin", () => {
  const SHA = "a".repeat(40);
  const pr = (head: string, over: Record<string, unknown> = {}) => ({
    merged_at: "2026-11-24T00:00:00Z",
    merge_commit_sha: SHA,
    base: { ref: "cli-channel/release" },
    head: { ref: head, repo: { full_name: "example/repo" } },
    ...over,
  });
  const origin = async (pulls: unknown[]) => {
    const { tmp } = await originAndClone();
    const gh = await fakeGh(tmp);
    await gh.setPulls(pulls);
    const r = check(["origin", SHA], ROOT, {
      ...gh.env,
      GITHUB_REPOSITORY: "example/repo",
    });
    return { r, calls: await gh.calls() };
  };

  test.each(["cli-channel/rc", "hotfix/crash"])(
    "publishes a merged PR from %s",
    async (head) => {
      const { r, calls } = await origin([pr(head)]);
      expect(r.code).toBe(0);
      expect(calls).toEqual([`api repos/example/repo/commits/${SHA}/pulls`]);
    },
  );

  test.each([
    ["no PR (a direct push)", []],
    ["a PR from another branch", [pr("feature/x")]],
    ["an unmerged PR", [pr("cli-channel/rc", { merged_at: null })]],
    [
      "a PR into another branch",
      [pr("cli-channel/rc", { base: { ref: "v3_rewrite" } })],
    ],
    [
      "a PR whose merge is another commit",
      [pr("cli-channel/rc", { merge_commit_sha: "b".repeat(40) })],
    ],
    [
      "a PR from a fork's cli-channel/rc",
      [
        pr("cli-channel/rc", {
          head: { ref: "cli-channel/rc", repo: { full_name: "fork/repo" } },
        }),
      ],
    ],
    [
      "a PR whose head repo was deleted",
      [pr("cli-channel/rc", { head: { ref: "cli-channel/rc", repo: null } })],
    ],
  ] as [string, unknown[]][])("publishes nothing for %s", async (_, pulls) => {
    const { r } = await origin(pulls);
    expect(r.code).toBe(1);
    expect(r.stderr).toContain(
      `${SHA} is not a merged PR from cli-channel/rc or hotfix/*; nothing published`,
    );
  });

  test("a failed gh call is reported as an annotation", async () => {
    const { tmp } = await originAndClone();
    const gh = await fakeGh(tmp);
    await gh.failApi();
    const r = check(["origin", SHA], ROOT, {
      ...gh.env,
      GITHUB_REPOSITORY: "example/repo",
      GITHUB_ACTIONS: "true",
    });
    expect(r.code).toBe(1);
    expect(r.stderr.trim()).toBe(
      `::error::origin check could not run: gh api repos/example/repo/commits/${SHA}/pulls failed: HTTP 502: Bad Gateway`,
    );
  });
});
