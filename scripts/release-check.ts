/**
 * The checks that guard `cli-channel/release`, run by the workflows:
 *
 * - `bun scripts/release-check.ts source <head-ref> <head-repo>`: a PR into
 *   `cli-channel/release` must come from this repo's (`GITHUB_REPOSITORY`)
 *   `cli-channel/rc` or `hotfix/*`.
 * - `bun scripts/release-check.ts contains`: run in a checkout of the PR head;
 *   fails if any change already on `origin/cli-channel/release` is missing.
 * - `bun scripts/release-check.ts origin <sha>`: the pushed commit must be the
 *   merge of a PR from this repo's `cli-channel/rc` or `hotfix/*` into
 *   `cli-channel/release`. Needs `gh`, `GH_TOKEN` and `GITHUB_REPOSITORY`.
 *
 * Comparing with `GITHUB_REPOSITORY` rather than a fixed name keeps the checks
 * working on a fork, for PRs within that fork.
 *
 * Exits 0 when the check passes, 1 when it fails. Imports nothing, so CI can
 * run it without `bun install`.
 */

const RELEASE_BRANCH = "cli-channel/release";

/** True for the branches a release may come from. */
export const isReleaseSource = (ref: string): boolean =>
  ref === "cli-channel/rc" || ref.startsWith("hotfix/");

/** Runs `cmd`; returns stdout, or throws with its stderr. */
function run(cmd: string[]): string {
  const proc = Bun.spawnSync(cmd, { stderr: "pipe" });
  if (!proc.success) {
    throw new Error(
      `${cmd.join(" ")} failed: ${proc.stderr.toString().trim()}`,
    );
  }
  return proc.stdout.toString().trim();
}

function fail(message: string): never {
  // An annotation on GitHub Actions; plain text elsewhere.
  console.error(process.env.GITHUB_ACTIONS ? `::error::${message}` : message);
  process.exit(1);
}

function checkSource(
  headRef: string | undefined,
  headRepo: string | undefined,
): void {
  const repo = process.env.GITHUB_REPOSITORY;
  if (!repo) {
    fail(
      "usage: release-check.ts source <head-ref> <head-repo> (needs GITHUB_REPOSITORY)",
    );
  }
  if (!headRef || !isReleaseSource(headRef) || headRepo !== repo) {
    fail(
      "PRs into cli-channel/release must come from cli-channel/rc or a hotfix/* branch in this repo",
    );
  }
}

function checkContains(): void {
  // Matches by diff (patch id), not hash, so cherry-picked fixes count.
  const missing = run([
    "git",
    "log",
    "--no-merges",
    "--cherry-pick",
    "--right-only",
    "--format=%h %s",
    `HEAD...origin/${RELEASE_BRANCH}`,
  ]);
  if (missing) {
    console.error(`Commits on ${RELEASE_BRANCH} missing from this branch:`);
    console.error(missing);
    fail(
      "Merge the hotfix into v3_rewrite, then v3_rewrite into cli-channel/rc.",
    );
  }
}

interface PullRequest {
  merged_at: string | null;
  merge_commit_sha: string | null;
  base: { ref: string };
  // `repo` is null when the head repo has been deleted.
  head: { ref: string; repo: { full_name: string } | null };
}

function checkOrigin(sha: string | undefined): void {
  const repo = process.env.GITHUB_REPOSITORY;
  if (!sha || !repo)
    fail("usage: release-check.ts origin <sha> (needs GITHUB_REPOSITORY)");
  const pulls = JSON.parse(
    run(["gh", "api", `repos/${repo}/commits/${sha}/pulls`]),
  ) as PullRequest[];
  const ok = pulls.some(
    (pr) =>
      pr.merged_at !== null &&
      pr.base.ref === RELEASE_BRANCH &&
      pr.merge_commit_sha === sha &&
      pr.head.repo?.full_name === repo &&
      isReleaseSource(pr.head.ref),
  );
  if (!ok) {
    fail(
      `${sha} is not a merged PR from cli-channel/rc or hotfix/*; nothing published`,
    );
  }
}

if (import.meta.main) {
  const [check, arg, arg2] = process.argv.slice(2);
  try {
    if (check === "source") checkSource(arg, arg2);
    else if (check === "contains") checkContains();
    else if (check === "origin") checkOrigin(arg);
    else
      fail(
        "usage: release-check.ts source <head-ref> <head-repo> | contains | origin <sha>",
      );
  } catch (error) {
    // A failed git or gh call: report it as an annotation, not a stack trace.
    fail(
      `${check} check could not run: ${error instanceof Error ? error.message : error}`,
    );
  }
}
