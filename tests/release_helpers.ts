/** Local git repositories and a fake `gh` for testing the release scripts. */
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { delimiter, join, resolve } from "node:path";
import { tempDir } from "./helpers.ts";

export const ROOT = resolve(import.meta.dir, "..");

/** Git with no user or system config, and a fixed identity. */
export function gitEnv(home: string): Record<string, string> {
  return {
    HOME: home,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: join(home, ".gitconfig"),
    GIT_AUTHOR_NAME: "Test",
    GIT_AUTHOR_EMAIL: "test@example.invalid",
    GIT_COMMITTER_NAME: "Test",
    GIT_COMMITTER_EMAIL: "test@example.invalid",
  };
}

export interface Result {
  code: number;
  stdout: string;
  stderr: string;
}

/** Runs `cmd` in `cwd` with `env` added; never throws. */
export function sh(
  cmd: string[],
  cwd: string,
  env: Record<string, string | undefined> = {},
): Result {
  const merged = Object.entries({ ...process.env, ...env }).filter(
    (entry): entry is [string, string] => entry[1] !== undefined,
  );
  const proc = Bun.spawnSync(cmd, {
    cwd,
    env: Object.fromEntries(merged),
    stdout: "pipe",
    stderr: "pipe",
  });
  return {
    code: proc.exitCode ?? 1,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString(),
  };
}

/** A bare `origin` and a clone of it, on branch `v3_rewrite`, with one commit. */
export async function originAndClone() {
  const tmp = await tempDir("dh-release-");
  const home = join(tmp, "home");
  await mkdir(home);
  const env = gitEnv(home);
  const origin = join(tmp, "origin.git");
  const work = join(tmp, "work");
  const git = (cwd: string, ...args: string[]): string => {
    const r = sh(["git", "-c", "commit.gpgsign=false", ...args], cwd, env);
    if (r.code !== 0) throw new Error(`git ${args.join(" ")}: ${r.stderr}`);
    return r.stdout.trim();
  };
  git(tmp, "init", "-q", "--bare", "-b", "v3_rewrite", origin);
  git(tmp, "clone", "-q", origin, work);
  git(work, "checkout", "-q", "-b", "v3_rewrite");
  await writeFile(join(work, "file.txt"), "base\n");
  git(work, "add", ".");
  git(work, "commit", "-q", "-m", "base");
  git(work, "push", "-q", "origin", "v3_rewrite");
  /** Commits a change to `name` on the current branch; returns its sha. */
  const commit = async (name: string, text: string, message = name) => {
    await writeFile(join(work, name), text);
    git(work, "add", name);
    git(work, "commit", "-q", "-m", message);
    return git(work, "rev-parse", "HEAD");
  };
  return { tmp, env, origin, work, git, commit };
}

/**
 * A fake `gh` on `PATH`: logs each call's arguments, one line per call, to
 * `log`. `release create <name>` records `<name>`, so a later
 * `release view <name>` succeeds; `api` prints the `pulls` file.
 */
export async function fakeGh(dir: string) {
  const bin = join(dir, "fake-bin");
  const state = join(dir, "fake-gh-state");
  await mkdir(bin, { recursive: true });
  await mkdir(state, { recursive: true });
  const log = join(dir, "gh.log");
  const pulls = join(dir, "pulls.json");
  await writeFile(log, "");
  await writeFile(pulls, "[]");
  await writeFile(
    join(bin, "gh"),
    `#!/bin/sh
printf '%s\\n' "$*" >> "${log}"
case "$1 $2" in
  "api "*) cat "${pulls}" ;;
  "release view")
    if [ -e "${state}/fail-view" ]; then echo "HTTP 403: API rate limit exceeded" >&2; exit 1; fi
    [ -e "${state}/$3" ] || { echo "release not found" >&2; exit 1; } ;;
  "release create") touch "${state}/$3" ;;
esac
`,
  );
  await chmod(join(bin, "gh"), 0o755);
  return {
    env: { PATH: `${bin}${delimiter}${process.env.PATH ?? ""}` },
    /** Every call so far, as argument strings. */
    calls: async () =>
      (await readFile(log, "utf8")).split("\n").filter(Boolean),
    setPulls: (value: unknown) => writeFile(pulls, JSON.stringify(value)),
    /** Makes every later `gh release view` fail with a non-404 error. */
    failView: () => writeFile(join(state, "fail-view"), ""),
  };
}
