import { mkdir } from "node:fs/promises";
import { basename, join } from "node:path";
import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { DhError, isErrno, timeoutMs, withTimeout } from "../../errors.ts";
import { isJson } from "../../output.ts";
import { BIN } from "../../version.ts";
import { configDir } from "../auth/store.ts";
import { childOwnsSignals, toolFailed } from "./run.ts";
import { accountScript, sudoRefused } from "./run-as.ts";
import type { RunAs } from "./spec.ts";
import { passStderr } from "./stderr.ts";
import { MAX_TAR_BYTES, readTar, TarError } from "./tar.ts";

/** Unix socket paths are limited to about 104 bytes; `%C` is 40 hex characters. */
const MAX_SOCKET_PATH = 100;
const SOCKET_NAME_LENGTH = 40;

/** `word` for a POSIX shell: in `'…'`, each `'` written `'\''`. */
export function shellQuote(word: string): string {
  return `'${word.replaceAll("'", `'\\''`)}'`;
}

/** Words as a shell would read them back: quoted only where needed. */
export function displayWords(words: string[]): string {
  return words
    .map((w) => (/^[\w@%+=:,./-]+$/.test(w) ? w : shellQuote(w)))
    .join(" ");
}

/** `words` as one remote command line, every word quoted, so the remote shell neither splits nor expands them. */
export function remoteCommand(words: string[]): string {
  return words.map(shellQuote).join(" ");
}

/** `ssh -G <host>`: the options ssh would use, as `<lowercase key> <value>` lines. */
export interface SshConfig {
  code: number;
  options: Map<string, string>;
  stderr: string;
}

/** Run once per host per `dh` invocation. */
const configs = new Map<string, Promise<SshConfig>>();

/** `ssh -G <host>`, once per `host` per process. */
export function sshConfig(host: string): Promise<SshConfig> {
  let config = configs.get(host);
  if (!config) {
    config = readSshConfig(host);
    configs.set(host, config);
  }
  return config;
}

async function readSshConfig(host: string): Promise<SshConfig> {
  const proc = spawnSsh(["ssh", "-G", host], host, {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  const options = new Map<string, string>();
  for (const line of stdout.split("\n")) {
    const [key, ...value] = line.trim().split(" ");
    if (key && !options.has(key)) options.set(key, value.join(" "));
  }
  return { code, options, stderr };
}

/**
 * Connection reuse, unless on Windows (its OpenSSH lacks it), the user set up
 * their own (`controlmaster` other than `false` in `ssh -G`), or the socket
 * path would be too long.
 */
async function multiplexOptions(host: string): Promise<string[]> {
  if (process.platform === "win32") return [];
  const dir = join(configDir(), "ssh");
  if (dir.length + 1 + SOCKET_NAME_LENGTH > MAX_SOCKET_PATH) return [];
  const config = await sshConfig(host);
  if (config.code !== 0 || config.options.get("controlmaster") !== "false") {
    return [];
  }
  await mkdir(dir, { recursive: true, mode: 0o700 });
  return [
    "-o",
    "ControlMaster=auto",
    "-o",
    `ControlPath=${dir}/%C`,
    "-o",
    "ControlPersist=60",
  ];
}

/**
 * How `ssh` treats the terminal. `tty`: allocates one (`-t`), so the tool and
 * `sudo` can prompt. `batch`: `-T` with `BatchMode`, so nothing waits on a
 * prompt nobody sees. `prompt`: `-T`, but ssh itself may ask (a password or
 * a host key), for an interactive copy whose output is binary.
 */
export type SshMode = "tty" | "batch" | "prompt";

/** The `ssh` argv to run `remote` on `host`. `ConnectTimeout` bounds connecting only. */
export async function sshArgv(
  host: string,
  remote: string,
  mode: SshMode,
  timeout = timeoutMs(),
): Promise<string[]> {
  const seconds = Math.max(1, Math.ceil(timeout / 1000));
  const modeArgs = {
    tty: ["-t"],
    batch: ["-T", "-o", "BatchMode=yes"],
    prompt: ["-T"],
  }[mode];
  return [
    "ssh",
    ...modeArgs,
    "-o",
    `ConnectTimeout=${seconds}`,
    ...(await multiplexOptions(host)),
    "--",
    host,
    remote,
  ];
}

/** One run of a plugin's tool on a server, over SSH. */
export interface SshRun {
  plugin: string;
  host: string;
  /** The tool's path on the server, for messages. */
  toolPath: string;
  /** The tool, the spec's argv, then the user's arguments. */
  words: string[];
  /** Allocate a terminal; see {@link SshMode}. */
  interactive: boolean;
  /** Return stdout instead of streaming it, for `-o json`. */
  captureStdout: boolean;
  /**
   * The cached `shell/` bundle's revision and its path on the host. The same
   * session first checks that the host's `revision` file still says it, and
   * runs nothing if not. With `interactive`, the check is silent: see
   * {@link SshRunResult.maybeChanged}.
   */
  checkRevision?: { shellDir: string; revision: string };
  /**
   * Run the tool as this role's service account: directly if the SSH user
   * is it, else through `sudo` (`-n` unless `interactive`). Unset: as the
   * SSH user.
   */
  runAs?: RunAs;
  /** The command's, so the tool handles Ctrl-C. */
  lifecycle?: CommandLifecycle;
}

/** What a run over SSH gave. */
export interface SshRunResult {
  /** The captured stdout, with `captureStdout`. */
  stdout?: string;
  /** With `checkRevision`: the bundle changed, so the tool didn't run. */
  changed?: boolean;
  /** With `runAs`, unless `interactive`: the account the host resolved. */
  account?: string;
  /**
   * With `checkRevision` and `interactive`: exit 254, which is either the
   * silent check (the tool didn't run) or the tool's own exit. The caller
   * finds out which by copying the bundle; `failure` is the error to report
   * if nothing changed.
   */
  maybeChanged?: { failure: DhError };
}

/**
 * The start of the stderr lines the remote script writes for `dh` (held
 * back, never shown): `revision` when the bundle changed, `as=<account>`
 * before `sudo`.
 */
const MARKER = "dh-ssh:";
/** The in-session check's exit code for a changed bundle. */
export const CHECK_EXIT = 254;

/**
 * The remote command for `run`: its words, or a `sh -c` script that first
 * checks the bundle's revision (`checkRevision`: exits 254 if it changed,
 * running nothing) and runs the tool as its service account (`runAs`). The
 * script reports to `dh` on stderr with {@link MARKER} lines, unless
 * `interactive` (with a terminal, stderr can't be told apart).
 */
export function remoteRunCommand(run: SshRun): string {
  if (run.checkRevision === undefined && run.runAs === undefined) {
    return remoteCommand(run.words);
  }
  const say = (what: string) =>
    run.interactive ? "" : `echo "${MARKER}${what}" >&2; `;
  const words = remoteCommand(run.words);
  const parts: string[] = [];
  if (run.checkRevision) {
    const file = shellQuote(`${run.checkRevision.shellDir}/revision`);
    const want = shellQuote(run.checkRevision.revision);
    parts.push(
      `[ "$(cat ${file} 2>/dev/null)" = ${want} ] || { ${say("revision")}exit ${CHECK_EXIT}; }`,
    );
  }
  if (run.runAs) {
    const sudo = run.interactive ? "sudo -u" : "sudo -n -u";
    parts.push(
      accountScript(run.runAs.account),
      `[ "$(id -un)" = "$a" ] && exec ${words}`,
      `${say("as=$a")}exec ${sudo} "$a" ${words}`,
    );
  } else parts.push(`exec ${words}`);
  return remoteCommand(["sh", "-c", parts.join("; ")]);
}

/**
 * Runs the tool over SSH. stdin and (unless captured) stdout are passed
 * through; stderr is too, as it arrives, keeping the end of it to classify a
 * failure (and holding back the in-session check's line).
 */
export async function runOverSsh(run: SshRun): Promise<SshRunResult> {
  const checked = run.checkRevision !== undefined && !run.interactive;
  const silent = run.checkRevision !== undefined && run.interactive;
  const scripted = run.checkRevision !== undefined || run.runAs !== undefined;
  const argv = await sshArgv(
    run.host,
    remoteRunCommand(run),
    run.interactive ? "tty" : "batch",
  );
  const proc = spawnSsh(argv, run.host, {
    stdin: "inherit",
    stdout: run.captureStdout ? "pipe" : "inherit",
    stderr: "pipe",
  });
  const release = childOwnsSignals(proc, run.lifecycle);
  try {
    const [stdout, stderr, code] = await Promise.all([
      run.captureStdout ? new Response(proc.stdout).text() : undefined,
      passStderr(
        proc.stderr,
        scripted && !run.interactive ? MARKER : undefined,
      ),
      proc.exited,
    ]);
    if (checked && code === CHECK_EXIT && stderr.markers.includes("revision")) {
      return { changed: true };
    }
    const account = stderr.markers
      .find((m) => m.startsWith("as="))
      ?.slice("as=".length);
    const refused =
      run.runAs && code !== 0 && code !== 255
        ? sudoRefused(stderr.tail, basename(run.toolPath), account, run.runAs, {
            sshHost: run.host,
          })
        : undefined;
    if (refused) throw refused;
    const failure = classifyExit(run, proc, code, stderr.tail);
    if (silent && code === CHECK_EXIT && !proc.signalCode && failure) {
      return { maybeChanged: { failure } };
    }
    if (failure) throw isJson() ? failure : withoutDetail(failure);
    return { stdout, ...(account ? { account } : {}) };
  } finally {
    release();
  }
}

/**
 * A short run on `host`, output captured: `-T` with `BatchMode`, or with
 * `prompt` ssh may ask for a password on the terminal. With `onTimeout`, ssh
 * is stopped and that error thrown if it hasn't finished within `DH_TIMEOUT`.
 */
export async function captureOverSsh(
  host: string,
  words: string[],
  onTimeout?: () => DhError,
  mode: "batch" | "prompt" = "batch",
): Promise<{ code: number; stdout: string; stderr: string }> {
  const argv = await sshArgv(host, remoteCommand(words), mode);
  const proc = spawnSsh(argv, host, {
    stdin: mode === "prompt" ? "inherit" : "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const output = Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  const [stdout, stderr, code] = onTimeout
    ? await withTimeout(output, timeoutMs(), () => {
        proc.kill();
        return onTimeout();
      })
    : await output;
  return { code, stdout, stderr };
}

/**
 * Closes the connections `dh` keeps open to `hosts` (`ControlMaster`), if
 * any; failures are ignored.
 */
export async function closeSshMasters(hosts: string[]): Promise<void> {
  if (process.platform === "win32") return;
  const path = `${join(configDir(), "ssh")}/%C`;
  await Promise.all(
    hosts.map(async (host) => {
      try {
        await Bun.spawn(
          ["ssh", "-O", "exit", "-o", `ControlPath=${path}`, "--", host],
          { stdin: "ignore", stdout: "ignore", stderr: "ignore" },
        ).exited;
      } catch {
        // No ssh, or nothing open.
      }
    }),
  );
}

/** What copying `shell/` over SSH found. */
export type ShellCopy =
  | { kind: "unchanged" }
  | { kind: "none" }
  | { kind: "copied"; revision: string; files: Map<string, Uint8Array> };

/** Where the installer writes the address clients use, on every node. */
const FRONTEND = "/etc/deephaven";

/** Before the copy, for an SSH-only login: the node's FRONTEND_HOST and FRONTEND_PORT, a line each (empty if missing). */
const FRONTEND_SCRIPT = `echo "$(cat ${FRONTEND}/FRONTEND_HOST 2>/dev/null)"
echo "$(cat ${FRONTEND}/FRONTEND_PORT 2>/dev/null)"
`;

/** The remote side of the copy: `$1` is the `shell/` path, `$2` the cached revision. */
const COPY_SCRIPT = `cd "$1" 2>/dev/null || exit 3
r=$(cat revision) || exit 4
[ "$r" = "$2" ] && { echo unchanged; exit 0; }
echo "$r"
exec tar -cf - .`;

/**
 * Copies the `shell/` bundle at `shellDir` on `host`, in one `ssh` run,
 * unless its revision is still `cached`. Exit 3 or 4 (no bundle there) is
 * `none`. A bad tar stream is `plugin_invalid`.
 */
export async function copyShellOverSsh(
  host: string,
  shellDir: string,
  cached: string | undefined,
  mode: "batch" | "prompt",
  timeout = timeoutMs(),
): Promise<ShellCopy> {
  return (await runCopy(host, shellDir, cached, mode, timeout, false)).copy;
}

/**
 * For `dh auth login --method ssh`: in one run, the node's cluster address
 * (`FRONTEND_HOST`, `FRONTEND_PORT`; empty when missing) and its `shell/`
 * bundle.
 */
export async function readClusterOverSsh(
  host: string,
  shellDir: string,
  mode: "batch" | "prompt",
): Promise<{ frontend: { host: string; port: string }; copy: ShellCopy }> {
  const r = await runCopy(host, shellDir, undefined, mode, timeoutMs(), true);
  return { frontend: r.frontend ?? { host: "", port: "" }, copy: r.copy };
}

async function runCopy(
  host: string,
  shellDir: string,
  cached: string | undefined,
  mode: "batch" | "prompt",
  timeout: number,
  frontend: boolean,
): Promise<{
  copy: ShellCopy;
  frontend?: { host: string; port: string };
}> {
  const remote = remoteCommand([
    "sh",
    "-c",
    (frontend ? FRONTEND_SCRIPT : "") + COPY_SCRIPT,
    "sh",
    shellDir,
    cached ?? "",
  ]);
  const argv = await sshArgv(host, remote, mode, timeout);
  const proc = spawnSsh(argv, host, {
    stdin: mode === "prompt" ? "inherit" : "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await withTimeout(
    Promise.all([
      readLimited(
        proc.stdout as ReadableStream<Uint8Array>,
        MAX_TAR_BYTES + 1024,
      ),
      new Response(proc.stderr).text(),
      proc.exited,
    ]),
    // Generous: a prompt may be waiting for the user.
    mode === "prompt" ? 10 * timeout : timeout,
    () => {
      proc.kill();
      return new DhError(
        "remote_unreachable",
        `Copying dh plugins from ${host} didn't finish in time`,
        "Set DH_TIMEOUT to allow longer.",
        undefined,
        { sshHost: host },
      );
    },
  );
  if (code === 255) throw sshFailure(host, undefined, stderr);
  let out = stdout ?? new Uint8Array(0);
  let address: { host: string; port: string } | undefined;
  if (frontend) {
    const [h, p, rest] = splitLines(out, 2);
    address = { host: h, port: p };
    out = rest;
  }
  if (code === 3 || code === 4)
    return { copy: { kind: "none" }, frontend: address };
  if (code !== 0 || stdout === undefined) {
    throw new DhError(
      "remote_unreachable",
      stdout === undefined
        ? `The dh plugins on ${host} are larger than ${MAX_TAR_BYTES / 1024 / 1024} MiB`
        : `Copying dh plugins from ${host} failed (exit ${code})`,
      undefined,
      lastLine(stderr),
      { sshHost: host },
    );
  }
  const [first, rest] = splitLines(out, 1);
  if (first === "unchanged") {
    return { copy: { kind: "unchanged" }, frontend: address };
  }
  try {
    return {
      copy: { kind: "copied", revision: first, files: readTar(rest) },
      frontend: address,
    };
  } catch (e) {
    throw new DhError(
      "plugin_invalid",
      `The dh plugins copied from ${host} were rejected`,
      undefined,
      e instanceof TarError ? e.message : String(e),
      { sshHost: host },
    );
  }
}

/** The first `n` lines of `bytes` as text, then the rest. */
function splitLines(bytes: Uint8Array, n: 1): [string, Uint8Array];
function splitLines(bytes: Uint8Array, n: 2): [string, string, Uint8Array];
function splitLines(bytes: Uint8Array, n: number): (string | Uint8Array)[] {
  const lines: string[] = [];
  let at = 0;
  for (let i = 0; i < n; i++) {
    const newline = bytes.indexOf(0x0a, at);
    const end = newline < 0 ? bytes.length : newline;
    lines.push(new TextDecoder().decode(bytes.subarray(at, end)).trim());
    at = Math.min(bytes.length, end + 1);
  }
  return [...lines, bytes.subarray(at)];
}

/** `stream`'s bytes, or undefined (and the rest discarded) past `limit`. */
async function readLimited(
  stream: ReadableStream<Uint8Array>,
  limit: number,
): Promise<Uint8Array | undefined> {
  const chunks: Uint8Array[] = [];
  let length = 0;
  for await (const chunk of stream) {
    length += chunk.length;
    if (length <= limit) chunks.push(chunk);
  }
  if (length > limit) return undefined;
  const out = new Uint8Array(length);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

/**
 * `e` without its detail, for people: the detail is the last line of stderr,
 * which they've just seen passed through. `-o json` keeps it, in one line.
 */
function withoutDetail(e: DhError): DhError {
  if (!e.detail) return e;
  return new DhError(e.code, e.headline, e.hint, undefined, e.fields);
}

/** What a run's exit means: nothing if it succeeded, else the error to throw. */
export function classifyExit(
  run: { plugin: string; host: string; toolPath: string },
  proc: { signalCode: string | null },
  code: number,
  stderr: string,
): DhError | undefined {
  if (code === 0 && !proc.signalCode) return undefined;
  if (code === 255) return sshFailure(run.host, run.plugin, stderr);
  if (code === 127) {
    return new DhError(
      "tool_not_found",
      `Tool not found on ${run.host}: ${run.toolPath}`,
      `Check that it's installed there, and the host: \`${BIN} plugin check ${run.plugin}\`.`,
      undefined,
      { sshHost: run.host },
    );
  }
  return toolFailed(basename(run.toolPath), proc, code, sudoHint(stderr), {
    sshHost: run.host,
  });
}

/**
 * An `ssh` exit 255: the host key, the login, or anything else about reaching
 * `host`. `plugin`: the plugin it ran for, if any, for the hint.
 */
export function sshFailure(
  host: string,
  plugin: string | undefined,
  stderr: string,
): DhError {
  const detail = lastLine(stderr);
  const fields = { sshHost: host };
  if (
    /Host key verification failed|REMOTE HOST IDENTIFICATION HAS CHANGED/.test(
      stderr,
    )
  ) {
    return new DhError(
      "ssh_host_unknown",
      `SSH host key for ${host} is unknown or changed`,
      `Connect once with \`ssh ${host}\` and check its host key.`,
      detail,
      fields,
    );
  }
  if (/Permission denied/.test(stderr)) {
    return new DhError(
      "ssh_auth_failed",
      `SSH login to ${host} failed`,
      `Set up key-based login, e.g. \`ssh-copy-id ${host}\`, or check ~/.ssh/config.`,
      detail,
      fields,
    );
  }
  return unreachable(host, plugin, stderr);
}

/** `remote_unreachable`: `host` couldn't be reached or resolved; `stderr`'s last line is the detail. */
export function unreachable(
  host: string,
  plugin: string | undefined,
  stderr: string,
): DhError {
  const set =
    plugin === undefined
      ? `${BIN} plugin ssh enable --ssh-host <host>`
      : `${BIN} plugin set ${plugin} --ssh-host <host>`;
  return new DhError(
    "remote_unreachable",
    `Couldn't reach ${host} over SSH`,
    `Check the host, or set it: \`${set}\`.`,
    lastLine(stderr),
    { sshHost: host },
  );
}

/** A hint for a `sudo` that wanted a password it couldn't ask for. */
export function sudoHint(stderr: string): string | undefined {
  return /sudo: (a password is required|a terminal is required)/.test(stderr)
    ? "sudo needs a password here: run it in a terminal."
    : undefined;
}

/** The last non-empty line of `text`. */
function lastLine(text: string): string | undefined {
  return (
    text
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean)
      .at(-1) || undefined
  );
}

type SshStdio = {
  stdin: "inherit" | "ignore";
  stdout: "inherit" | "pipe";
  stderr: "pipe";
};

/** Starts `ssh`; not on `PATH` is `tool_not_found`. */
function spawnSsh<S extends SshStdio>(argv: string[], host: string, stdio: S) {
  try {
    return Bun.spawn(argv, stdio);
  } catch (e) {
    if (!isErrno(e, "ENOENT")) throw e;
    throw new DhError(
      "tool_not_found",
      "ssh not found",
      "Install OpenSSH.",
      undefined,
      { sshHost: host },
    );
  }
}
