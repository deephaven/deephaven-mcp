import { stat } from "node:fs/promises";
import { basename } from "node:path";
import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { DhError, isErrno } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { sudoArgv, sudoRefused } from "./run-as.ts";
import type { RunAs } from "./spec.ts";
import { passStderr } from "./stderr.ts";

/** One run of a plugin's tool on this machine. */
export interface ToolRun {
  /** The plugin, for hints. */
  plugin: string;
  /** Absolute path of the tool. */
  path: string;
  /** The spec's argv, then the user's arguments. */
  args: string[];
  /** Return stdout instead of streaming it, for `-o json`. */
  captureStdout: boolean;
  /** Run it as this service account, through `sudo`. */
  sudo?: { account: string; runAs: RunAs; interactive: boolean };
  /** The command's, so the tool handles Ctrl-C. */
  lifecycle?: CommandLifecycle;
}

/**
 * Runs the tool with no shell, passing stdin, stderr and (unless captured)
 * stdout through, so prompts and pipes work as when running it directly.
 * Returns the captured stdout; a non-zero exit is `tool_failed` with `toolExit`.
 */
export async function runTool(run: ToolRun): Promise<string | undefined> {
  await assertToolExists(run);
  const proc = spawnTool(run);
  const release = childOwnsSignals(proc, run.lifecycle);
  try {
    // Through sudo without a terminal, stderr is kept to tell its refusal apart.
    const kept = run.sudo && !run.sudo.interactive;
    const [stdout, stderr, code] = await Promise.all([
      run.captureStdout
        ? new Response(proc.stdout as ReadableStream).text()
        : undefined,
      kept ? passStderr(proc.stderr as ReadableStream<Uint8Array>) : undefined,
      proc.exited,
    ]);
    const tool = basename(run.path);
    if (code !== 0 && run.sudo && stderr) {
      const refused = sudoRefused(
        stderr.tail,
        tool,
        run.sudo.account,
        run.sudo.runAs,
        {},
      );
      if (refused) throw refused;
    }
    if (code !== 0) throw toolFailed(tool, proc, code);
    return stdout;
  } finally {
    release();
  }
}

/**
 * While `child` runs: the terminal's Ctrl-C reaches it as well, so `dh` leaves
 * that to the child and waits for it to exit (a second one stops `dh`, as for
 * any command); other signals are forwarded. Returns the undo.
 */
export function childOwnsSignals(
  child: Pick<Bun.Subprocess, "kill">,
  lifecycle: CommandLifecycle | undefined,
): () => void {
  const forward = (signal: NodeJS.Signals) => {
    if (signal !== "SIGINT") child.kill(signal);
  };
  if (lifecycle) {
    lifecycle.setSignalDelegate(forward);
    return () => lifecycle.setSignalDelegate(undefined);
  }
  process.on("SIGINT", forward);
  return () => process.off("SIGINT", forward);
}

/** `tool_failed` for `tool`, which exited with `code` or was stopped by a signal. */
export function toolFailed(
  tool: string,
  proc: { signalCode: string | null },
  code: number,
  hint?: string,
  fields: Record<string, unknown> = {},
): DhError {
  const how = proc.signalCode
    ? `was stopped by ${proc.signalCode}`
    : `exited with ${code}`;
  return new DhError("tool_failed", `${tool} ${how}`, hint, undefined, {
    toolExit: code,
    ...fields,
  });
}

/** `tool_not_found` if nothing is at the tool's path. Existence only: another user may be the one who can run it. */
async function assertToolExists(run: ToolRun): Promise<void> {
  try {
    await stat(run.path);
  } catch (e) {
    if (!isErrno(e, "ENOENT")) throw e;
    throw new DhError(
      "tool_not_found",
      `Tool not found: ${run.path}`,
      `Install it here, or set where it is: \`${BIN} plugin set ${run.plugin} --path <path>\`.`,
    );
  }
}

/** Starts the tool, through `sudo` if it runs as a service account; no `sudo` here is `tool_not_found`. */
function spawnTool(run: ToolRun) {
  const argv = run.sudo
    ? sudoArgv(run.sudo.account, [run.path, ...run.args], run.sudo.interactive)
    : [run.path, ...run.args];
  try {
    return Bun.spawn(argv, {
      stdin: "inherit",
      stdout: run.captureStdout ? "pipe" : "inherit",
      stderr: run.sudo && !run.sudo.interactive ? "pipe" : "inherit",
    });
  } catch (e) {
    if (!isErrno(e, "ENOENT")) throw e;
    throw new DhError(
      "tool_not_found",
      "sudo not found",
      `${run.plugin} runs its tool as ${run.sudo?.account} through sudo; install it, or pass your own credentials.`,
    );
  }
}
