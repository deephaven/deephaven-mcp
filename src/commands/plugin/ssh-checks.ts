/**
 * The `host`, `connect` and `tool` steps of `dh plugin check`, shared with
 * `dh plugin set` and `dh plugin ssh enable`, which check `--ssh-host` hosts
 * before saving them.
 */

import {
  captureOverSsh,
  classifyExit,
  sshConfig,
  sshFailure,
  unreachable,
} from "../../actions/plugins/ssh.ts";
import { DhError } from "../../errors.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";

/** One step of `dh plugin check`, as listed in its `-o json`. */
export interface Check {
  step: string;
  /** The SSH host it ran on; `null` for a `local` plugin. */
  sshHost: string | null;
  ok: boolean;
  detail?: string;
  ms: number;
}

export type Step = (name: string, fn: () => Promise<string>) => Promise<void>;

/** What a host is checked for: a plugin and its tool's path there, or (unset) discovery. */
export interface HostCheckTarget {
  plugin?: string;
  toolPath?: string;
}

/**
 * Runs one step: on success notes `✔ <step>  <column><detail>`; on failure
 * throws its error with `checks` added. `column`: the host and spacing, when
 * several hosts are checked.
 */
export async function runStep(
  checks: Check[],
  name: string,
  sshHost: string | null,
  column: string,
  fn: () => Promise<string>,
): Promise<void> {
  const start = performance.now();
  const ms = () => Math.round(performance.now() - start);
  let detail: string;
  try {
    detail = await fn();
  } catch (e) {
    checks.push({ step: name, sshHost, ok: false, ms: ms() });
    if (!(e instanceof DhError)) throw e;
    throw new DhError(e.code, e.headline, e.hint, e.detail, {
      ...e.fields,
      checks,
    });
  }
  checks.push({ step: name, sshHost, ok: true, detail, ms: ms() });
  note(`✔ ${name.padEnd(7)}  ${column}${detail}`);
}

/** A {@link Step} on `host` of `hosts`: with several, each line names its host. */
export function stepOnHost(
  checks: Check[],
  hosts: string[],
  host: string,
): Step {
  const many = hosts.length > 1;
  const width = Math.max(...hosts.map((h) => h.length));
  return (name, fn) =>
    runStep(checks, name, host, many ? `${host.padEnd(width)}  ` : "", fn);
}

/**
 * The `host` (`ssh -G`), `connect` (a `true` run) and, with a `toolPath`,
 * `tool` (`test -x`) steps on `host`. `many`: the host is shown beside each
 * step, so `connect` doesn't repeat it.
 */
export async function checkSshLogin(
  t: HostCheckTarget,
  host: string,
  many: boolean,
  step: Step,
  mode: "batch" | "prompt" = "batch",
): Promise<void> {
  await step("host", async () => {
    const c = await sshConfig(host);
    if (c.code !== 0) throw unreachable(host, t.plugin, c.stderr);
    const o = c.options;
    return `${o.get("hostname") ?? host} as ${o.get("user") ?? "?"}, port ${o.get("port") ?? "22"}`;
  });
  await step("connect", async () => {
    const start = performance.now();
    const r = await captureOverSsh(host, ["true"], undefined, mode);
    if (r.code === 255) throw sshFailure(host, t.plugin, r.stderr);
    const failure = classifyExit(
      { plugin: t.plugin ?? "", host, toolPath: "true" },
      { signalCode: null },
      r.code,
      r.stderr,
    );
    if (failure) throw failure;
    const took = `in ${Math.round(performance.now() - start)} ms`;
    return many ? took : `${host} ${took}`;
  });
  const toolPath = t.toolPath;
  if (toolPath === undefined) return;
  await step("tool", async () => {
    const r = await captureOverSsh(host, ["test", "-x", toolPath]);
    if (r.code === 255) throw sshFailure(host, t.plugin, r.stderr);
    if (r.code !== 0) {
      throw new DhError(
        "tool_not_found",
        `Tool not found on ${host}: ${toolPath}`,
        `Install it there, or check the host: \`${BIN} plugin set ${t.plugin} --ssh-host <host>\`.`,
        undefined,
        { sshHost: host },
      );
    }
    return toolPath;
  });
}

/**
 * Checks each of `hosts` before it's saved: `host`, `connect` and, with a
 * `toolPath`, `tool`, printed as `dh plugin check` prints them. The first
 * failure throws that step's error.
 */
export async function checkSshHostsToSave(
  t: HostCheckTarget,
  hosts: string[],
): Promise<void> {
  const checks: Check[] = [];
  for (const host of hosts) {
    const step = stepOnHost(checks, hosts, host);
    await checkSshLogin(t, host, hosts.length > 1, step);
  }
}
