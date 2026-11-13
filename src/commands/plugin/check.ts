import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { basename } from "node:path";
import { Option } from "commander";
import { requireAuth } from "../../actions/auth/session.ts";
import { disconnectServer } from "../../actions/auth/signin.ts";
import type { Discovery, Plugin } from "../../actions/plugins/discover.ts";
import { importPlugin, leafIn } from "../../actions/plugins/js-run.ts";
import type { JsNode } from "../../actions/plugins/plugin-json.ts";
import {
  accountScript,
  isCurrentUser,
  localAccount,
  sudoRefused,
} from "../../actions/plugins/run-as.ts";
import {
  hostsDefaultFirst,
  localPath,
  readConfig,
  requireProfile,
  sshSettings,
} from "../../actions/plugins/settings.ts";
import type { RunAs } from "../../actions/plugins/spec.ts";
import { captureOverSsh, sshFailure } from "../../actions/plugins/ssh.ts";
import { checkJsRevision } from "../../actions/plugins/sync.ts";
import type { Context } from "../../cli/command.ts";
import { collect, defineCommand } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { askPlugin } from "./index.ts";
import { askSshHost } from "./questions.ts";
import {
  type Check,
  checkSshLogin,
  runStep,
  stepOnHost,
} from "./ssh-checks.ts";

interface CheckOptions {
  sshHost?: string[];
}

/** `dh plugin check <name>`: each step a run of the plugin needs, in order, stopping at the first that fails. */
export function checkCommand(found: Discovery) {
  return defineCommand<CheckOptions>({
    name: "check",
    description:
      "Check that a plugin's tool can run: SSH host and login, the tool, and the account it runs as.",
    args: [{ name: "<name>", description: "The plugin, e.g. ctl." }],
    options: [
      new Option(
        "--ssh-host <host>",
        "Check only this one of the plugin's SSH hosts (default: each in turn).",
      ).argParser(collect),
    ],

    async run(ctx, [nameArg], opts) {
      const plugin = askPlugin(found, nameArg ?? "");
      if (plugin.state !== "ready") {
        throw new DhError(
          plugin.state === "unavailable"
            ? "plugin_unavailable"
            : "plugin_invalid",
          `${plugin.name}: ${plugin.reason ?? plugin.state}`,
        );
      }
      if (plugin.exec === "js") return await checkJs(ctx, plugin);
      const spec = plugin.spec;
      if (!spec) throw new DhError("internal", `${plugin.name} has no spec`);
      const config = await readConfig(ctx.store);
      const checks: Check[] = [];
      let sshHosts: string[] | null = null;
      if (plugin.exec === "ssh") {
        const { profile } = requireProfile(config, ctx.profileFlag);
        const ssh = sshSettings(config, profile, plugin.name);
        const flag = opts.sshHost?.at(-1);
        sshHosts =
          flag === undefined
            ? hostsDefaultFirst(ssh)
            : [askSshHost(ssh, plugin.name, flag)];
        for (const host of sshHosts) {
          const step = stepOnHost(checks, sshHosts, host);
          await checkSshLogin(
            { plugin: plugin.name, toolPath: spec.tool.path },
            host,
            sshHosts.length > 1,
            step,
          );
          const runAs = spec.tool.runAs;
          if (runAs) {
            await step("runAs", () =>
              runAsOverSsh(host, plugin.name, spec.tool.path, runAs),
            );
          }
        }
      } else {
        const path = localPath(config, spec);
        await runStep(checks, "tool", null, "", async () => {
          try {
            await access(path, constants.X_OK);
          } catch {
            throw new DhError(
              "tool_not_found",
              `Tool not found: ${path}`,
              `Install it here, or set where it is: \`${BIN} plugin set ${plugin.name} --path <path>\`.`,
            );
          }
          return path;
        });
        const runAs = spec.tool.runAs;
        if (runAs) {
          await runStep(checks, "runAs", null, "", () =>
            runAsHere(spec.tool.path, runAs),
          );
        }
      }
      return {
        human: `✔ ${plugin.name} is ready to run`,
        json: { plugin: plugin.name, exec: plugin.exec, sshHosts, checks },
      };
    },
  });
}

/**
 * The account `runAs` resolves to on `host`, and that the SSH user can
 * become it: `<account> (you)`, or `<account> (sudo)` after `sudo -n true`.
 */
async function runAsOverSsh(
  host: string,
  plugin: string,
  toolPath: string,
  runAs: RunAs,
): Promise<string> {
  const script = `${accountScript(runAs.account)}; echo "$a"; [ "$(id -un)" = "$a" ] && { echo you; exit 0; }; sudo -n -u "$a" true && echo sudo`;
  const r = await captureOverSsh(host, ["sh", "-c", script]);
  if (r.code === 255) throw sshFailure(host, plugin, r.stderr);
  const [account, how] = r.stdout.trim().split("\n");
  if (r.code === 0 && account && how) return `${account} (${how})`;
  throw (
    sudoRefused(r.stderr, basename(toolPath), account, runAs, {
      sshHost: host,
    }) ?? runAsFailed(account, r.code, r.stderr, { sshHost: host })
  );
}

/** {@link runAsOverSsh} on this machine. */
async function runAsHere(toolPath: string, runAs: RunAs): Promise<string> {
  const account = await localAccount(runAs.account);
  if (isCurrentUser(account)) return `${account} (you)`;
  let proc: ReturnType<typeof Bun.spawn<"ignore", "ignore", "pipe">>;
  try {
    proc = Bun.spawn(["sudo", "-n", "-u", account, "true"], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "pipe",
    });
  } catch {
    throw new DhError("tool_not_found", "sudo not found");
  }
  const [stderr, code] = await Promise.all([
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (code === 0) return `${account} (sudo)`;
  throw (
    sudoRefused(stderr, basename(toolPath), account, runAs, {}) ??
    runAsFailed(account, code, stderr, {})
  );
}

function runAsFailed(
  account: string | undefined,
  code: number,
  stderr: string,
  fields: Record<string, unknown>,
): DhError {
  return new DhError(
    "tool_failed",
    `Couldn't run as ${account ?? "the service account"} (exit ${code})`,
    undefined,
    stderr.trim().split("\n").at(-1) || undefined,
    { toolExit: code, ...fields },
  );
}

/** A `js` plugin's steps: sign-in, the revision check, and loading its module. */
async function checkJs(ctx: Context, plugin: Plugin) {
  const checks: Check[] = [];
  const step = (name: string, fn: () => Promise<string>) =>
    runStep(checks, name, null, "", fn);
  await step("sign-in", async () => {
    const session = await requireAuth(
      ctx.profileFlag,
      ctx.store,
      ctx.lifecycle,
    );
    disconnectServer(session);
    return `${session.user} on ${session.origin}`;
  });
  const source = plugin.source;
  if (source.kind === "web" && source.origin) {
    const origin = source.origin;
    await step("revision", async () => {
      const check = await checkJsRevision(origin, {
        revision: source.manifest?.revision,
        etag: source.state?.etag,
      });
      if (check.kind !== "unchanged") {
        throw new DhError(
          "plugin_unavailable",
          `${origin}'s dh plugins changed since they were fetched`,
          `Run \`${BIN} plugin sync\`.`,
        );
      }
      return "unchanged";
    });
  }
  await step("module", async () => {
    const definition = await importPlugin(plugin);
    const leaves = leafPaths(plugin);
    for (const path of leaves) leafIn(plugin, definition, path);
    return `${leaves.length} commands`;
  });
  return {
    human: `✔ ${plugin.name} is ready to run`,
    json: { plugin: plugin.name, exec: "js", sshHosts: null, checks },
  };
}

/** Every leaf's path in the plugin's `plugin.json`, top-level noun first. */
function leafPaths(plugin: Plugin): string[][] {
  const json = plugin.pluginJson;
  if (!json) return [];
  const walk = (nodes: JsNode[], above: string[]): string[][] =>
    nodes.flatMap((n) =>
      n.commands ? walk(n.commands, [...above, n.name]) : [[...above, n.name]],
    );
  return json.nouns
    ? walk(json.nouns, [])
    : walk(json.commands ?? [], [json.name]);
}
