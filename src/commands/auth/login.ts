import { resolve } from "node:path";
import { Option } from "commander";
import { commitSignIn, type SavedSignIn } from "../../actions/auth/flows.ts";
import { trustAndResolveServer } from "../../actions/auth/session.ts";
import {
  connectServer,
  disconnectServer,
  offeredMethods,
  type Session,
  signIn,
} from "../../actions/auth/signin.ts";
import type { Config, Method, Store } from "../../actions/auth/store.ts";
import type { Manifest } from "../../actions/plugins/bundle.ts";
import { serverSsh, setServerSsh } from "../../actions/plugins/settings.ts";
import {
  type PartSync,
  syncJs,
  syncShell,
} from "../../actions/plugins/sync.ts";
import {
  type CommandSpec,
  collect,
  defineCommand,
  type Output,
} from "../../cli/command.ts";
import { DhError, reasonOf } from "../../errors.ts";
import { note } from "../../output.ts";
import type { Prompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import { checkSshHostsToSave } from "../plugin/ssh-checks.ts";
import { sshLogin } from "./login-ssh.ts";
import { logout } from "./logout.ts";
import {
  migratedOutput,
  migrateLegacy,
  shouldOfferMigration,
} from "./migrate.ts";
import {
  askAddSignIn,
  askLoginTarget,
  askMakeDefault,
  askSignInCredentials,
  askSignInMethod,
  askSshSetup,
  confirmSsoAccount,
  priorSignIn,
} from "./questions.ts";
import { use } from "./use.ts";

/** Every `--method` choice. */
export const METHODS: Method[] = [
  "saml",
  "password",
  "private-key",
  "psk",
  "basic",
  "anonymous",
  "custom",
  "ssh",
];

/** `dh auth login`'s flags, camel-cased by commander; see `options` below for each. `browser` is false with `--no-browser`. */
export interface LoginOptions {
  method?: Method;
  username?: string;
  passwordStdin?: boolean;
  passwordEnv?: string;
  pskStdin?: boolean;
  pskEnv?: string;
  handler?: string;
  tokenStdin?: boolean;
  tokenEnv?: string;
  privateKeyFile?: string;
  copyKey?: boolean;
  caCert?: string;
  operateAs?: string;
  browser?: boolean;
  expectUser?: string;
  default?: boolean;
  yes?: boolean;
  plugins?: "ssh" | "none";
  /** `--method ssh` only. */
  sshHost?: string[];
  sshBundle?: string;
  name?: string;
}

/** `dh auth login`. */
export function loginCommand(): CommandSpec<LoginOptions> {
  return defineCommand<LoginOptions>({
    name: "login",
    description: "Sign in and save a profile.",
    args: [{ name: "[server]" }],
    options: [
      new Option("--method <method>", "Sign-in method.").choices(METHODS),
      new Option("--username <user>", "Username."),
      new Option("--password-stdin", "Read the password from stdin."),
      new Option(
        "--password-env <var>",
        "Read the password from this env var.",
      ),
      new Option("--psk-stdin", "Read the pre-shared key from stdin."),
      new Option(
        "--psk-env <var>",
        "Read the pre-shared key from this env var.",
      ),
      new Option("--handler <class>", "Community custom auth handler class."),
      new Option("--token-stdin", "Read the custom auth token from stdin."),
      new Option(
        "--token-env <var>",
        "Read the custom auth token from this env var.",
      ),
      new Option(
        "--private-key-file <path>",
        "Use an existing Enterprise key file.",
      ),
      new Option("--copy-key", "Save the key itself instead of its path."),
      new Option(
        "--ca-cert <file>",
        "Trust this CA certificate for the server.",
      ),
      new Option("--operate-as <user>", "Enterprise user to operate as."),
      new Option(
        "--no-browser",
        "Print the SSO URL without opening a browser.",
      ),
      new Option(
        "--expect-user <user>",
        "Fail unless SSO signs in as this user.",
      ),
      new Option("--default", "Make this the default profile."),
      new Option("--no-default", "Don't make this the default profile."),
      new Option("-y, --yes", "Migrate a legacy config without asking."),
      new Option(
        "--ssh-host <host>",
        "With --method ssh: SSH destination for the cluster's nodes; repeat for several (default: the server's hostname).",
      ).argParser(collect),
      new Option(
        "--ssh-bundle <path>",
        "With --method ssh: the dh plugins' path on the nodes (default: /usr/illumon/latest/dh-cli).",
      ),
      new Option(
        "--name <name>",
        "With --method ssh: the profile's name (default: <host>:ssh).",
      ),
      new Option(
        "--plugins <choice>",
        "The server's admin tools over SSH: ssh sets it up, none declines (not asked again).",
      ).choices(["ssh", "none"]),
    ],

    async run(ctx, [serverArg], opts) {
      await ctx.store.ensureDir();
      let state = await ctx.store.read();
      // First run: migrating a legacy config replaces signing in, unless nothing migrated.
      if (!serverArg && shouldOfferMigration(state)) {
        const result = await migrateLegacy(ctx, { yes: opts.yes }, true);
        if (result && result.migrated > 0) {
          return migratedOutput(result);
        }
        state = await ctx.store.read();
      }

      // An explicit server outranks DH_PROFILE, but not --profile.
      const profile = ctx.profileFlag ?? (serverArg ? undefined : ctx.profile);
      const target = await askLoginTarget(
        ctx.ui,
        state,
        serverArg,
        profile,
        opts,
      );
      if (target === "use") return await use.run(ctx, [], {});
      if (target === "remove") return await logout.run(ctx, [], {});
      if (opts.method === "ssh")
        return await sshLogin(ctx, target.server, opts);
      if (opts.sshHost || opts.sshBundle || opts.name !== undefined) {
        throw new DhError(
          "usage",
          "--ssh-host, --ssh-bundle and --name are for --method ssh",
        );
      }
      const prior = priorSignIn(state, target.profile);
      let resolved: Awaited<ReturnType<typeof trustAndResolveServer>>;
      try {
        resolved = await trustAndResolveServer(
          target.server,
          state.config.servers,
          opts.caCert,
          target.rerun,
          ctx.lifecycle,
        );
      } catch (e) {
        if (!(e instanceof DhError) || e.code !== "server_unreachable") throw e;
        if (await askSshInstead(ctx.ui, target.server, e, opts)) {
          return await sshLogin(ctx, target.server, opts);
        }
        throw new DhError(
          e.code,
          e.headline,
          `Or set up SSH access: \`${BIN} auth login ${target.server} --method ssh\``,
          e.detail,
          e.fields,
        );
      }
      const { origin, kind } = resolved;
      const into = await askSignInTarget(ctx.ui, state.config, target, origin);

      const server = await connectServer(
        origin,
        kind,
        ctx.store.cacheDir,
        ctx.lifecycle,
      );
      try {
        // Sign in, then confirm the account before anything is saved.
        const methods = await offeredMethods(server, ctx.lifecycle);
        const method = await askSignInMethod(
          ctx.ui,
          kind,
          methods,
          prior,
          opts,
        );
        const { request, toSave } = await askSignInCredentials(
          ctx.ui,
          method,
          prior,
          opts,
        );
        const session = await signIn(
          server,
          request,
          "auth_failed",
          ctx.lifecycle,
        );
        try {
          ctx.lifecycle.onCleanup(() => disconnectServer(session));
          note(
            request.method === "anonymous"
              ? "✔ Signed in anonymously"
              : `✔ Signed in as ${session.user}`,
          );
          await confirmSsoAccount(ctx.ui, session, method, opts);

          // Save: the last question first, then the key upload and the write.
          const { config } = await ctx.store.read();
          // Adding a sign-in to an SSH-only profile keeps its default status.
          const makeDefault =
            into === undefined
              ? await askMakeDefault(ctx.ui, config, session, opts.default)
              : opts.default;
          const js = await fetchJsPlugins(ctx.store, session);
          const ssh = await askSshSetup(
            ctx.ui,
            config,
            session.origin,
            js?.shell ?? [],
            opts.plugins,
          );
          const caCert = opts.caCert ? resolve(opts.caCert) : undefined;
          const saved = await commitSignIn(
            ctx.store,
            session,
            { ...toSave, caCert },
            makeDefault,
            ctx.lifecycle,
            into,
          );
          if (saved.key)
            note(`✔ Authorized this computer (key "${saved.key}")`);
          for (const warning of saved.warnings) note(warning);
          const shell = await setUpShellPlugins(
            ctx.store,
            session.origin,
            ssh,
            ctx.ui.interactive,
          );
          return loggedInOutput(saved, session, {
            js: js ? js.plugins.map((p) => p.name) : null,
            ssh: shell,
          });
        } finally {
          disconnectServer(session);
        }
      } finally {
        disconnectServer(server);
      }
    },
  });
}

/**
 * After the web server couldn't be reached: whether to set up SSH access
 * instead. Offered only in a terminal, and only without `--method`.
 */
async function askSshInstead(
  ui: Prompter,
  server: string,
  e: DhError,
  opts: LoginOptions,
): Promise<boolean> {
  if (!ui.interactive || opts.method) return false;
  note(`✖ Could not reach ${server} over the web (${e.detail ?? e.headline}).`);
  return await ui.confirm(
    "Set up SSH access to this cluster instead? Admin tools only, no Deephaven sign-in.",
    true,
    "--method ssh",
  );
}

/**
 * The SSH-only profile this sign-in is added to, if any: `--profile` naming
 * one, or the only profile on `origin` being one (asked). A server entry
 * already at another origin than the profile's is `usage`.
 */
async function askSignInTarget(
  ui: Prompter,
  config: Config,
  target: { profile?: string },
  origin: string,
): Promise<string | undefined> {
  let into: string | undefined;
  const named = target.profile ? config.profiles[target.profile] : undefined;
  if (named?.method === "ssh") into = target.profile;
  if (!target.profile) {
    const here = Object.entries(config.profiles).filter(
      ([, p]) => p.server === origin,
    );
    const [only] = here;
    if (here.length === 1 && only?.[1].method === "ssh") {
      if (await askAddSignIn(ui, only[1].name)) into = only[0];
    }
  }
  const from = into ? config.profiles[into]?.server : undefined;
  if (from && from !== origin && config.servers[origin]) {
    throw new DhError(
      "usage",
      `Both ${from} and ${origin} are saved; log out of one first`,
    );
  }
  return into;
}

/**
 * Fetches the server's `js/` plugins, with the session's groups; a failure is
 * a warning, not a failed login. Returns the manifest, if there is one.
 */
async function fetchJsPlugins(
  store: Store,
  session: Session,
): Promise<Manifest | undefined> {
  try {
    const sync = await syncJs(store, session.origin, {
      groups: true,
      groupsOf: async () =>
        session.enterprise
          ? ((await session.enterprise.client.getGroupsForUser()) as string[])
          : [],
    });
    const names = sync.manifest?.plugins.map((p) => p.name) ?? [];
    if (names.length > 0) note(`✔ Plugins: ${names.join(", ")}`);
    return sync.manifest;
  } catch (e) {
    note(
      `! Couldn't fetch the server's plugins: ${reasonOf(e)}. Retry with \`${BIN} plugin sync\`.`,
    );
    return undefined;
  }
}

/**
 * The SSH answer: `none` records the decline; `ssh` checks the connection,
 * enables SSH and copies `shell/`. Unanswered, an enabled server is copied.
 * A failure is a warning. Returns whether SSH is enabled now.
 */
async function setUpShellPlugins(
  store: Store,
  origin: string,
  answer: "ssh" | "none" | undefined,
  prompt: boolean,
): Promise<"enabled" | "declined" | null> {
  if (answer === "none") {
    await store.update(({ config }) => {
      Object.assign(config, setServerSsh(config, origin, { enabled: false }));
    });
    note(
      `  SSH plugins not set up; \`${BIN} plugin ssh enable\` to change that.`,
    );
    return "declined";
  }
  const config = (await store.read()).config;
  if (answer === undefined && !serverSsh(config, origin).enabled) return null;
  try {
    if (answer === "ssh") {
      await checkSshHostsToSave({}, [serverSsh(config, origin).defaultHost]);
    }
    const sync = await syncShell(
      store,
      setServerSsh(config, origin, { enabled: true }),
      origin,
      { prompt },
    );
    await store.update(({ config }) => {
      Object.assign(config, setServerSsh(config, origin, { enabled: true }));
    });
    const names = sync.revision ? `: ${shellNames(sync)}` : "";
    note(`✔ SSH plugins${names}`);
    return "enabled";
  } catch (e) {
    note(
      `! Couldn't set up the SSH plugins: ${reasonOf(e)}. Retry with \`${BIN} plugin ssh enable\`.`,
    );
    return serverSsh(config, origin).enabled ? "enabled" : null;
  }
}

/** The plugins a shell sync left in the cache. */
function shellNames(sync: PartSync): string {
  return [...sync.added, ...sync.changed].join(", ") || "unchanged";
}

function loggedInOutput(
  saved: SavedSignIn,
  session: Session,
  plugins: { js: string[] | null; ssh: "enabled" | "declined" | null },
): Output {
  return {
    human: [
      saved.isDefault
        ? `Logged in. Default profile: ${saved.name}`
        : `Logged in. Profile: ${saved.name}`,
      `Run \`${BIN} --help\` to learn more.`,
    ],
    json: {
      profile: saved.name,
      server: session.origin,
      user: session.user,
      default: saved.isDefault,
      plugins,
    },
  };
}
