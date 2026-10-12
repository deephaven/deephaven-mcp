import { resolve } from "node:path";
import { InvalidArgumentError, Option } from "commander";
import { commitSignIn } from "../../auth/flows.ts";
import { findServer } from "../../auth/session.ts";
import { connect, disconnect, offers, signIn } from "../../auth/signin.ts";
import type { Method } from "../../auth/store.ts";
import { defineCommand } from "../../cli/command.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";
import { logout } from "./logout.ts";
import { isFirstRun, migratedOutput, migrateLegacy } from "./migrate.ts";
import {
  askCredentials,
  askDefault,
  askMethod,
  askWhere,
  confirmAccount,
  knownIdentity,
} from "./questions.ts";

export const METHODS: Method[] = [
  "saml",
  "password",
  "private-key",
  "psk",
  "basic",
  "anonymous",
  "custom",
];

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
  timeout?: number;
  operateAs?: string;
  browser?: boolean;
  expectUser?: string;
  default?: boolean;
  yes?: boolean;
}

export const login = defineCommand<LoginOptions>({
  name: "login",
  description: "Sign in and save a profile.",
  args: [{ name: "[server]" }],
  options: [
    new Option("--method <method>", "Sign-in method.").choices(METHODS),
    new Option("--username <user>", "Username."),
    new Option("--password-stdin", "Read the password from stdin."),
    new Option("--password-env <var>", "Read the password from this env var."),
    new Option("--psk-stdin", "Read the pre-shared key from stdin."),
    new Option("--psk-env <var>", "Read the pre-shared key from this env var."),
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
    new Option("--ca-cert <file>", "Trust this CA certificate for the server."),
    new Option("--timeout <seconds>", "How long to wait for SSO sign-in.")
      .argParser(seconds)
      .default(300),
    new Option("--operate-as <user>", "Enterprise user to operate as."),
    new Option("--no-browser", "Print the SSO URL without opening a browser."),
    new Option(
      "--expect-user <user>",
      "Fail unless SSO signs in as this user.",
    ),
    new Option("--default", "Make this the default profile."),
    new Option("--no-default", "Don't make this the default profile."),
    new Option("-y, --yes", "Migrate a legacy config without asking."),
  ],

  async run(ctx, [serverArg], opts) {
    await ctx.store.ensureDir();
    let state = await ctx.store.read();
    if (!serverArg && isFirstRun(state)) {
      const migrated = await migrateLegacy(ctx, { yes: opts.yes }, true);
      if (migrated) return migratedOutput(migrated);
      state = await ctx.store.read();
    }

    // An explicit server outranks DH_PROFILE, but not --profile.
    const profile = ctx.profileFlag ?? (serverArg ? undefined : ctx.profile);
    const where = await askWhere(ctx.ui, state, serverArg, profile, opts);
    if (where === "remove") return await logout.run(ctx, [], {});
    const known = knownIdentity(state, where.profile);
    const { origin, kind } = await findServer(
      where.server,
      state.config.servers,
      opts.caCert,
      where.rerun,
    );

    const server = await connect(origin, kind, ctx.store.cacheDir);
    try {
      const offered = await offers(server);
      const offer = await askMethod(ctx.ui, kind, offered, known, opts);
      const { how, save } = await askCredentials(ctx.ui, offer, known, opts);
      const session = await signIn(server, how);
      try {
        note(
          how.method === "anonymous"
            ? "✔ Signed in anonymously"
            : `✔ Signed in as ${session.user}`,
        );
        await confirmAccount(ctx.ui, session, offer, opts);
        const { config } = await ctx.store.read();
        const makeDefault = await askDefault(
          ctx.ui,
          config,
          session,
          opts.default,
        );
        const caCert = opts.caCert ? resolve(opts.caCert) : undefined;
        const saved = await commitSignIn(
          ctx.store,
          session,
          { ...save, caCert },
          makeDefault,
        );
        if (saved.key) note(`✔ Authorized this computer (key "${saved.key}")`);
        for (const warning of saved.warnings) note(warning);

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
          },
        };
      } finally {
        disconnect(session);
      }
    } finally {
      disconnect(server);
    }
  },
});

function seconds(value: string): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new InvalidArgumentError("Not a positive number.");
  }
  return n;
}
