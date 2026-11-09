import { resolve } from "node:path";
import { Option } from "commander";
import {
  commitSignInAction,
  type SavedSignIn,
} from "../../actions/auth/flows.ts";
import { trustAndResolveServerAction } from "../../actions/auth/session.ts";
import {
  connectServerAction,
  disconnectServer,
  offeredMethodsAction,
  type Session,
  signInAction,
} from "../../actions/auth/signin.ts";
import type { Method } from "../../actions/auth/store.ts";
import { defineCommand, type Output } from "../../cli/command.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";
import { logout } from "./logout.ts";
import {
  migratedOutput,
  migrateLegacy,
  shouldOfferMigration,
} from "./migrate.ts";
import {
  askLoginTarget,
  askMakeDefault,
  askSignInCredentials,
  askSignInMethod,
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
    const prior = priorSignIn(state, target.profile);
    const { origin, kind } = await trustAndResolveServerAction(
      target.server,
      state.config.servers,
      opts.caCert,
      target.rerun,
      ctx.lifecycle,
    );

    const server = await connectServerAction(
      origin,
      kind,
      ctx.store.cacheDir,
      ctx.lifecycle,
    );
    try {
      ctx.lifecycle.onCleanup(() => disconnectServer(server));
      // Sign in, then confirm the account before anything is saved.
      const methods = await offeredMethodsAction(server, ctx.lifecycle);
      const method = await askSignInMethod(ctx.ui, kind, methods, prior, opts);
      const { request, toSave } = await askSignInCredentials(
        ctx.ui,
        method,
        prior,
        opts,
      );
      const session = await signInAction(
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
        const makeDefault = await askMakeDefault(
          ctx.ui,
          config,
          session,
          opts.default,
        );
        const caCert = opts.caCert ? resolve(opts.caCert) : undefined;
        const saved = await commitSignInAction(
          ctx.store,
          session,
          { ...toSave, caCert },
          makeDefault,
          ctx.lifecycle,
        );
        if (saved.key) note(`✔ Authorized this computer (key "${saved.key}")`);
        for (const warning of saved.warnings) note(warning);
        return loggedInOutput(saved, session);
      } finally {
        disconnectServer(session);
      }
    } finally {
      disconnectServer(server);
    }
  },
});

function loggedInOutput(saved: SavedSignIn, session: Session): Output {
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
}
