import { type Command, InvalidArgumentError, Option } from "commander";
import { login, METHODS } from "./login.ts";
import { logout } from "./logout.ts";
import { list, rename, status, use } from "./manage.ts";
import { migrateCommand } from "./migrate.ts";

function seconds(value: string): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new InvalidArgumentError("Not a positive number.");
  }
  return n;
}

/** Adds `auth` and its subcommands; `command()` makes them inherit the root's settings. */
export function addAuthCommand(root: Command): void {
  const auth = root
    .command("auth")
    .description("Sign in to Deephaven servers and manage profiles.")
    .action(() => list());

  auth
    .command("login")
    .description("Sign in and save a profile.")
    .argument("[server]")
    .addOption(
      new Option("--method <method>", "Sign-in method.").choices(METHODS),
    )
    .option("--username <user>", "Username.")
    .option("--password-stdin", "Read the password from stdin.")
    .option("--password-env <var>", "Read the password from this env var.")
    .option("--psk-stdin", "Read the pre-shared key from stdin.")
    .option("--psk-env <var>", "Read the pre-shared key from this env var.")
    .option("--handler <class>", "Community custom auth handler class.")
    .option("--token-stdin", "Read the custom auth token from stdin.")
    .option(
      "--token-env <var>",
      "Read the custom auth token from this env var.",
    )
    .option("--private-key-file <path>", "Use an existing Enterprise key file.")
    .option("--copy-key", "Save the key itself instead of its path.")
    .option("--ca-cert <file>", "Trust this CA certificate for the server.")
    .option(
      "--timeout <seconds>",
      "How long to wait for SSO sign-in.",
      seconds,
      300,
    )
    .option("--operate-as <user>", "Enterprise user to operate as.")
    .option("--no-browser", "Print the SSO URL without opening a browser.")
    .option("--expect-user <user>", "Fail unless SSO signs in as this user.")
    .option("--default", "Make this the default profile.")
    .option("--no-default", "Don't make this the default profile.")
    .option("-y, --yes", "Migrate a legacy config without asking.")
    .action((server, _options, cmd: Command) =>
      login(cmd.optsWithGlobals(), server),
    );

  auth
    .command("use")
    .description("Set the default profile.")
    .argument(
      "[profile]",
      "Profile name, or :<user> for that user on the default's server.",
    )
    .action((profile, _options, cmd: Command) =>
      use(cmd.optsWithGlobals(), profile),
    );

  auth
    .command("rename")
    .description("Rename a profile.")
    .argument("<profile>")
    .argument("<name>")
    .action((profile, name) => rename(profile, name));

  auth
    .command("logout")
    .description("Revoke this computer's key and remove a profile.")
    .argument("[profile]")
    .option("--all", "Remove every profile.")
    .action((profile, _options, cmd: Command) =>
      logout(cmd.optsWithGlobals(), profile),
    );

  auth
    .command("status")
    .description("Check the profile against its server.")
    .action((_options, cmd: Command) => status(cmd.optsWithGlobals()));

  auth
    .command("migrate")
    .description("Migrate profiles from a legacy dhcli / Deephaven MCP config.")
    .option("--from <path>", "Legacy config file or directory.")
    .option("-y, --yes", "Migrate everything without asking.")
    .action((_options, cmd: Command) => migrateCommand(cmd.optsWithGlobals()));
}
