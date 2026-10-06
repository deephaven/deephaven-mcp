import { Command, EnumType } from "@cliffy/command";
import { importCommand } from "./import.ts";
import { login, METHODS } from "./login.ts";
import { logout } from "./logout.ts";
import { list, rename, status, use } from "./manage.ts";

export function authCommand() {
  const loginCommand = new Command()
    .description("Sign in and save a profile.")
    .type("method", new EnumType(METHODS))
    .arguments("[server:string]")
    .option("--method <method:method>", "Sign-in method.")
    .option("--username <user:string>", "Username.")
    .option("--password-stdin", "Read the password from stdin.")
    .option(
      "--password-env <var:string>",
      "Read the password from this env var.",
    )
    .option("--psk-stdin", "Read the pre-shared key from stdin.")
    .option(
      "--psk-env <var:string>",
      "Read the pre-shared key from this env var.",
    )
    .option("--handler <class:string>", "Community custom auth handler class.")
    .option("--token-stdin", "Read the custom auth token from stdin.")
    .option(
      "--token-env <var:string>",
      "Read the custom auth token from this env var.",
    )
    .option(
      "--private-key-file <path:string>",
      "Use an existing Enterprise key file.",
    )
    .option("--copy-key", "Save the key itself instead of its path.")
    .option(
      "--ca-cert <file:string>",
      "Trust this CA certificate for the server.",
    )
    .option("--timeout <seconds:number>", "How long to wait for SSO sign-in.", {
      default: 300,
    })
    .option("--operate-as <user:string>", "Enterprise user to operate as.")
    .option("--no-browser", "Print the SSO URL without opening a browser.")
    .option(
      "--expect-user <user:string>",
      "Fail unless SSO signs in as this user.",
    )
    .option("--default", "Make this the default profile.")
    .option("--no-default", "Don't make this the default profile.")
    .option("-y, --yes", "Accept confirmation prompts.")
    .action((options, server) => login(options, server));

  return new Command()
    .description("Sign in to Deephaven servers and manage profiles.")
    .action(() => list())
    .command("login", loginCommand)
    .command("use", "Set the default profile.")
    .arguments("[profile:string]")
    .action((options, profile) => use(options, profile))
    .command("rename", "Rename a profile.")
    .arguments("<profile:string> <name:string>")
    .action((_options, profile, name) => rename(profile, name))
    .command("logout", "Revoke this computer's key and remove a profile.")
    .arguments("[profile:string]")
    .option("--all", "Remove every profile.")
    .action((options, profile) => logout(options, profile))
    .command("status", "Check the profile against its server.")
    .action((options) => status(options))
    .command("import", "Import a legacy dhcli / Deephaven MCP config.")
    .option("--from <path:string>", "Legacy config file or directory.")
    .option("-y, --yes", "Import everything without asking.")
    .action((options) => importCommand(options))
    .reset();
}
