import { describe, expect, test } from "bun:test";
import { isUsageFor, parseHelp } from "../src/help.ts";
import { readToolHelp, scaffoldPlugin } from "../src/scaffold.ts";

describe("parseHelp", () => {
  // Real picocli output (a Deephaven node tool), trimmed; continuation lines are
  // indented two columns past the description.
  const PICOCLI = [
    "find: Failed to restore initial working directory: /home/x: Permission denied",
    "Usage: deephaven [-hV] <command>",
    "Control a Deephaven node: its lifecycle, its cluster membership, and its",
    "material.",
    "  -h, --help      Show this help message and exit.",
    "  -V, --version   Print version information and exit.",
    "Commands:",
    "  start      Start the node, or one service, and wait until it is ready",
    "  tls        Package the TLS keystore and truststore, and write the",
    "               config-client bootstrap",
    "  node       Create, list or revoke a node's etcd identities and credentials",
    "               file",
    "  coreplus   Install the Core+ worker distribution (install --file, list,",
    "               set-default)",
    "",
    "With no service, the lifecycle verbs apply to every service on this node except",
    "etcd.",
  ].join("\n");

  test("joins picocli's wrapped command descriptions, indented past their column", () => {
    const help = parseHelp(PICOCLI, ["deephaven"]);
    expect(help.subcommands).toEqual(["start", "tls", "node", "coreplus"]);
    expect(help.descriptions).toEqual({
      start: "Start the node, or one service, and wait until it is ready",
      tls: "Package the TLS keystore and truststore, and write the config-client bootstrap",
      node: "Create, list or revoke a node's etcd identities and credentials file",
      coreplus:
        "Install the Core+ worker distribution (install --file, list, set-default)",
    });
    expect(help.description).toBe(
      "Control a Deephaven node: its lifecycle, its cluster membership, and its material.",
    );
  });

  test("the plugin's own description comes from the top-level help when it has one", async () => {
    const runner = {
      run: async () => PICOCLI,
      host: "h",
    };
    const top = await readToolHelp("/opt/deephaven/bin/deephaven", runner);
    const scaffold = await scaffoldPlugin(top, "andy", false);
    expect(scaffold.spec.description).toBe(
      "Control a Deephaven node: its lifecycle, its cluster membership, and its material.",
    );
  });

  // Real dhctl output.
  test("reads subcommands from the top-level usage line", () => {
    const text =
      "Usage: dhctl [intraday|metadata|logger] [truncate|delete|rescan|help|list|validate|update|generate] [arguments]";
    expect(parseHelp(text, ["dhctl"])).toMatchObject({
      subcommands: ["intraday", "metadata", "logger"],
      description: undefined,
    });
  });

  test("reads a group's subcommands, skipping help and instructions", () => {
    const text = [
      "Usage: dhctl intraday [truncate|delete|rescan|help] [arguments]",
      "For action specific help, specify an action and --help.",
    ].join("\n");
    expect(parseHelp(text, ["dhctl", "intraday"])).toMatchObject({
      subcommands: ["truncate", "delete", "rescan"],
      hasHelpCommand: true,
    });
  });

  test("finds the usage line after an error prefix", () => {
    const text = [
      "Action failed: Usage: dhctl metadata [list|validate|update] [arguments]",
      "For action specific help, specify an action and --help.",
      "",
      "Error: action '--help' is not valid for metadata",
    ].join("\n");
    expect(parseHelp(text, ["dhctl", "metadata"]).subcommands).toEqual([
      "list",
      "validate",
      "update",
    ]);
  });

  test("a Commons CLI leaf has no subcommands, and its prose is the description", () => {
    const text = [
      "usage: dhctl metadata list [-e <arg>] [-f <arg>] [-h] -t <arg>  [-v]",
      "Lists all of the metadata snapshots in the specified namespaces and tables.",
      "",
      "Options are as follows:",
      " -e,--partition-filter <arg>   A filter expression.",
    ].join("\n");
    expect(parseHelp(text, ["dhctl", "metadata", "list"])).toMatchObject({
      subcommands: [],
      description:
        "Lists all of the metadata snapshots in the specified namespaces and tables.",
    });
    const rescan =
      "usage: dhctl intraday rescan [-e <arg>] [-h]\n -e,--exclude-dis <arg>   do not send requests";
    const parsed = parseHelp(rescan, ["dhctl", "intraday", "rescan"]);
    expect(parsed.subcommands).toEqual([]);
    expect(parsed.description).toBeUndefined();
  });

  test("a description wrapped over lines is read to the end of the sentence", () => {
    const text = [
      "usage: dhctl metadata validate [-e <arg>] [-h] -t <arg>  [-v]",
      "Validates that each metadata snapshot present for the specified namespaces and tables",
      "matches the actual state present at each location and generates a report.",
      "",
      "Options are as follows:",
    ].join("\n");
    expect(parseHelp(text, ["dhctl", "metadata", "validate"]).description).toBe(
      "Validates that each metadata snapshot present for the specified namespaces and tables matches the actual state present at each location and generates a report.",
    );
  });

  test("a one-item list is a candidate, unless it's a placeholder", () => {
    const logger =
      "Action failed: Usage: dhctl logger [generate] [arguments]\nError: action '--help' is not valid for logger";
    expect(parseHelp(logger, ["dhctl", "logger"])).toMatchObject({
      subcommands: [],
      candidate: "generate",
    });
    expect(
      parseHelp("Usage: dhctl [arguments]", ["dhctl"]).candidate,
    ).toBeUndefined();
    expect(
      isUsageFor("usage: dhctl logger generate [-h]", [
        "dhctl",
        "logger",
        "generate",
      ]),
    ).toBe(true);
    expect(isUsageFor(logger, ["dhctl", "logger", "generate"])).toBe(false);
  });

  test("a 'Supported subcommands' line lists subcommands (dhconfig)", () => {
    // Real `dhconfig acls users --help`, shortened.
    const text = [
      "usage: dhconfig acls users [--configfile <arg>] [--diskprops] [--etcd] [-h] [-k <arg> | -user <arg>] [-pf <arg>]  [-v]",
      "Supported subcommands [add|delete|list|remove-password|remove-runas|set-password|set-runas]",
      "    --configfile <arg>   use the named properties file instead of the default",
      " -h,--help               print help for a acls command",
    ].join("\n");
    expect(parseHelp(text, ["dhconfig", "acls", "users"]).subcommands).toEqual([
      "add",
      "delete",
      "list",
      "remove-password",
      "remove-runas",
      "set-password",
      "set-runas",
    ]);
  });

  test("argparse-style {a,b} lists, and a tool named by its full path", () => {
    const text = "usage: /opt/bin/tool {add,remove} ...";
    expect(parseHelp(text, ["tool"]).subcommands).toEqual(["add", "remove"]);
  });

  test("argparse: the list after options, descriptions from positional arguments", () => {
    // Real Python 3 argparse output.
    const text = [
      "usage: argtool [-h] {db,serve} ...",
      "",
      "Demo tool.",
      "",
      "positional arguments:",
      "  {db,serve}",
      "    db        Database things.",
      "    serve     Run the server.",
      "",
      "options:",
      "  -h, --help  show this help message and exit",
    ].join("\n");
    expect(parseHelp(text, ["argtool"])).toMatchObject({
      subcommands: ["db", "serve"],
      descriptions: { db: "Database things.", serve: "Run the server." },
      description: "Demo tool.",
    });
  });

  test("commander: a Commands section with arguments and wrapped descriptions", () => {
    // Real `dh auth --help`, shortened.
    const text = [
      "Usage: dh auth [options] [command]",
      "Version: 3.0.0",
      "",
      "Sign in to Deephaven servers and manage profiles.",
      "Commands:",
      "  login [options] [server]    Sign in and save a profile.",
      "  rename <profile> <name>     Rename a profile.",
      "  migrate [options]           Migrate profiles from a legacy dhcli / Deephaven",
      "                              MCP config.",
      "  help [command]              display help for command",
    ].join("\n");
    expect(parseHelp(text, ["dh", "auth"])).toMatchObject({
      subcommands: ["login", "rename", "migrate"],
      hasHelpCommand: true,
      descriptions: {
        login: "Sign in and save a profile.",
        migrate: "Migrate profiles from a legacy dhcli / Deephaven MCP config.",
      },
      description: "Sign in to Deephaven servers and manage profiles.",
    });
  });

  test("Cobra: usage on the line after Usage:, and an Available Commands section", () => {
    const text = [
      "App manages things.",
      "",
      "Usage:",
      "  app [command]",
      "",
      "Available Commands:",
      "  completion  Generate the autocompletion script for the specified shell",
      "  help        Help about any command",
      "  serve       Run the HTTP server",
      "",
      "Flags:",
      "  -h, --help   help for app",
    ].join("\n");
    expect(parseHelp(text, ["app"])).toMatchObject({
      subcommands: ["completion", "serve"],
      hasHelpCommand: true,
      descriptions: { serve: "Run the HTTP server" },
    });
    expect(isUsageFor("Usage:\n  app serve [flags]", ["app", "serve"])).toBe(
      true,
    );
  });

  test("Click and clap: placeholders on the usage line, then a Commands section", () => {
    const click = [
      "Usage: app [OPTIONS] COMMAND [ARGS]...",
      "",
      "  App manages things.",
      "",
      "Options:",
      "  --help  Show this message and exit.",
      "",
      "Commands:",
      "  db     Manage the database.",
      "  serve  Run the HTTP server.",
    ].join("\n");
    expect(parseHelp(click, ["app"]).subcommands).toEqual(["db", "serve"]);
    const clap = [
      "App manages things",
      "",
      "Usage: app <COMMAND>",
      "",
      "Commands:",
      "  build  Compile the project",
      "  help   Print this message or the help of the given subcommand(s)",
      "",
      "Options:",
      "  -h, --help  Print help",
    ].join("\n");
    expect(parseHelp(clap, ["app"])).toMatchObject({
      subcommands: ["build"],
      hasHelpCommand: true,
    });
  });

  test("an entry's description is its last column; examples aren't continuations", () => {
    // Real `bun --help` layout, shortened: name, example, description.
    const text = [
      "Usage: bun <command> [...flags] [...args]",
      "",
      "Commands:",
      "  run       ./my-script.ts       Execute a file with Bun",
      "            lint                 Run a package.json script",
      "  test                           Run unit tests with Bun",
    ].join("\n");
    expect(parseHelp(text, ["bun"])).toMatchObject({
      subcommands: ["run", "test"],
      descriptions: {
        run: "Execute a file with Bun",
        test: "Run unit tests with Bun",
      },
    });
  });

  test("Commons CLI options: spellings, values, wrapped descriptions, required", () => {
    // Real `dhctl metadata list --help`, shortened. `-pf`'s description wraps
    // onto a line starting `--user`, which isn't an option.
    const text = [
      "usage: dhctl metadata list [-e <arg>] [-h] [-k <arg> | -user <arg>] [-pf <arg>] -t <arg>  [-v]",
      "Lists all of the metadata snapshots in the specified namespaces and tables.",
      "",
      "Options are as follows:",
      " -e,--partition-filter <arg>   A filter expression to select which column partitions to validate.",
      "                               The Partition column is always `Partition`",
      " -h,--help                     print help for a metadata command",
      " -pf,--pwfile <arg>            specify a file containing the base64 encoded password for the user that is set with",
      "                               --user",
      " -t,--table-name <arg>         The table or tables to act upon.",
      " -user,--user <arg>            specify a user for authentication",
      " -v,--verbose                  print additional logging",
      "",
      "Examples:",
      "    dhctl metadata list --table-name '*' --file /tmp/LocationData.csv",
    ].join("\n");
    expect(parseHelp(text, ["dhctl", "metadata", "list"]).options).toEqual([
      {
        flags: "-e, --partition-filter <arg>",
        description:
          "A filter expression to select which column partitions to validate. The Partition column is always `Partition`",
      },
      {
        flags: "-pf, --pwfile <arg>",
        description:
          "specify a file containing the base64 encoded password for the user that is set with --user",
      },
      {
        flags: "-t, --table-name <arg>",
        description: "The table or tables to act upon.",
        required: true,
      },
      {
        flags: "-user, --user <arg>",
        description: "specify a user for authentication",
      },
      { flags: "-v, --verbose", description: "print additional logging" },
    ]);
  });

  test("a usage line wrapped onto the next line isn't read as options", () => {
    // Real `dhctl intraday truncate --help`, shortened.
    const text = [
      "usage: dhctl intraday truncate [-d] [-e <arg>] [-h] [-part <arg>] [-s",
      "       <arg>]  [-v]",
      " -d,--dry-run                 print what actions would be performed",
      " -s,--singlePartition <arg>   single partition to act on",
    ].join("\n");
    expect(
      parseHelp(text, ["dhctl", "intraday", "truncate"]).options.map(
        (o) => o.flags,
      ),
    ).toEqual(["-d, --dry-run", "-s, --singlePartition <arg>"]);
  });

  test("argparse, Cobra and Click option lines", () => {
    const argparse = [
      "usage: tool serve [-h] [--port PORT]",
      "",
      "options:",
      "  -h, --help   show this help message and exit",
      "  --port PORT  Port to listen on.",
    ].join("\n");
    expect(parseHelp(argparse, ["tool", "serve"]).options).toEqual([
      { flags: "--port <port>", description: "Port to listen on." },
    ]);
    const cobra = [
      "Usage:",
      "  app serve [flags]",
      "",
      "Flags:",
      "  -a, --addr string   Address to listen on",
      "  -h, --help          help for serve",
    ].join("\n");
    expect(parseHelp(cobra, ["app", "serve"]).options).toEqual([
      { flags: "-a, --addr <string>", description: "Address to listen on" },
    ]);
    const click = [
      "Usage: app serve [OPTIONS]",
      "",
      "Options:",
      "  -v, --verbose  Talk more.",
      "  --name TEXT    Who to greet.",
      "  --help         Show this message and exit.",
    ].join("\n");
    expect(parseHelp(click, ["app", "serve"]).options).toEqual([
      { flags: "-v, --verbose", description: "Talk more." },
      { flags: "--name <text>", description: "Who to greet." },
    ]);
  });

  test("prose mentioning usage isn't a usage line", () => {
    const text = "See the manual for usage details.\nUsage: tool [a|b]";
    expect(parseHelp(text, ["tool"]).subcommands).toEqual(["a", "b"]);
  });

  test("a usage line for another command, or none at all, has no subcommands", () => {
    expect(parseHelp("Usage: other [a|b]", ["dhctl"]).subcommands).toEqual([]);
    expect(parseHelp("no usage here", ["dhctl"]).subcommands).toEqual([]);
    expect(
      parseHelp("Usage: dhctl [arguments]", ["dhctl"]).subcommands,
    ).toEqual([]);
  });
});
