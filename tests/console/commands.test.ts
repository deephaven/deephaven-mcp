import { expect, spyOn, test } from "bun:test";
import { Command } from "commander";
import * as auth from "../../src/actions/auth/session.ts";
import { GLOBAL_OPTIONS, registerCommand } from "../../src/cli/command.ts";
import { CommandLifecycle } from "../../src/cli/lifecycle.ts";
import { rootCommand } from "../../src/commands/root.ts";
import { tempDir } from "../helpers.ts";

const COMMANDS = [
  ["console"],
  ["console", "detail"],
  ["console", "use", "abc123"],
  ["console", "create"],
  ["console", "default"],
  ["console", "delete"],
];

// DH_PROFILE is resolved by authentication's own precedence; commands must
// forward only an explicit --profile so cached console targets can win.
test.each(
  COMMANDS.flatMap(
    (argv) =>
      [
        [argv.join(" "), argv, undefined],
        [
          `${argv.join(" ")} --profile flagged`,
          [...argv, "--profile", "flagged"],
          "flagged",
        ],
      ] as const,
  ),
)(
  "%s authenticates with the --profile flag, not DH_PROFILE",
  async (_label, argv, expected) => {
    const { DH_PROFILE, DH_CONFIG_DIR } = process.env;
    process.env.DH_PROFILE = "from-env";
    process.env.DH_CONFIG_DIR = await tempDir("dh-console-commands-");
    const stop = new Error("stop at authentication");
    const requireAuth = spyOn(auth, "requireAuth").mockRejectedValue(stop);
    try {
      const root = new Command("dh");
      for (const option of GLOBAL_OPTIONS) root.addOption(option);
      registerCommand(root, rootCommand(), new CommandLifecycle());
      await expect(root.parseAsync(argv, { from: "user" })).rejects.toBe(stop);
      expect(requireAuth).toHaveBeenCalledTimes(1);
      expect(requireAuth.mock.calls[0]?.[0]).toBe(expected);
    } finally {
      requireAuth.mockRestore();
      restoreEnv("DH_PROFILE", DH_PROFILE);
      restoreEnv("DH_CONFIG_DIR", DH_CONFIG_DIR);
    }
  },
);

function restoreEnv(name: string, value: string | undefined) {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}
