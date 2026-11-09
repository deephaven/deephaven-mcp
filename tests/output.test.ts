import { expect, test } from "bun:test";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { RUNTIME_FLAGS } from "../scripts/build.ts";

const outputUrl = pathToFileURL(
  resolve(import.meta.dir, "../src/output.ts"),
).href;
const errorsUrl = pathToFileURL(
  resolve(import.meta.dir, "../src/errors.ts"),
).href;

async function runOutput(format: string, operation: string) {
  const script = `
    import { result, reportError, setFormat } from ${JSON.stringify(outputUrl)};
    import { DhError, cleanupError } from ${JSON.stringify(errorsUrl)};
    import { authActionReport, profileSaveReport, profileRemovalReport, profileMigrationReport } from ${JSON.stringify(new URL("../src/actions/auth/reporting.ts", import.meta.url).href)};
    setFormat(${JSON.stringify(format)});
    ${operation}
  `;
  const proc = Bun.spawn([process.execPath, ...RUNTIME_FLAGS, "-e", script], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, code };
}

test("empty success emits nothing in human or JSON mode", async () => {
  for (const format of ["human", "json"]) {
    const output = await runOutput(
      format,
      `result(() => { throw new Error("evaluated empty output"); }, null, true);`,
    );
    expect(output).toEqual({ stdout: "", stderr: "", code: 0 });
  }
});

test("JSON failure reports keep detail and recovery argv separate", async () => {
  const output = await runOutput(
    "json",
    `reportError(
      new DhError("cancelled", "Interrupted", undefined, "Received SIGINT"),
      {
        command: "console create",
        target: "id with ; $'x",
        recoveryCommand: ["dh", "console", "detail", "id with ; $'x"],
      },
    );`,
  );
  expect(output.code).toBe(0);
  expect(output.stdout).toBe("");
  expect(JSON.parse(output.stderr)).toEqual({
    type: "error",
    error: "Interrupted",
    code: "cancelled",
    exit: 3,
    command: "console create",
    detail: "Received SIGINT",
    target: "id with ; $'x",
    recoveryCommand: ["dh", "console", "detail", "id with ; $'x"],
  });
});

test("human failure quotes recovery arguments as one shell argument", async () => {
  const output = await runOutput(
    "human",
    `reportError(
      new DhError("cancelled", "Interrupted"),
      { recoveryCommand: ["dh", "console", "detail", "id with ; $'x"] },
    );`,
  );
  expect(output.stderr).toContain(
    process.platform === "win32"
      ? "Follow up: dh console detail 'id with ; $''x'"
      : "Follow up: dh console detail 'id with ; $'\\''x'",
  );
});

test("routine human cancellation prints only Cancelled and exits 3", async () => {
  for (const headline of ["Cancelled", "Interrupted"]) {
    const output = await runOutput(
      "human",
      `process.exitCode = reportError(
        new DhError("cancelled", ${JSON.stringify(headline)}, undefined, "Interrupted by SIGINT"),
        { command: "auth login", target: "localhost", profile: "prod", outcome: "not_started" },
      );`,
    );
    expect(output).toEqual({ stdout: "", stderr: "✖ Cancelled.\n", code: 3 });
  }
});

test("consequential human cancellation preserves operation and recovery context", async () => {
  const output = await runOutput(
    "human",
    `process.exitCode = reportError(
      new DhError("cancelled", "Interrupted while starting console", "Check whether it started", "Server accepted the create"),
      {
        command: "console create", target: "abc123", phase: "starting",
        outcome: "unknown", elapsedSeconds: 10, observation: null,
        human: { summary: "Interrupted while starting console", details: ["Result unknown."] },
      },
    );`,
  );
  expect(output.code).toBe(3);
  expect(output.stderr).toContain("Interrupted while starting console");
  expect(output.stderr).toContain("Server accepted the create");
  expect(output.stderr).toContain("Check whether it started");
  expect(output.stderr).toContain("Result unknown.");
  expect(output.stderr).not.toContain("Last observed: none");
  expect(output.stderr).not.toContain("Command:");
  expect(output.stderr).not.toContain("Phase:");
  expect(output.stderr).not.toContain("Type: error");
  expect(output.stderr).not.toContain("Code: cancelled");
  expect(output.stderr).not.toContain("Exit: 3");
});

test("external waits include context and show elapsed time only from five seconds", async () => {
  for (const elapsedSeconds of [0, 4, 5, 95]) {
    const output = await runOutput(
      "human",
      `process.exitCode = reportError(
        new DhError("cancelled", "Interrupted", undefined, "Interrupted by SIGINT"),
        {
          ...authActionReport("waiting for SSO sign-in", "https://example.com"),
          command: "auth login",
          target: "https://example.com", profile: "prod",
          phase: "signing_in", elapsedSeconds: ${elapsedSeconds},
        },
      );`,
    );
    expect(output.code).toBe(3);
    expect(output.stdout).toBe("");
    const timing = elapsedSeconds >= 5 ? `  Waited ${elapsedSeconds}s.\n` : "";
    expect(output.stderr).toBe(
      `✖ Cancelled while waiting for SSO sign-in at https://example.com.\n${timing}`,
    );
  }
});

test("operation facts and actionable guidance render consistently in human and JSON reports", async () => {
  const report = {
    command: "console create",
    operation: "console startup",
    target: "abc123",
    profile: "prod",
    phase: "starting",
    elapsedSeconds: 95,
    progress: "Request accepted.",
    outcome: "unknown",
    continuation: "continues",
    observation: {
      state: "Initializing",
      observedAt: "2026-01-01T12:01:30Z",
      ageSeconds: 5,
    },
    retryWarning: "Check the existing operation before retrying.",
    recoveryCommand: ["dh", "console", "detail", "abc123"],
  };
  for (const format of ["human", "json"]) {
    const output = await runOutput(
      format,
      `const report = ${JSON.stringify(report)};
      report.human = {
        summary: "Cancelled while waiting for console " + report.target + " (profile " + report.profile + ") to start.",
        details: [
          "Last observed: " + report.observation.state + ", " + report.observation.ageSeconds + "s ago.",
          "Startup continues on the server; readiness is not yet confirmed.",
          report.retryWarning,
        ],
        recoveryLabel: "Check status",
      };
      process.exitCode = reportError(
        cleanupError([new Error("Close failed")], new DhError("cancelled", "Interrupted", undefined, "Interrupted by SIGTERM")),
        report,
      );`,
    );
    expect(output.code).toBe(3);
    expect(output.stdout).toBe("");
    if (format === "json") {
      expect(JSON.parse(output.stderr)).toEqual({
        type: "error",
        error: "Interrupted",
        code: "cancelled",
        exit: 3,
        detail: "Interrupted by SIGTERM",
        cleanupDetail: "Close failed",
        ...report,
      });
    } else {
      expect(output.stderr).toBe(
        [
          "✖ Cancelled while waiting for console abc123 (profile prod) to start.",
          "  Last observed: Initializing, 5s ago.",
          "  Startup continues on the server; readiness is not yet confirmed.",
          "  Check the existing operation before retrying.",
          "  Waited 95s.",
          "  Check status: dh console detail abc123",
          "  Cleanup failed: Close failed",
          "",
        ].join("\n"),
      );
    }
  }
});

test("remote continuation is explicit rather than inferred from cancellation", async () => {
  for (const [continuation, message] of [
    ["stopped", "Remote work has stopped."],
    ["unknown", "Whether remote work continues is unknown."],
  ]) {
    const output = await runOutput(
      "human",
      `reportError(new DhError("cancelled", "Interrupted"), {
        continuation: ${JSON.stringify(continuation)},
        human: { summary: "Cancelled.", details: [${JSON.stringify(message)}] },
      });`,
    );
    expect(output.stderr).toBe(`✖ Cancelled.\n  ${message}\n`);
  }
});

test("operation summaries do not get a second metadata-based description", async () => {
  const output = await runOutput(
    "human",
    `reportError(new DhError("cancelled", "Interrupted"), {
      human: { summary: "Cancelled while waiting for console abc123 to start." },
      operation: "console startup", target: "abc123", profile: "prod",
      phase: "starting", elapsedSeconds: 95, outcome: "unknown", continuation: "continues",
    });`,
  );
  expect(output.stderr).toBe(
    "✖ Cancelled while waiting for console abc123 to start.\n  Waited 95s.\n",
  );
});

test("failure reasons and guidance remain separate from cleanup diagnostics", async () => {
  const output = await runOutput(
    "human",
    `process.exitCode = reportError(
      cleanupError([new Error("Close failed")],
        new DhError("server_unreachable", "Connection failed", "Check your network.", "Connection refused.")),
      { operation: "connection", target: "https://example.com" },
    );`,
  );
  expect(output.code).toBe(5);
  expect(output.stderr).toBe(
    "✖ Connection failed\n  Connection refused.\n  Check your network.\n  Cleanup failed: Close failed\n",
  );
});

test("auth reports select operation-specific context without repeating metadata", async () => {
  for (const [factory, action, explanation] of [
    [
      "profileSaveReport",
      "saving the profile",
      "Sign-in completed. The profile may have been saved; key cleanup may be incomplete.\n  Check saved profiles before retrying.",
    ],
    [
      "profileRemovalReport",
      "removing profiles",
      "Profile removal or key revocation may be incomplete.\n  Check remaining profiles before retrying.",
    ],
    [
      "profileMigrationReport",
      "migrating profiles",
      "Some profiles may have been saved. Check saved profiles before retrying migration.",
    ],
  ]) {
    const output = await runOutput(
      "human",
      `process.exitCode = reportError(new DhError("cancelled", "Interrupted"), ${factory}(""));`,
    );
    expect(output.stderr).toBe(
      `✖ Cancelled while ${action}.\n  ${explanation}\n  Check profiles: dh auth\n`,
    );
    expect(output.code).toBe(3);
  }
});

test("operation presentation does not replace real failure reasons or recovery quoting", async () => {
  const output = await runOutput(
    "human",
    `process.exitCode = reportError(
      new DhError("auth_failed", "Credentials rejected", "Check credentials.", "Invalid password."),
      authActionReport("signing in", "https://example.com"),
    );`,
  );
  expect(output.stderr).toBe(
    "✖ Credentials rejected\n  While signing in at https://example.com.\n  Invalid password.\n  Check credentials.\n",
  );
  expect(output.code).toBe(4);
});

test("routine cancellation does not hide cleanup failures", async () => {
  const output = await runOutput(
    "human",
    `process.exitCode = reportError(cleanupError(
      [new Error("Could not close connection")],
      new DhError("cancelled", "Interrupted", undefined, "Interrupted by SIGTERM"),
    ), { command: "auth login" });`,
  );
  expect(output).toEqual({
    code: 3,
    stdout: "",
    stderr: "✖ Cancelled.\n  Cleanup failed: Could not close connection\n",
  });
});
