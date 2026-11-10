import type { FailureReport } from "../../errors.ts";
import { BIN } from "../../version.ts";

/** Explain an unavailable worker state without inferring a server-side cause. */
export function consoleStateDetails(state: string): string[] {
  if (state !== "Unknown") return [];
  return [
    "No worker state is available. Startup may be pending; the reason is not confirmed.",
  ];
}

/** Describe interruption of a console operation before its target is resolved. */
export function consoleActionReport(
  action: string,
  target?: string,
  profile?: string,
): FailureReport {
  const where = target ? ` console ${target}` : " consoles";
  const identity = profile ? ` (profile ${profile})` : "";
  return {
    command: `console ${action}`,
    target,
    profile,
    operation: `console ${action}`,
    human: {
      summary: `Interrupted while ${action}${where}${identity}.`,
      context: `While ${action}${where}${identity}.`,
    },
  };
}

/** Establish the reporting context for a create action. */
export function consoleStartupReport(): FailureReport {
  return {
    command: "console create",
    operation: "console startup",
    phase: "starting",
    outcome: "unknown",
    human: {
      summary: "Interrupted while waiting for the console to start.",
      recoveryLabel: "Check whether it started",
    },
  };
}

/** Attach a reserved shortcode and recovery command before submitting creation. */
export function consoleCreateRequestReport(
  shortcode: string,
  profileName: string | undefined,
): FailureReport {
  const profile = profileName ?? "environment";
  return {
    target: shortcode,
    profile: profileName,
    phase: "creating",
    outcome: "unknown",
    continuation: "unknown",
    recoveryCommand: consoleRecoveryCommand(shortcode, profileName),
    human: {
      summary: `Interrupted while waiting for console ${shortcode} (profile ${profile}) to start.`,
      details: [
        "The create result is not confirmed. Check this console before retrying.",
      ],
      recoveryLabel: "Check whether it started",
    },
  };
}

/** Record server acceptance without claiming the console has finished starting. */
export function consoleStartupAcceptedReport(
  shortcode: string,
  profileName?: string,
): FailureReport {
  return {
    target: shortcode,
    phase: "starting",
    outcome: "unknown",
    continuation: "continues",
    progress:
      "The server accepted the create request; startup is not confirmed.",
    human: {
      summary: `Interrupted while waiting for console ${shortcode} (profile ${profileName ?? "environment"}) to start.`,
      details: [
        "The server accepted the request. The console may still be starting; exiting dh does not cancel it.",
      ],
      recoveryLabel: "Check whether it started",
    },
  };
}

/** Attach the resolved delete target before submitting its request. */
export function consoleDeleteRequestReport(
  shortcode: string,
  profile: string | undefined,
): FailureReport {
  return {
    target: shortcode,
    profile,
    phase: "deleting",
    outcome: "unknown",
    continuation: "unknown",
    recoveryCommand: consoleRecoveryCommand(shortcode, profile),
  };
}

/** Keep recovery on the saved profile even when the target is not yet cached. */
function consoleRecoveryCommand(shortcode: string, profile?: string): string[] {
  const command = [BIN, "console", "detail", shortcode];
  if (profile) command.push("--profile", profile);
  return command;
}

/** Record server acceptance without claiming deletion has completed. */
export function consoleDeleteAcceptedReport(
  human?: FailureReport["human"],
): FailureReport {
  return {
    outcome: "unknown",
    continuation: "continues",
    progress:
      "The server accepted the delete request; deletion is not confirmed.",
    human: {
      ...human,
      summary:
        human?.summary ?? "Interrupted while waiting for console deletion.",
      details: [
        "The server accepted the delete request; deletion may still complete after dh exits.",
      ],
      recoveryLabel: "Check whether it was deleted",
    },
  };
}

/** Record a fresh state observation while waiting for startup or deletion. */
export function consoleObservationReport(
  phase: "starting" | "deleting",
  state: string,
  human?: FailureReport["human"],
): FailureReport {
  return {
    phase,
    outcome: "unknown",
    continuation: "continues",
    progress:
      state === "Unknown" ? consoleStateDetails(state).join(" ") : undefined,
    observation: {
      state,
      observedAt: new Date().toISOString(),
      ageSeconds: 0,
    },
    human: {
      ...human,
      summary: human?.summary ?? `Interrupted while ${phase} a console.`,
      details: [
        phase === "deleting"
          ? "The server accepted the delete request; deletion may still complete after dh exits."
          : "The server accepted the request. The console may still be starting; exiting dh does not cancel it.",
        ...consoleStateDetails(state),
      ],
    },
  };
}

/** Record a terminal startup state without implying the CLI stopped the worker. */
export function consoleStartupFailedReport(state: string): FailureReport {
  return {
    ...consoleObservationReport("starting", state),
    outcome: "failed",
    continuation: "stopped",
    progress: `Console startup ended with state: ${state}.`,
    human: {
      summary: `Console startup ended with state: ${state}.`,
      details: [`Console startup ended with state: ${state}.`],
      recoveryLabel: "Inspect the console",
    },
  };
}

/** Record confirmed startup, retaining the most recent state observation. */
export function consoleReadyReport(): FailureReport {
  return {
    outcome: "succeeded",
    continuation: "stopped",
    progress: "The console is ready.",
  };
}

/** Record confirmed disappearance of a deleted console. */
export function consoleDeletedReport(): FailureReport {
  return {
    phase: "deleting",
    outcome: "succeeded",
    continuation: "stopped",
    progress: "The console was deleted.",
  };
}
