import type { FailureReport } from "../../errors.ts";
import { BIN } from "../../version.ts";

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
      details: ["The console keeps starting on the server after dh exits."],
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
    recoveryCommand: [BIN, "console", "detail", shortcode],
    human: {
      summary: `Interrupted while waiting for console ${shortcode} (profile ${profile}) to start.`,
      details: [
        "The server accepted the create request; startup is not confirmed.",
        "The console keeps starting on the server after dh exits.",
      ],
      recoveryLabel: "Check whether it started",
    },
  };
}

/** Record server acceptance without claiming the console has finished starting. */
export function consoleStartupAcceptedReport(shortcode: string): FailureReport {
  return {
    target: shortcode,
    phase: "starting",
    outcome: "unknown",
    continuation: "continues",
    progress:
      "The server accepted the create request; startup is not confirmed.",
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
    recoveryCommand: [BIN, "console", "detail", shortcode],
  };
}

/** Record server acceptance without claiming deletion has completed. */
export function consoleDeleteAcceptedReport(): FailureReport {
  return {
    outcome: "unknown",
    continuation: "continues",
    progress:
      "The server accepted the delete request; deletion is not confirmed.",
  };
}

/** Record a fresh state observation while waiting for startup or deletion. */
export function consoleObservationReport(
  phase: "starting" | "deleting",
  state: string,
): FailureReport {
  return {
    phase,
    outcome: "unknown",
    continuation: "continues",
    observation: {
      state,
      observedAt: new Date().toISOString(),
      ageSeconds: 0,
    },
  };
}

/** Record a terminal startup state without implying the CLI stopped the worker. */
export function consoleStartupFailedReport(state: string): FailureReport {
  return {
    ...consoleObservationReport("starting", state),
    outcome: "failed",
    continuation: "stopped",
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
