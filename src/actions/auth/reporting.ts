import type { FailureReport, HumanFailure } from "../../errors.ts";
import { BIN } from "../../version.ts";

/** Builds plain report data at the action that knows what is happening. */
export function authActionReport(
  action: string,
  target?: string,
): FailureReport & { human: HumanFailure } {
  const where = target ? ` at ${target}` : "";
  return {
    operation: action,
    target,
    human: {
      summary: `Cancelled while ${action}${where}.`,
      context: `While ${action}${where}.`,
    },
  };
}

export function profileSaveReport(origin: string): FailureReport {
  return profileChangeReport(
    "saving the profile",
    [
      "Sign-in completed. The profile may have been saved; key cleanup may be incomplete.",
      "Check saved profiles before retrying.",
    ],
    origin,
  );
}

export function profileRemovalReport(): FailureReport {
  return profileChangeReport("removing profiles", [
    "Profile removal or key revocation may be incomplete.",
    "Check remaining profiles before retrying.",
  ]);
}

export function profileMigrationReport(origin: string): FailureReport {
  return profileChangeReport(
    "migrating profiles",
    [
      "Some profiles may have been saved. Check saved profiles before retrying migration.",
    ],
    origin,
  );
}

export function oldKeyCleanupReport(profile: string): FailureReport {
  const report = profileChangeReport(
    `cleaning up old keys for profile ${profile}`,
    ["The profile was saved; old-key cleanup may be incomplete."],
  );
  return {
    ...report,
    profile,
    outcome: "succeeded",
    progress: "Profile saved.",
  };
}

function profileChangeReport(
  action: string,
  details: string[],
  target?: string,
): FailureReport {
  const report = authActionReport(action, target);
  return {
    ...report,
    human: { ...report.human, details, recoveryLabel: "Check profiles" },
    outcome: "unknown",
    recoveryCommand: [BIN, "auth"],
  };
}
