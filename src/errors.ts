/** Stable error codes; part of the public contract listed by `dh agents`. */
export const ERROR_CODES = {
  internal: { exit: 1, description: "Unexpected internal error" },
  usage: {
    exit: 2,
    description: "Bad arguments, or input needed without a TTY",
  },
  console_required: {
    exit: 2,
    description: "No console is selected",
  },
  console_not_found: {
    exit: 2,
    description: "Console does not exist",
  },
  cancelled: { exit: 3, description: "Cancelled by the user" },
  auth_required: { exit: 4, description: "No profile" },
  auth_failed: {
    exit: 4,
    description: "Server rejected credentials at login",
  },
  auth_expired: {
    exit: 4,
    description: "Server rejected the stored credential",
  },
  credential_unavailable: {
    exit: 4,
    description: "Profile's env var or key file missing",
  },
  console_forbidden: {
    exit: 4,
    description: "Server denied access to the console",
  },
  key_upload_failed: {
    exit: 4,
    description: "Uploading this computer's key to DHE failed",
  },
  migration_failed: {
    exit: 4,
    description: "Some legacy config entries weren't migrated",
  },
  server_unreachable: { exit: 5, description: "Network or TLS failure" },
  server_unsupported: {
    exit: 5,
    description: "Not a supported Deephaven server",
  },
  console_not_ready: {
    exit: 5,
    description: "Console is not running",
  },
  console_unsupported: {
    exit: 5,
    description: "Server does not support this console operation or setting",
  },
  console_failed: {
    exit: 5,
    description: "Server rejected the console operation",
  },
} as const;

/** A key of {@link ERROR_CODES}. */
export type ErrorCode = keyof typeof ERROR_CODES;

/** Optional command context attached to a structured failure report. */
export interface FailureReport {
  /** Operation-specific prose; never serialized into JSON. */
  human?: HumanFailure;
  command?: string;
  target?: string;
  profile?: string;
  operation?: string;
  phase?: string;
  elapsedSeconds?: number;
  progress?: string;
  outcome?: "not_started" | "succeeded" | "failed" | "unknown";
  continuation?: "continues" | "stopped" | "unknown";
  retryWarning?: string;
  observation?: {
    state: string;
    observedAt: string;
    ageSeconds: number;
  } | null;
  recoveryCommand?: string[];
}

export interface HumanFailure {
  /** Cancellation summary; ordinary failures retain their own error summary. */
  summary: string;
  context?: string;
  details?: string[];
  recoveryLabel?: string;
}

/** Thrown for every expected failure; `main.ts` turns `code` into an exit code and `hint` into advice. */
export class DhError extends Error {
  /** Cleanup diagnostics that must remain visible even for routine cancellation. */
  cleanupDetail?: string;

  /** The message without `detail`. */
  readonly headline: string;

  /** `detail`: the underlying reason, from {@link reasonOf}; appended to the message. */
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly hint?: string,
    readonly detail?: string,
  ) {
    super(detail ? `${message}: ${detail}` : message);
    this.name = "DhError";
    this.headline = message;
  }

  get exit(): number {
    return ERROR_CODES[this.code].exit;
  }
}

const MAX_REASON = 200;

/** One short line for any thrown value; the jsapi rejects with non-Errors (e.g. GWT `Event`s). */
export function describe(reason: unknown): string {
  const text = messageOf(reason).replace(/\s+/g, " ").trim();
  return text.length > MAX_REASON ? `${text.slice(0, MAX_REASON)}…` : text;
}

/** The best text a thrown value carries: an Error's message, a string, or a GWT event's `detail`. */
function messageOf(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  if (typeof reason === "string") return reason;
  // biome-ignore lint/suspicious/noExplicitAny: GWT event objects are untyped.
  const detail = (reason as any)?.detail;
  if (typeof detail === "string") return detail;
  return String(reason);
}

/** {@link describe}, or undefined if it says nothing useful. */
export function reasonOf(reason: unknown): string | undefined {
  const text = describe(reason);
  return !text || text.startsWith("[object ") ? undefined : text;
}

/** A cancellation error carrying the abort reason as detail when it is useful. */
export function cancellationError(reason?: unknown): DhError {
  return new DhError(
    "cancelled",
    "Cancelled",
    undefined,
    reason === undefined ? undefined : reasonOf(reason),
  );
}

export function interruptedError(reason?: unknown): DhError {
  if (reason instanceof DhError && reason.code === "cancelled") return reason;
  return new DhError(
    "cancelled",
    "Interrupted",
    undefined,
    reason === undefined ? undefined : reasonOf(reason),
  );
}

export function commandUsageError(message: string, command: string): DhError {
  return new DhError(
    "usage",
    message.replace(/^error: /, ""),
    `Run \`${command} --help\` for usage.`,
  );
}

export function asDhError(error: unknown): DhError {
  if (error instanceof DhError) return error;
  return new DhError(
    "internal",
    error instanceof Error ? error.message : String(error),
  );
}

/** Adds cleanup diagnostics without replacing the command's primary error. */
export function cleanupError(failures: unknown[], primary?: DhError): DhError {
  const detail = failures
    .map((error) => (error instanceof Error ? error.message : String(error)))
    .join("; ");
  if (!primary) {
    return new DhError("internal", "Command cleanup failed", undefined, detail);
  }
  const error = new DhError(
    primary.code,
    primary.headline,
    primary.hint,
    primary.detail,
  );
  error.cleanupDetail = detail;
  return error;
}

/** `server_unreachable` for `origin`. `cause`: why it failed; without one, the error says it timed out. */
export function serverUnreachable(origin: string, cause?: unknown): DhError {
  return new DhError(
    "server_unreachable",
    `Could not reach ${origin}`,
    cause === undefined ? "Timed out" : describe(cause),
  );
}

/** Rejects with `onTimeout()` if `promise` hasn't settled within `ms`. */
export async function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  onTimeout: () => DhError,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(onTimeout()), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** How long one server call may take: `$DH_TIMEOUT` seconds, default 30. */
export function timeoutMs(): number {
  const raw = Number(process.env.DH_TIMEOUT);
  return (Number.isFinite(raw) && raw > 0 ? raw : 30) * 1000;
}

/** A Node file-system error with this `code`, e.g. `ENOENT`. */
export function isErrno(e: unknown, code: string): boolean {
  return e instanceof Error && (e as NodeJS.ErrnoException).code === code;
}
