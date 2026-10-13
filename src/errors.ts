/** Stable error codes; part of the public contract listed by `dh agents`. */
export const ERROR_CODES = {
  internal: { exit: 1, description: "Unexpected internal error" },
  usage: {
    exit: 2,
    description: "Bad arguments, or input needed without a TTY",
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
  key_upload_failed: {
    exit: 4,
    description: "Uploading this computer's key to DHE failed",
  },
  server_unreachable: { exit: 5, description: "Network or TLS failure" },
  server_unsupported: {
    exit: 5,
    description: "Not a supported Deephaven server",
  },
} as const;

export type ErrorCode = keyof typeof ERROR_CODES;

/** Thrown for every expected failure; `main.ts` turns `code` into an exit code and `hint` into advice. */
export class DhError extends Error {
  /** `detail`: the underlying reason, from {@link reasonOf}; appended to the message. */
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly hint?: string,
    readonly detail?: string,
  ) {
    super(detail ? `${message}: ${detail}` : message);
    this.name = "DhError";
  }

  get exit(): number {
    return ERROR_CODES[this.code].exit;
  }
}

const MAX_REASON = 200;

/** One short line for any thrown value; the jsapi rejects with non-Errors (e.g. GWT `Event`s). */
export function describe(reason: unknown): string {
  // biome-ignore lint/suspicious/noExplicitAny: GWT event objects are untyped.
  const detail = (reason as any)?.detail;
  const text = (
    reason instanceof Error
      ? reason.message
      : typeof reason === "string"
        ? reason
        : typeof detail === "string"
          ? detail
          : String(reason)
  )
    .replace(/\s+/g, " ")
    .trim();
  return text.length > MAX_REASON ? `${text.slice(0, MAX_REASON)}…` : text;
}

/** {@link describe}, or undefined if it says nothing useful. */
export function reasonOf(reason: unknown): string | undefined {
  const text = describe(reason);
  return !text || text.startsWith("[object ") ? undefined : text;
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
