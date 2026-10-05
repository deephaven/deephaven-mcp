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
  key_upload_denied: { exit: 4, description: "DHE rejected the key upload" },
  server_unreachable: { exit: 5, description: "Network or TLS failure" },
  server_unsupported: {
    exit: 5,
    description: "Not a supported Deephaven server",
  },
} as const;

export type ErrorCode = keyof typeof ERROR_CODES;

export class DhError extends Error {
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly hint?: string,
  ) {
    super(message);
    this.name = "DhError";
  }

  get exit(): number {
    return ERROR_CODES[this.code].exit;
  }
}

/** The jsapi rejects with non-Error values (e.g. GWT `Event` objects). */
export function describe(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  if (typeof reason === "string") return reason;
  // deno-lint-ignore no-explicit-any
  const detail = (reason as any)?.detail;
  if (typeof detail === "string") return detail;
  return String(reason);
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

export function timeoutMs(): number {
  const raw = Number(Deno.env.get("DH_TIMEOUT"));
  return (Number.isFinite(raw) && raw > 0 ? raw : 30) * 1000;
}
