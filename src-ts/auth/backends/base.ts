/**
 * The {@link AuthBackend} abstract base class and related error type.
 *
 * A backend is a small, stateless object that inspects a mapping of HTTP
 * request headers and decides whether the request carries valid
 * credentials. It implements a **two-phase contract** whose split is the
 * reason {@link Principal} and {@link Credentials} are separate types:
 *
 * 1. {@link AuthBackend.authenticate} verifies the headers and returns a
 *    {@link Principal} on success (or `undefined` to pass the request to
 *    the next backend). The returned principal is **non-secret** identity
 *    only; its existence is the proof that authentication succeeded.
 * 2. {@link AuthBackend.deriveCredentials} is called only for the winning
 *    backend and turns the already-verified {@link Principal} plus the
 *    original headers into a secret-bearing {@link Credentials} instance
 *    that downstream session factories use to authenticate to Deephaven.
 *
 * This split lets the middleware log "who" freely and lets the secret
 * material be materialised only at the moment a consumer needs it, rather
 * than being carried around on a long-lived identity object.
 *
 * **Backend chaining**: The middleware tries every registered backend in
 * registration order. The first backend whose {@link authenticate} returns a
 * {@link Principal} wins; the middleware then calls its {@link deriveCredentials}
 * and attaches both the principal and the credentials to the request context.
 * Returning `undefined` from {@link authenticate} means "not my request";
 * throwing {@link AuthenticationError} means "my request, but the credential
 * is invalid" — that short-circuits the chain with `401`.
 *
 * {@link AuthenticationError} is re-exported from `exceptions` for convenience.
 */

import { AuthenticationError } from "../../exceptions.js";
import { Credentials, Principal } from "../credentials/index.js";

export { AuthenticationError };

/** Default realm advertised in the `WWW-Authenticate` challenge header. */
export const _DEFAULT_REALM = "deephaven-mcp";

// ---------------------------------------------------------------------------
// AuthBackend
// ---------------------------------------------------------------------------

/**
 * Abstract base class implemented by every authentication backend.
 *
 * Instances are expected to be cheap to construct and safe to share
 * across requests (backends should hold only immutable configuration;
 * any mutable state belongs to the call site).
 *
 * Subclasses must:
 * - Declare a non-empty static `name` property (a short, stable identifier
 *   such as `"psk"` or `"password"`).
 * - Implement {@link authenticate} and {@link deriveCredentials} (both async).
 * - Implement {@link _challengeScheme} to return the `WWW-Authenticate` scheme token.
 *
 * Subclasses may optionally override:
 * - {@link _challengeHeaders} to add a `headers="…"` clause to their challenge.
 * - {@link challenge} to return a fully bespoke string.
 */
export abstract class AuthBackend {
  /**
   * Short, stable identifier used in logs and the `WWW-Authenticate`
   * challenge header (for example `"psk"`). Must be set by every concrete
   * subclass.
   */
  abstract readonly name: string;

  /** Realm string advertised in the `WWW-Authenticate` challenge header. */
  readonly realm: string;

  /**
   * @param options - Optional configuration.
   * @param options.realm - Realm string advertised in the `WWW-Authenticate` challenge header.
   *   Defaults to `"deephaven-mcp"` when `undefined`.
   */
  constructor(options?: { realm?: string }) {
    this.realm = options?.realm ?? _DEFAULT_REALM;
  }

  /**
   * Phase 1: verify the request headers and return verified identity.
   *
   * Implementations inspect `headers` for the specific fields this
   * backend knows how to verify and, on success, return a
   * {@link Principal} containing the verified identity. The
   * principal **must not** carry secret material (passwords, key
   * bytes, tokens); such material is re-read from `headers` during
   * {@link deriveCredentials} if it is needed downstream.
   *
   * The three return outcomes map to three middleware behaviours:
   *
   * - {@link Principal} — this backend claims the request and has
   *   verified it; the middleware will call {@link deriveCredentials}
   *   on this backend next.
   * - `undefined` — this backend does not recognise the request (e.g.
   *   the header it looks for is absent); the middleware proceeds to
   *   the next backend in the chain.
   * - Throwing {@link AuthenticationError} — this backend claims the
   *   request but the credential is invalid; the middleware
   *   short-circuits the chain with `401 Unauthorized`.
   *
   * @param headers - Lowercase-keyed view of the request headers. The
   *   middleware lowercases all header names before calling this method,
   *   so implementations look up values using lowercase names.
   * @returns A {@link Principal} built from the verified headers, or
   *   `undefined` to pass the request to the next backend.
   * @throws {AuthenticationError} If the backend recognises the request
   *   but the credential is invalid.
   */
  abstract authenticate(headers: Record<string, string>): Promise<Principal | undefined>;

  /**
   * Phase 2: build the secret-bearing credentials for the principal.
   *
   * Called by the middleware only for the backend whose
   * {@link authenticate} returned `principal` (i.e. exactly once per
   * authenticated request). The result is attached to the request context
   * and ultimately handed to a downstream session factory.
   *
   * Implementations typically combine verified fields from
   * `principal` (e.g. `principal.subject` as the username) with
   * fields re-read from `headers` (e.g. the password, the key
   * bytes) to construct the appropriate concrete {@link Credentials}
   * subclass. Re-reading from `headers` rather than stashing secrets on
   * the {@link Principal} keeps the principal non-secret.
   *
   * @param principal - The principal returned by this backend's
   *   {@link authenticate} for this request. Its fields have already
   *   been verified and may be trusted.
   * @param headers - The same lowercase-keyed header view that was passed
   *   to {@link authenticate}, available for re-reading values that were
   *   not captured on the {@link Principal}.
   * @returns A concrete {@link Credentials} subclass appropriate for this
   *   backend. The caller is expected to consume it promptly and drop it;
   *   credentials must not be logged or persisted.
   */
  abstract deriveCredentials(
    principal: Principal,
    headers: Record<string, string>,
  ): Promise<Credentials>;

  /**
   * Return the `WWW-Authenticate` scheme token for this backend.
   *
   * Examples: `"Bearer"`, `"DeephavenPassword"`, `"DeephavenPrivateKey"`.
   *
   * @returns The scheme token used as the first whitespace-separated
   *   token of the `WWW-Authenticate` challenge string.
   */
  protected abstract _challengeScheme(): string;

  /**
   * Return the headers this backend expects, for the challenge string.
   *
   * Backends that document the headers a client must supply (typical
   * for username/password and private-key flows) override this to
   * return a non-empty array of lowercase header names. The default
   * empty array omits the `headers="…"` clause from the challenge
   * entirely (typical for bearer-token flows like PSK).
   *
   * @returns Lowercase header names, or `[]` for none.
   */
  protected _challengeHeaders(): string[] {
    return [];
  }

  /**
   * Return the value for a `WWW-Authenticate` challenge header.
   *
   * Emitted by the middleware on `401` responses. Multiple backends
   * contribute one challenge each, and the middleware joins them with
   * `", "` on the single response header.
   *
   * The default implementation formats `'{scheme} realm="{realm}"'`
   * and appends `', headers="{h1}, {h2}, …"'` if {@link _challengeHeaders}
   * returns a non-empty array. Subclasses with bespoke needs may override
   * this method directly.
   *
   * @returns A single `WWW-Authenticate` scheme/parameter string
   *   (for example `'Bearer realm="deephaven-mcp"'`).
   */
  challenge(): string {
    const base = `${this._challengeScheme()} realm="${this.realm}"`;
    const hdrs = this._challengeHeaders();
    if (hdrs.length === 0) {
      return base;
    }
    return `${base}, headers="${hdrs.join(", ")}"`;
  }

  /**
   * Return the value of `headerName`, or `undefined` if absent.
   *
   * Throws {@link AuthenticationError} if the header is present but
   * empty, so callers can distinguish "not my header" (`undefined`) from
   * "my header but malformed" (exception) without repeating the
   * two-step guard in every backend.
   *
   * @param headers - Lowercase-keyed request headers.
   * @param headerName - The lowercase header name to look up.
   * @returns The non-empty header value, or `undefined` if the header is absent.
   * @throws {AuthenticationError} If the header is present but empty.
   */
  protected _requireHeader(
    headers: Record<string, string>,
    headerName: string,
  ): string | undefined {
    const value = headers[headerName];
    if (value === undefined) {
      return undefined;
    }
    if (!value) {
      throw new AuthenticationError(`${headerName} header must not be empty.`);
    }
    return value;
  }

  /**
   * Build a {@link Principal} tagged with this backend's name.
   *
   * Centralises the convention that every authenticator records its
   * identity in `Principal.raw["backend"]` so that audit logs and
   * cross-backend debugging can attribute a verified request to the
   * backend that accepted it.
   *
   * @param subject - Canonical caller identifier ({@link Principal.subject}).
   * @param options - Optional options.
   * @param options.displayName - Human-readable label ({@link Principal.displayName}).
   *   Defaults to `subject` when `undefined`.
   * @param options.extraRaw - Additional non-secret claims to merge into
   *   {@link Principal.raw} alongside the `"backend"` key. Keys in `extraRaw`
   *   take precedence over the auto-set `"backend"` key only if explicitly provided.
   * @returns A frozen {@link Principal} whose `raw` dict contains at least
   *   `{"backend": this.name}`.
   */
  protected _makePrincipal(
    subject: string,
    options?: {
      displayName?: string;
      extraRaw?: Record<string, string>;
    },
  ): Principal {
    const raw: Record<string, string> = { backend: this.name };
    if (options?.extraRaw) {
      Object.assign(raw, options.extraRaw);
    }
    return new Principal(
      subject,
      options?.displayName ?? subject,
      raw,
    );
  }
}
