/**
 * Username/password authentication backend.
 *
 * Verifies that the caller has supplied a username/password pair (and an
 * optional operate-as identity) in dedicated HTTP headers, and packages
 * the values into a {@link PasswordCredentials} for downstream consumers
 * that will perform the real authentication against an upstream system.
 *
 * **Headers**:
 * - `X-Deephaven-Username` (required): the authenticating username.
 * - `X-Deephaven-Password` (required): the password.
 * - `X-Deephaven-Effective-User` (optional): identity to operate as
 *   after authenticating. Only honoured when the backend is constructed
 *   with `allowEffectiveUser=true`; otherwise present-and-set raises
 *   {@link AuthenticationError}.
 *
 * The backend performs *syntactic* validation only — it checks that the
 * required headers are present and well-formed. It does not contact any
 * upstream system: that round-trip happens later, when a consumer (for
 * example a session factory) spends the derived credential.
 */

import { Credentials, PasswordCredentials, Principal } from "../credentials/index.js";
import { AuthBackend, AuthenticationError } from "./base.js";
import {
  HEADER_EFFECTIVE_USER,
  HEADER_PASSWORD,
  HEADER_USERNAME,
} from "./headers.js";

/**
 * Authenticate requests carrying username/password headers.
 */
export class PasswordBackend extends AuthBackend {
  /** Short, stable identifier used in logs and `WWW-Authenticate` challenges. */
  readonly name = "password";

  /** Whether a non-empty `X-Deephaven-Effective-User` header is permitted. */
  readonly allowEffectiveUser: boolean;

  /**
   * @param options - Optional configuration.
   * @param options.allowEffectiveUser - If `true`, accept an optional
   *   `X-Deephaven-Effective-User` header and propagate its value into
   *   {@link PasswordCredentials}. If `false` (default), the header must be
   *   absent or empty; a present-and-non-empty value causes the request to
   *   be rejected with {@link AuthenticationError}.
   * @param options.realm - Realm string advertised in the `WWW-Authenticate`
   *   challenge header. Defaults to `"deephaven-mcp"`.
   */
  constructor(options?: { allowEffectiveUser?: boolean; realm?: string }) {
    super({ realm: options?.realm });
    this.allowEffectiveUser = options?.allowEffectiveUser ?? false;
  }

  /**
   * Return a {@link Principal} iff the password headers are present.
   *
   * Returns `undefined` when the request is not claiming password auth
   * (`X-Deephaven-Password` absent); this lets a chain fall through
   * to other backends. Raises when the request is malformed
   * (only one of the required headers present, empty username, or
   * effective-user header used while disallowed).
   *
   * @param headers - Lowercase-keyed request headers.
   * @returns `Principal(subject=<username>, …)` on success; `undefined`
   *   if `X-Deephaven-Password` is absent.
   * @throws {AuthenticationError} If `X-Deephaven-Password` is present
   *   but `X-Deephaven-Username` is missing or empty; if
   *   `X-Deephaven-Password` is empty; or if `X-Deephaven-Effective-User`
   *   is present while `allowEffectiveUser` is `false`.
   */
  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    const password = this._requireHeader(headers, HEADER_PASSWORD);
    if (password === undefined) {
      return undefined;
    }
    const username = this._requireHeader(headers, HEADER_USERNAME);
    if (!username) {
      throw new AuthenticationError(
        `${HEADER_USERNAME} header is required with ${HEADER_PASSWORD}.`,
      );
    }
    const effectiveUser = headers[HEADER_EFFECTIVE_USER];
    if (effectiveUser && !this.allowEffectiveUser) {
      throw new AuthenticationError(
        `${HEADER_EFFECTIVE_USER} header is not permitted on this server.`,
      );
    }

    const extraRaw = effectiveUser ? { effective_user: effectiveUser } : undefined;
    return this._makePrincipal(username, { extraRaw });
  }

  /**
   * Return a {@link PasswordCredentials} for the authenticated principal.
   *
   * Re-reads the password and effective-user headers; `username`
   * comes from `principal.subject` so the two sources stay in sync.
   *
   * @param principal - Principal previously returned by {@link authenticate}.
   * @param headers - Lowercase-keyed request headers.
   * @returns Credentials carrying the username, password, and optional
   *   effective-user identity for a downstream consumer to spend against
   *   an upstream system.
   */
  async deriveCredentials(
    principal: Principal,
    headers: Record<string, string>,
  ): Promise<PasswordCredentials> {
    const password = headers[HEADER_PASSWORD]!;
    let effectiveUser: string | undefined = undefined;
    if (this.allowEffectiveUser) {
      const rawEffective = headers[HEADER_EFFECTIVE_USER];
      if (rawEffective) {
        effectiveUser = rawEffective;
      }
    }
    return new PasswordCredentials(principal.subject, password, effectiveUser);
  }

  /**
   * Return the `DeephavenPassword` scheme token for the challenge.
   *
   * @returns `"DeephavenPassword"`.
   */
  protected _challengeScheme(): string {
    return "DeephavenPassword";
  }

  /**
   * Return the headers required by this backend.
   *
   * @returns Lowercase names of the required username and password headers.
   */
  protected override _challengeHeaders(): string[] {
    return [HEADER_USERNAME, HEADER_PASSWORD];
  }
}

export { Credentials };
