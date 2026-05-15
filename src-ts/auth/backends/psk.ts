/**
 * Pre-shared-key (PSK) authentication backend.
 *
 * Verifies a single configured PSK presented by the caller in the HTTP
 * `X-Deephaven-PSK` header, using a constant-time comparison. Suitable
 * for "Jupyter-style" deployments where one shared secret authenticates
 * every request and there is no per-user identity to distinguish.
 *
 * On success the backend returns a {@link Principal} whose `subject`
 * defaults to `"psk"` (configurable per instance) and a
 * {@link PSKCredentials} carrying the verified key, so that consumers
 * that need to forward the PSK to an upstream service can do so.
 */

import * as crypto from "node:crypto";
import { Credentials, Principal, PSKCredentials } from "../credentials/index.js";
import { AuthBackend, AuthenticationError } from "./base.js";
import { HEADER_PSK } from "./headers.js";

/**
 * Authenticate requests against a configured pre-shared key.
 */
export class PSKBackend extends AuthBackend {
  /**
   * Short, stable identifier used in logs and `WWW-Authenticate` challenges.
   */
  readonly name = "psk";

  /** The key the request must present. */
  readonly expectedPsk: string;

  /** `Principal.subject` value returned on successful authentication. */
  readonly principalSubject: string;

  /**
   * @param expectedPsk - The pre-shared key the client must present. Must be non-empty;
   *   an empty string would make every unauthenticated request succeed.
   * @param options - Optional configuration.
   * @param options.principalSubject - Identity surfaced on the {@link Principal}
   *   returned by {@link authenticate}. Defaults to `"psk"`.
   * @param options.realm - Realm string advertised in the `WWW-Authenticate` challenge.
   *   Defaults to `"deephaven-mcp"`.
   * @throws {Error} If `expectedPsk` is empty.
   */
  constructor(
    expectedPsk: string,
    options?: { principalSubject?: string; realm?: string },
  ) {
    super({ realm: options?.realm });
    if (!expectedPsk) {
      throw new Error("PSKBackend requires a non-empty PSK.");
    }
    this.expectedPsk = expectedPsk;
    this.principalSubject = options?.principalSubject ?? "psk";
  }

  /**
   * Return a {@link Principal} iff the PSK header matches the configured PSK.
   *
   * A missing `X-Deephaven-PSK` header returns `undefined` (the request is not
   * claiming PSK auth, and a multi-backend chain could legitimately pass it to
   * another backend). A present `X-Deephaven-PSK` header whose value is empty
   * or does not match raises {@link AuthenticationError}.
   *
   * @param headers - Lowercase-keyed request headers.
   * @returns `Principal(subject=this.principalSubject, ...)` on success;
   *   `undefined` if the `X-Deephaven-PSK` header is absent.
   * @throws {AuthenticationError} If the header is present but empty, or
   *   its value does not match the configured PSK.
   */
  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    const presented = this._requireHeader(headers, HEADER_PSK);
    if (presented === undefined) {
      return undefined;
    }
    // Constant-time comparison to prevent timing attacks
    if (!timingSafeEqual(presented, this.expectedPsk)) {
      throw new AuthenticationError(`Invalid pre-shared key in ${HEADER_PSK} header.`);
    }
    return this._makePrincipal(this.principalSubject);
  }

  /**
   * Return a {@link PSKCredentials} carrying the PSK from the request.
   *
   * Forwards the **observed** `X-Deephaven-PSK` header value rather than
   * the server's configured {@link expectedPsk}. The two are byte-equal at
   * this point — {@link authenticate} only returns a {@link Principal} after
   * constant-time comparison confirmed equality — but reading from `headers`
   * here mirrors how {@link PasswordBackend} and {@link PrivateKeyBackend}
   * build their credentials and is robust to a future world in which
   * {@link expectedPsk} becomes mutable.
   *
   * @param _principal - Unused; present to satisfy the {@link AuthBackend} contract.
   * @param headers - Lowercase-keyed request headers. The `X-Deephaven-PSK`
   *   entry is required and guaranteed to be present by {@link authenticate}.
   * @returns The verified pre-shared key, available to downstream consumers.
   */
  async deriveCredentials(
    _principal: Principal,
    headers: Record<string, string>,
  ): Promise<PSKCredentials> {
    return new PSKCredentials(headers[HEADER_PSK]!);
  }

  /**
   * Return the `DeephavenPSK` scheme token for the challenge string.
   *
   * @returns `"DeephavenPSK"`.
   */
  protected _challengeScheme(): string {
    return "DeephavenPSK";
  }

  /**
   * Return the header required by this backend.
   *
   * @returns A one-element array containing the lowercase `X-Deephaven-PSK`
   *   header name, included in the `WWW-Authenticate` challenge.
   */
  protected override _challengeHeaders(): string[] {
    return [HEADER_PSK];
  }
}

/**
 * Constant-time string comparison to prevent timing attacks.
 *
 * @param a - First string.
 * @param b - Second string.
 * @returns `true` if the strings are equal.
 */
export function _timingSafeEqual(a: string, b: string): boolean {
  return timingSafeEqual(a, b);
}

function timingSafeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) {
    // Use a dummy comparison to maintain constant time
    crypto.timingSafeEqual(bufA, Buffer.alloc(bufA.length));
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

export { Credentials };
