/**
 * Private-key authentication backend.
 *
 * Accepts private-key authentication where the client delivers the key as
 * a base64-encoded blob in an HTTP header (never a filesystem path — the
 * server never touches client-side files). The backend produces a
 * {@link PrivateKeyCredentials} whose decoded bytes a downstream consumer
 * spends against an upstream system.
 *
 * **Headers**:
 * - `X-Deephaven-Username` (required): the username the key
 *   authenticates as. Stored on the resulting `Principal.subject`.
 * - `X-Deephaven-Private-Key` (required): Base64-encoded contents of the
 *   caller's private-key file.
 *
 * Like the password backend, {@link authenticate} performs only syntactic
 * validation; the real round-trip authentication happens when a consumer
 * spends the derived {@link PrivateKeyCredentials}.
 */

import { Credentials, Principal, PrivateKeyCredentials } from "../credentials/index.js";
import { AuthBackend, AuthenticationError } from "./base.js";
import { HEADER_PRIVATE_KEY, HEADER_USERNAME } from "./headers.js";

/**
 * Authenticate requests carrying a private-key header.
 */
export class PrivateKeyBackend extends AuthBackend {
  /** Short, stable identifier used in logs and `WWW-Authenticate` challenges. */
  readonly name = "private_key";

  /**
   * @param options - Optional configuration.
   * @param options.realm - Realm string advertised in the `WWW-Authenticate`
   *   challenge header. Defaults to `"deephaven-mcp"`.
   */
  constructor(options?: { realm?: string }) {
    super({ realm: options?.realm });
  }

  /**
   * Return a {@link Principal} iff the private-key headers are present.
   *
   * Returns `undefined` when the request is not claiming private-key auth
   * (`X-Deephaven-Private-Key` absent). Raises when the request is
   * malformed (missing username, empty key header, or key not valid base64).
   *
   * @param headers - Lowercase-keyed request headers.
   * @returns `Principal(subject=<username>, …)` on success; `undefined`
   *   if `X-Deephaven-Private-Key` is absent.
   * @throws {AuthenticationError} If the key header is present but empty;
   *   if `X-Deephaven-Username` is missing or empty; or if the key header
   *   is not valid base64.
   */
  async authenticate(headers: Record<string, string>): Promise<Principal | undefined> {
    const keyHeader = this._requireHeader(headers, HEADER_PRIVATE_KEY);
    if (keyHeader === undefined) {
      return undefined;
    }
    const username = this._requireHeader(headers, HEADER_USERNAME);
    if (!username) {
      throw new AuthenticationError(
        `${HEADER_USERNAME} header is required with ${HEADER_PRIVATE_KEY}.`,
      );
    }
    // Validate base64 at authenticate time so malformed requests fail
    // at the edge rather than deep inside the consumer that spends the credential.
    try {
      const decoded = Buffer.from(keyHeader, "base64");
      // Verify round-trip: re-encode and check it matches (strict base64 validation)
      const reEncoded = decoded.toString("base64");
      // Remove padding for comparison since base64 may have different padding
      if (!isValidBase64(keyHeader)) {
        throw new Error("Invalid base64");
      }
    } catch {
      throw new AuthenticationError(
        `${HEADER_PRIVATE_KEY} header is not valid base64.`,
      );
    }

    return this._makePrincipal(username);
  }

  /**
   * Return a {@link PrivateKeyCredentials} for the authenticated principal.
   *
   * The key header was already validated as base64 during {@link authenticate};
   * we decode again here (rather than cache the decoded bytes on the principal)
   * to keep {@link Principal} secret-free. Additionally, the decoded bytes are
   * converted to text here, at the producing edge, so that downstream consumers
   * of {@link PrivateKeyCredentials} can rely on the `keyText: string` field
   * without re-validating.
   *
   * Deephaven private-key files are documented as plain text files whose
   * structured content is ASCII: `///#` comment lines plus `key value` lines
   * where the values are base64-encoded DER. We decode as UTF-8 rather than
   * strict ASCII so that human-written comment lines are allowed to contain
   * Unicode. Since ASCII is a strict subset of UTF-8, every conformant file
   * is accepted; only genuinely binary garbage is rejected.
   *
   * @param _principal - Unused; required by the {@link AuthBackend} contract.
   * @param headers - Lowercase-keyed request headers.
   * @returns Credentials carrying the decoded key text for a downstream consumer.
   * @throws {AuthenticationError} If the base64-decoded bytes are not valid UTF-8.
   */
  async deriveCredentials(
    _principal: Principal,
    headers: Record<string, string>,
  ): Promise<PrivateKeyCredentials> {
    const keyHeader = headers[HEADER_PRIVATE_KEY]!;
    const keyBytes = Buffer.from(keyHeader, "base64");
    // Verify the decoded bytes are valid UTF-8
    let keyText: string;
    try {
      // In Node.js, we can try to create a TextDecoder with fatal mode
      const decoder = new TextDecoder("utf-8", { fatal: true });
      keyText = decoder.decode(keyBytes);
    } catch {
      throw new AuthenticationError(
        `${HEADER_PRIVATE_KEY} header decodes to bytes that are not valid UTF-8.`,
      );
    }
    return new PrivateKeyCredentials(keyText);
  }

  /**
   * Return the `DeephavenPrivateKey` scheme token for the challenge.
   *
   * @returns `"DeephavenPrivateKey"`.
   */
  protected _challengeScheme(): string {
    return "DeephavenPrivateKey";
  }

  /**
   * Return the headers required by this backend.
   *
   * @returns Lowercase names of the required username and private-key headers.
   */
  protected override _challengeHeaders(): string[] {
    return [HEADER_USERNAME, HEADER_PRIVATE_KEY];
  }
}

/**
 * Check if a string is valid base64.
 *
 * @param str - The string to check.
 * @returns `true` if the string is valid base64.
 */
export function _isValidBase64(str: string): boolean {
  return isValidBase64(str);
}

function isValidBase64(str: string): boolean {
  // Strict base64 validation: only contains valid characters and correct padding
  const base64Regex = /^[A-Za-z0-9+/]*={0,2}$/;
  if (!base64Regex.test(str)) {
    return false;
  }
  // Check length is multiple of 4 (with padding) or proper padding
  return str.length % 4 === 0 || (str.length % 4 === 2 && str.endsWith("==")) ||
    (str.length % 4 === 3 && str.endsWith("="));
}

export { Credentials };
