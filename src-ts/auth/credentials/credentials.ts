/**
 * Mechanism-only credential classes: the secret-bearing "what" of authentication.
 *
 * Each concrete subclass represents a single kind of bearer material (a
 * shared token, a username/password pair, a private key). The types are
 * defined purely by the *shape* of the material they carry; this module
 * is intentionally agnostic about which consumer accepts which kind.
 * Deciding whether a given credential can authenticate against a given
 * system is the responsibility of whichever consumer takes one as input
 * (for example a session factory's switch statement or a registry's
 * acceptance check).
 *
 * Class hierarchy:
 * {@link Credentials} is the abstract base class and the single type used in
 * function signatures throughout the codebase. The three concrete
 * subclasses inherit from it:
 *
 * - {@link PSKCredentials} — a single pre-shared key string verified by the authenticator.
 * - {@link PasswordCredentials} — username/password (and optional identity to operate as).
 * - {@link PrivateKeyCredentials} — UTF-8 text of a private-key file.
 *
 * Consumers branch on concrete type via `instanceof` checks local to
 * the consuming module; this package exposes no helpers for such branching.
 *
 * **Sensitivity**: These classes carry secret material (passwords, PSKs, decoded key
 * bytes). They must never be logged, serialised, or persisted. Call sites
 * should drop the secret as soon as it has been exchanged for a
 * long-lived handle (e.g. a session object).
 *
 * As a defence-in-depth measure, every concrete subclass overrides
 * `toString()` to redact its secret fields. `String(creds)`,
 * `` `${creds}` ``, and any other route that goes through `toString()`
 * therefore produce `"PSKCredentials(psk=[REDACTED])"` rather than the
 * plaintext secret. Call sites that genuinely need the secret must read
 * the typed property (`creds.psk`, `creds.password`, `creds.keyText`) explicitly.
 *
 * **Identity and hashing**: TypeScript has no built-in structural equality/hashing.
 * Consumers that need cache-key semantics must implement their own equality check
 * by comparing typed fields directly.
 */

import { REDACTED } from "../../redaction.js";

// ---------------------------------------------------------------------------
// Abstract base class
// ---------------------------------------------------------------------------

/**
 * Abstract base class for mechanism-only credentials.
 *
 * Every concrete credential kind is a subclass of {@link Credentials}.
 * The base class carries no fields and no behaviour because credentials
 * are pure data — dispatching on the concrete type is the responsibility
 * of whichever consumer takes a {@link Credentials} as input.
 *
 * Direct instantiation is forbidden at runtime: attempting `new Credentials()`
 * throws {@link TypeError}. Subclasses must call `super()` and are expected
 * to be immutable value objects.
 */
export abstract class Credentials {
  /** Forbid direct instantiation of the abstract base class. */
  constructor() {
    if (new.target === Credentials) {
      throw new TypeError(
        "Credentials is an abstract base class; instantiate a concrete subclass instead.",
      );
    }
  }
}

// ---------------------------------------------------------------------------
// PSKCredentials
// ---------------------------------------------------------------------------

/**
 * Pre-shared key bearer material.
 *
 * Carries the PSK string the caller presented and the authenticator
 * verified (typically by constant-time comparison against a configured
 * value). Consumers that need to forward the key to an upstream
 * service can read {@link psk} directly; consumers that only need to
 * know "the caller is authenticated" can ignore the field.
 *
 * The default `toString()` is overridden with a secret-redacting
 * implementation so that accidental `` `${creds}` `` or
 * `logger.info("%s", creds)` cannot leak the PSK.
 */
export class PSKCredentials extends Credentials {
  /** The verified pre-shared key. */
  readonly psk: string;

  /**
   * @param psk - The verified pre-shared key.
   */
  constructor(psk: string) {
    super();
    this.psk = psk;
    Object.freeze(this);
  }

  /** Return a redacted representation that never reveals the PSK. */
  toString(): string {
    return `PSKCredentials(psk=${REDACTED})`;
  }
}

// ---------------------------------------------------------------------------
// PasswordCredentials
// ---------------------------------------------------------------------------

/**
 * Username/password bearer material, with optional operate-as identity.
 *
 * Carries the fields needed for classic password authentication plus
 * an optional "operate as" identity for consumers that support
 * sudo-style delegation. Whether a given consumer honours
 * {@link effectiveUser} is a property of that consumer; this
 * class is only responsible for carrying the value.
 *
 * The default `toString()` is overridden with a secret-redacting
 * implementation so that accidental `` `${creds}` `` or
 * `logger.info("%s", creds)` cannot leak the password. {@link username}
 * and {@link effectiveUser} are not secrets and remain visible for debugging.
 */
export class PasswordCredentials extends Credentials {
  /** The authenticating username. */
  readonly username: string;
  /** The user's password. */
  readonly password: string;
  /**
   * Optional identity to operate as after authenticating. When `undefined`,
   * the authenticated user is also the effective user.
   */
  readonly effectiveUser: string | undefined;

  /**
   * @param username - The authenticating username.
   * @param password - The user's password.
   * @param effectiveUser - Optional identity to operate as after authenticating.
   */
  constructor(username: string, password: string, effectiveUser?: string) {
    super();
    this.username = username;
    this.password = password;
    this.effectiveUser = effectiveUser;
    Object.freeze(this);
  }

  /** Return a representation that redacts the password field only. */
  toString(): string {
    // Python repr uses single-quote string notation like 'alice'
    const uRepr = `'${this.username}'`;
    const euRepr =
      this.effectiveUser === undefined ? "None" : `'${this.effectiveUser}'`;
    return (
      `PasswordCredentials(username=${uRepr}, ` +
      `password=${REDACTED}, effective_user=${euRepr})`
    );
  }
}

// ---------------------------------------------------------------------------
// PrivateKeyCredentials
// ---------------------------------------------------------------------------

/**
 * Private-key bearer material.
 *
 * Carries the decoded contents of a private-key file as UTF-8 text.
 * Keeping the payload in memory (rather than as a file path) lets
 * consumers present the key to the underlying auth API without
 * requiring filesystem access on the server side; how the text is
 * consumed (e.g. wrapped in a text stream, parsed as PEM) is the
 * consumer's concern.
 *
 * The default `toString()` is overridden with a secret-redacting
 * implementation so that accidental `` `${creds}` `` or
 * `logger.info("%s", creds)` cannot leak the key material. The key
 * length in characters is shown because it is useful for debugging
 * and does not reveal key content.
 */
export class PrivateKeyCredentials extends Credentials {
  /**
   * Decoded keyfile contents — the raw text of the private-key file.
   * Always valid UTF-8: the producing backend validates the bytes it
   * receives on the wire, so downstream consumers can rely on the
   * `string` type without re-validating.
   */
  readonly keyText: string;

  /**
   * @param keyText - Decoded keyfile contents (the raw text of the private-key file).
   */
  constructor(keyText: string) {
    super();
    this.keyText = keyText;
    Object.freeze(this);
  }

  /** Return a representation that shows key length but redacts contents. */
  toString(): string {
    return `PrivateKeyCredentials(key_text=${REDACTED}, ${this.keyText.length} chars)`;
  }
}
