/**
 * Verified caller identity — the non-secret "who" half of authentication.
 *
 * {@link Principal} is one of the two outputs an authenticator produces
 * after inspecting a request's headers; the other is a
 * {@link Credentials} instance. They encode orthogonal facts and exist so
 * the two can be handled with different policies:
 *
 * | Aspect                 | Principal                  | Credentials                                          |
 * |------------------------|----------------------------|------------------------------------------------------|
 * | Answers                | "who is calling?"          | "what bearer material authenticates downstream?"     |
 * | Contains secrets       | No                         | Yes                                                  |
 * | Safe to log            | Yes                        | Never                                                |
 * | Lifetime on the server | Full request/session       | Dropped as soon as it is exchanged for a session     |
 *
 * **Where a Principal comes from**:
 *
 * An authenticator (see `AuthBackend`) implements a two-phase contract:
 *
 * 1. `authenticate(headers) -> Principal | undefined` — inspects the request
 *    headers and verifies them. On success it returns a {@link Principal}
 *    built from the verified values; the *existence* of the returned
 *    {@link Principal} is the proof that authentication succeeded. On
 *    "not my request" it returns `undefined` so the chain can try the
 *    next authenticator.
 * 2. `deriveCredentials(principal, headers) -> Credentials` — combines
 *    the already-verified {@link Principal} with the original headers to
 *    produce the secret-bearing {@link Credentials} instance used by
 *    downstream session factories.
 *
 * The split lets the non-secret identity be constructed, stashed, and
 * logged freely while the secret material is only materialised at the
 * moment it is needed and is not retained on the {@link Principal}.
 *
 * **Field semantics**:
 *
 * - `subject` — the canonical, stable identifier for the caller. For
 *   shared-bearer flows this is conventionally a fixed string (e.g.
 *   `"community"`) because there is only one logical caller; for
 *   per-user flows it is the authenticated username. Consumers that need
 *   a username (e.g. Deephaven Enterprise password auth) read it from
 *   `subject` so the identity verified during `authenticate` is the
 *   same identity used during `deriveCredentials`.
 * - `displayName` — free-form human-readable label for log output;
 *   authenticators set it equal to `subject` when they have nothing else to add.
 * - `raw` — authenticator-defined bag of non-secret extras, useful for
 *   auditing and for forward-compatibility with richer identity providers
 *   (IdP claims, etc.). Never carries secret material.
 */

// ---------------------------------------------------------------------------
// Principal
// ---------------------------------------------------------------------------

/**
 * Verified caller identity produced by an authenticator.
 *
 * Returning a {@link Principal} from an authenticator's `authenticate` call
 * is the proof that the request's headers were verified; consumers that
 * receive a {@link Principal} may therefore treat its fields as trusted.
 *
 * A {@link Principal} is non-secret by design (see the module docstring).
 * It is paired with, but distinct from, a {@link Credentials} value: the
 * principal says *who*, the credentials carry the secret material used
 * to authenticate *downstream*.
 */
export class Principal {
  /**
   * Canonical, stable identifier for the caller. Used as the log/audit
   * key and, where applicable, as the username that downstream consumers
   * (e.g. a Deephaven Enterprise session factory) authenticate as.
   */
  readonly subject: string;

  /**
   * Human-readable label for log output. May equal {@link subject} when
   * the authenticator has nothing else to add.
   */
  readonly displayName: string;

  /**
   * Authenticator-defined non-secret extras (for example the name of the
   * authenticator that produced this principal, or IdP claims). Never
   * contains secret material. The `Record<string, string>` shape is fixed,
   * but the set of keys and their meanings is authenticator-defined and
   * deliberately left unconstrained by the type system.
   */
  readonly raw: Readonly<Record<string, string>>;

  /**
   * @param subject - Canonical, stable identifier for the caller.
   * @param displayName - Human-readable label for log output.
   * @param raw - Authenticator-defined non-secret extras. Defaults to `{}`.
   */
  constructor(
    subject: string,
    displayName: string,
    raw: Record<string, string> = {},
  ) {
    this.subject = subject;
    this.displayName = displayName;
    this.raw = raw;
    Object.freeze(this);
  }
}
