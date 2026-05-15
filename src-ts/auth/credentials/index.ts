/**
 * Identity and bearer-material data types for the `auth` framework.
 *
 * This subpackage holds **pure data** — the verified caller identity
 * ({@link Principal}) and the mechanism-only credential classes
 * returned by backends ({@link PSKCredentials},
 * {@link PasswordCredentials}, {@link PrivateKeyCredentials}), all of
 * which inherit from the abstract base {@link Credentials}.
 *
 * The module has **no behavioral coupling** to the rest of the system:
 * it depends only on the small {@link REDACTED} constant module, and is
 * therefore importable from any consumer (a session factory, a future CLI,
 * tests) without dragging in backend or middleware machinery.
 */

export {
  Credentials,
  PasswordCredentials,
  PrivateKeyCredentials,
  PSKCredentials,
} from "./credentials.js";
export { Principal } from "./principal.js";
