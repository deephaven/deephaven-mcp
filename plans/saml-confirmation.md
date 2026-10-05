# SAML sign-in confirmation

A dedicated SAML entry path that makes the user actively sign in and confirm the
account. `dh` uses it. Every other sign-in is unchanged.

## Path

`<samlConfirmUrl>?key=<key>` (default `/dh-saml/confirm`)

- Starts a SAML sign-in for `key` with `ForceAuthn=true`.
- After the IdP responds, shows the confirmation page.
- `/dh-saml/?key=…` is untouched.

## Confirmation page

```text
Sign in as user-a@example.com?

[Continue]   [Use a different account]   [Cancel]
```

- Shown after the IdP response validates, before the key is bound.
- Continue → bind the key → success page.
- Use a different account → restart at `<samlConfirmUrl>?key=<key>`.
- Cancel → fail the key. The waiting login fails immediately.
- Not confirmed within the browser session lifetime → the key fails.

## Configuration

| Property                                     | Effect                                                                                                |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `authentication.client.samlauth.confirm.url` | Set when supported. Exposed via `authentication.client.configuration.list`, like `samlauth.login.url` |

## `dh`

| Server                   | `dh`                                                                                                                   |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| Advertises `confirm.url` | Open `<confirm.url>?key=<key>`.                                                                                        |
| Doesn't                  | Open `<login.url>?key=<key>`. After SSO: `Use user-a@example.com? (Y/n)` in the terminal. No → discard, nothing saved. |

- `--expect-user <u>`: check the account without prompting. Mismatch →
  `auth_failed`, nothing saved.

## Compatibility

- The web UI, vscode-deephaven and the Python `saml()` use `login.url` →
  unchanged.
- Older servers don't advertise `confirm.url` → `dh` uses the terminal prompt.
- The IdP's registered assertion consumer URL is unchanged. No IdP
  reconfiguration.

## Implementation (iris)

| Path                                                                | Change                                                                                                                                                                        |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `saml/saml-auth-module/.../servlets/LoginRequest.java`              | Extract the request-building logic, taking `forceAuthn` and `confirm`. `/dh-saml/` calls it with the server default and `false`.                                              |
| `saml/saml-auth-module/.../servlets/ConfirmLoginRequest.java` (new) | `GET /dh-saml/confirm?key=`. Calls the shared logic with `forceAuthn=true, confirm=true`. Marks the request ID as confirm in the HttpSession.                                 |
| `saml/saml-auth-module/.../servlets/AssertionConsumer.java`         | Request ID marked confirm: store pending `{key, irisId, expiration}` in the HttpSession and render the page, with no `addNewKey` and no group sync yet. Otherwise: unchanged. |
| `saml/saml-auth-module/.../servlets/ConfirmLogin.java` (new)        | `POST /dh-saml/confirm/decision`. Continue → group sync, `addNewKey`, success page. Cancel → `failNewKey`. Different account → redirect to `/dh-saml/confirm?key=`.           |
| `saml/saml-auth-module/.../SAMLAuthModule.java`                     | Register both servlets. Publish `confirm.url`.                                                                                                                                |
| `saml/saml-common/.../SAMLConstants.java`                           | Paths and the client config property.                                                                                                                                         |
| `saml/saml-auth-module/src/main/resources/webapp/static/saml.css`   | Page styles.                                                                                                                                                                  |

- The assertion comes back to the existing ACS URL. The confirm marker lives
  only in the HttpSession and is never a client parameter.
- POST only, with a CSRF token bound to the HttpSession.
- `X-Frame-Options: DENY` and `frame-ancestors 'none'`.
- HTML-escape the user name.
- Pending entry: one per key, single use, removed on any outcome.
- Confirm time counts toward the session lifetime (`waitforusertimeoutmillis` −
  1 s). `dh` retries its login wait.

## Out of scope

- Confirmation on `/dh-saml/`.
- Sign-in links generated by someone else: `/dh-saml/` still completes silently.
  The fix belongs there (e.g. a one-time secret returned on `redirect` and
  required by `login()`). Separate plan.
- `redirect` on the confirm path.
- Passing an account hint to the IdP.
- SAML single logout.
- Non-SAML methods.

## Done when

- [ ] `/dh-saml/` behaves as before for the web UI, vscode-deephaven and the
      Python `saml()`.
- [ ] `/dh-saml/confirm` sends `ForceAuthn="true"` and shows the page. The key
      binds only after Continue.
- [ ] Cancel makes the waiting login fail immediately.
- [ ] "Use a different account" restarts at the confirm path for the same key.
- [ ] Cross-site POSTs and framing are rejected.
- [ ] `dh` uses `confirm.url` when it is advertised, and the terminal prompt
      otherwise.
