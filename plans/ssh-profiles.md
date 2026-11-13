# SSH-only profiles

Setting up access to a cluster without a web sign-in. Follows from
[cluster-plugins.md](cluster-plugins.md), whose `shell` plugins are
discovered and run over SSH and must keep working when the web server is
down. Today every profile comes from `dh auth login`, which needs the web
server to find the server's address, its kind and how to sign in, and then
to sign in. So an admin who needs `dh ctl` while the web server is down, or
who has SSH but no Deephaven account, can't set anything up.

Status: design, not started.

## What a tool run needs

A `shell` plugin over SSH needs the cluster's identity (to find
`servers.<origin>.ssh` and the `shell/` cache) and SSH access. It doesn't need
a Deephaven credential: the tools authenticate themselves, as the service
account the spec's `tool.runAs` names (`sudo -u irisadmin`, which uses the
`iris` key), or with the user's own `--key` or `--user`
([cluster-plugins.md](cluster-plugins.md#running-as-a-service-account)). A
profile is only how a command finds the cluster.

## Model

A **profile** is how this machine reaches one cluster: a Deephaven
sign-in, SSH access, or both.

| Profile has       | `js` plugins, built-ins that sign in | `shell` plugins |
| ----------------- | ------------------------------------ | -------------- |
| Sign-in only      | yes                                  | after `dh plugin ssh enable` |
| SSH only          | no: `auth_required` (see below)      | yes            |
| Both              | yes                                  | yes            |

- One selector for every command stays: `--profile`, `DH_PROFILE`, the
  default profile. `dh auth` lists every profile, SSH-only ones included.
- SSH settings stay per server (`servers.<origin>.ssh`), shared by every
  profile on the cluster, as in cluster-plugins.md. An SSH-only profile is a
  profile on a server whose `ssh.enabled` is true and that has no sign-in.
- Prior art: kubectl contexts are a cluster plus an optional user, and
  clusters exist without users (`kubectl config set-cluster`); a Docker
  context can be nothing but an SSH endpoint (`docker context create
  --docker host=ssh://…`).

Rejected: a separate `--server` selector (or "cluster" entries) for `shell`
plugins. It would be a second way to pick a target, with its own default and
listing, that every plugin command, `dh agents` and the docs must explain,
and a user with both would have to keep two defaults in step.

## The cluster's address over SSH

Profiles are keyed by the server's origin, which login normally finds by
probing ports over the web. Without the web, `dh` reads it from the node:
the DHE installer writes `/etc/deephaven/FRONTEND_HOST` (the FQDN clients
use: `DH_ENVOY_FQDN` with Envoy, else `DH_WEB_FQDN`) and
`/etc/deephaven/FRONTEND_PORT` on every node. The origin is
`https://<FRONTEND_HOST>:<FRONTEND_PORT>`, the same one a web login finds
for that address.

If they're missing or unreadable, the address the user typed is used, if it
has a port (`https://dhe.example.com:8000`); without one, `usage`: `Can't
tell which port dhe.example.com's web server uses; give the full URL, e.g.
https://dhe.example.com:8000`. `dh` never guesses a port.

On a node, `dh` reads the same two files locally. That answers
cluster-plugins.md's "this cluster" question: the active profile is on this
cluster when its origin equals the local one.

## Creating one: `dh auth login --method ssh`

```text
dh auth login <server> --method ssh [--ssh-host <host>]… [--name <name>] [--[no-]default]
```

`ssh` joins `--method`'s choices. Interactively, it's also offered when the
web server can't be reached:

```text
$ dh auth login dhe.example.com
✖ Could not reach dhe.example.com over the web (timed out).
? Set up SSH access to this cluster instead? Admin tools only, no Deephaven sign-in. (Y/n)
? SSH host (dhe.example.com):
✔ Connected to dhe.example.com as admin
✔ Cluster https://dhe.example.com:8000 (Deephaven Enterprise 2026.01.261102164822g2f6a8ebf11)
✔ Plugins: ctl, cluster, iriscat, crcat
✔ Saved profile "dhe.example.com:ssh" (default)
```

Without a terminal or with `--no-prompt`, nothing is offered: an
unreachable server is `server_unreachable` as today, with the hint `` Or set
up SSH access: `dh auth login <server> --method ssh` ``.

Steps (questions first, then actions, as in "Commands are questions and
actions" in [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)):

1. SSH hosts: `--ssh-host` (repeatable), else asked, defaulting to the typed
   address's hostname; with `--no-prompt`, that default.
2. Connection check: `dh plugin check`'s host and connect steps. `BatchMode`
   only with `--no-prompt`, so a person can answer a password or 2FA prompt
   (the connection is then reused, `ControlMaster`).
3. One `ssh` run reads `FRONTEND_HOST`, `FRONTEND_PORT` and copies `shell/`:
   the cluster-plugins.md copy script, preceded by two lines with the
   files' contents (empty if missing).
4. No `shell/` bundle there: `server_unsupported`, `No dh plugins at
   /usr/illumon/latest/dh-cli on <host>`, with the hint that this DHE
   release may predate them, or to set the path with `--ssh-bundle <path>`.
   Nothing is saved: an SSH-only profile with no plugins does nothing.
5. Save, under the lock: the server entry (`kind: "enterprise"`, `ssh:
   {enabled: true, hosts, defaultHost}`, plus `bundle` if given), the cache,
   and the profile. Default profile rules as for any login.

The profile: `{name, server, method: "ssh", createdAt}`, no `user`, and no
credential in `credentials.json`. Name: `<FRONTEND_HOST>:ssh` unless
`--name`; made unique as login does today.

An existing server entry is reused (its `caCert`, other profiles, plugin
overrides); `ssh.hosts` is replaced only if `--ssh-host` was given.

## Using one

- `shell` plugins: as in cluster-plugins.md.
- `js` plugins: not registered. The `js` manifest is still fetched when the
  web server answers (the fetch is anonymous), so `dh plugin list` can name
  them; plugins limited by `groups` aren't named, since the user's groups
  aren't known without a sign-in.
- Built-in commands that sign in (`requireAuth`): `auth_required`,
  `` Profile "dhe.example.com:ssh" has SSH access only `` with the hint
  `` To sign in: `dh auth login --profile dhe.example.com:ssh` ``.
- `dh auth` / `dh auth status`: method `ssh`, the SSH hosts (default first),
  and the cluster version from the cache. With `-o json`, `"method": "ssh"`,
  `"user": null`, `"sshHosts": […]`.
- `dh plugin list`: with no sign-in, ends with
  `` Sign in to use this server's other plugins: `dh auth login --profile <name>` ``
  naming the plugins from the `js` manifest, when it was fetched.
- `dh auth logout --profile <name>`: deletes the profile; closes the
  server's SSH masters if no other profile uses the server. The server's
  `ssh` settings and caches stay, as with any logout.

## Adding a sign-in later

`dh auth login --profile <name>` on an SSH-only profile signs in as login
does today (resolving the server over the web, asking the method) and adds
the sign-in to that profile: `user`, `method`, and the credential. The name,
default status and the server's `ssh` settings are unchanged.

If the web login resolves a different origin than the profile's (the user
reaches the web server through another name than `FRONTEND_HOST`), the
server entry is re-keyed to the resolved origin: its settings and caches
move, and every profile on it follows. A server entry already at that origin
is `usage`: `` Both https://a and https://b are saved; log out of one first ``.

`dh auth login <server>` without `--profile`, resolving to an origin whose
only profile is SSH-only, asks `` Add a sign-in to profile "<name>"? ``
(default yes, flag `--profile`); without a terminal, it creates a new
profile as today.

The reverse, SSH for a signed-in profile, is `dh plugin ssh enable`
(cluster-plugins.md). Removing SSH from a profile that has both is
`dh plugin ssh disable`; removing the sign-in alone isn't offered (log out,
then `--method ssh`).

## Errors

| Code                 | Exit | When                                                                                             |
| -------------------- | ---- | ------------------------------------------------------------------------------------------------ |
| `auth_required`      | 4    | Existing; now also a command that signs in, run with an SSH-only profile. Hint as above           |
| `server_unsupported` | 5    | Existing; now also `--method ssh` finding no bundle                                               |
| `usage`              | 2    | No port known for the cluster (no `FRONTEND_*` files and none typed); re-keying onto a saved origin |

## Decisions

1. **SSH-only is a kind of profile, not a separate target.** One selector,
   one default, one listing. Rejected: a `--server` selector for `shell`
   plugins (see [Model](#model)).
2. **The cluster names itself.** `FRONTEND_HOST`/`FRONTEND_PORT` give the
   origin a web login would find, so an SSH-only profile and a later sign-in
   share one server entry. Rejected: keying SSH-only profiles by SSH host
   (they'd never match a web login), and guessing a port.
3. **`--method ssh` on `dh auth login`, offered when the web is
   unreachable.** Login is already the one place access is set up, and
   agents already know its flags. Rejected: a new `dh auth ssh` or `dh
   cluster add` command.
4. **No plugins, no profile.** `--method ssh` fails if the node has no
   bundle, rather than saving a profile that can do nothing.

## Open questions

1. Confirm on a real DHE install that `/etc/deephaven/FRONTEND_HOST` and
   `FRONTEND_PORT` exist on every node and are world-readable (the docs
   assistant says so; not checked), and whether `https` is always right.
2. Should `--method ssh` be in the interactive method picker when the web
   server *is* reachable (an admin who wants SSH only)? Proposed: no;
   `--method ssh` covers it.

## Validation

- [x] `dh auth login dhe.example.com --method ssh --ssh-host infra-1
      --no-prompt`, with a fake `ssh` whose node has `FRONTEND_HOST`
      `dhe.example.com`, `FRONTEND_PORT` `8000` and `examples/dh-cli/shell/`:
      saves profile `dhe.example.com:ssh` on `https://dhe.example.com:8000`
      with `ssh.hosts` `["infra-1"]`, makes no HTTP request, and `dh ctl …`
      then runs over SSH.
- [x] The same without the `FRONTEND_*` files and with `dhe.example.com`
      typed: `usage` naming the full-URL form; with
      `https://dhe.example.com:8000` typed: saved on that origin.
- [x] No bundle on the node: `server_unsupported`, nothing saved.
- [ ] In a terminal, `dh auth login dhe.example.com` against an unreachable
      web server offers SSH; with `--no-prompt` it's `server_unreachable`
      with the `--method ssh` hint. Only the
      `--no-prompt` half is tested; the tests have no terminal.
- [x] With an SSH-only default profile, `dh dbserver …` isn't registered and a
      built-in that signs in gives `auth_required` with the
      `dh auth login --profile` hint.
- [x] `dh auth -o json` lists the profile with `"method": "ssh"` and
      `"user": null`.
- [x] `dh auth login --profile dhe.example.com:ssh --method password …`
      adds the sign-in: same name, `ssh` settings unchanged, `dbserver` now
      registered. Tested
      with `--method private-key`: a password login uploads a new key, which
      the fake server doesn't do.
- [x] That login resolving `https://dhe-web.example.com:8000` instead
      re-keys the server entry and its caches; both profiles on it follow.
- [x] `dh auth logout --profile dhe.example.com:ssh` deletes the profile and
      keeps `servers.<origin>.ssh`.
- [ ] On a node whose `FRONTEND_*` files match the active profile's origin,
      `shell` plugins come from the local bundle; for another cluster's
      profile, over SSH. Not tested: it needs
      `/etc/deephaven/` and `/usr/illumon/` on the test machine. Phase 4 checks
      it on a real node.

## Relevant Files

- `src/actions/auth/store.ts`: `Method` gains `ssh`; `Profile.user` optional;
  re-keying a server entry.
- `src/commands/auth/login.ts`, `src/commands/auth/questions.ts`:
  `--method ssh`, `--ssh-host`, `--ssh-bundle`, the offer on
  `server_unreachable`, adding a sign-in to an SSH-only profile.
- `src/actions/auth/flows.ts`: the `ssh` flow (questions, then actions).
- `src/actions/auth/session.ts`: `requireAuth`'s `auth_required` for SSH-only
  profiles.
- `src/commands/auth/status.ts`, `src/commands/auth/index.ts`,
  `src/commands/auth/logout.ts`: SSH-only display and logout.
- `src/actions/plugins/ssh.ts`: the setup run (`FRONTEND_*` plus the copy).
- `src/actions/plugins/settings.ts`: "this cluster" from the local `FRONTEND_*`
  files.
- `src/errors.ts`: hints for the new cases.
- `docs/AUTH.md`: SSH-only profiles.
- `plans/cluster-plugins.md`: link here from its open questions; replace
  its "this cluster" rule.
