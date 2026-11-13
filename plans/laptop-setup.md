# Laptop setup: the SSH offer at login

On a laptop, plugins for tools that live on the server start out `local`
and can't run. `dh plugin list` says so, and `dh auth login` to an
Enterprise server offers, once, to run them on that server over SSH. Design:
[the design doc](https://claude.ai/artifact/QbDVebtCpHu33kaHU8nAEV)
(Walkthrough 1). Builds on [ssh-plugins.md](ssh-plugins.md) and
[ssh-hosts.md](ssh-hosts.md); comes after both, and after
[versions.md](versions.md).

Status: implemented, and tested against fake Enterprise and Community servers (`tests/auth/login_plugins.test.ts`).

## Contract

### "Not installed here" in `dh plugin list`

- A `local` plugin whose tool (the machine's `path`, else the spec's
  `tool.path`) doesn't exist (`stat` fails with `ENOENT`) gets
  ` (not installed here)` at the end of its human line:
  `  ctl   local: /usr/illumon/latest/bin/dhctl (not installed here)`.
- JSON entries add `"installed"`: `true` / `false` for `local`, `null` for
  `ssh`.
- When one or more plugins show it, a last line:
  `` ! Their tools aren't on this machine. To run them on a server: `dh plugin set <name> --exec ssh` ``.

### The offer

Asked by `dh auth login`, with the other questions (after sign-in succeeds,
before anything is saved; see "Commands are questions and actions" in
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)), when all hold:

1. The server's kind is `enterprise`.
2. There are **candidates**: loaded plugins with no `plugins.<name>.exec`
   setting, whose tool isn't installed here (as above), and not in
   `config.sshOfferDeclined`.
3. The prompter is interactive (a terminal, no `--no-prompt`), or
   `--plugins` was given.

Question (`ui.confirm`, default yes, flag `--plugins`):
`` Run these plugins' tools (<n1>, <n2>, …) on this server over SSH? `` with
the candidate names, sorted.

- Yes (or `--plugins ssh`): the save also writes
  `plugins.<name>.exec = "ssh"` for each candidate. The human output adds
  `` ✔ <n1>, <n2> run on <origin> over SSH (check with `dh plugin check <name>`) ``.
- No (or `--plugins local`): the save adds the candidate names to
  `config.sshOfferDeclined` (a new optional `string[]` on `Config` in
  `src/auth/store.ts`, kept sorted and unique), so later logins don't ask
  about them again.
- Not asked (no candidates, a Community server, or not interactive without
  `--plugins`): nothing changes; login never fails over this.
- `--plugins <ssh|local>`: new `dh auth login` option (commander
  `.choices(["ssh", "local"])`), help
  `For plugins whose tools aren't on this machine: run them on the server over SSH (ssh), or don't ask (local).`
  On a Community server, or with no candidates, it's ignored.
- `-o json` login output adds `"pluginsOverSsh"`: the names set to `ssh`
  (`[]` when none).

## Checklist

- [x] `installed` and the "not installed here" text and footer in `dh plugin list`
- [x] `Config.sshOfferDeclined`
- [x] The offer in `dh auth login`: conditions, question, `--plugins`, saving with the sign-in
- [x] Login output: human line and `pluginsOverSsh`
- [x] Tests for every automated Validation item
- [x] `plans/plugins.md`: Commands table (`--plugins` on login), decision 12 (SSH stays explicit; the login offer is one more explicit way)

## Validation

- [x] `bun run check` and `bun test` pass
- [x] `dh plugin list` with a spec whose `tool.path` doesn't exist: ` (not installed here)`, `"installed": false`, and the footer; with one that exists: neither
- [x] `dh auth login <enterprise> --plugins ssh` (fixture Enterprise server as in `tests/auth`) with two candidates writes `exec: "ssh"` for both, `pluginsOverSsh` lists them; `--plugins local` writes `sshOfferDeclined` and no `exec`
- [x] A plugin with an `exec` setting, one whose tool exists, and one in `sshOfferDeclined` are never candidates
- [x] Without a terminal and without `--plugins`, login succeeds and writes nothing about plugins
- [x] A Community login never asks and ignores `--plugins`
- [x] Interactive (fake `Prompter`): the question lists the candidates sorted; answering no, then logging in again, doesn't ask

## Relevant Files

- `src/auth/store.ts` — `Config.sshOfferDeclined`
- `src/commands/plugin/index.ts` — `list`: `installed`, text, footer
- `src/commands/auth/login.ts` — `--plugins`; ask and save with the sign-in
- `src/commands/auth/questions.ts` — the offer question
- `src/auth/flows.ts` — `commitSignIn` saves the plugin settings in the same update
- `src/plugins/settings.ts` — candidates (installed check, no `exec`, not declined)
- `src/main.ts` — `pluginGroup`'s loaded plugins reachable from login (pass the `PluginLoad`, or load specs in the action)
- `tests/plugins.test.ts`, `tests/auth/` (login tests)
- `plans/plugins.md`
