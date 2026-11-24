# Release channels

Each install of `dh` follows one release channel and updates to that channel's
build. `release` is the default; `rc` carries release candidates; `alpha-<id>`
channels carry experiments. How channels are published is covered in
[Release architecture](RELEASE_ARCHITECTURE.md).

## Commands

| Command | Argument | Options | Purpose |
| --- | --- | --- | --- |
| `dh channel` | — | — | List `release` and `rc` with their current versions, marking the channel this install follows. |
| `dh channel use` | `<channel>` channel name | — | Follow a channel and update to its build now. |

```
$ dh channel
● release   3.1.1
  rc        3.2.0-rc.3
```

`●` marks the channel this install follows. Only `release` and `rc` are
listed, plus the followed channel when it's neither. An install following an
`alpha-<id>` channel you were given shows it as a third row:

```
$ dh channel
  release     3.1.1
  rc          3.2.0-rc.3
● alpha-bar   3.2.1-alpha-bar.0
```

Other `alpha-<id>` channels are never listed: you follow one only when someone
gives you its name. Versions come from each channel's
`manifest.json`: an unpublished channel shows `not published`, and one that
can't be checked shows `?`, with a note naming it. The command still succeeds.

`dh channel use` checks the channel and downloads its build before saving
anything, so a misspelled or unpublished channel, or a failed download, leaves
the install as it was. `dh channel use release`
switches back to the default.

## Installing on a channel

Set `DH_CHANNEL` when installing to follow that channel: the install script
installs the channel's current build and saves the channel in `dh.meta`, a
small file next to the executable (`dh.exe.meta` on Windows), so every later
update comes from it. Without `DH_CHANNEL`, the install follows
`release`.

```sh
curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-release/install.sh | DH_CHANNEL=rc sh
```

```powershell
$env:DH_CHANNEL = 'rc'; irm https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-release/install.ps1 | iex
```

> [!NOTE]
> Only the install scripts read `DH_CHANNEL`: setting it before a `dh` command
> changes nothing. To switch an installed `dh`, use `dh channel use`.

To try a channel side by side, install a second copy with its own
`DH_INSTALL_DIR`. Each copy follows its own channel and keeps its own profiles.

To install a specific version instead (`3.1.1`, `v3.1.1` or `cli-v3.1.1`,
pre-releases included), set `DH_INSTALL_VERSION`. To stay on it, also set
`DH_AUTO_UPDATE=off`; otherwise the next `dh` command updates to the channel's
build.

## JSON output

| Command | stdout |
| --- | --- |
| `dh channel` | `{"channels": [{"channel", "version", "following"}], "build", "version"}` |
| `dh channel use` | `{"channel", "from", "to"}` |

In `dh channel`, `channels` lists the same rows as the human output, in the
same order; `version` is `null` when the channel isn't published or couldn't be
checked, and `following` is `true` on the followed channel. `build` and
`version` at the top level are the running build's channel and version. On a
`dev` build, `channels` is empty and `build` is `dev`.

In `dh channel use`, `from` equals `to` when the install was already current.

## Behavior and limits

- `dh channel use` updates the channel in `dh.meta`. If the file is missing or
  unreadable, the install follows `release`.
- `dh` installs its channel's build whenever the version differs, even if it is
  lower, so switching channels can move to an older version.
- If the running build is from another channel than the one followed, for
  example after installing a pinned version or testing with `DH_UPDATE_URL`, `dh channel` says
  so and how to fix it:

  ```
    release   3.1.1
  ● rc        3.2.0-rc.3
  ! Running 3.1.0 from the release channel, not rc.
    Run `dh channel use rc` to install rc's build now.
  ```

- `dh channel use` always installs the channel's current build if the version
  differs, even when the install already follows that channel, so it also works
  as a manual update while `DH_AUTO_UPDATE=off`.
- Builds on channels other than `release` show a `Channel:` line in `dh --help`
  and keep profiles in their own folder (`~/.deephaven/cli-rc`, …; see
  [Where profiles are stored](AUTH.md#where-profiles-are-stored)). Each starts
  empty, so sign in once per channel.
- `bun run dev` and local `bun run build` binaries are `dev` builds: they never
  update, `dh channel` shows `dev (never updates)`, and `dh channel use` fails
  with `usage`. Install `dh` with the install script to follow a channel.
- `dh channel use` errors:
  - `usage`: an invalid channel name, the reserved name `dev`, or a dev build.
  - `channel_not_found` (exit 2): the channel has no published build.
  - `server_unreachable`: the manifest or the build couldn't be downloaded.
    Nothing is saved; run `dh channel use` again.
