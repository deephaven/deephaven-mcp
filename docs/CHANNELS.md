# Release channels

Each install of `dh` follows one release channel and updates to that channel's
build. `release` is the default; `rc` carries release candidates; `alpha-<id>`
channels carry experiments. How channels are published is covered in
[Release architecture](RELEASE_ARCHITECTURE.md).

## Commands

| Command | Argument | Options | Purpose |
| --- | --- | --- | --- |
| `dh channel` | — | — | Show the channel this install follows. |
| `dh channel use` | `<channel>` channel name | — | Follow a channel and update to its build now. |

`dh channel use` checks the channel before saving anything, so a misspelled or
unpublished channel leaves the install as it was. `dh channel use release`
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
prereleases included), set `DH_INSTALL_VERSION`. To stay on it, also set
`DH_AUTO_UPDATE=off`; otherwise the next `dh` command updates to the channel's
build.

## JSON output

| Command | stdout |
| --- | --- |
| `dh channel` | `{"channel", "build", "version"}` |
| `dh channel use` | `{"channel", "from", "to"}` |

`channel` is the channel the install follows; `build` and `version` are the
running build's channel and version. On a `dev` build, `channel` is `dev` too.
In `dh channel use`, `from` equals `to` when the install was already current.

## Behavior and limits

- `dh channel use` updates the channel in `dh.meta`. If the file is missing or
  unreadable, the install follows `release`.
- `dh` installs its channel's build whenever the version differs, even if it is
  lower, so switching channels can move to an older version.
- If the running build is from another channel than the one followed, for
  example after a failed download or with a pinned version, `dh channel` says
  so and how to fix it:

  ```
  rc
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
  update, and `dh channel` shows `dev (never updates)`. `dh channel use` needs
  an installed `dh`; on a local `bun run build` binary it installs the
  channel's build over it.
- Errors: an invalid name or `dev` fails with `usage`; a channel with no
  published build fails with `channel_not_found` (exit 2); a failed download
  fails with `server_unreachable`. If the download fails after the channel is
  saved, it installs at the next update check.
