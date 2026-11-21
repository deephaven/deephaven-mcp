# Release channels

Each install of `dh` follows one release channel and updates to that channel's
build. `release` is the default; `rc` carries release candidates; `alpha-<id>`
channels carry experiments. How channels are published is covered in
[Release architecture](RELEASE_ARCHITECTURE.md).

## Commands

| Command | Argument | Options | Purpose |
| --- | --- | --- | --- |
| `dh channel` | — | — | Show the channel this install follows, and whether it was saved or is the default. |
| `dh channel use` | `<channel>` channel name | — | Follow a channel and update to its build now. |

`dh channel use` checks the channel before saving anything, so a misspelled or
unpublished channel leaves the install as it was. `dh channel use release`
switches back to the default.

## Installing on a channel

The install scripts read `DH_CHANNEL` (default `release`); `dh` itself ignores
it.

```sh
curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-release/install.sh | DH_CHANNEL=rc sh
```

```powershell
$env:DH_CHANNEL = 'rc'; irm https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-release/install.ps1 | iex
```

To try a channel side by side, install a second copy with its own
`DH_INSTALL_DIR`; each copy follows its own channel. `DH_INSTALL_VERSION` pins a
version instead (`3.1.1`, `v3.1.1` or `cli-v3.1.1`, prereleases included); the
install still follows `DH_CHANNEL` from its next update.

## JSON output

| Command | stdout |
| --- | --- |
| `dh channel` | `{"channel", "source": "saved" \| "default", "build"}` |
| `dh channel use` | `{"channel", "from", "to"}` |

`channel` is the channel the install follows; `build` is the running build's
channel (`dev` when run from source). In `dh channel use`, `from` equals `to`
when the install was already current.

## Behavior and limits

- The channel is saved in `dh.meta` (`dh.exe.meta` on Windows) next to the
  executable. The install scripts write it; an unreadable file means `release`.
- `dh` installs its channel's build whenever the version differs, even if it is
  lower, so switching channels can move to an older version.
- The running build keeps its own channel until it is replaced: after
  `dh channel use release`, an `rc` build stays until the release build installs.
  `dh channel` says so.
- Builds on channels other than `release` show a `Channel:` line in `dh --help`
  and keep profiles in their own folder (`~/.deephaven/cli-rc`, …; see
  [Where profiles are stored](AUTH.md#where-profiles-are-stored)). Each starts
  empty, so sign in once per channel.
- `bun run dev` and local `bun run build` binaries are `dev` builds: they never
  update, and `dh channel use` needs an installed `dh`.
- Errors: an invalid name or `dev` fails with `usage`; a channel with no
  published build fails with `channel_not_found` (exit 2); a failed download
  fails with `server_unreachable`. If the download fails after the channel is
  saved, it installs at the next update check.
