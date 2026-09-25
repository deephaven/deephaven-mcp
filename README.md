# dh — Deephaven CLI

A single compiled binary, built with Deno, that updates itself from a central
manifest.

```sh
deno task build          # compile for this platform -> dist/
deno task test           # compiles two versions and proves the binary updates itself
deno task demo:update    # same, but leaves a server running so you can try it by hand
deno task check          # fmt + lint + type-check
```

## Auto-update

After a run, `dh` fetches the manifest, at most once per `DH_UPDATE_INTERVAL`
(default 24 hours). If the manifest lists a newer version for this platform,
`dh` downloads it, checks the SHA-256, and swaps the new binary in place of its
own executable. The time of the last check is kept in
`<executable>.last-update-check`.

```json
{
  "version": "0.2.0",
  "binaries": {
    "aarch64-apple-darwin": {
      "url": "dh-aarch64-apple-darwin",
      "sha256": "..."
    }
  }
}
```

`deno task build` writes `dist/manifest.json` next to the binary. To publish,
upload both files to the update location.

| Env var              | Effect                                                |
| -------------------- | ----------------------------------------------------- |
| `DH_UPDATE_URL`      | Manifest URL (default in `src/updater.ts`)            |
| `DH_UPDATE_INTERVAL` | Hours between update checks (default 24; 0 = always)  |
| `DH_AUTO_UPDATE=off` | Disable auto-update                                   |
| `DH_DEBUG=1`         | Print update errors (otherwise updates fail silently) |

Update URLs must use HTTPS, except `localhost`/`127.0.0.1`. When `dh` runs from
source (`deno task dev`), it never updates itself.
