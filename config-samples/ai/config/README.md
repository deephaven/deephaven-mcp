# Example Deephaven MCP Configuration Tree

This directory mirrors the on-disk layout that `dh-mcp-systems-server`
reads at startup. Copy any of the files you need into your own config
directory and edit them in place.

## Default Location

- POSIX (Linux/macOS): `~/.deephaven/ai/config/`
- Windows: `%APPDATA%/Deephaven/ai/config/`

Override with the `DH_AI_DATA_DIR` environment variable or the
`--config-dir` CLI flag.

## Layout

```text
config_dir/
├── server.json                      # PSK (HTTP transport only)
├── cli.json                         # optional; dhcli CLI defaults (see docs/CLI.md)
├── community/
│   ├── settings.json                # optional
│   └── sessions/
│       ├── local_dev.json           # one file per static session
│       └── ...                      # filename stem == session name
└── enterprise/
    ├── settings.json                # optional; carries enterprise-wide timeouts
    └── systems/
        ├── prod.json                # one file per enterprise system
        └── ...                      # filename stem == system_name
```

Every section is optional, but the server requires at least one
session or enterprise system to be configured. Permissions on the
config directory are audited at startup (POSIX-strict, Windows
best-effort).

## Files in This Example

- `server.json` — PSK material for the HTTP transport.
- `cli.json` — `dhcli` CLI defaults (output format, daemon control, request timeouts, docs server, sticky context); all defaults shown.
- `community/settings.json` — community-wide settings + session_creation defaults.
- `community/sessions/local_dev.json` — a static community session.
- `enterprise/settings.json` — enterprise-wide settings (timeouts; may be `{}` to take defaults).
- `enterprise/systems/prod.json` — a password-auth enterprise system.
- `enterprise/systems/staging.json` — a private-key enterprise system.

See `docs/CONFIGURATION.md` for the full schema reference (the
authoritative end-user guide); `docs/ENV.md` documents the single
`DH_AI_DATA_DIR` environment variable.
