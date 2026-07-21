# Migrating a v1 Configuration to v2

Deephaven MCP v1 kept all configuration in one file (named by the
now-removed `DH_MCP_CONFIG_FILE` environment variable). v2 reads a
directory of small files instead. One command converts your old file.

## Get the converter

The converter is a single, standalone Python file — standard library
only, nothing to install. Download it, then run it with any `python3`:

```bash
curl -O https://raw.githubusercontent.com/deephaven/deephaven-mcp/main/scripts/convert_config_v1_to_v2.py
```

(From a repository checkout it is already present at
`scripts/convert_config_v1_to_v2.py`.)

## Convert in three steps

1. **Preview** what will be written (optional, changes nothing):

   ```bash
   python3 convert_config_v1_to_v2.py /path/to/deephaven_mcp.json --dry-run
   ```

2. **Convert.** This writes into the directory the server already reads,
   so the result is used automatically:

   ```bash
   python3 convert_config_v1_to_v2.py /path/to/deephaven_mcp.json
   ```

3. **Validate** the result:

   ```bash
   dhcli config validate
   ```

The converter copies your secrets, splits the old file into the v2
layout, and restricts the new files to your user account (the server
will not start if others can read them). On Windows, POSIX permission
bits do not apply; the converter writes with default ACLs, so verify
the directory is not shared before running the server.

## Options

| Option | Description |
| --- | --- |
| `v1_config` (required) | Path to the v1 file to convert (the one `DH_MCP_CONFIG_FILE` named). |
| `-o`, `--output DIR` | Write to `DIR` instead of the default config directory. |
| `--dry-run` | List the files that would be written, then exit without writing. |
| `-y`, `--yes` | Skip all prompts. Write into an existing directory, but never delete one. |
| `-h`, `--help` | Show usage and exit. |

## Where it writes

With no `--output`, the converter writes to the directory the server
reads by default:

- macOS or Linux: `~/.deephaven/ai/config/`
- Windows: `%APPDATA%/Deephaven/ai/config/`
- If you have set `DH_AI_DATA_DIR`: `$DH_AI_DATA_DIR/config`

If you used `--output DIR`, validate that directory with
`dhcli --config-dir DIR config validate`.

## If you see a warning

The converter resolves the common cases on its own — secrets,
username/password ("Basic") logins, Docker images, and disabled session
creation all convert automatically. It prints a warning only when a
choice is genuinely yours to make, for example a setting with no v2
equivalent or an incomplete TLS client certificate. Each warning names
the exact file and field. Fix those, then re-run `dhcli config validate`.

If a required secret is missing entirely from the old file — an auth type
other than anonymous with no token (or no `*_env_var` reference), a
password login with no password, or a private-key login with no key path
— the converter stops with an error and writes nothing. Add the secret to
the v1 file (inline or as an environment-variable reference) and run the
converter again.
