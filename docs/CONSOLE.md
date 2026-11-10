# Interactive consoles

`dh console` lists CLI-created Enterprise consoles; `●` marks the console in
use. Shortcodes are reserved permanently, including after a failed create or
delete.

## Commands

| Command | Argument | Options | Purpose |
| --- | --- | --- | --- |
| `dh console` | — | — | List consoles. |
| `dh console detail` | `[console]` shortcode or full server ID | — | Show settings and status; with no target, show the selected console. |
| `dh console use` | `<console>` shortcode or full server ID | — | Select a console for its profile. |
| `dh console create` | `[settings]` JSON object | Settings below; `--json <settings>`; `--no-use` | Create and select a console unless `--no-use` is set. |
| `dh console default` | `[settings]` JSON object | Settings below; `--json <settings>`; `--reset` | Show or change global creation defaults. |
| `dh console delete` | `[console]` shortcode or full server ID | `-f, --force` | Delete the selected console or target; prompts unless forced. |

`--profile <name>` applies to every command. `use` changes only that profile's
console, not the default profile.

## Creation settings

| CLI option | JSON field | Type | Meaning |
| --- | --- | --- | --- |
| `--language` | `language` | `python` or `groovy` | Script language. |
| `--heap-size-gb` | `heapSizeGb` | Positive number | Heap size in GB. |
| `--server` | `server` | String | Query server name. |
| `--engine` | `engine` | String | Worker engine name. |
| — | `jvmArgs` | Array of strings | JVM arguments. |
| — | `environment` | String-to-string object | Environment variables. |
| — | `autoDeleteTimeoutSeconds` | Positive integer | Auto-delete timeout. |

JSON may be positional or passed with `--json`; flags override matching JSON
fields. For example:

```sh
dh console create --json '{"language":"python","heapSizeGb":8,"environment":{"MODE":"test"}}'
```

Defaults are global across profiles and validated against each create's server.
In JSON defaults, `null` resets a field to the server value; `--reset` clears
all overrides.

## Behavior and limits

- Each profile's selection is keyed by server, login user, and operate-as user.
  Renaming a profile preserves it. A known shortcode or full ID resolves its
  profile; use `--profile` if a full ID is ambiguous.
- With no saved selection, one running console is selected automatically.
  Deleted selections are cleared.
- Community has one shared console: list with `dh console`, select with
  `dh console use shared`. Details, settings, create, and delete are Enterprise
  only.
- Create and delete wait for server confirmation. Ctrl-C stops the CLI wait,
  not accepted server work. For an unknown outcome, inspect with
  `dh console detail <shortcode>` before retrying.
- `detail` includes JVM arguments and environment values; treat its output as
  sensitive. JSON results go to stdout; errors and warnings go to stderr.
