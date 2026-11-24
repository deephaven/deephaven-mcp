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
| `dh console exec` | `[file]` local script path | `-c, --code [code]`; `--console <shortcode-or-id>` | Run a script or inline code on a console. |
| `dh exec` | `[file]` local script path | Same options as `dh console exec` | Top-level alias for `dh console exec`. |

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
| — | `autoDeleteTimeoutSeconds` | Positive integer | Disconnect grace period in seconds (default 600). |

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
  profile; use `--profile` if a shortcode or full ID is ambiguous.
- With no saved selection, one running console is selected automatically.
  Deleted selections are cleared.
- Community has one shared console: list with `dh console`, select with
  `dh console use shared`. Enterprise does not assign `shared` as a new
  shortcode. If an older Enterprise reservation conflicts, use
  `dh console use shared --profile <community-profile>`. Details, settings,
  create, and delete are Enterprise only.
- Create and delete wait for server confirmation. Ctrl-C stops the CLI wait,
  not accepted server work. For an unknown outcome, inspect with
  `dh console detail <shortcode>` before retrying.
- `detail` includes JVM arguments and environment values; treat its output as
  sensitive. JSON results go to stdout; errors and warnings go to stderr.
- `dh console exec <file>` and `dh exec <file>` run the file verbatim relative
  to the invocation directory. `dh exec -c '<code>'` runs inline code;
  `dh exec -c` reads code from piped or redirected stdin. Do not pass a file
  and `-c` together.
- With no `--console`, the profile's current console is used. If it has none,
  one sole running Enterprise console is used. If no Enterprise console is
  running, Enterprise creates one; if multiple Enterprise consoles are running,
  use `--console` to choose one. Community uses its shared console.
  `--console` always targets the named console and does not create it.
- Execution reports created, updated, and removed exports. `-o json` adds the
  console identity and change categories. `-o silent` suppresses success
  output, while warnings and failures remain on stderr.
- Opening an execution session is limited by `DH_TIMEOUT` (seconds, default 30);
  the script itself has no CLI timeout. Interrupting the CLI does not
  necessarily stop a running script on the server.
