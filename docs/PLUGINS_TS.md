# TypeScript plugins

Plugins add commands to `dh` that come from your Deephaven server, so the CLI
grows with the plugins installed there. A plugin can add:

- **Object views:** ways to look at a kind of object, as `dh object`
  subcommands. `deephaven.ui` adds `dh object ui` for elements and
  `dh object ui-dashboard` for dashboards.
- **Commands:** new top-level nouns, built on the Deephaven JS API.

There's nothing to install in `dh`: it finds plugins on the server and keeps
them up to date. How it works is in
[TypeScript plugins architecture](PLUGINS_TS_ARCHITECTURE.md); writing one is
in [Writing a TypeScript plugin](PLUGINS_TS_AUTHORING.md).

## Looking at objects

`dh object` lists the objects in the current console. Each shows the
subcommand that displays it: `table` for tables, an object view's name for
types a plugin handles, and otherwise the object's type:

```text
$ dh object
a:table
trades:table
el:ui
dash:ui-dashboard
fig:deephaven.plot.express.DeephavenFigure
mpl:matplotlib.figure.Figure
```

`el` is a `deephaven.ui` element and `dash` a `deephaven.ui` dashboard, which
`dh object ui` and `dh object ui-dashboard` display. No installed plugin
displays `fig` or `mpl`, so they show their type.

With `-o json`, each object also has `command`, which is `null` when nothing
displays it:

```json
[
  {"name": "a", "type": "Table", "command": "table"},
  {"name": "trades", "type": "Table", "command": "table"},
  {"name": "el", "type": "deephaven.ui.Element", "command": "ui"},
  {"name": "dash", "type": "deephaven.ui.Dashboard", "command": "ui-dashboard"},
  {"name": "fig", "type": "deephaven.plot.express.DeephavenFigure", "command": null},
  {"name": "mpl", "type": "matplotlib.figure.Figure", "command": null}
]
```

### An object view: `dh object ui-dashboard`

```text
$ dh object ui-dashboard dash
[dashboard]
  [row]
    [panel] "Trades"
      [table:0] 150 rows, 7 columns
    [stack]
      [panel] "Prices" [active]
        [table:1] 20 rows, 3 columns
      [panel] "Chart"
        [deephaven.plot.express.DeephavenFigure:2]
  [panel] "Controls"
    [picker] "Symbol" selected="AAPL" (12 options)
    [button] "Refresh"

Inspect a nested object: dh object <view> dash --ref <id>
```

`dh object ui el` shows an element the same way, with the element itself at
the top instead of `[dashboard]`.

- Panels, layout and controls are shown as a tree, with the state they start
  in. `dh` opens the object fresh on each run; clicking or typing into it
  isn't supported.
- **Objects inside it are numbered** (`[table:0]`). They usually have no name
  in the console, so the number is how you reach one. Each is labeled the way
  `dh object` would list it: the subcommand that displays it, or its type when
  none does, as for the figure here.
- The hint line appears only when some numbered object has a subcommand.
- With `-o json`, the tree is returned as nested objects instead.

### Nested objects: `--ref`

Every `dh object` subcommand takes `--ref <id>` to act on an object inside
another one:

```text
$ dh object table dash --ref 0 --sort Price --limit 5
```

`dh` opens `dash`, finds its object `0`, and runs `dh object table` on it, with
all of that command's options. The subcommand must be the one the tree's label
names; an object labeled with its type has no subcommand to reach it with.

- Numbers come from the object as it first renders. If the code behind it
  changes, they can change too, so use a number soon after listing it.
- Refs reach one level: the objects the dashboard holds, not objects inside
  those.

## Plugin commands

A plugin's commands appear in `dh --help` and `dh agents` like built-in ones:

```text
$ dh dbserver
NAME       HOST            PORT  TYPE        CLASS
query-1    dh-query-1      8084  DISPATCHER  Query
merge-1    dh-merge-1      8084  DISPATCHER  Merge
all-query                        GROUP       Query
```

They run as you, with your profile's sign-in, and can do what you could do in
the web UI. They can't read your files, other profiles or saved credentials. A
command made for one kind of server fails with `usage` on a profile of the
other kind, for example `dbserver needs an Enterprise profile`.

## Managing plugins

| Command | Purpose |
| --- | --- |
| `dh plugin` | List the plugins `dh` knows about, by where they come from |
| `dh plugin sync` | Fetch the server's plugins now, and the current console's |

```text
$ dh plugin
https://dhe.example.com:8000/iriside/js-plugins/ (commands)
  @deephaven/js-plugin-ui@0.43.1     no commands
https://dhe.example.com:8000/iriside/worker-kind/DeephavenCommunity/plugins/ (commands)
  @example/dh-dbserver@1.2.0         commands: dbserver
console k7m2qd: https://dhe.example.com:8000/worker/w1/js-plugins/ (object views)
  @deephaven/js-plugin-ui@0.44.0     object views: ui, ui-dashboard
  @example/dh-dbserver@1.2.0         no object views
  @example/js-plugin-heatmap@0.3.0   not used: needs dh >=3.4.0
```

- **Sources:** commands come from the server, and object views from the
  worker of the current console. Each line shows what the package adds from
  that source; one package can appear in several. On Community the server is
  both, so its heading says `(commands, object views)`.
- **Plugins that can't be used** stay listed with the reason: `needs dh
  <range>`, `unsupported api <n>`, `unsupported spec <n>`, `invalid:
  <reason>`, or `conflicts with <package>`.
- Only packages with a CLI side are listed.
- **Keeping up to date:** `dh` checks the server for new and changed plugins
  after other commands, at most once every `DH_PLUGIN_REFRESH` hours (default
  24; `0` checks after every command, `off` never), in the background, so
  commands never wait for it. `dh object` always checks the console's worker
  before it runs. `dh auth login` and `dh plugin sync` check right away.

## Errors

| Code | Exit | Meaning |
| --- | --- | --- |
| `plugin_failed` | 6 | The plugin's code failed. The message is the plugin's; `DH_DEBUG=1` adds its stack |
| `plugin_invalid` | 2 | The plugin's files are malformed, or its module doesn't match its spec |
| `plugin_unavailable` | 2 | The plugin's files are gone from the cache or the server. `dh plugin sync` |
| `not_found` | 2 | A plugin reported something missing, such as a `--ref` number the object doesn't have |
| `conflict` | 2 | A plugin reported a conflicting state |
| `permission_denied` | 4 | A plugin reported that you aren't allowed to do it |
| `usage` | 2 | An unknown `dh object` subcommand, a `--ref` whose object the subcommand doesn't handle, or a command for the other kind of server |

A plugin that reaches for something it isn't allowed, such as a credential,
fails with `plugin_failed` and a message naming it, for example
`EnterpriseClient.createAuthToken is not available to plugins`.

## Environment variables

| Variable | Effect |
| --- | --- |
| `DH_PLUGIN_REFRESH` | Hours between background checks (default 24); `0` after every command; `off` never |
| `DH_PLUGIN_PACKAGES` | Local plugin packages to use instead of the server's, for plugin authors (see [Writing a TypeScript plugin](PLUGINS_TS_AUTHORING.md#testing-locally)) |
| `DH_DEBUG=1` | Show plugin stacks and background check failures |
