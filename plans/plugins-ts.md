# TypeScript plugins

Let Deephaven web plugins carry a CLI side that adds `dh object` views and
top-level commands, discovered from the servers that already serve web plugins
and run in a sandbox that reaches the server only through the JS API. The
design is in [TypeScript plugins architecture](../docs/PLUGINS_TS_ARCHITECTURE.md);
the user-facing surface is in [TypeScript plugins](../docs/PLUGINS_TS.md) and
[Writing a TypeScript plugin](../docs/PLUGINS_TS_AUTHORING.md). Those docs ship
in PR 1 and describe the finished behavior: the implementation must match
them, or change them in the same PR and say so.

Names, formats, paths and output below are exact; internal function and module
names are up to the implementer, matching existing patterns. Nothing has
shipped, so nothing migrates.

## Assumed: `dh object`

The `dh object` design is a separate PR that lands first. This plan relies on
exactly this from it:

- `dh object` lists the target console's named objects, one per line as
  `<name>:<label>`, and with `-o json` as an array of `{name, type}`, where
  `type` is the server's type string (`Table`, `deephaven.ui.Dashboard`).
- `dh object table <name>` shows a table, with its own options. Built-in
  subcommands for other non-widget types, if that design adds them, behave the
  same here.
- The target console is chosen as for `dh exec` (`--console`, the current
  console, …), and `dh console object` is the same command.

## Implementation

### 1. Sandbox and JS API boundary (PR 2)

**Engine**

- `quickjs-emscripten-core` with `@jitl/quickjs-wasmfile-release-sync` (no
  asyncify), versions pinned. The `.wasm` is embedded in the compiled binary
  (`import … with { type: "file" }`) and read from Bun's virtual file system.
- One runtime per plugin invocation. Memory limit 256 MiB
  (`setMemoryLimit`), stack 1 MiB (`setMaxStackSize`). An interrupt handler
  stops any synchronous stretch of plugin code longer than `timeoutMs()`
  (`DH_TIMEOUT`, default 30 s): `plugin_failed`,
  `<package>: plugin code ran for more than <n> s without yielding`. Ctrl-C
  sets `ctx.signal.aborted`, interrupts running plugin code, and ends as
  `cancelled`, as for built-in commands.
- No module loader: the module is evaluated as an ES module, and any `import`
  in it, static or dynamic, fails.
- **Teardown,** in order: remove every listener the runtime registered,
  release every stand-in, dispose the runtime, then disconnect what `ctx`
  opened. A listener call arriving after teardown is dropped.

**The boundary**

- Each JS API value handed to plugin code is a stand-in carrying a declared
  type. Each property read, property write and method call is checked against
  that type's members; anything else throws a `TypeError` in the plugin with
  exactly `<Type>.<member> is not available to plugins`.
- **Generated** from `@deephaven/jsapi-types`,
  `@deephaven-enterprise/jsapi-types`, `packages/cli-plugin`'s `ctx` types and
  `types/jsapi-augment.d.ts`, by `bun run generate:boundary`. The output is
  committed. `bun run check` regenerates it and fails with
  `boundary is stale: run bun run generate:boundary` if it differs. The
  generator uses the TypeScript compiler API through the dev dependency
  `"typescript5": "npm:typescript@5.9.3"` (the repo's `typescript` is 7, which
  has no JS compiler API). No other description of the API exists.
- **Typing what crosses** (by the member's declared type):

| Declared type | Plugin gets |
| --- | --- |
| `string`, `number`, `boolean`, `null`, `undefined`, literal types | The value |
| Arrays, tuples and object-literal types of the above | A copy |
| Typed arrays (`Uint8Array`, …) | A copy, as an array of numbers |
| A declared class or interface | A stand-in of that type |
| A function type | A callable stand-in |
| `Promise<T>` | A plugin-side promise of what `T` gives |
| A union | The first member, in declaration order, the value matches: a primitive by `typeof`, a class by `instanceof`, an interface or object-literal type by being an object |
| `any`, `unknown`, or an unbound type parameter | A stand-in of the most specific declared class the value is an `instanceof` (checked against the runtime classes the types declare); otherwise unavailable |

- **From the plugin:** arguments and property writes must be plain data,
  stand-ins, or functions where the declared parameter is a function type, and
  must match the declared type, else
  `TypeError: <Type>.<member> expects <declared type>`. Writes are allowed only
  to non-`readonly` properties and setters.
- JS API exceptions are rethrown in the plugin as `Error` with the same
  message.
- More than 100,000 stand-ins held at once: `plugin_failed`,
  `<package>: plugin holds more than 100000 JS API objects`.
- **Events:** a listener's argument is typed by the `addEventListener`
  overload whose `name` parameter is the event's literal name. With none, it's
  an unbound type parameter (above).
- **`@sensitive`:** a member tagged `@sensitive` in any of its declarations is
  unavailable; a value whose declared type is tagged is never handed over, even
  through `any`.
- **Augmentation** (`types/jsapi-augment.d.ts`): declaration merging only,
  used by `dh`'s own code as well as the generator. Where an upstream name is a
  `type` alias (which TypeScript can't merge), the file declares an `interface`
  of the same name in `declare namespace augment.core` (for
  `@deephaven/jsapi-types`) or `augment.enterprise` (for
  `@deephaven-enterprise/jsapi-types`), and the generator merges it into the
  alias. Each entry has a comment naming the upstream change that would remove
  it. Initial content:

| Entry | Why |
| --- | --- |
| `getAuthConfigValues(): Promise<[string, string][]>` on `EnterpriseClient` and `dh.CoreClient`, moved from `src/actions/auth/jsapi.ts` | Narrower than published (DH-24026, DH-24027) |
| `ConsoleServerAddress.serverClass: string` (`augment.enterprise`) | Returned by `getDbServers()`, missing from the types |
| `dh.Table.addEventListener(name: "updated", cb: (e: dh.Event<dh.ViewportData>) => void): () => void` | Event payload |
| `dh.Widget.addEventListener(name: "message", cb: (e: dh.Event<dh.WidgetMessageDetails>) => void): () => void` | Event payload |
| `EnterpriseClient.addEventListener` for `configadded`, `configupdated` and `configremoved`, each `(e: CustomEvent<QueryInfo>) => void` | Event payload |
| `@sensitive` on `login`, `relogin`, `createAuthToken`, `challengeResponse` and `getChallengeNonce` (`EnterpriseClient`); `login` and `relogin` (`dh.CoreClient`); `shareObject` and `getSharedObject` (`dh.IdeSession` and `dh.IdeConnection`); and the types `RefreshToken` and `LoginCredentials` in both packages | Secrets |

**`ctx` (API v1)**, exported as types by `@deephaven/cli-plugin`:

```ts
interface BaseContext {
  args: Record<string, string | string[] | undefined>;
  options: Record<string, string | number | boolean | string[] | number[] | undefined>;
  profile: { name: string; server: string; kind: "enterprise" | "community"; user: string };
  json: boolean;
  signal: { readonly aborted: boolean };
  progress(message: string): void;
  fail(code: "usage" | "not_found" | "conflict" | "permission_denied" | "plugin_failed",
       message: string, hint?: string): never;
  enterprise(): Promise<EnterpriseClient>;
  community(): Promise<dh.CoreClient>;
  connection(target?: string | number | QueryInfo): Promise<dh.IdeConnection>;
  dh: {
    FilterValue: typeof dh.FilterValue;
    FilterCondition: typeof dh.FilterCondition;
    Table: Pick<typeof dh.Table, `EVENT_${string}`>;
    Widget: Pick<typeof dh.Widget, `EVENT_${string}`>;
    i18n: { DateTimeFormat: typeof dh.i18n.DateTimeFormat; NumberFormat: typeof dh.i18n.NumberFormat };
  };
}
interface CommandContext extends BaseContext {
  console(): Promise<dh.IdeSession>; // only on leaves with `console: true`
}
interface ObjectContext extends BaseContext {
  console(): Promise<dh.IdeSession>;
  object: { name: string; type: string; ref: number | null; widget: dh.Widget };
  objectCommand(type: string): string | null;
}
```

| Call | Behavior | Failures (code: message) |
| --- | --- | --- |
| `enterprise()` | Signs in through `requireAuth` on first call; the same client after | Community profile: `usage`: `<command> needs an Enterprise profile` |
| `community()` | As above | Enterprise profile: `usage`: `<command> needs a Community profile` |
| `connection()` with no target | Community: the server. Enterprise: the console's worker | Enterprise without a console: `usage`: `<command> needs a persistent query to connect to` |
| `connection(target)` | Enterprise: the worker of the query named (string), with that serial (number), or that `QueryInfo`, connected as `dh console` connects to workers | Community: `usage`: `<command> can't target a query on Community`. No such query: `not_found`: `No persistent query <target>`. Not running: `console_not_ready` |
| `console()` | The console `--console` chooses, by `dh exec`'s rules | As `dh exec`'s targeting |

- `ctx.object.widget` is opened by `dh` from the console; the plugin doesn't
  fetch it.
- These are the only starting points. `dh` disconnects everything at
  teardown.

**Rendering:** `rows` (array of objects: a table for people), `object` (one
object: `key: value` lines), `text` (a string, printed as is; `-o json` gives
`{"text": …}`) and `none`, with the leaf's `human(result)` overriding the
people's rendering. A result that isn't plain data, or doesn't match the
declared kind: `plugin_failed`, `<package>: result isn't <kind>`.

**Error codes** added to `ERROR_CODES` (and so to `dh agents`):

| Code | Exit | `dh agents` description |
| --- | --- | --- |
| `plugin_failed` | 6 | A plugin's code failed |
| `plugin_invalid` | 2 | A plugin's files are malformed or inconsistent |
| `plugin_unavailable` | 2 | A plugin's files are missing; run dh plugin sync |
| `not_found` | 2 | Something the command named doesn't exist |
| `conflict` | 2 | The request conflicts with the current state |
| `permission_denied` | 4 | Not allowed for this user |

At run time:

- `plugin_failed`: `run` threw anything but `ctx.fail`, a limit was hit, or
  `ctx.fail("plugin_failed", …)`. The message is the error's; its stack only
  with `DH_DEBUG`.
- `plugin_invalid`: the module's digest doesn't match the spec
  (`<package>: module doesn't match its digest`); its default export didn't
  come from `definePlugin` (`<package>: module's default export isn't a
  plugin`); or it lacks the invoked leaf or view
  (`<package>: module has no <command path>`).
- `plugin_unavailable`: a registered command's package isn't in the cache
  (`<package>@<version> isn't cached`, hint `Run dh plugin sync.`).
- A package outside its `dh` range or API, or rejected by a name conflict,
  isn't registered, so invoking its noun is commander's `usage` for an unknown
  command; `dh plugin` shows why.

**`DH_PLUGIN_PACKAGES`:** package directories separated by `path.delimiter`.
Each is read in place (`package.json`, spec, module) on every run, never
cached, and replaces any same-named package from every source in both roles.
It's how PR 2 is tested before discovery exists.

### 2. Packages, sources and commands (PR 3)

**Declaration:** `package.json` `deephaven.cli.spec`, a string path relative
to the package. Normalized, it must stay inside the package.

**Spec** (`plugin.json`), written by `deephaven-cli-plugin build`:

| Field | Value |
| --- | --- |
| `schema` | `1`. Another value: listed `unsupported spec <n>` |
| `api` | Plugin API major version; `dh` supports `[1]`. Otherwise listed `unsupported api <n>` |
| `dh` | semver range of `dh` versions; outside it listed `needs dh <range>` |
| `name`, `version` | The package's; must equal the manifest entry's |
| `module` | Path of the module relative to `plugin.json`'s directory, staying inside the package |
| `digest` | `sha256:<hex>` of the module's bytes |
| `objects` | Array of views: `{ name, description, help?, types: string[], refs: boolean, options, output }`. `refs` is whether the view has `resolveRef` |
| `commands` | Array of command nodes: `{ name, description, help?, requires?, console?, args?, options?, output?, commands? }`. A node has `commands` (a group) or `output` (a leaf), not both. `requires` (`enterprise`, `community`, `any`; default `any`) applies to the node and its children |

- **Args:** `{ name, description, optional: boolean, variadic: boolean }`, in
  order; only the last may be variadic, and required ones come first.
- **Options:** `{ key, flags, type, description, multiple?, choices?,
  required?, default? }`, with `type` `string`, `number` or `boolean`. `flags`
  is derived from the camelCase `key`: `--<kebab> <value>` (string, number),
  `--<kebab> <value...>` (with `multiple`), `--<kebab>` (boolean defaulting to
  `false`), `--no-<kebab>` (boolean defaulting to `true`). A number option's
  value that isn't a number is `usage`: `--<kebab> must be a number`.
- **Output:** `{ kind, fields? }`; `kind` is `rows`, `object`, `text` or
  `none`; `fields` (required for `rows` and `object`, absent otherwise) maps a
  name to `{ type, description, nullable? }`, `type` one of `string`, `number`,
  `boolean`, `object`, `array`.
- Unknown fields are ignored.

The authoring guide's example builds to exactly this spec (key order as
shown; the `digest` depends on the bundle):

```json
{
  "schema": 1,
  "api": 1,
  "dh": ">=3.2.0",
  "name": "@deephaven/js-plugin-ui",
  "version": "0.45.0",
  "module": "index.js",
  "digest": "sha256:…",
  "objects": [
    {
      "name": "ui",
      "description": "Show a deephaven.ui element as a tree.",
      "types": ["deephaven.ui.Element"],
      "refs": true,
      "options": [],
      "output": { "kind": "object", "fields": { "tree": { "type": "object", "description": "The rendered tree." } } }
    },
    {
      "name": "ui-dashboard",
      "description": "Show a deephaven.ui dashboard as a tree.",
      "types": ["deephaven.ui.Dashboard"],
      "refs": true,
      "options": [],
      "output": { "kind": "object", "fields": { "tree": { "type": "object", "description": "The rendered tree." } } }
    }
  ],
  "commands": [
    {
      "name": "dbserver",
      "description": "List the cluster's query and merge servers.",
      "requires": "enterprise",
      "args": [],
      "options": [
        { "key": "class", "flags": "--class <value>", "type": "string", "description": "Only servers of this class, e.g. Query or Merge." }
      ],
      "output": {
        "kind": "rows",
        "fields": {
          "name": { "type": "string", "description": "Server name." },
          "host": { "type": "string", "nullable": true, "description": "Host; null for a group." },
          "port": { "type": "number", "nullable": true, "description": "Port; null for a group." },
          "type": { "type": "string", "description": "DISPATCHER or GROUP." },
          "class": { "type": "string", "description": "Server class." }
        }
      }
    }
  ]
}
```

(The guide's example is one package with both a view and a command, for
illustration.)

**Sources and fetching** exactly as in the architecture doc's
[Sources](../docs/PLUGINS_TS_ARCHITECTURE.md#sources). Every request has a
`timeoutMs()` timeout and no credentials. On Enterprise, command sources are
`/iriside/js-plugins/` plus `/iriside/worker-kind/<kind>/plugins/` for each
worker kind whose `protocols` include `Community`. A `404` on `manifest.json`
is "no plugins", not an error; other failures keep the source's previous cache
and are reported by `dh plugin sync`.

**Cache** (`<config dir>/plugins/ts/`):

| Path | Content |
| --- | --- |
| `packages/<encodeURIComponent(name)>@<version>/` | `package.json`, the spec and the module at their package-relative paths, and `used`: the ISO time a source last listed it or a command from it last ran |
| `sources/<encodeURIComponent(base)>.json` | `{ "fetchedAt": "<ISO>", "packages": [{ "name", "version", "cli": true \| false }] }` |
| `consoles.json` | `{ "<profile id>/<console shortcode>": "<worker base URL>" }`; on Community the key is `<profile id>/shared` |
| `checked` | ISO time of the last background check |

`dh plugin sync` removes package directories whose `used` is more than 30
days old.

**Refresh:** the background check runs after any command except plugin
commands, `dh plugin …` and `dh auth login`, when `checked` is older than
`DH_PLUGIN_REFRESH` hours (default `24`; `0` every run; `off` never; any other
value that isn't a non-negative number means `24`). It writes `checked`,
starts `dh plugin sync --no-prompt --profile <active>` detached with stdio
ignored (stderr inherited with `DH_DEBUG`), and doesn't wait.

**`dh auth login`:** after saving the profile, syncs its command sources
before returning (each request with `timeoutMs()`). Commands found:
`✔ Plugins: <noun>, …`; none: no line. Failure is a warning, not a failed
login: `! Couldn't fetch the server's plugins: <reason>. Retry with
\`dh plugin sync\`.` With `-o json`, the login result gains
`"plugins": ["<noun>", …]`, or `null` after a failure.

**Names:** view names and nouns match `^[a-z][a-z0-9-]*$` and aren't built-in;
views' types aren't `dh.VariableType` constants other than `OTHERWIDGET`.
Across packages, view names, view types and nouns are unique; a clash rejects
every package involved for that name or type, listed
`conflicts with <package>`.

**`dh plugin`** (bare noun; no `list` subcommand):

- One heading per source, in this order: `DH_PLUGIN_PACKAGES` if set, the
  command sources (Enterprise: `/iriside/js-plugins/` then worker kinds by
  name), then the current console's worker if `consoles.json` has it. A heading
  is `<base> (<roles>)`, with roles `commands`, `object views`, or
  `commands, object views` (Community); the console's is
  `console <shortcode>: <base> (object views)`; local packages are
  `DH_PLUGIN_PACKAGES (commands, object views)`.
- Under each heading, one line per CLI package: two spaces, `<name>@<version>`
  padded to the longest in the whole listing plus three spaces, then what it
  adds in that source's roles joined by `; ` (`commands: <noun>, …` or
  `no commands`; `object views: <view>, …` or `no object views`), or
  `not used: <reason>`.
- A source with no CLI packages: one line, `  no CLI plugins`. No profile and
  no `DH_PLUGIN_PACKAGES`: `No profile. Log in with \`dh auth login\` to see
  its plugins.`, exit 0.
- Reasons: `needs dh <range>`, `unsupported api <n>`, `unsupported spec <n>`,
  `conflicts with <package>`, and `invalid: <problem>` where `<problem>` is one
  of `spec not found`, `spec isn't JSON`, `spec <field>: <problem>`,
  `name or version differs from the manifest`,
  `module doesn't match its digest`, `<path> is outside the package`,
  `view <view> can't handle <type>`.
- `-o json`:

```json
{
  "sources": [
    {
      "base": "https://dhe.example.com:8000/iriside/js-plugins/",
      "roles": ["commands"],
      "console": null,
      "fetchedAt": "2026-12-01T10:00:00.000Z",
      "packages": [
        { "name": "@deephaven/js-plugin-ui", "version": "0.43.1",
          "commands": [], "objects": ["ui", "ui-dashboard"],
          "status": "ok", "reason": null }
      ]
    }
  ]
}
```

`roles` holds `commands` and/or `objects`; `console` is the shortcode for a
console's worker; `base` is `DH_PLUGIN_PACKAGES` for local packages, with
`fetchedAt` `null`; `status` is `ok` or `unused`, with `reason` the text above.
`commands` and `objects` list what the package declares, whatever the source's
role.

**`dh plugin sync`:** refreshes the command sources and, if there's a current
console, its worker. One line per source, headed as in `dh plugin`:
`<heading>: <n> CLI plugins (<a> added, <c> changed, <r> removed)` or
`<heading>: failed: <reason>`. Prints every line, then exits with the first
failure's code (`server_unreachable` for network and TLS failures). `-o json`:
`{ "sources": [{ "base", "roles", "console", "plugins": <n>, "added": [...],
"changed": [...], "removed": [...], "error": null | { "code", "message" } }] }`.

**`dh agents`:** plugin nouns carry `plugin` (package name) and
`pluginKind: "ts"`; views appear under `dh object` with `plugin`,
`pluginKind` and `types`.

**`@deephaven/cli-plugin`** (`packages/cli-plugin/`, published): `definePlugin`,
the `ObjectView` and `ctx` types, and the `deephaven-cli-plugin` bin:

- `deephaven-cli-plugin build <entry> --out <dir>`: imports the entry once
  (in Node or Bun), validates it, writes `<dir>/plugin.json`, and bundles the
  entry to `<dir>/index.js` with esbuild (`format: "esm"`,
  `platform: "neutral"`, everything bundled). Reads `name` and `version` from
  the nearest `package.json`; `module` is `index.js`.
- Validation errors print `<path>: <problem>` and exit 1, `<path>` being the
  dotted path in the definition (`commands.dbserver.options.class`):
  `default export must come from definePlugin`,
  `name "<n>" must match ^[a-z][a-z0-9-]*$`, `--<flag> is reserved by dh`,
  `output.fields is required for <kind>`,
  `a view can't handle <type>`, `only the last argument can be variadic`,
  `required arguments must come first`, and esbuild's
  `imports Node built-in <module>` for a Node built-in.

### 3. Object views (PR 4, after the `dh object` PR)

- **Registration before parsing:** when `argv` is a `dh object` (or
  `dh console object`) subcommand other than a built-in, and not `--help`,
  `dh` resolves the target console, fetches its worker's source (manifest,
  then new or changed packages), records it in `consoles.json`, and registers
  that worker's views. Then it parses. With `--help`, views come from
  `consoles.json`'s cache, and are fetched only if that console has none.
  Unknown after that: `usage`, `No dh object subcommand <x>. Run \`dh object\`
  to see what each object uses.`
- **Listing:** a named object's label is the built-in subcommand for its type
  (`table` for `Table`), else the view handling it, else its type string.
  `-o json` adds `command` (the subcommand, or `null`).
- **`--ref <n>`:** on every `dh object` subcommand. `dh` opens `<name>` as a
  widget, runs the `resolveRef` of the view handling `<name>`'s type, and
  fetches the `WidgetExportedObject` it returns. That runtime stays alive until
  the command ends; the subcommand then runs in the same process over the same
  connection.
  - No view handles `<name>`'s type, or it has no `resolveRef`: `usage`,
    `<name> (<type>) has no nested objects dh can reach`.
  - `resolveRef` fails with `not_found`: `not_found`,
    `<name> has no object <n>`.
  - The object's type isn't one the invoked subcommand handles: `usage`,
    `<name> --ref <n> is <type>; use dh object <sub> <name> --ref <n>`, or
    `<name> --ref <n> is <type>, which no dh object subcommand displays`.
  - Otherwise the subcommand runs on that object: a built-in on the fetched
    object; a view with `ctx.object` set to it, `name` the outer name and
    `ref` `<n>`.
- **`objectCommand(type)`:** the built-in subcommand for `type`, else the
  registered view for it, else `null`.

### 4. `dh object ui` and `dh object ui-dashboard` (deephaven-plugins)

In `plugins/ui/src/js`: a CLI entry built with `deephaven-cli-plugin build`
into `dist/cli/` after the web build, and `deephaven.cli.spec` in
`package.json`.

- **Views:** `ui` (type `deephaven.ui.Element`) and `ui-dashboard` (type
  `deephaven.ui.Dashboard`), sharing their code; both `refs: true`, no
  options, output `object` with field `tree`, and `human` printing the tree.
- **Rendering:** sends `setState` with empty state, as the web client does on
  open, and waits for the first `documentPatched`. A `documentError` instead
  is `plugin_failed` with the server's message, and nothing on stdout.
- **Numbering:** one counter, starting with the widget's initial
  `exportedObjects` and continuing through each message's, in arrival order,
  as `deephaven.ui`'s web client counts; it matches `__dhObid`.
- **Tree lines,** two spaces per level:

| Node | Line |
| --- | --- |
| `deephaven.ui.components.<X>` | `[<x>]`, `<X>` lowercased |
| `deephaven.ui.html.<tag>` | `[html.<tag>]` |
| `deephaven.ui.elements.UITable` | The table it shows, as an exported object (below) |
| Another `deephaven.ui.elements.<X>` | `[<x>]` |
| An exported object | `[<label>:<n>]`, `<label>` from `objectCommand`, else its type; for a table, then ` <rows> rows, <columns> columns` |
| A plain text child | The text, quoted |

  After the bracket: the node's label, quoted, from its `title` prop, else its
  `label` prop, else its text children joined; `[active]` on the child of a
  `stack` at its `active_item_index` (the first when unset);
  `selected="<key>"` from `selected_key`, else `default_selected_key`; and
  `(<n> options)` for a picker's items. Quoted text longer than 120
  characters is cut to 119 and `…`.
- **Hint:** only when some exported object has a subcommand, after a blank
  line, `Inspect a nested object: dh object <view> <name> --ref <id>` and
  then `  e.g. dh object <sub> <name> --ref <n> for [<sub>:<n>]`, with the
  lowest-numbered exported object that has a subcommand.
- **JSON:** `tree` is the root node; a node is
  `{ "element": "<x>", "label"?: "…", "active"?: true, "selected"?: "…",
  "options"?: <n>, "children"?: [...] }`, an exported object is
  `{ "ref": <n>, "type": "<type>", "command": "<sub>" | null,
  "rows"?: <n>, "columns"?: <n> }`, and a text child is a string.
- **`resolveRef(ctx, n)`:** renders, then returns exported object `n`, or
  `ctx.fail("not_found", …)`.

## Stack

| # | PR | Contents |
| --- | --- | --- |
| 1 | Design and surface | The three docs and this plan |
| 2 | Sandbox | Section 1: engine, generated boundary, augmentation, `ctx`, rendering, error codes, `DH_PLUGIN_PACKAGES` |
| 3 | Commands | Section 2: declaration, spec, sources, cache, refresh, `dh auth login`, `dh plugin`, `dh plugin sync`, `dh agents`, `@deephaven/cli-plugin` |
| 4 | Object views | Section 3, after the `dh object` PR |
| — | deephaven-plugins | Section 4 |
| — | deephaven-core (follow-up) | Serve `package.json` under development registration |

## Out of scope

- Interacting with objects: clicking, typing, callables, live updates.
- Refs more than one level deep, and objects that appear only after
  interaction.
- Commands discovered from a worker's Python environment.
- Signing plugin packages.
- `dh object table`'s options (its own design).
- File-system (`shell`) plugins (their own design; see
  [Shared with file-system plugins](../docs/PLUGINS_TS_ARCHITECTURE.md#shared-with-file-system-plugins)).
- Upstreaming the augmentation to deephaven-core and Iris; tracked separately.

## Done when

Each item is a behavior the implementation must show. IDs
(`plugins-ts-<area>-<n>`, area `sandbox`, `cmd`, `obj` or `ui`) are stable so
tests can reference them: numbers are append-only, and a removed or moved
item's ID isn't reused.

### PR 2: Sandbox

- [ ] **plugins-ts-sandbox-1** In a compiled `dh`, plugin code finds
      `process`, `require`, `fetch`, `Bun`, `XMLHttpRequest` and `WebSocket`
      undefined; `import()` and a static `import` fail; and
      `Function("return this")()` returns a global with none of them.
- [ ] **plugins-ts-sandbox-2** Against a Community server, a plugin opens a
      table with `getTable`, reads `columns` and a viewport's rows, receives
      three `updated` events from a ticking table with a typed `detail`, and
      stops them with the returned remover.
- [ ] **plugins-ts-sandbox-3** `constructor`, `__proto__`, a member absent
      from the types, and each `@sensitive` member fail with
      `<Type>.<member> is not available to plugins`; a listener on
      `refreshtokenupdated` receives no `RefreshToken`.
- [ ] **plugins-ts-sandbox-4** An infinite loop ends as `plugin_failed` after
      `DH_TIMEOUT`; unbounded allocation, unbounded recursion and holding
      100,001 objects each end as `plugin_failed`; `dh` disconnects and exits
      normally in every case.
- [ ] **plugins-ts-sandbox-5** A JS API error inside `run` reaches the plugin
      as an exception with the API's message; uncaught, it's `plugin_failed`
      with that message, and `DH_DEBUG=1` adds the plugin's stack.
- [ ] **plugins-ts-sandbox-6** `bun run check` fails with
      `boundary is stale: run bun run generate:boundary` after editing
      `types/jsapi-augment.d.ts` until the boundary is regenerated, and the
      regenerated file is the only other change; `serverClass` is readable on
      a `getDbServers()` result.
- [ ] **plugins-ts-sandbox-7** Reading a 1,000-row by 10-column viewport cell
      by cell takes under 3× the time of the same reads in-process, on the
      same machine; the compiled binary grows by under 1 MB.
- [ ] **plugins-ts-sandbox-8** Each `ctx` starting-point failure in section
      1's table gives its code and message; `connection("<query>")` reaches a
      running persistent query's worker on Enterprise.
- [ ] **plugins-ts-sandbox-9** `rows`, `object`, `text` and `none` render for
      people and `-o json` as section 1 says; `human` changes only the
      people's output; a non-data result is `plugin_failed`.
- [ ] **plugins-ts-sandbox-10** `ctx.fail` with each code exits with that
      code's exit and prints its hint; the six new codes appear in
      `dh agents` with their descriptions.
- [ ] **plugins-ts-sandbox-11** Ctrl-C during `run` ends as `cancelled` with
      `ctx.signal.aborted` true in the plugin; after any run, no listener the
      plugin added fires again.
- [ ] **plugins-ts-sandbox-12** Each run-time `plugin_invalid` and
      `plugin_unavailable` case in section 1 gives its message.
- [ ] **plugins-ts-sandbox-13** `DH_PLUGIN_PACKAGES` runs a local package's
      command with no network request for plugins, and a rebuild is picked up
      on the next run.

### PR 3: Commands

- [ ] **plugins-ts-cmd-1** A CLI-only package installed in a Core+ worker
      kind's `jsPlugins` directory adds its noun on Enterprise after
      `dh plugin sync`; the web UI loads its `main` without errors.
- [ ] **plugins-ts-cmd-2** Startup registers commands from the cache with no
      network request; `dh --help` and `dh agents` list them with `plugin`
      and `pluginKind: "ts"`.
- [ ] **plugins-ts-cmd-3** A package without `deephaven.cli`, or a source
      without `manifest.json`, adds nothing and isn't an error; each
      `invalid: <problem>` in section 2 is listed for its case and the package
      isn't registered.
- [ ] **plugins-ts-cmd-4** Among command sources the higher version of a
      package registers; two packages claiming a noun, or a noun equal to a
      built-in, are listed `conflicts with …` and not registered.
- [ ] **plugins-ts-cmd-5** `dh plugin` and `dh plugin -o json` match section
      2 for an Enterprise profile with a cached console, a Community profile,
      `DH_PLUGIN_PACKAGES`, and no profile; `not used` shows each reason.
- [ ] **plugins-ts-cmd-6** `dh plugin sync` prints a line per source and
      exits with the first failure's code; packages unused for 30 days are
      removed.
- [ ] **plugins-ts-cmd-7** The background check starts after an ordinary
      command when due, adds no time to it, runs after every command with
      `DH_PLUGIN_REFRESH=0`, never with `off`, and treats `abc` as 24.
- [ ] **plugins-ts-cmd-8** `dh auth login` prints `✔ Plugins: …` when the
      server has commands, warns and still succeeds when the fetch fails, and
      its `-o json` result has `plugins`.
- [ ] **plugins-ts-cmd-9** `deephaven-cli-plugin build` on the authoring
      guide's example writes the spec in section 2 (but `digest`), and each
      validation error in section 2 is produced for its case.
- [ ] **plugins-ts-cmd-10** A `requires: "enterprise"` command run on a
      Community profile is `usage` with `<command> needs an Enterprise
      profile`; one with `console: true` takes `--console`.
- [ ] **plugins-ts-cmd-11** Plugin fetches send no credentials, refuse a
      redirect `assertTrusted` rejects, and use the profile's CA.

### PR 4: Object views

- [ ] **plugins-ts-obj-1** `dh object` labels a handled type with its view, a
      table `table`, and anything else with its type string; `-o json` adds
      `command`.
- [ ] **plugins-ts-obj-2** `dh object <view> <name>` works the first time
      it's used against a console, with nothing cached; after that,
      `dh object <view> --help` needs no connection; an unknown subcommand is
      `usage` with section 3's message.
- [ ] **plugins-ts-obj-3** When the worker serves a new version of a view's
      package, the next run uses it, including a new option.
- [ ] **plugins-ts-obj-4** `dh object table <name> --ref <n>` runs the table
      command, with its options, on the nested table; a view reached by
      `--ref` gets `ctx.object.ref`; each `--ref` error in section 3 gives its
      message.
- [ ] **plugins-ts-obj-5** `dh agents` lists views under `dh object` with
      `plugin`, `pluginKind` and `types`; a view claiming `Table` is rejected
      as `invalid: view <view> can't handle Table`.

### deephaven-plugins: `dh object ui` and `dh object ui-dashboard`

- [ ] **plugins-ts-ui-1** On a Community server with `deephaven.ui`,
      `dh object ui-dashboard` on a dashboard holding a `ui.table`, a plotly
      express figure, a stack, a picker and a button prints the tree as in
      section 4, matching `docs/PLUGINS_TS.md`'s example, with the hint line.
- [ ] **plugins-ts-ui-2** `dh object ui` on an element prints it with the
      element at the root; `-o json` returns the tree as section 4's JSON.
- [ ] **plugins-ts-ui-3** Each `[table:<n>]` it prints opens with
      `dh object table <name> --ref <n>` and shows that table.
- [ ] **plugins-ts-ui-4** A component that raises during render ends as
      `plugin_failed` (exit 6) with the server's message and no stdout.
- [ ] **plugins-ts-ui-5** The web UI renders the package's elements and
      dashboards from the same build, with no new console errors.
