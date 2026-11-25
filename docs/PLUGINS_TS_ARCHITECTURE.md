# TypeScript plugins architecture

How `dh` finds, loads and runs TypeScript plugins.

A **TypeScript plugin** adds commands to `dh`. It ships inside a Deephaven web
plugin's npm package, next to the web bundle, and is served by the same
infrastructure that serves web plugins today. Most will pair with a web
plugin, such as `deephaven.ui`; a **CLI-only plugin** is a package whose web
side does nothing. Plugin code runs in a sandbox in `dh`, and reaches the
server only through the Deephaven JS API objects `dh` hands it.

Using plugins is covered in [TypeScript plugins](PLUGINS_TS.md); writing one is
in [Writing a TypeScript plugin](PLUGINS_TS_AUTHORING.md). File-system
(`shell`) plugins, which run a cluster's tools, have their own design; see
[Shared with file-system plugins](#shared-with-file-system-plugins).

## At a glance

| A plugin can add | Example | Discovered from | Registered |
| --- | --- | --- | --- |
| **Object views**: `dh object` subcommands, each for some object types | `dh object ui el` | The worker of the console `dh object` targets | When that worker serves it |
| **Commands**: top-level nouns | `dh dbserver` | The profile's server (see [Sources](#sources)) | Always, from the cache |

- **One package, one plugin.** A package declares its CLI side in
  `package.json` and may add object views, commands, or both.
- **No server changes.** Discovery reads files every Deephaven server already
  serves for web plugins: `/js-plugins/manifest.json` and each plugin's
  directory. A server with no CLI plugins just has none.
- **The JS API types are the only source of truth** for what plugin code may
  call. Anything missing from them is fixed in the types, not described a
  second way in `dh` (see [The JS API boundary](#the-js-api-boundary)).
- **Startup never connects** for commands; they're registered from a local
  cache. `dh object` connects to its console anyway, so it checks that
  console's worker before parsing.

## Packaging

A web plugin is an npm package. When installed, `npm pack` output is served
whole at `<base>/<package name>/…`, `package.json` included: for plugins in a
Python wheel (`deephaven.ui`'s `_js/`), for Enterprise's packaged set and for
directories an admin installs. The CLI side is two more files in that package:

```text
@deephaven/js-plugin-ui/
  package.json           # "deephaven": { "cli": { "spec": "dist/cli/plugin.json" } }
  dist/index.js          # the web plugin (main)
  dist/cli/plugin.json   # the CLI spec: what the plugin adds, written by the build
  dist/cli/index.js      # the CLI module: one self-contained ES module
```

- **The declaration** is `package.json`'s `deephaven.cli.spec`: the spec's
  path, relative to the package. Nothing else is read from `package.json`.
  The `deephaven` field is the one deephaven-core already anticipates for
  bespoke plugin settings (deephaven-core#4817); `dh` ignores its other keys.
- **The web manifest is unchanged.** It lists only `name`, `version`, `main`
  and `loader`, so `dh` reads each package's own `package.json`.
- **A CLI-only plugin** still has a `main`: a module whose export is an empty
  multi-plugin, `{ name, type: "MultiPlugin", plugins: [] }`. The web UI loads
  every manifest entry's `main`; this one registers nothing. Web UIs before
  `@deephaven/plugin` 1.17.0 (no `MultiPlugin`) log
  `Plugin '<name>' is missing an exported value.` and carry on. No new
  attribute marks such plugins.
- **Core's development registration** (`deephaven.jsPlugins.<name>=<dir>`,
  for web-plugin authors running a local server against their source) serves
  only `main`'s top-level directory, not `package.json`, so `dh` doesn't see
  plugins registered that way. A follow-up asks Core to serve `package.json` in
  that mode too, in line with deephaven-core#4817. Until then, plugin authors
  use `DH_PLUGIN_PACKAGES` (see
  [Writing a TypeScript plugin](PLUGINS_TS_AUTHORING.md#testing-locally)).

### The spec

`dist/cli/plugin.json` is written by the build; authors never edit it. Its
exact fields are in the plan. It holds the API version and `dh` range, the
module's path and digest, and for each object view and command its name,
description, arguments, options, output schema and help, so `dh` can build
help, `dh agents` and argument parsing without loading the module.

## Sources

A **source** is a base URL serving a web-plugin manifest. These paths need no
login on any Deephaven server, so `dh` sends no credentials. Requests follow
the existing trust rules: `https:` (or `http:` on loopback), the profile's CA,
and `assertTrusted` on every redirect.

| Role | Community | Enterprise |
| --- | --- | --- |
| Commands | `<server>/js-plugins/` | `<server>/iriside/js-plugins/`, and `<server>/iriside/worker-kind/<kind>/plugins/` for each Core+ worker kind |
| Object views | `<server>/js-plugins/` | `<worker>/js-plugins/` of the targeted console |

- **Object views come from the worker that holds the object.** That's where
  plugins installed in the worker's Python environment are served, at the
  version that created the object. On Enterprise, `<worker>` is the console's
  `designated.jsApiUrl` without its trailing `jsapi/dh-core.js`, as the web UI
  derives it. On Community the server is the worker, so one source has both
  roles.
- **Commands come from the cluster,** since they don't need a console.
  Enterprise has two kinds of cluster-wide set. `/iriside/js-plugins/` is the
  web app's packaged set. `/iriside/worker-kind/<kind>/plugins/` serves a
  Core+ worker kind's `WorkerKind.<kind>.jsPlugins` directory, which an admin
  can add plugins to without changing the web app; every worker of that kind
  loads it too. Core+ kinds are those whose `protocols` include `Community`, in
  `getServerConfigValues().workerKinds`, so finding commands on Enterprise
  signs in.
- **Versions are chosen per role.** Among command sources the highest
  `version` (semver) of a package wins, as the web UI resolves worker-kind
  collisions. For object views only the console's worker counts.

### Fetching a source

1. `GET <base>manifest.json`. A `404` means no plugins.
2. For each entry not already cached at that `name` and `version`:
   `GET <base><name>/package.json`. No `deephaven.cli`, or a `404`: not a CLI
   plugin, cached as such for that version.
3. `GET` the spec, then the module; check the module's digest.

A source is cached as its list of `name@version` entries; a package version's
files are cached once and shared by every source serving it.

## Registering commands

- **Commands** are registered at startup from the active profile's command
  sources' cache, without connecting. `dh plugin sync`, `dh auth login` and a
  periodic background check refresh them. The check runs after other commands
  at most once per `DH_PLUGIN_REFRESH` hours, as a detached `dh plugin sync`
  the command doesn't wait for.
- **Object views:** for any `dh object` subcommand other than built-ins and
  `--help`, `dh` resolves the target console, fetches its worker's
  `manifest.json` and any new or changed package, and registers that worker's
  views before parsing. So a run always uses the worker's current version, with
  its current options. `--help` uses the cache, and fetches only if that
  console has no cache yet.

### Names

Object view names and command nouns follow the built-in rule
(`^[a-z][a-z0-9-]*$`) and can't be built-in names (`dh object table`, `dh auth`
and so on). Two packages claiming the same view name, the same object type or
the same noun are both rejected for it; `dh plugin` shows why.

An object view handles widget types only: any type except the ones
`dh.VariableType` names for tables, trees and figures (every constant but
`OTHERWIDGET`). Those have their own `dh object` subcommands.

## Running a plugin

Each invocation starts a fresh QuickJS runtime (compiled to WebAssembly and
embedded in `dh`), loads the module, calls the leaf's `run`, renders its result
and disposes the runtime.

- **Nothing ambient:** no `process`, `require`, `fetch`, `Bun`, file system,
  network or module loading. The plugin's only capabilities are the `ctx` it is
  given.
- **Limits:** 256 MiB of memory, a 1 MiB stack, at most 100,000 JS API
  objects held per run, and no single stretch of synchronous plugin code longer
  than `DH_TIMEOUT`. Waiting on a server call isn't plugin code; Ctrl-C cancels
  as for built-in commands.
- **Teardown:** before disposing a runtime, `dh` removes every listener it
  registered and releases every JS API object it held, then disconnects what
  `ctx` opened.
- **Size:** the engine adds about 0.6 MB to `dh`.

### Starting points

A plugin starts from what `ctx` hands it, at the level it needs:

| `ctx` | Returns | Available to |
| --- | --- | --- |
| `enterprise()` | `EnterpriseClient`, signed in: the cluster | Any plugin, on an Enterprise profile |
| `community()` | `dh.CoreClient`, signed in: the server | Any plugin, on a Community profile |
| `connection(target?)` | `dh.IdeConnection`: a worker | Any plugin |
| `console()` | `dh.IdeSession`: the console `--console` chooses, as for `dh exec` | Object views; commands that declare `console: true` |
| `object` | The object being viewed, as a `dh.Widget` | Object views |
| `dh` | Value classes: filters, formats, event names | Any plugin |

- **`connection(target)`**: on Community, the server (a target is `usage`).
  On Enterprise, a persistent query's worker, given its name, its serial or a
  `QueryInfo` the plugin read from the client. The target comes from the
  plugin's own arguments or from data it read; `dh` adds no flag for it. With
  no target, the console's worker when the plugin has a console, else `usage`.
- **`console()`** gives scripting: `runCode`, and the session's objects. Object
  views always have one, the console holding the object. A command gets one
  only if it declares `console: true`, which also gives it the `--console`
  option.

Everything else a plugin reaches follows from these through the JS API types.
Because no client constructor is a starting point, a plugin can't open a
connection of its own.

### The JS API boundary

Plugin code uses the ordinary JS API: `await session.getTable("t")`,
`table.columns`, `table.addEventListener(...)`. Each object it holds is a
stand-in; `dh` keeps the real one and performs every access.

- **Every stand-in has a declared type,** and each property read, property
  write and method call is checked against that type's members in the JS API
  types. A member's declared type types what it returns. So a plugin reaches
  exactly what the public API describes, starting from `ctx`, and never the
  client's internals. Anything else throws
  `<Type>.<member> is not available to plugins`.
- **Calls keep their shape:** synchronous members stay synchronous,
  promise-returning ones stay async, listeners and the functions they return
  work, and JS API errors reach the plugin as exceptions. Values cross as
  copies of plain data.
- **Untyped values:** a value typed `any`, or by a type parameter nothing binds
  (`addEventListener<T>` without an overload for the event), becomes the most
  specific JS API class it's an instance of, from the classes the types
  declare (`dh.Table`, `dh.Widget`, …), or is unavailable. The class list comes
  from the types, so this is the same mechanism, not a second one.
- **The checks are generated** at `dh`'s build from
  `@deephaven/jsapi-types` and `@deephaven-enterprise/jsapi-types`, and from
  `@deephaven/cli-plugin`'s own types for `ctx`. Nothing else describes the
  API.
- **Corrections go into the types.** Until they ship there, `dh` carries them
  in one declaration-merging file, `types/jsapi-augment.d.ts`, which `dh`'s own
  code also uses, and which empties as the upstream types catch up. It starts
  with:
  - **Event payloads:** an `addEventListener` overload per event name, so a
    listener's argument has a type. Today every Core event is `Event<T>` with
    `T` unbound.
  - **`@sensitive`:** a TSDoc tag on members and types that carry secrets
    (tokens, passwords, keys, shared tickets). A sensitive member is
    unavailable; a value of a sensitive type is never handed over.
  - **Missing members,** such as `ConsoleServerAddress.serverClass`.

  The exact list is in the plan.
- **Version skew:** the checks reflect the type packages `dh` was built with.
  A member newer than those is unavailable until `dh` updates; a plugin
  needing it says so with its `dh` range.

### Trust

Plugin code acts with the user's session, as the web UI's plugins do in the
browser, and can do whatever the user could do through the JS API. It can't
read files, credentials or other profiles, run processes, or reach any host
but through `dh`. That's what makes it reasonable for `dh` to run code served
by a worker, which whoever controls the worker's Python environment can change.

## Nested objects: `--ref`

A `deephaven.ui` dashboard holds tables and figures that usually have no name
in the console. They reach a client as objects the widget exports, numbered in
the order they arrive (`__dhObid` in the document; `deephaven.ui`'s web client
counts them the same way). Rendering only starts when the client sends
`setState`, so finding object `n` needs the plugin's protocol, not just `dh`.

- **An object view that can resolve refs** has `resolveRef`, and its output
  shows each nested object's ref (`[table:0]`). It labels each one with
  `ctx.objectCommand(type)`: the `dh object` subcommand that displays that
  type, or `null`, in which case the label is the type itself. So a label is
  always what `dh object` would list, and the hint never names a subcommand
  that doesn't exist.
- **`dh object <view> <name> --ref <n>`** opens `<name>`, asks the view
  handling `<name>`'s type for ref `n`, and runs `<view>` on the object it
  returns, in the same `dh` process and over the same connection. The runtime
  that resolved the ref stays alive, holding the outer object open, until the
  command ends. The nested object's type must be one `<view>` handles.
- **Refs are one level deep,** and reflect the object's initial state: each
  run is a fresh connection, and interaction is out of scope.

## Shared with file-system plugins

The two designs are separate but meet in these places; each must keep them
compatible.

| Shared | Rule |
| --- | --- |
| Command names | One namespace: built-ins, TypeScript nouns and `shell` nouns. A clash across kinds rejects both, as within a kind |
| `dh plugin` | One listing; each source shows its kind and role |
| `dh plugin sync` and the background check | One command and one check refresh every kind |
| Cache | `<config dir>/plugins/ts/` for TypeScript plugins; other kinds use their own subdirectory |
| `dh agents` | Plugin nouns carry `plugin` and `pluginKind` (`ts` or `shell`) |
| Error codes | `plugin_failed`, `plugin_invalid`, `plugin_unavailable` mean the same for every kind |

## Where it lives

| Piece | Where |
| --- | --- |
| Runtime, sandbox, JS API boundary, discovery, `dh object <view>`, `dh plugin` | This repo, `src/` |
| `@deephaven/cli-plugin`: `definePlugin`, `ctx` types, the build command | This repo, `packages/cli-plugin/`, published to npm |
| `dh object ui` and `dh object ui-dashboard` | deephaven-plugins, `plugins/ui/src/js` (a second build of the same package) |
| Type corrections | deephaven-core (`web/client-api/types`, generated from Java) and Iris (`web/client-ui/packages/jsapi-types`); meanwhile `types/jsapi-augment.d.ts` here |

No Core or Iris change is needed to ship this. Admin installation on
Enterprise uses the existing `WorkerKind.<kind>.jsPlugins` directories.
Serving `package.json` under Core's development registration is a follow-up.
