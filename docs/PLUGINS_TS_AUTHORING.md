# Writing a TypeScript plugin

A TypeScript plugin is a module in a Deephaven web plugin's npm package that
adds object views (`dh object <view>`), commands, or both. This page covers
writing, building, testing and installing one. Using plugins is in
[TypeScript plugins](PLUGINS_TS.md); the design is in
[TypeScript plugins architecture](PLUGINS_TS_ARCHITECTURE.md).

## The shape of a plugin

```ts
// src/cli/index.ts
import { definePlugin, type ObjectView } from "@deephaven/cli-plugin";

// Shared by both views: deephaven.ui elements and dashboards render the same way.
const view = (description: string, type: string): ObjectView => ({
  description,
  types: [type],
  output: {
    kind: "object",
    fields: { tree: { type: "object", description: "The rendered tree." } },
  },
  async run(ctx) {
    const document = await render(ctx.object.widget); // your protocol code
    return { tree: toTree(document, ctx) };
  },
  human: (result) => treeLines(result.tree),
  // Makes `dh object <view> <name> --ref <n>` reach objects inside these.
  async resolveRef(ctx, ref) {
    return await exportedObject(ctx.object.widget, ref);
  },
});

export default definePlugin({
  dh: ">=3.2.0",

  objects: {
    ui: view("Show a deephaven.ui element as a tree.", "deephaven.ui.Element"),
    "ui-dashboard": view("Show a deephaven.ui dashboard as a tree.", "deephaven.ui.Dashboard"),
  },

  commands: {
    // `dh dbserver`: a noun that lists, as built-in nouns do.
    dbserver: {
      description: "List the cluster's query and merge servers.",
      requires: "enterprise",
      options: {
        class: { type: "string", description: "Only servers of this class, e.g. Query or Merge." },
      },
      output: {
        kind: "rows",
        fields: {
          name: { type: "string", description: "Server name." },
          host: { type: "string", nullable: true, description: "Host; null for a group." },
          port: { type: "number", nullable: true, description: "Port; null for a group." },
          type: { type: "string", description: "DISPATCHER or GROUP." },
          class: { type: "string", description: "Server class." },
        },
      },
      async run(ctx) {
        const client = await ctx.enterprise();
        const wanted = ctx.options.class?.toLowerCase();
        return (await client.getDbServers())
          .filter((s) => !wanted || s.serverClass.toLowerCase() === wanted)
          .map((s) => {
            const group = s.type === "GROUP"; // a group's host and port are placeholders
            return {
              name: s.name,
              host: group ? null : s.host,
              port: group ? null : s.port,
              type: s.type,
              class: s.serverClass,
            };
          });
      },
    },
  },
});
```

- **`objects`** adds `dh object` subcommands. `types` are the object types a
  view handles, as the server names them (`dh object -o json` shows them).
  Usually one view handles one type; a view may list several. Views handle
  widget types only: tables, trees and figures have their own subcommands.
  `dh` supplies the object's name argument, `--ref` and `--console`; declare
  only your own options. `dh object detail` runs your view with none given,
  or, if one is `required`, points to your view instead of running it.
- **`commands`** adds top-level nouns. A command with `commands` is a group;
  one with `run` is a leaf, and a noun can be a leaf itself, as `dbserver` is.
  `requires` is `enterprise`, `community` or `any` (the default). A leaf with
  `console: true` gets `ctx.console()` and the `--console` option.
- **Arguments** are a list: `args: [{ name: "query", description: "…",
  optional?: true, variadic?: true }]`, in order. They arrive in `ctx.args`.
- **Options** are `string`, `number` or `boolean`, with `multiple`,
  `choices`, `required` and `default`. Keys are camelCase in code and
  `--kebab-case` on the command line; a boolean that defaults to `true` is
  turned off with `--no-<name>`. Flags `dh` owns can't be used: `-h`/`--help`,
  `-o`/`--output`, `--profile`, `--no-prompt`, `--no-input`, `--console` and
  `--ref`.
- **Output** declares what `-o json` returns, which `--help` and `dh agents`
  document: `rows`, `object`, `text` or `none`. A leaf's optional `human`
  replaces the default rendering for people; it never changes `-o json`.
- **Help:** `description` is the one-line summary. An optional `help` string
  is shown below the usage in `--help`.
- **Errors:** throw, or call `ctx.fail(code, message, hint?)` with one of
  `usage`, `not_found`, `conflict`, `permission_denied` or `plugin_failed`.

### `ctx`

Every leaf gets the parsed `args` and `options`, the active `profile`,
`json` (true with `-o json`), `progress(message)` (stderr, silent with
`-o json`), `signal` (aborted on Ctrl-C) and `fail`. To reach the server, it
starts at the level it needs:

| `ctx` | What it is | Available |
| --- | --- | --- |
| `enterprise()` | The signed-in Enterprise client (`EnterpriseClient`): the cluster | On an Enterprise profile |
| `community()` | The signed-in Community client (`dh.CoreClient`): the server | On a Community profile |
| `connection(target?)` | A worker (`dh.IdeConnection`). On Enterprise, `target` is a persistent query's name, serial or `QueryInfo`; omitted, the console's worker. On Community, the server; no target | Always |
| `console()` | The console `--console` chooses (`dh.IdeSession`), for running code | Object views; commands with `console: true` |
| `object` | `{ name, type, ref, widget }`: the object being viewed as a `dh.Widget`; `ref` is the `--ref` it was reached by, or `null` | Object views |
| `objectCommand(type)` | The `dh object` subcommand that displays `type`, or `null`. Label nested objects with it, falling back to the type | Object views |
| `dh` | JS API value classes: filters (`FilterValue`, `FilterCondition`), formats (`i18n.DateTimeFormat`, `i18n.NumberFormat`) and the event names on `Table` and `Widget` | Always |

The target for `connection` comes from your own arguments, such as a
`<query>` argument, or from data you read, such as a `QueryInfo` from
`client.getKnownConfigs()`. From there it's the ordinary JS API, typed by
`@deephaven/jsapi-types` and `@deephaven-enterprise/jsapi-types`.

## What runs where

Your module runs in a sandbox inside `dh`, not in Node or a browser:

- **No Node or browser APIs:** no `fs`, `process`, `fetch`, `window` or
  `document`, and no `import` at run time. Bundle every dependency, and use
  only ones that don't need those.
- **The JS API works as typed.** Each object you hold is a stand-in that `dh`
  checks against the published types. A member missing from them, or marked
  `@sensitive` (tokens, passwords, keys), fails with
  `<Type>.<member> is not available to plugins`. If you need one that's
  missing, it belongs in the types; ask for it there.
- **Values are copies.** Reading many cells one at a time is slower than in a
  browser; read a viewport once and work from the rows.
- **Limits:** 256 MiB of memory, 100,000 JS API objects held at once, and no
  stretch of synchronous code longer than `DH_TIMEOUT` (default 30 s).
- **Cleanup is automatic:** when your command ends, `dh` removes your
  listeners and closes what `ctx` opened.

## Building

```sh
npm install --save-dev @deephaven/cli-plugin
npx deephaven-cli-plugin build src/cli/index.ts --out dist/cli
```

The build bundles the module and its dependencies into one ES module,
`dist/cli/index.js`, and writes `dist/cli/plugin.json`: everything `dh` needs
to list your commands and options without running your code. Run it after the
web build. A dependency on Node built-ins, a reserved flag or an invalid name
fails the build with `<path>: <problem>`.

Declare it in `package.json`, and make sure `files` includes it:

```json
{
  "name": "@deephaven/js-plugin-ui",
  "main": "dist/index.js",
  "files": ["dist"],
  "deephaven": { "cli": { "spec": "dist/cli/plugin.json" } }
}
```

### A CLI-only plugin

A package with commands but no web side still needs a `main`, because the web
UI loads every plugin's `main`. Make it register nothing:

```js
// dist/index.js
module.exports = { name: "@example/dh-dbserver", type: "MultiPlugin", plugins: [] };
```

## Testing locally

Point `dh` at your package directory instead of a server:

```sh
DH_PLUGIN_PACKAGES=~/src/dh-dbserver dh plugin
DH_PLUGIN_PACKAGES=~/src/dh-dbserver dh dbserver
```

`DH_PLUGIN_PACKAGES` takes one or more package directories, separated by `:`
(`;` on Windows). Each is read in place, as if served, and replaces a package
of the same name from any source, for both commands and object views. Rebuild
and run again; nothing is cached.

Core's development registration (`deephaven.jsPlugins.<name>=<dir>`) doesn't
serve `package.json`, so `dh` doesn't find plugins through it yet; use
`DH_PLUGIN_PACKAGES` for the CLI side.

## Installing

Install the package the way web plugins are installed. What decides where
`dh` finds it:

| Server | To provide | Install it |
| --- | --- | --- |
| Community | Either | As a Python package that registers the JS plugin (as `deephaven.ui` does), or in the server's `<configDir>/js-plugins/` with an entry in its `manifest.json` |
| Enterprise | Object views | In the Python environment of the workers that create those objects |
| Enterprise | Commands | In a Core+ worker kind's `WorkerKind.<kind>.jsPlugins` directory: the package under `<dir>/<name>/`, and an entry in `<dir>/manifest.json`. Restart the web API server to serve it |

On Enterprise, an object view comes from the worker that holds the object, so
installing it in the web app alone isn't enough. Commands come from the
cluster, so a package installed only in a worker's Python environment adds no
commands.
