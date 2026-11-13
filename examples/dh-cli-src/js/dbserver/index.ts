// Source of the example `dbserver` plugin. `cli-plugin-kit build` turns it
// into examples/dh-cli/js/dbserver/ (plugin.json + index.js).
import { definePlugin } from "@deephaven/cli-plugin/enterprise";

/**
 * A server as `getDbServers()` returns it. `serverClass` is on every one the
 * server sends (iris `IrisClient.getConsoleServerAddress`) but missing from
 * `@deephaven-enterprise/jsapi-types`' `ConsoleServerAddress`.
 */
interface DbServer {
  name: string;
  /** "Group" for a server group. */
  host: string;
  port: number;
  /** `DISPATCHER` or `GROUP`. */
  type: string;
  /** From `iris.db.<n>.class`, e.g. `Query` or `Merge`; any string. */
  serverClass: string;
}

export default definePlugin({
  name: "dbserver",
  description: "List the cluster's query and merge servers.",
  requires: "enterprise",
  commands: {
    list: {
      description:
        "List the dispatchers and server groups persistent queries run on.",
      options: {
        class: {
          type: "string",
          description: "Only servers of this class, e.g. Query or Merge.",
        },
      },
      output: {
        kind: "rows",
        fields: {
          name: { type: "string", description: "Server name." },
          host: { type: "string", description: "Host; Group for a group." },
          port: { type: "number", description: "Dispatcher port." },
          type: { type: "string", description: "DISPATCHER or GROUP." },
          class: { type: "string", description: "Server class." },
        },
      },
      async run(ctx) {
        const { client } = await ctx.enterprise();
        const servers = (await client.getDbServers()) as unknown as DbServer[];
        const wanted = (ctx.options.class as string | undefined)?.toLowerCase();
        return servers
          .filter((s) => !wanted || s.serverClass.toLowerCase() === wanted)
          .map((s) => ({
            name: s.name,
            host: s.host,
            port: s.port,
            type: s.type,
            class: s.serverClass,
          }));
      },
    },
  },
});
