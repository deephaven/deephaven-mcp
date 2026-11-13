// ../../../../packages/cli-plugin/src/define.ts
var API_VERSION = 1;
function defineWith(plugin) {
  return { ...plugin, api: API_VERSION };
}

// ../../../../packages/cli-plugin/src/enterprise.ts
function definePlugin(plugin) {
  return defineWith(plugin);
}

// index.ts
var index_default = definePlugin({
  name: "dbserver",
  description: "List the cluster's query and merge servers.",
  requires: "enterprise",
  commands: {
    list: {
      description: "List the dispatchers and server groups persistent queries run on.",
      options: {
        class: {
          type: "string",
          description: "Only servers of this class, e.g. Query or Merge."
        }
      },
      output: {
        kind: "rows",
        fields: {
          name: { type: "string", description: "Server name." },
          host: { type: "string", description: "Host; Group for a group." },
          port: { type: "number", description: "Dispatcher port." },
          type: { type: "string", description: "DISPATCHER or GROUP." },
          class: { type: "string", description: "Server class." }
        }
      },
      async run(ctx) {
        const { client } = await ctx.enterprise();
        const servers = await client.getDbServers();
        const wanted = ctx.options.class?.toLowerCase();
        return servers.filter((s) => !wanted || s.serverClass.toLowerCase() === wanted).map((s) => ({
          name: s.name,
          host: s.host,
          port: s.port,
          type: s.type,
          class: s.serverClass
        }));
      }
    }
  }
});
export {
  index_default as default
};
