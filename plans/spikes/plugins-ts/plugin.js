// A CLI plugin as dh would download it: an ES module whose default export
// handles one command. It gets only `ctx`.
export default {
  async run(ctx) {
    // Everything a hostile plugin would reach for. Each must fail.
    const probes = {};
    const probe = async (name, fn) => {
      try {
        const v = await fn();
        probes[name] = `REACHABLE: ${String(v).slice(0, 60)}`;
      } catch (e) {
        probes[name] = `blocked (${String(e && e.message ? e.message : e).slice(0, 60)})`;
      }
    };
    await probe("process.env", () => process.env.HOME);
    await probe("require('fs')", () => require("fs"));
    await probe("import('node:fs')", () => import("node:fs"));
    await probe("fetch", () => fetch("https://example.com"));
    await probe("Bun", () => Bun.version);
    await probe("globalThis escape", () =>
      Function("return this")().process.env.HOME,
    );
    await probe("XMLHttpRequest", () => new XMLHttpRequest());
    await probe("WebSocket", () => new WebSocket("wss://example.com"));

    // Real work, through the only API it was given.
    const element = await ctx.fetchObject(ctx.args.name);
    const counts = {};
    const walk = (node) => {
      if (!node || typeof node !== "object") return;
      if (node.type) counts[node.type] = (counts[node.type] ?? 0) + 1;
      for (const child of node.children ?? []) walk(child);
    };
    walk(element.document);
    ctx.progress(`walked ${element.type}`);
    return { name: ctx.args.name, type: element.type, counts, probes };
  },
};
