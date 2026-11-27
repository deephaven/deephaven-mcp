// A command plugin written against the ordinary JS API shape. It runs in
// QuickJS; every object it touches is a proxy the host checks.
export default {
  async run(ctx) {
    // Ordinary JS API usage: open a table, read columns, read a viewport.
    const t = await ctx.session.getTable("t");
    const columns = t.columns.map((c) => `${c.name}:${c.type}`);
    t.setViewport(0, 4);
    const vp = await t.getViewportData();
    const rows = vp.rows.map((r) => t.columns.map((c) => r.get(c)));

    // Events across the boundary: a ticking table's updates.
    const ticking = await ctx.session.getTable("ticking");
    ticking.setViewport(0, 2);
    const updates = await new Promise((resolve) => {
      let n = 0;
      const remove = ticking.addEventListener(ctx.Table.EVENT_UPDATED, (e) => {
        n += 1;
        if (n === 3) {
          remove();
          resolve(e.detail.rows.length);
        }
      });
    });

    // Attempts to step outside the allowlist.
    const attempts = {};
    const attempt = (name, fn) => {
      try {
        const v = fn();
        attempts[name] = `REACHABLE: ${String(v).slice(0, 50)}`;
      } catch (e) {
        attempts[name] = `blocked (${String(e.message).slice(0, 70)})`;
      }
    };
    attempt("table.constructor", () => t.constructor);
    attempt("table.__proto__", () => t.__proto__);
    attempt("an internal field", () => t.lastVisibleState);
    attempt("session.runCode", () => ctx.session.runCode);
    attempt("session.close", () => ctx.session.close);
    attempt("raw host call", () => __call(1, "constructor", "[]"));
    attempt("Function escape", () => Function("return this")().process);

    const size = t.size;
    t.close();
    ticking.close();
    ctx.log(`read ${rows.length} rows, saw 3 ticks`);
    return { size, columns, rows, ticksLastRowCount: updates, attempts };
  },
};
