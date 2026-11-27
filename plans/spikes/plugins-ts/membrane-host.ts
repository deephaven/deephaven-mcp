// Spike: expose a real Community JS API session inside QuickJS through a
// type-directed membrane. Usage: bun membrane-host.ts <plugin.js>
import variant from "@jitl/quickjs-wasmfile-release-sync";
import wasmPath from "@jitl/quickjs-wasmfile-release-sync/wasm" with {
  type: "file",
};
import { writeSync } from "node:fs";
import {
  newQuickJSWASMModuleFromVariant,
  newVariant,
  type QuickJSHandle,
} from "quickjs-emscripten-core";
import { loadCommunityApi } from "../../../src/actions/auth/jsapi.ts";

// ---- The allowlist: in a real build, generated from @deephaven/jsapi-types. ----
type TypeRef =
  | "data" // copied as plain JSON data
  | "cell" // a table cell: primitive, else its string form
  | { handle: string }
  | { fn: TypeRef } // a function value the plugin may call, returning this
  | { array: TypeRef }
  | { promise: TypeRef };
type Member =
  | { kind: "prop"; type: TypeRef }
  | {
      kind: "method";
      returns: TypeRef;
      /** Which argument is a callback, and the type of what it receives. */
      callback?: { arg: number; receives: TypeRef };
    };
const SCHEMA: Record<string, Record<string, Member>> = {
  IdeSession: {
    getTable: { kind: "method", returns: { promise: { handle: "Table" } } },
  },
  Table: {
    size: { kind: "prop", type: "data" },
    columns: { kind: "prop", type: { array: { handle: "Column" } } },
    setViewport: { kind: "method", returns: { handle: "Subscription" } },
    getViewportData: {
      kind: "method",
      returns: { promise: { handle: "ViewportData" } },
    },
    addEventListener: {
      kind: "method",
      returns: { fn: "data" }, // the listener's remover
      callback: { arg: 1, receives: { handle: "TableEvent" } },
    },
    close: { kind: "method", returns: "data" },
  },
  TableStatic: { EVENT_UPDATED: { kind: "prop", type: "data" } },
  Subscription: { close: { kind: "method", returns: "data" } },
  Column: {
    name: { kind: "prop", type: "data" },
    type: { kind: "prop", type: "data" },
  },
  ViewportData: {
    offset: { kind: "prop", type: "data" },
    rows: { kind: "prop", type: { array: { handle: "Row" } } },
  },
  Row: { get: { kind: "method", returns: "cell" } },
  TableEvent: {
    type: { kind: "prop", type: "data" },
    detail: { kind: "prop", type: { handle: "ViewportData" } },
  },
};

// ---- A real server session (dh does this with the profile's credentials). ----
writeSync(2, "stage: top\n");
const origin = "http://localhost:10000";
const dh = await loadCommunityApi(new URL(origin), `${import.meta.dir}/cache`, 30_000);
writeSync(2, "stage: loaded api\n");
const { NodeHttp2gRPCTransport } = await import("@deephaven/jsapi-nodejs");
const client = new dh.CoreClient(origin, { transportFactory: NodeHttp2gRPCTransport.factory });
writeSync(2, "stage: logging in\n");
await client.login({ type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS });
writeSync(2, "stage: ide\n");
const ide = await client.getAsIdeConnection();
writeSync(2, "stage: session\n");
const session = await ide.startSession("python");
writeSync(2, "stage: runCode\n");
await session.runCode(
  "from deephaven import time_table, empty_table\nt = empty_table(1000).update(['X = i', 'Y = X * 2', 'S = `row` + X'])\nticking = time_table('PT0.1S').update('V = i')",
);

writeSync(2, "stage: quickjs\n");
// ---- QuickJS ----
const QuickJS = await newQuickJSWASMModuleFromVariant(
  newVariant(variant, {
    wasmBinary: await Bun.file(wasmPath).arrayBuffer(),
  }),
);
const runtime = QuickJS.newRuntime();
runtime.setMemoryLimit(64 * 1024 * 1024);
const vm = runtime.newContext();

const handles = new Map<number, { obj: unknown; type: string; returns?: TypeRef }>();
let nextHandle = 1;
const refused: string[] = [];

type Desc =
  | { k: "v"; v: unknown }
  | { k: "a"; items: Desc[] }
  | { k: "h"; id: number; type: string }
  | { k: "m" };

function isPlainData(v: unknown, depth = 0): boolean {
  if (depth > 20) return false;
  if (v === null || ["string", "number", "boolean"].includes(typeof v)) {
    return true;
  }
  if (Array.isArray(v)) return v.every((x) => isPlainData(x, depth + 1));
  if (typeof v === "object") {
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null) return false;
    return Object.values(v as object).every((x) => isPlainData(x, depth + 1));
  }
  return v === undefined;
}

function describe(value: unknown, type: TypeRef): Desc {
  if (type === "data") {
    if (!isPlainData(value)) throw new TypeError("not plain data");
    return { k: "v", v: value };
  }
  if (type === "cell") {
    const prim =
      value === null || ["string", "number", "boolean"].includes(typeof value);
    return { k: "v", v: prim ? value : String(value) };
  }
  if ("array" in type) {
    if (!Array.isArray(value)) throw new TypeError("expected an array");
    return { k: "a", items: value.map((x) => describe(x, type.array)) };
  }
  if ("fn" in type) {
    if (typeof value !== "function") throw new TypeError("expected a function");
    const id = nextHandle++;
    handles.set(id, { obj: value, type: "__fn", returns: type.fn });
    return { k: "f", id } as unknown as Desc;
  }
  if ("handle" in type) {
    if (value == null) return { k: "v", v: null };
    const id = nextHandle++;
    handles.set(id, { obj: value, type: type.handle });
    return { k: "h", id, type: type.handle };
  }
  throw new TypeError("a promise can't be described synchronously");
}

function lookup(id: number, prop: string): { obj: any; member: Member; type: string } {
  const h = handles.get(id);
  if (!h) throw new TypeError(`no handle ${id}`);
  if (h.type === "__fn" && prop === "__invoke") {
    // Called as a plain function, never as a method of anything.
    const fn = h.obj as (...a: unknown[]) => unknown;
    return { obj: { __invoke: (...a: unknown[]) => fn(...a) }, member: { kind: "method", returns: h.returns as TypeRef }, type: "function" };
  }
  const member = Object.hasOwn(SCHEMA[h.type] ?? {}, prop)
    ? SCHEMA[h.type]?.[prop]
    : undefined;
  if (!member) {
    refused.push(`${h.type}.${prop}`);
    throw new TypeError(`${h.type}.${prop} is not available to plugins`);
  }
  return { obj: h.obj, member, type: h.type };
}

const toVm = (s: string) => vm.newString(s);
const throwIn = (e: unknown) => {
  throw e instanceof Error ? e : new Error(String(e));
};

// __get(id, prop) -> JSON descriptor
const getFn = vm.newFunction("__get", (idH, propH) => {
  try {
    const { obj, member } = lookup(vm.getNumber(idH), vm.getString(propH));
    if (member.kind === "method") return toVm(JSON.stringify({ k: "m" }));
    return toVm(JSON.stringify(describe(obj[vm.getString(propH)], member.type)));
  } catch (e) {
    return { error: vm.newError(String((e as Error).message)) };
  }
});
vm.setProp(vm.global, "__get", getFn);
getFn.dispose();

// __call(id, prop, argsJson) -> JSON descriptor, or a promise of one
const callFn = vm.newFunction("__call", (idH, propH, argsH) => {
  try {
    const prop = vm.getString(propH);
    const { obj, member, type } = lookup(vm.getNumber(idH), prop);
    if (member.kind !== "method") throw new TypeError(`${type}.${prop} isn't a method`);
    const raw = JSON.parse(vm.getString(argsH)) as { k: string; v?: unknown; id?: number }[];
    const args = raw.map((a, i) => {
      if (a.k === "v") {
        if (!isPlainData(a.v)) throw new TypeError("bad argument");
        return a.v;
      }
      if (a.k === "h") return handles.get(a.id as number)?.obj;
      if (a.k === "cb" && member.callback?.arg === i) {
        const cbId = a.id as number;
        const receives = member.callback.receives;
        return (event: unknown) => {
          const dispatch = vm.getProp(vm.global, "__dispatch");
          const idArg = vm.newNumber(cbId);
          const payload = vm.newString(JSON.stringify([describe(event, receives)]));
          const r = vm.callFunction(dispatch, vm.undefined, idArg, payload);
          for (const h of [dispatch, idArg, payload]) h.dispose();
          if (r.error) r.error.dispose();
          else r.value.dispose();
          runtime.executePendingJobs();
        };
      }
      throw new TypeError("bad argument");
    });
    const result = obj[prop](...args);
    const returns = member.returns;
    if (typeof returns === "object" && "promise" in returns) {
      const d = vm.newPromise();
      Promise.resolve(result).then(
        (v) => {
          const s = vm.newString(JSON.stringify(describe(v, returns.promise)));
          d.resolve(s);
          s.dispose();
        },
        (e) => {
          const err = vm.newError(String(e?.message ?? e));
          d.reject(err);
          err.dispose();
        },
      );
      d.settled.then(() => runtime.executePendingJobs());
      return d.handle;
    }
    return toVm(JSON.stringify(describe(result, returns)));
  } catch (e) {
    return { error: vm.newError(String((e as Error).message)) };
  }
});
vm.setProp(vm.global, "__call", callFn);
callFn.dispose();

const logFn = vm.newFunction("__log", (m) => {
  ((x: string) => writeSync(2, `${x}\n`))(`  [plugin] ${vm.getString(m)}`);
});
vm.setProp(vm.global, "__log", logFn);
logFn.dispose();

// The plugin-side shim: turns descriptors into proxies. Untrusted like the
// plugin; it only makes calls the host checks anyway.
const shim = `
const HANDLE = Symbol("handle");
const callbacks = new Map(); let nextCb = 1;
function decode(d) {
  if (d.k === "v") return d.v;
  if (d.k === "a") return d.items.map(decode);
  if (d.k === "h") return wrap(d.id, d.type);
  if (d.k === "f") return (...args) => { const out = __call(d.id, "__invoke", JSON.stringify(args.map(encodeArg))); return typeof out === "string" ? decode(JSON.parse(out)) : out.then((s) => decode(JSON.parse(s))); };
  throw new Error("bad descriptor");
}
function encodeArg(a) {
  if (typeof a === "function") { const id = nextCb++; callbacks.set(id, a); return { k: "cb", id }; }
  if (a && typeof a === "object" && a[HANDLE] !== undefined) return { k: "h", id: a[HANDLE] };
  return { k: "v", v: a };
}
function wrap(id, type) {
  return new Proxy({}, { get(_, prop) {
    if (prop === HANDLE) return id;
    if (typeof prop !== "string" || prop === "then") return undefined;
    if (prop === "toString") return () => "[" + type + "]";
    const r = JSON.parse(__get(id, prop));
    if (r.k !== "m") return decode(r);
    return (...args) => {
      const out = __call(id, prop, JSON.stringify(args.map(encodeArg)));
      return typeof out === "string" ? decode(JSON.parse(out)) : out.then((s) => decode(JSON.parse(s)));
    };
  }});
}
globalThis.__dispatch = (id, json) => { const f = callbacks.get(id); if (f) f(...JSON.parse(json).map(decode)); };
globalThis.__wrap = wrap;
`;
vm.unwrapResult(vm.evalCode(shim, "shim.js")).dispose();

// ctx: the session and the dh.Table statics, as handles.
const sessionId = nextHandle++;
handles.set(sessionId, { obj: session, type: "IdeSession" });
const staticsId = nextHandle++;
handles.set(staticsId, { obj: dh.Table, type: "TableStatic" });
const ctxH = vm.unwrapResult(
  vm.evalCode(
    `({ session: __wrap(${sessionId}, "IdeSession"), Table: __wrap(${staticsId}, "TableStatic"), log: __log })`,
  ),
);

const source = await Bun.file(process.argv[2] ?? "membrane-plugin.js").text();
writeSync(2, "stage: plugin start\n");
const t0 = performance.now();
const mod = vm.unwrapResult(vm.evalCode(source, "plugin.js", { type: "module" }));
const plugin = vm.getProp(mod, "default");
const run = vm.getProp(plugin, "run");
const call = vm.unwrapResult(vm.callFunction(run, plugin, ctxH));
const settled = vm.resolvePromise(call);
runtime.executePendingJobs();
const result = await settled;
const t1 = performance.now();
let out: unknown;
if (result.error) {
  out = { error: vm.dump(result.error) };
  result.error.dispose();
} else {
  out = { value: vm.dump(result.value) };
  result.value.dispose();
}
((x: string) => writeSync(1, `${x}\n`))(JSON.stringify(out, null, 2));
((x: string) => writeSync(2, `${x}\n`))(`refused by the allowlist: ${[...new Set(refused)].join(", ")}`);
((x: string) => writeSync(2, `${x}\n`))(`plugin run: ${(t1 - t0).toFixed(0)} ms, handles created: ${handles.size}`);
for (const h of [call, run, plugin, mod, ctxH]) h.dispose();
vm.dispose();
runtime.dispose();
await client.disconnect?.();
process.exit(0);
