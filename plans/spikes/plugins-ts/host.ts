// What dh would do: run a downloaded plugin in QuickJS (WebAssembly), giving it
// only a promise-based ctx. Usage: host <plugin.js> [loop|memory]
// Compile: bun build --compile host.ts --outfile host
import variant from "@jitl/quickjs-wasmfile-release-sync";
import wasmPath from "@jitl/quickjs-wasmfile-release-sync/wasm" with {
  type: "file",
};
import {
  newQuickJSWASMModuleFromVariant,
  newVariant,
  type QuickJSContext,
  type QuickJSHandle,
} from "quickjs-emscripten-core";

const t0 = performance.now();
// Embedded in the compiled binary; read from Bun's virtual filesystem.
const wasmBinary = await Bun.file(wasmPath).arrayBuffer();
const QuickJS = await newQuickJSWASMModuleFromVariant(
  newVariant(variant, { wasmBinary }),
);
const tLoad = performance.now();

const pluginPath = process.argv[2] ?? "plugin.js";
const mode = process.argv[3];
const source =
  mode === "loop"
    ? "export default { async run() { for (;;) {} } }"
    : mode === "memory"
      ? "export default { async run() { const a = []; for (;;) a.push(new Array(1e5).fill(1)); } }"
      : await Bun.file(pluginPath).text();

const runtime = QuickJS.newRuntime();
runtime.setMemoryLimit(32 * 1024 * 1024);
runtime.setMaxStackSize(1024 * 1024);
const deadline = Date.now() + 2_000;
runtime.setInterruptHandler(() => Date.now() > deadline);
// No module loader: `import(...)` of anything fails.
const vm = runtime.newContext();

/** A host function the plugin may await: returns a QuickJS promise resolved with JSON data. */
function hostAsync(
  vm: QuickJSContext,
  name: string,
  fn: (...args: unknown[]) => Promise<unknown>,
): QuickJSHandle {
  return vm.newFunction(name, (...handles) => {
    const args = handles.map((h) => vm.dump(h));
    const deferred = vm.newPromise();
    fn(...args).then(
      (value) => {
        const result = vm.unwrapResult(
          vm.evalCode(`(${JSON.stringify(value)})`),
        );
        deferred.resolve(result);
        result.dispose();
      },
      (error) => {
        const err = vm.newError(String(error));
        deferred.reject(err);
        err.dispose();
      },
    );
    deferred.settled.then(() => runtime.executePendingJobs());
    return deferred.handle;
  });
}

// ctx: plain data plus the functions dh chooses to give.
const ctx = vm.newObject();
const args = vm.unwrapResult(vm.evalCode(`({ name: "dashboard" })`));
vm.setProp(ctx, "args", args);
args.dispose();
const fetchObject = hostAsync(vm, "fetchObject", async (name) => {
  await Bun.sleep(20); // dh's own server call would happen here
  return {
    type: "deephaven.ui.Dashboard",
    name,
    document: {
      type: "dashboard",
      children: [
        { type: "panel", children: [{ type: "table" }, { type: "text" }] },
        { type: "panel", children: [{ type: "table" }] },
      ],
    },
  };
});
vm.setProp(ctx, "fetchObject", fetchObject);
fetchObject.dispose();
const progress = vm.newFunction("progress", (m) => {
  console.error(`  [plugin] ${vm.getString(m)}`);
});
vm.setProp(ctx, "progress", progress);
progress.dispose();

const outcome = await (async () => {
  const mod = vm.evalCode(source, "plugin.js", { type: "module" });
  if (mod.error) return { error: vm.dump(mod.error) };
  const ns = mod.value;
  const plugin = vm.getProp(ns, "default");
  const run = vm.getProp(plugin, "run");
  const call = vm.callFunction(run, plugin, ctx);
  for (const h of [run, plugin, ns]) h.dispose();
  if (call.error) return { error: vm.dump(call.error) };
  const settled = vm.resolvePromise(call.value);
  call.value.dispose();
  runtime.executePendingJobs();
  const result = await settled;
  if (result.error) {
    const e = vm.dump(result.error);
    result.error.dispose();
    return { error: e };
  }
  const value = vm.dump(result.value);
  result.value.dispose();
  return { value };
})();
const tRun = performance.now();

console.log(JSON.stringify(outcome, null, 2));
console.error(
  `load ${(tLoad - t0).toFixed(1)} ms, run ${(tRun - tLoad).toFixed(1)} ms`,
);
ctx.dispose();
vm.dispose();
runtime.dispose();
