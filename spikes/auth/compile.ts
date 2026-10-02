// Compiles every check with the same permissions as scripts/build.ts.
// Usage: deno task compile   -> bin/<check>
import { fromFileUrl, join } from "@std/path";

const DIR = fromFileUrl(new URL(".", import.meta.url));
const CHECKS = ["crypto.ts", "dhc.ts", "dhe.ts", "ink.tsx"];

for (const check of CHECKS) {
  const output = join(DIR, "bin", check.replace(/\.tsx?$/, ""));
  const start = performance.now();
  const { success } = await new Deno.Command(Deno.execPath(), {
    args: [
      "compile",
      "--quiet",
      "--allow-env",
      "--allow-net",
      "--allow-read",
      "--allow-write",
      "--allow-run",
      "--allow-sys",
      "--output",
      output,
      join(DIR, check),
    ],
    cwd: DIR,
  }).spawn().status;
  const { size } = success ? await Deno.stat(output) : { size: 0 };
  const ms = Math.round(performance.now() - start);
  console.log(
    `${success ? "PASS" : "FAIL"} compile ${check} (${ms} ms, ${
      (size / 1e6).toFixed(0)
    } MB)`,
  );
}
