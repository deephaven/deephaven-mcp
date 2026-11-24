import { ensureCa } from "../../src/actions/auth/session.ts";
import { readSecret } from "../../src/cli/secret.ts";
import { readStdin } from "../../src/cli/stdin.ts";
import { setFormat } from "../../src/output.ts";

const ca = process.env.TEST_RESTART_CA;
if (!ca) throw new Error("Missing test CA");

if (process.env.DH_LOADED_CA_CERT === ca) {
  const input = await Bun.stdin.text();
  console.log(JSON.stringify({ args: process.argv.slice(2), input }));
} else {
  if (process.env.TEST_RESTART_READ === "text") await readStdin();
  if (process.env.TEST_RESTART_READ === "secret")
    await readSecret("password", true, undefined, async () => "");
  setFormat("silent");
  await ensureCa(
    ca,
    [],
    process.env.TEST_RESTART_REPLACE ? ["replacement", "--", "-target"] : undefined,
  );
  throw new Error("Expected restart");
}
