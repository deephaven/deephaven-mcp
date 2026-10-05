import { encodeBase64 } from "@std/encoding/base64";

/** One-time SAML key: 96 CSPRNG bytes, base64. */
export function samlNonce(): string {
  return encodeBase64(crypto.getRandomValues(new Uint8Array(96)));
}

export function signInUrl(base: string, nonce: string): string {
  const url = new URL(base);
  url.searchParams.set("key", nonce);
  return url.href;
}

/** Best effort; the URL is always printed too. */
export async function openBrowser(url: string): Promise<boolean> {
  const [cmd, ...args] = Deno.build.os === "darwin"
    ? ["open", url]
    // Not `cmd /c start`: cmd would expand the %XX escapes in the URL.
    : Deno.build.os === "windows"
    ? ["rundll32", "url.dll,FileProtocolHandler", url]
    : ["xdg-open", url];
  try {
    const { success } = await new Deno.Command(cmd, {
      args,
      stdin: "null",
      stdout: "null",
      stderr: "null",
    }).output();
    return success;
  } catch {
    return false;
  }
}
